// ============================================================
// Тревога · Белгород — backend
// - парсит публичную веб-версию телеграм-канала @mchs31 каждые 10 сек
// - отдаёт готовую ленту фронтенду через /api/feed
// - рассылает Web Push уведомления подписчикам (Android + iOS 16.4+)
//
// ВАЖНО: этот сервер должен работать НЕПРЕРЫВНО (Node-процесс).
// Обычный статический хостинг (только HTML/CSS/JS) для него не подходит —
// нужен хостинг с поддержкой Node.js (Render, Railway, Fly.io, VPS и т.п.).
// См. README.md для инструкции по развёртыванию.
// ============================================================

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const cheerio = require('cheerio');
const webpush = require('web-push');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const POLL_MS = 10000; // частота обновления парсера — 10 секунд
const DATA_DIR = path.join(__dirname, 'data');
const SUBS_FILE = path.join(DATA_DIR, 'subscriptions.json');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const ANALYTICS_FILE = path.join(DATA_DIR, 'analytics.json');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ===== Пароль администратора =====
// Задайте ADMIN_PASSWORD в переменных окружения хостинга для постоянного пароля.
// Если не задан — генерируется временный и выводится в логи сервера при старте
// (посмотрите вкладку Logs на Render сразу после деплоя).
let ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
if (!ADMIN_PASSWORD) {
  ADMIN_PASSWORD = crypto.randomBytes(6).toString('hex');
  console.log('\n[!] ADMIN_PASSWORD не задан в переменных окружения — сгенерирован временный пароль администратора:');
  console.log('    ADMIN_PASSWORD=' + ADMIN_PASSWORD);
  console.log('    Зафиксируйте свой постоянный пароль в переменных окружения для продакшена.\n');
}
const SESSION_TTL_MS = 12 * 60 * 60 * 1000; // 12 часов
// Сессии администратора раньше жили только в памяти процесса — на хостингах,
// которые перезапускают/«усыпляют» Node-процесс (Render free tier и т.п.),
// это молча разлогинивало админа, хотя токен в браузере оставался. Теперь
// сессии сохраняются на диск и переживают перезапуск сервера.
const SESSIONS_FILE = path.join(DATA_DIR, 'admin-sessions.json');
const adminSessions = new Map(Object.entries(loadJson(SESSIONS_FILE, {})));
// подчищаем протухшие сессии сразу при старте
for (const [token, expiresAt] of adminSessions) {
  if (expiresAt < Date.now()) adminSessions.delete(token);
}
function saveSessions() {
  saveJson(SESSIONS_FILE, Object.fromEntries(adminSessions));
}

// ===== VAPID (ключи для Web Push) =====
// Сгенерируйте свои командой: npx web-push generate-vapid-keys
// и задайте через переменные окружения VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY.
// Ниже — временная автогенерация, чтобы сервер сразу запускался,
// но ключи будут меняться при каждом перезапуске, если их не зафиксировать в .env —
// для продакшена обязательно задайте свои постоянные ключи.
let VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
let VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) {
  const generated = webpush.generateVAPIDKeys();
  VAPID_PUBLIC_KEY = generated.publicKey;
  VAPID_PRIVATE_KEY = generated.privateKey;
  console.log('\n[!] VAPID-ключи не заданы в переменных окружения — сгенерированы временные.');
  console.log('    Зафиксируйте их для продакшена (иначе подписки будут слетать при каждом перезапуске):');
  console.log('    VAPID_PUBLIC_KEY=' + VAPID_PUBLIC_KEY);
  console.log('    VAPID_PRIVATE_KEY=' + VAPID_PRIVATE_KEY + '\n');
}
webpush.setVapidDetails('mailto:admin@example.com', VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// ===== Хранилище подписок и состояния (простые JSON-файлы) =====
function loadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf-8')); } catch (e) { return fallback; }
}
function saveJson(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf-8');
}
let subscriptions = loadJson(SUBS_FILE, []); // [{ subscription, regions, sound, vibro }]
let state = loadJson(STATE_FILE, { seenIds: [], feed: [] });
let channels = loadJson(CHANNELS_FILE, ['mchs31', 'LiveOnlain']);
if (!fs.existsSync(CHANNELS_FILE)) saveJson(CHANNELS_FILE, channels);

// Некоторые каналы освещают события только конкретного города (например, канал
// «Предупреждение» пишет исключительно про город Белгород) — для них региональные
// ключевые слова из текста поста не нужны и могут ошибочно перекидывать сообщение
// в другой район. Админ может закрепить фиксированный регион за таким каналом.
const CHANNEL_REGIONS_FILE = path.join(DATA_DIR, 'channel-regions.json');
let channelRegionOverride = loadJson(CHANNEL_REGIONS_FILE, {}); // { channelName: 'belgorod' | ... }
if (!fs.existsSync(CHANNEL_REGIONS_FILE)) saveJson(CHANNEL_REGIONS_FILE, channelRegionOverride);

const ALARM_CONFIG_FILE = path.join(DATA_DIR, 'alarm-config.json');
// Какие типы сообщений и для каких районов включают ГРОМКОЕ push-уведомление
// (со звуком/вибрацией и требованием реакции). Остальное приходит тихо.
let alarmConfig = loadJson(ALARM_CONFIG_FILE, { types: ['rocket', 'drone'], regions: ['all'], enabled: true });
if (!fs.existsSync(ALARM_CONFIG_FILE)) saveJson(ALARM_CONFIG_FILE, alarmConfig);

function isAlarmTriggering(item) {
  if (!alarmConfig.enabled) return false;
  if (!alarmConfig.types.includes(item.t)) return false;
  if (alarmConfig.regions.includes('all')) return true;
  return alarmConfig.regions.includes(item.region);
}
let channelHealth = {}; // { channelName: { ok, lastPollAt, lastError, count } }

// Принимает "https://t.me/LiveOnlain", "t.me/s/LiveOnlain", "@LiveOnlain" или просто "LiveOnlain"
function parseChannelInput(input) {
  if (!input) return null;
  let s = String(input).trim();
  s = s.replace(/^(https?:\/\/)?(t|telegram)\.me\/(s\/)?/i, '');
  s = s.replace(/^@/, '');
  s = s.split(/[/?#]/)[0];
  s = s.trim();
  if (!/^[a-zA-Z0-9_]{3,64}$/.test(s)) return null;
  return s;
}
let analytics = loadJson(ANALYTICS_FILE, {
  totalVisits: 0,
  uniqueVisitors: [],       // массив фингерпринтов (хэш IP+UA), без сырых IP
  dailyCounts: {},          // { 'YYYY-MM-DD': n }
  hourlyToday: { day: null, hours: new Array(24).fill(0) },
  referrers: {},            // { 'direct' | домен: n }
  devices: { mobile: 0, desktop: 0, tablet: 0 },
  browsers: {},             // { Chrome: n, Safari: n, ... }
  recent: []                // последние посещения [{t, path, ref, device, browser}]
});
// ВАЖНАЯ ПРАВКА: loadJson() при существующем файле возвращает его as-is, БЕЗ
// подстановки дефолтных полей. Если analytics.json был создан более ранней
// версией сервера и в нём не хватает, скажем, поля 'browsers' — recordVisit()
// падал на первом же обращении к analytics.browsers[...] с TypeError, который
// проглатывался try/catch. Из-за этого saveAnalyticsSoon() (последняя строка в
// функции) не вызывалась вообще — новые визиты переставали сохраняться, и
// вкладка «Статистика» в админке навсегда замирала на "—". Теперь недостающие
// поля подставляются сразу после загрузки, самостоятельно "подлечивая" старый файл.
analytics.totalVisits = analytics.totalVisits || 0;
analytics.uniqueVisitors = Array.isArray(analytics.uniqueVisitors) ? analytics.uniqueVisitors : [];
analytics.dailyCounts = analytics.dailyCounts || {};
analytics.hourlyToday = analytics.hourlyToday || { day: null, hours: new Array(24).fill(0) };
if (!Array.isArray(analytics.hourlyToday.hours) || analytics.hourlyToday.hours.length !== 24) {
  analytics.hourlyToday = { day: null, hours: new Array(24).fill(0) };
}
analytics.referrers = analytics.referrers || {};
analytics.devices = analytics.devices || { mobile: 0, desktop: 0, tablet: 0 };
analytics.browsers = analytics.browsers || {};
analytics.recent = Array.isArray(analytics.recent) ? analytics.recent : [];
let analyticsDirty = false;
function saveAnalyticsSoon() {
  analyticsDirty = true;
}
setInterval(() => {
  if (analyticsDirty) { saveJson(ANALYTICS_FILE, analytics); analyticsDirty = false; }
}, 5000);

// ===== Логи сервера (для вкладки «Логи» в админке) =====
// Раньше при сбое (ошибка опроса канала, ошибка отправки push, необработанное
// исключение в каком-либо API-роуте) единственным следом была строка в
// консоли процесса — если админ не смотрел консоль хостинга в этот момент,
// причина проблемы (например, почему статистика вдруг осталась пустой)
// терялась безвозвратно. Теперь всё это ещё и пишется в лог-буфер, который
// виден прямо в админке.
const LOGS_FILE = path.join(DATA_DIR, 'logs.json');
const MAX_LOGS = 500;
let logs = loadJson(LOGS_FILE, []);
let logsDirty = false;
function addLog(level, message, meta) {
  const entry = { t: Date.now(), level: level, message: String(message), meta: meta || null };
  logs.push(entry);
  if (logs.length > MAX_LOGS) logs = logs.slice(-MAX_LOGS);
  logsDirty = true;
  const prefix = level === 'error' ? '[ERROR]' : level === 'warn' ? '[WARN]' : '[INFO]';
  console.log(prefix, message, meta ? JSON.stringify(meta) : '');
}
setInterval(() => {
  if (logsDirty) { saveJson(LOGS_FILE, logs); logsDirty = false; }
}, 5000);
addLog('info', 'Сервер запускается');

// Ловим то, что иначе молча уронило бы процесс без единой строки в логах.
process.on('uncaughtException', (err) => {
  addLog('error', 'Необработанное исключение: ' + err.message, { stack: err.stack });
});
process.on('unhandledRejection', (reason) => {
  addLog('error', 'Необработанный отказ промиса: ' + (reason && reason.message ? reason.message : String(reason)));
});

// ===== Классификация сообщений =====
// ВАЖНО: 'belgorod' проверяется ПЕРВЫМ и намеренно самый широкий (город + область
// в целом) — большинство сообщений общего канала-предупреждения относятся именно
// к городу Белгород / области целиком, а не к конкретному району.
// Остальные ключевые слова раньше были слишком общими (например, 'красн' совпадал
// с любым словом, содержащим эти буквы, а не только с «Красной Яругой») — из-за
// этого случайные сообщения без упоминания района ошибочно попадали в конкретный
// район. Теперь используются точные фразы вместо расплывчатых подстрок.
const REGION_KEYWORDS = [
  ['belgorod', ['белгород', 'белгороду', 'белгородской', 'белгородский', 'белгородском', 'белгородская', 'по области', 'области']],
  ['valuiki', ['валуйк']],
  ['shebekino', ['шебекин']],
  ['graivoron', ['грайворон']],
  ['stary-oskol', ['старый оскол', 'старом осколе', 'старооскольск']],
  ['gubkin', ['губкин']],
  ['korocha', ['короч']],
  ['krasnaya-yaruga', ['красная яруга', 'красной яруге', 'красноярружск']],
];

const REGION_NAMES = {
  belgorod: 'Белгород', valuiki: 'Валуйки', shebekino: 'Шебекино',
  graivoron: 'Грайворон', 'stary-oskol': 'Старый Оскол', gubkin: 'Губкин',
  korocha: 'Короча', 'krasnaya-yaruga': 'Красная Яруга'
};

// Признаки рекламных / нерелевантных постов — такие сообщения отбрасываем
const AD_PATTERNS = [
  /реклам/i, /promo/i, /подпис\w+ на канал/i, /erid/i, /18\+.*реклама/i,
  /по вопросам сотрудничества/i, /vk\.cc\/|clck\.ru\//i
];

function detectRegion(text) {
  const lower = text.toLowerCase();
  for (const [key, words] of REGION_KEYWORDS) {
    if (words.some((w) => lower.includes(w))) return key;
  }
  return 'belgorod'; // канал в целом про область — по умолчанию центр
}

// Общие формулировки вида «Большая активность БПЛА. Будьте бдительны. Берегите себя.» —
// это ситуативное предупреждение о фоновой обстановке, а не сигнал «БПЛА над вами прямо
// сейчас». Поднимать по нему тревогу (звук/вибрация/красный статус) не нужно — но и
// выкидывать из ленты не стоит, показываем как обычное информационное сообщение.
const GENERAL_CAUTION_RE = /будьте бдительны|берегите себя|сохраняйте спокойствие/i;
const DIRECT_THREAT_RE = /обнаружен|зафиксирован|курс[ыа]?\s+на|направля(ется|ются)|над\s|заход[ит]*|атак|сбит|поражен|подлета|приближа/i;

function classify(text) {
  const lower = text.toLowerCase();
  if (/отбой/.test(lower)) return { t: 'cancel', i: '✅', tag: 'Отбой / отмена' };

  // Ракетная опасность (в т.ч. пуски/удары с самолётов противника — приравниваем к РО,
  // это тот же уровень угрозы и та же реакция «в укрытие»).
  if (/ракетн(ая|ой) опасност/.test(lower)) {
    return { t: 'rocket', i: '🚀', tag: 'Ракетная опасность' };
  }
  if (/(пуск\w*|удар\w*)[^.!?\n]{0,25}(самол[её]та?|авиац\w*)\s+противника/.test(lower) ||
      /авиац(ия|ионн\w*)[^.!?\n]{0,25}(противника|опасност\w*|удар\w*)/.test(lower)) {
    return { t: 'rocket', i: '✈️', tag: 'Авиационная опасность' };
  }

  if (/бпла|беспилотник|дрон/.test(lower)) {
    const isGeneralNotice = GENERAL_CAUTION_RE.test(lower) && !DIRECT_THREAT_RE.test(lower);
    if (isGeneralNotice) {
      return { t: 'notice', i: 'ℹ️', tag: 'Активность БПЛА в регионе' };
    }
    return { t: 'drone', i: '🛸', tag: 'БПЛА обнаружен' };
  }

  if (/укрыти/.test(lower)) return { t: 'shelter', i: '🏃', tag: 'В укрытие' };
  if (/повторн/.test(lower)) return { t: 'repeat', i: '💬', tag: 'Повторно' };
  return { t: 'other', i: '📰', tag: 'Сообщение канала' };
}

function isAd(text) {
  return AD_PATTERNS.some((re) => re.test(text));
}

// ===== Аналитика посещений (свой мини-«Метрика») =====
function todayKey() {
  return new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' }); // YYYY-MM-DD
}

function detectDevice(ua) {
  if (/iPad|Tablet/i.test(ua)) return 'tablet';
  if (/Mobi|Android|iPhone/i.test(ua)) return 'mobile';
  return 'desktop';
}

function detectBrowser(ua) {
  if (/Edg\//.test(ua)) return 'Edge';
  if (/OPR\/|Opera/.test(ua)) return 'Opera';
  if (/Chrome\//.test(ua) && !/Edg\//.test(ua)) return 'Chrome';
  if (/Safari\//.test(ua) && !/Chrome\//.test(ua)) return 'Safari';
  if (/Firefox\//.test(ua)) return 'Firefox';
  return 'Другое';
}

function refDomain(ref) {
  if (!ref) return 'Прямой заход';
  try {
    const u = new URL(ref);
    return u.hostname.replace(/^www\./, '');
  } catch (e) {
    return 'Прямой заход';
  }
}

function recordVisit(req) {
  try {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    const ua = req.headers['user-agent'] || '';
    const ref = refDomain(req.headers['referer'] || req.headers['referrer']);
    const fingerprint = crypto.createHash('sha256').update(ip + '|' + ua).digest('hex').slice(0, 20);
    const device = detectDevice(ua);
    const browser = detectBrowser(ua);
    const day = todayKey();

    analytics.totalVisits += 1;
    if (!analytics.uniqueVisitors.includes(fingerprint)) {
      analytics.uniqueVisitors.push(fingerprint);
      if (analytics.uniqueVisitors.length > 20000) analytics.uniqueVisitors = analytics.uniqueVisitors.slice(-20000);
    }
    analytics.dailyCounts[day] = (analytics.dailyCounts[day] || 0) + 1;
    // храним только последние 90 дней, чтобы файл не рос бесконечно
    const days = Object.keys(analytics.dailyCounts).sort();
    if (days.length > 90) delete analytics.dailyCounts[days[0]];

    if (analytics.hourlyToday.day !== day) {
      analytics.hourlyToday = { day, hours: new Array(24).fill(0) };
    }
    const hour = new Date().toLocaleString('en-US', { hour: '2-digit', hour12: false, timeZone: 'Europe/Moscow' });
    const hourNum = parseInt(hour, 10) % 24;
    analytics.hourlyToday.hours[hourNum] += 1;

    analytics.referrers[ref] = (analytics.referrers[ref] || 0) + 1;
    analytics.devices[device] = (analytics.devices[device] || 0) + 1;
    analytics.browsers[browser] = (analytics.browsers[browser] || 0) + 1;

    analytics.recent.unshift({ t: new Date().toISOString(), path: req.path, ref, device, browser });
    if (analytics.recent.length > 100) analytics.recent = analytics.recent.slice(0, 100);

    saveAnalyticsSoon();
  } catch (err) {
    addLog('error', 'Ошибка учёта посещения: ' + err.message, { stack: err.stack });
  }
}

// ===== Парсер публичной веб-версии Telegram (t.me/s/<channel>) =====
async function fetchChannelMessages(channel) {
  const url = `https://t.me/s/${channel}`;
  const res = await axios.get(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; TrevogaBelgorodBot/1.0)' },
    timeout: 10000
  });
  const $ = cheerio.load(res.data);
  const messages = [];

  $('.tgme_widget_message').each((_, el) => {
    const $el = $(el);
    const idAttr = $el.attr('data-post') || '';
    const textEl = $el.find('.tgme_widget_message_text').first();
    if (!textEl.length) return; // сообщение без текста (только медиа) — пропускаем

    // Заменяем <br> на переносы перед извлечением текста
    textEl.find('br').replaceWith('\n');
    const text = textEl.text().trim();
    if (!text) return;

    const timeEl = $el.find('.tgme_widget_message_date time').first();
    const datetime = timeEl.attr('datetime') || null;
    // id включает имя канала, чтобы сообщения разных каналов никогда не пересекались
    const id = (idAttr || (channel + '/' + (datetime || text.slice(0, 40))));

    messages.push({ id, text, datetime, channel });
  });

  return messages;
}

function formatFeedItem(msg) {
  const cls = classify(msg.text);
  const region = channelRegionOverride[msg.channel] || detectRegion(msg.text);
  const hasRealTime = !!msg.datetime;
  const dt = hasRealTime ? new Date(msg.datetime) : new Date();
  const time = dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' });
  const date = dt.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', timeZone: 'Europe/Moscow' });
  return {
    id: msg.id,
    t: cls.t, i: cls.i, tag: cls.tag,
    txt: msg.text.length > 400 ? msg.text.slice(0, 400) + '…' : msg.text,
    time, date, region,
    isNew: false,
    ts: dt.getTime(),
    iso: dt.toISOString(),
    hasRealTime,
    source: msg.channel,
    sources: [msg.channel]
  };
}

// ===== Дедупликация одинаковых сообщений из разных каналов =====
function normalizeText(text) {
  return text.toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function wordSet(text) {
  return new Set(normalizeText(text).split(' ').filter(Boolean));
}
function jaccardSimilarity(a, b) {
  let inter = 0;
  a.forEach((w) => { if (b.has(w)) inter++; });
  const union = a.size + b.size - inter;
  return union === 0 ? 0 : inter / union;
}
const DEDUPE_WINDOW_MS = 15 * 60 * 1000; // сообщения из разных каналов об одном и том же обычно приходят почти одновременно
const DEDUPE_SIMILARITY = 0.55;

function dedupeItems(items) {
  const sorted = items.slice().sort((a, b) => a.ts - b.ts); // старые первыми — канонический экземпляр стабилен между опросами
  const result = [];
  for (const it of sorted) {
    const itWords = wordSet(it.txt);
    let dup = null;
    for (const r of result) {
      if (r.t !== it.t || r.region !== it.region) continue;
      if (Math.abs(r.ts - it.ts) > DEDUPE_WINDOW_MS) continue;
      if (jaccardSimilarity(itWords, wordSet(r.txt)) >= DEDUPE_SIMILARITY) { dup = r; break; }
    }
    if (dup) {
      if (it.source && dup.sources.indexOf(it.source) === -1) dup.sources.push(it.source);
    } else {
      result.push(it);
    }
  }
  return result.sort((a, b) => b.ts - a.ts); // новые сверху для отображения
}

// Типы сообщений, которые реально относятся к тревогам/БПЛА/укрытиям —
// всё остальное (общие посты канала не по теме) в ленту не попадает.
const ALERT_TYPES = ['rocket', 'drone', 'cancel', 'shelter', 'repeat', 'notice'];

// ===== Основной цикл опроса (сразу по всем каналам-источникам) =====
async function pollOnce() {
  try {
    let raw = [];
    for (const channel of channels) {
      try {
        const msgs = await fetchChannelMessages(channel);
        raw = raw.concat(msgs);
        channelHealth[channel] = { ok: true, lastPollAt: Date.now(), lastError: null, count: msgs.length };
      } catch (err) {
        channelHealth[channel] = { ok: false, lastPollAt: Date.now(), lastError: err.message, count: 0 };
        addLog('error', `Ошибка опроса канала @${channel}: ${err.message}`);
      }
    }

    const fresh = raw.filter((m) => !isAd(m.text));
    const allItems = fresh.map(formatFeedItem).sort((a, b) => b.ts - a.ts);
    const alertItems = allItems.filter((it) => ALERT_TYPES.includes(it.t));
    const items = dedupeItems(alertItems);

    const seen = new Set(state.seenIds);
    const newItems = items.filter((it) => !seen.has(String(it.id)));

    if (newItems.length) {
      newItems.forEach((it) => { it.isNew = true; });
      addLog('info', `Новых сообщений в ленте: ${newItems.length}`, { types: newItems.map((it) => it.t) });
      // помечаем как новые только первые несколько минут — на фронте это условно,
      // здесь просто фиксируем факт появления для push-рассылки
      for (const it of newItems) {
        await notifySubscribers(it);
      }
    }

    // Ручные сообщения (отправленные из админки) не приходят из парсера каналов —
    // сохраняем их поверх свежей выборки, иначе следующий же цикл опроса их сотрёт.
    const manualItems = (state.feed || []).filter((it) => it.manual);
    state.feed = manualItems.concat(items)
      .sort((a, b) => b.ts - a.ts)
      .slice(0, 60); // храним последние 60 сообщений
    // seenIds строим по ВСЕМ сообщениям канала (включая нерелевантные),
    // чтобы off-topic посты не пересчитывались и не «просачивались» после правок фильтра
    state.seenIds = allItems.slice(0, 200).map((it) => String(it.id));
    saveJson(STATE_FILE, state);
    lastPollOk = true;
    lastPollAt = Date.now();
  } catch (err) {
    lastPollOk = false;
    addLog('error', 'Ошибка опроса канала: ' + err.message, { stack: err.stack });
  }
}

let lastPollOk = false;
let lastPollAt = 0;

// ===== Push-рассылка при новом сообщении =====
async function notifySubscribers(item) {
  const isUrgent = isAlarmTriggering(item);
  const regionLabel = item.region === 'all' ? 'по всей области' : (REGION_NAMES[item.region] || item.region);
  const title = isUrgent ? `🚨 ТРЕВОГА · ${regionLabel}` : `${item.i} ${item.tag}`;
  const body = item.txt;

  const stillValid = [];
  for (const entry of subscriptions) {
    const matches = item.region === 'all' || (entry.regions && entry.regions.includes(item.region));
    if (!matches) { stillValid.push(entry); continue; }
    // Каждый подписчик сам решает, нужен ли ему звук/вибрация при тревоге —
    // раньше сервер слал всем один и тот же payload и игнорировал entry.sound/entry.vibro.
    const payload = JSON.stringify({
      title, body, tag: 'trevoga-' + item.id,
      urgent: isUrgent, url: './',
      sound: entry.sound !== false,
      vibro: entry.vibro !== false
    });
    try {
      await webpush.sendNotification(entry.subscription, payload);
      stillValid.push(entry);
    } catch (err) {
      if (err.statusCode === 404 || err.statusCode === 410) {
        // подписка больше не действительна (пользователь отписался/удалил приложение) — удаляем
      } else {
        stillValid.push(entry);
        addLog('error', 'Ошибка отправки push: ' + err.statusCode + ' ' + err.message);
      }
    }
  }
  subscriptions = stillValid;
  saveJson(SUBS_FILE, subscriptions);
}

// ===== Express API =====
const app = express();
app.use(cors());
app.use(express.json());

// Учёт посещений — считаем только реальные открытия страницы, не запросы к API/статике
app.use((req, res, next) => {
  if (req.method === 'GET' && (req.path === '/' || req.path === '/index.html')) {
    recordVisit(req);
  }
  next();
});

// Запрещаем поисковикам индексировать админку
app.get('/robots.txt', (req, res) => {
  res.type('text/plain').send('User-agent: *\nDisallow: /admin.html\nDisallow: /api/admin\n');
});

app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/feed', (req, res) => {
  res.json({ items: state.feed, ok: lastPollOk, updatedAt: lastPollAt });
});

app.get('/api/vapid-public-key', (req, res) => {
  res.json({ publicKey: VAPID_PUBLIC_KEY });
});

app.post('/api/subscribe', (req, res) => {
  const { subscription, regions, sound, vibro } = req.body;
  if (!subscription || !subscription.endpoint) return res.status(400).json({ error: 'invalid subscription' });

  const existingIdx = subscriptions.findIndex((s) => s.subscription.endpoint === subscription.endpoint);
  const entry = { subscription, regions: regions || ['belgorod'], sound: !!sound, vibro: !!vibro };
  if (existingIdx >= 0) subscriptions[existingIdx] = entry;
  else subscriptions.push(entry);
  saveJson(SUBS_FILE, subscriptions);
  res.json({ ok: true });
});

app.post('/api/unsubscribe', (req, res) => {
  const { endpoint } = req.body;
  subscriptions = subscriptions.filter((s) => s.subscription.endpoint !== endpoint);
  saveJson(SUBS_FILE, subscriptions);
  res.json({ ok: true });
});

// ============================================================
// АДМИНКА
// ============================================================
function requireAdmin(req, res, next) {
  const auth = req.headers['authorization'] || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
  const expiresAt = token && adminSessions.get(token);
  if (!expiresAt || expiresAt < Date.now()) {
    if (token && expiresAt) { adminSessions.delete(token); saveSessions(); }
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

app.post('/api/admin/login', (req, res) => {
  const { password } = req.body || {};
  if (password !== ADMIN_PASSWORD) {
    addLog('warn', 'Неудачная попытка входа в админку');
    return res.status(401).json({ error: 'wrong password' });
  }
  const token = crypto.randomBytes(24).toString('hex');
  adminSessions.set(token, Date.now() + SESSION_TTL_MS);
  saveSessions();
  addLog('info', 'Успешный вход в админку');
  res.json({ token, expiresIn: SESSION_TTL_MS });
});

app.get('/api/admin/stats', requireAdmin, (req, res) => {
  // Раньше этот роут ничем не был защищён от исключений — если, например,
  // state.feed или analytics.* оказывались повреждены (не тот тип), сервер
  // отвечал HTML-страницей ошибки Express вместо JSON. Админка ожидает
  // JSON и на такой ответ падает в res.json() молча (см. apiGet) — итог:
  // все цифры/графики просто остаются пустыми без единого сообщения об
  // ошибке. Теперь любая проблема здесь: 1) не роняет ответ как HTML,
  // 2) обязательно попадает в лог, который виден во вкладке «Логи».
  try {
    const feedByType = {};
    (Array.isArray(state.feed) ? state.feed : []).forEach((it) => { feedByType[it.t] = (feedByType[it.t] || 0) + 1; });

    const subsByRegion = {};
    (Array.isArray(subscriptions) ? subscriptions : []).forEach((s) => {
      (s.regions || []).forEach((r) => { subsByRegion[r] = (subsByRegion[r] || 0) + 1; });
    });

    res.json({
      visits: {
        total: analytics.totalVisits || 0,
        uniqueVisitors: (analytics.uniqueVisitors || []).length,
        today: (analytics.dailyCounts && analytics.dailyCounts[todayKey()]) || 0,
        dailyCounts: analytics.dailyCounts || {},
        hourlyToday: analytics.hourlyToday || { day: null, hours: new Array(24).fill(0) },
        referrers: analytics.referrers || {},
        devices: analytics.devices || {},
        browsers: analytics.browsers || {},
        recent: (analytics.recent || []).slice(0, 30)
      },
      subscribers: {
        total: (subscriptions || []).length,
        byRegion: subsByRegion
      },
      feed: {
        total: (state.feed || []).length,
        byType: feedByType
      },
      parser: {
        ok: lastPollOk,
        lastPollAt,
        channels,
        channelHealth
      },
      alarmConfig
    });
  } catch (err) {
    addLog('error', 'Ошибка сборки статистики: ' + err.message, { stack: err.stack });
    res.status(500).json({ error: 'stats build failed: ' + err.message });
  }
});

// ===== Логи сервера =====
app.get('/api/admin/logs', requireAdmin, (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 200, MAX_LOGS);
  const level = req.query.level;
  let out = logs;
  if (level && ['info', 'warn', 'error'].includes(level)) {
    out = out.filter((l) => l.level === level);
  }
  res.json({ logs: out.slice(-limit).reverse() });
});

// ===== Управление источниками (каналами) =====
app.get('/api/admin/channels', requireAdmin, (req, res) => {
  res.json({ channels, channelHealth, channelRegions: channelRegionOverride });
});

// Закрепить (или снять) фиксированный регион за каналом — например, канал,
// который пишет только про сам город Белгород, не нужно классифицировать
// по ключевым словам из текста.
app.post('/api/admin/channel-region', requireAdmin, (req, res) => {
  const parsed = parseChannelInput((req.body || {}).channel);
  const region = (req.body || {}).region;
  if (!parsed) return res.status(400).json({ error: 'invalid channel' });
  if (!channels.includes(parsed)) return res.status(404).json({ error: 'unknown channel' });
  if (!region || region === 'auto') {
    delete channelRegionOverride[parsed];
  } else if (REGION_NAMES[region] || region === 'all') {
    channelRegionOverride[parsed] = region;
  } else {
    return res.status(400).json({ error: 'invalid region' });
  }
  saveJson(CHANNEL_REGIONS_FILE, channelRegionOverride);
  addLog('info', `Регион канала @${parsed} изменён на: ${region || 'auto'}`);
  res.json({ ok: true, channelRegions: channelRegionOverride });
});

app.post('/api/admin/channels', requireAdmin, (req, res) => {
  const parsed = parseChannelInput((req.body || {}).channel);
  if (!parsed) return res.status(400).json({ error: 'invalid channel' });
  if (channels.includes(parsed)) return res.status(409).json({ error: 'already added' });
  channels.push(parsed);
  saveJson(CHANNELS_FILE, channels);
  addLog('info', `Добавлен канал-источник: @${parsed}`);
  pollOnce(); // сразу опросить новый канал, не дожидаясь следующего цикла
  res.json({ ok: true, channels });
});

app.delete('/api/admin/channels', requireAdmin, (req, res) => {
  const parsed = parseChannelInput((req.body || {}).channel);
  if (!parsed) return res.status(400).json({ error: 'invalid channel' });
  if (channels.length <= 1) return res.status(400).json({ error: 'must keep at least one channel' });
  channels = channels.filter((c) => c.toLowerCase() !== parsed.toLowerCase());
  delete channelHealth[parsed];
  saveJson(CHANNELS_FILE, channels);
  addLog('info', `Удалён канал-источник: @${parsed}`);
  res.json({ ok: true, channels });
});

// ===== Настройки звуковой тревоги (какие типы/районы дают громкий push) =====
app.get('/api/admin/alarm-config', requireAdmin, (req, res) => {
  res.json(alarmConfig);
});

app.post('/api/admin/alarm-config', requireAdmin, (req, res) => {
  const { types, regions, enabled } = req.body || {};
  if (Array.isArray(types)) alarmConfig.types = types.filter((t) => TYPE_META[t]);
  if (Array.isArray(regions)) alarmConfig.regions = regions;
  if (typeof enabled === 'boolean') alarmConfig.enabled = enabled;
  saveJson(ALARM_CONFIG_FILE, alarmConfig);
  addLog('info', 'Настройки тревоги изменены', alarmConfig);
  res.json({ ok: true, alarmConfig });
});

// Ручное сообщение в ленту (любого типа, включая произвольные объявления администрации)
const TYPE_META = {
  rocket: { i: '🚀', tag: 'Ракетная опасность' },
  drone: { i: '🛸', tag: 'БПЛА обнаружен' },
  cancel: { i: '✅', tag: 'Отбой / отмена' },
  shelter: { i: '🏃', tag: 'В укрытие' },
  repeat: { i: '💬', tag: 'Повторно' },
  notice: { i: 'ℹ️', tag: 'Информационное сообщение' },
  admin: { i: '📢', tag: 'Сообщение администрации' }
};

app.post('/api/admin/message', requireAdmin, async (req, res) => {
  const { type, region, text, tag } = req.body || {};
  if (!text || !text.trim()) return res.status(400).json({ error: 'text required' });
  const meta = TYPE_META[type] || TYPE_META.admin;
  const dt = new Date();

  const item = {
    id: 'manual-' + dt.getTime(),
    t: TYPE_META[type] ? type : 'admin',
    i: meta.i,
    tag: tag || meta.tag,
    txt: text.trim(),
    time: dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' }),
    date: dt.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', timeZone: 'Europe/Moscow' }),
    region: region || 'belgorod',
    isNew: true,
    ts: dt.getTime(),
    iso: dt.toISOString(),
    hasRealTime: true,
    manual: true
  };

  state.feed.unshift(item);
  state.feed = state.feed.slice(0, 60);
  saveJson(STATE_FILE, state);
  addLog('info', `Ручное сообщение отправлено в ленту: ${item.tag} (${item.region})`);
  await notifySubscribers(item);
  res.json({ ok: true, item });
});

// Быстрая тревога (РО/БПЛА) по конкретному району или по всей области —
// аналог «Быстрого отбоя», чтобы не заполнять форму ручной отправки на каждый чих.
const QUICK_ALERT_META = {
  rocket: { i: '🚀', tag: 'Ракетная опасность', verb: 'Объявлена ракетная опасность' },
  drone: { i: '🛸', tag: 'БПЛА обнаружен', verb: 'Обнаружен БПЛА' }
};
app.post('/api/admin/quick-alert', requireAdmin, async (req, res) => {
  const { type, region, text } = req.body || {};
  const meta = QUICK_ALERT_META[type];
  if (!meta) return res.status(400).json({ error: 'type must be rocket or drone' });
  const dt = new Date();
  const regionLabel = (!region || region === 'all') ? 'по всей области' : (REGION_NAMES[region] || region);
  const item = {
    id: 'manual-quick-' + dt.getTime(),
    t: type, i: meta.i, tag: meta.tag,
    txt: text && text.trim() ? text.trim() : `${meta.verb} ${regionLabel === 'по всей области' ? regionLabel : '— ' + regionLabel}. Будьте бдительны, при сигнале сирены — в укрытие.`,
    time: dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' }),
    date: dt.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', timeZone: 'Europe/Moscow' }),
    region: region || 'all',
    isNew: true,
    ts: dt.getTime(),
    iso: dt.toISOString(),
    hasRealTime: true,
    manual: true
  };
  state.feed.unshift(item);
  state.feed = state.feed.slice(0, 60);
  saveJson(STATE_FILE, state);
  addLog('info', `Быстрая тревога отправлена: ${meta.tag} (${item.region})`);
  await notifySubscribers(item);
  res.json({ ok: true, item });
});

// Быстрый отбой — по конкретному району или сразу по всей области
app.post('/api/admin/cancel', requireAdmin, async (req, res) => {
  const { region, text } = req.body || {};
  const dt = new Date();
  const item = {
    id: 'manual-cancel-' + dt.getTime(),
    t: 'cancel', i: '✅', tag: 'Отбой / отмена',
    txt: text && text.trim() ? text.trim() : 'Отбой ракетной опасности. Угроза обстрела сохраняется. Берегите себя.',
    time: dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' }),
    date: dt.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', timeZone: 'Europe/Moscow' }),
    region: region || 'all',
    isNew: true,
    ts: dt.getTime(),
    iso: dt.toISOString(),
    hasRealTime: true,
    manual: true
  };
  state.feed.unshift(item);
  state.feed = state.feed.slice(0, 60);
  saveJson(STATE_FILE, state);
  addLog('info', `Отбой отправлен (${region || 'all'})`);
  await notifySubscribers(item);
  res.json({ ok: true, item });
});

// Удаление записи из ленты (админка) — например, если ручное сообщение
// отправлено по ошибке или устарело.
app.delete('/api/admin/feed/:id', requireAdmin, (req, res) => {
  const id = req.params.id;
  const before = state.feed.length;
  state.feed = (state.feed || []).filter((it) => String(it.id) !== String(id));
  if (state.feed.length === before) return res.status(404).json({ error: 'not found' });
  saveJson(STATE_FILE, state);
  addLog('info', `Запись удалена из ленты: ${id}`);
  res.json({ ok: true, feed: state.feed });
});

// ===== Глобальный обработчик ошибок =====
// Без этого необработанное исключение в любом роуте отдаёт HTML-страницу
// ошибки Express. Админка (admin.html) всегда ждёт JSON и на HTML-ответ
// падает ТИХО (res.json() бросает исключение, пойманное пустым catch) —
// именно так статистика могла молча оставаться пустой без единого
// сообщения об ошибке на экране. Теперь любая такая ошибка: 1) всегда
// отдаётся как JSON, 2) всегда попадает в лог (вкладка «Логи» в админке).
app.use((err, req, res, next) => {
  addLog('error', `Необработанная ошибка роута ${req.method} ${req.path}: ${err.message}`, { stack: err.stack });
  if (res.headersSent) return next(err);
  res.status(500).json({ error: 'internal error: ' + err.message });
});

app.listen(PORT, () => {
  console.log(`Тревога · Белгород — сервер запущен на порту ${PORT}`);
  addLog('info', `Сервер запущен на порту ${PORT}`);
  pollOnce();
  setInterval(pollOnce, POLL_MS);
});

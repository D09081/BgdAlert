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
// Задайте ADMIN_PASSWORD в переменных окружения хостинга для постоянного пароля
// — это самый надёжный вариант и он всегда в приоритете.
//
// БАГ, из-за которого админка вдруг переставала пускать по «правильному» паролю:
// если ADMIN_PASSWORD не задан, раньше пароль генерировался заново в памяти при
// КАЖДОМ старте процесса. А хостинги вроде Render на бесплатном тарифе
// перезапускают («усыпляют») процесс после простоя — значит пароль менялся
// сам собой, и старый переставал подходить, хотя пользователь ничего не путал.
// Теперь при отсутствии ADMIN_PASSWORD пароль генерируется только один раз и
// сохраняется на диск (data/admin-password.json) — при следующих перезапусках
// того же контейнера берётся уже сохранённый, а не новый случайный.
// Важно: если хостинг не даёт постоянный диск (например, каждый НОВЫЙ деплой,
// а не просто "сон/пробуждение", стирает файловую систему) — пароль всё равно
// сгенерируется заново при таком деплое. Поэтому для продакшена всё же лучше
// один раз явно задать ADMIN_PASSWORD в переменных окружения хостинга.
const ADMIN_PASSWORD_FILE = path.join(DATA_DIR, 'admin-password.json');
let ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
if (!ADMIN_PASSWORD) {
  const saved = loadJson(ADMIN_PASSWORD_FILE, null);
  if (saved && saved.password) {
    ADMIN_PASSWORD = saved.password;
    console.log('\n[i] ADMIN_PASSWORD не задан в переменных окружения — использован ранее сохранённый пароль:');
    console.log('    ADMIN_PASSWORD=' + ADMIN_PASSWORD);
    console.log('    Он не поменяется при перезапуске. Для полной надёжности всё же задайте ADMIN_PASSWORD в env.\n');
  } else {
    ADMIN_PASSWORD = crypto.randomBytes(6).toString('hex');
    saveJson(ADMIN_PASSWORD_FILE, { password: ADMIN_PASSWORD });
    console.log('\n[!] ADMIN_PASSWORD не задан в переменных окружения — сгенерирован и СОХРАНЁН на диск временный пароль администратора:');
    console.log('    ADMIN_PASSWORD=' + ADMIN_PASSWORD);
    console.log('    Он сохранится между перезапусками, но зафиксируйте свой постоянный пароль в переменных окружения для продакшена.\n');
  }
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

// ===== Telegram-бот — резервный канал доставки, не зависящий от Google/Apple push =====
// Задайте TELEGRAM_BOT_TOKEN (получить у @BotFather в Telegram) и TELEGRAM_BOT_USERNAME
// (без @, например trevoga_belgorod_bot) в переменных окружения хостинга — без них
// бот просто не запускается, остальной сайт работает как обычно.
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_BOT_USERNAME = process.env.TELEGRAM_BOT_USERNAME || '';
const TG_API = TELEGRAM_BOT_TOKEN ? `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}` : null;
const TG_SUBS_FILE = path.join(DATA_DIR, 'telegram-subs.json');
let tgSubscriptions = loadJson(TG_SUBS_FILE, []); // [{ chatId, regions: ['all'], joinedAt }]
const TG_ADMINS_FILE = path.join(DATA_DIR, 'telegram-admins.json');
let tgAdmins = loadJson(TG_ADMINS_FILE, []); // [chatId, ...] — чаты, прошедшие /admin <пароль>
function saveTgAdmins() { saveJson(TG_ADMINS_FILE, tgAdmins); }
function isTgAdmin(chatId) { return tgAdmins.includes(chatId); }
// Не персистим — это временное состояние на время сессии, а не данные:
let tgPendingAction = {}; // chatId -> 'add_channel' (ждём текстовый ответ после нажатия кнопки)
let tgFeedCache = {};     // chatId -> [id, id, ...] — индекс кнопки → реальный id записи ленты
                          // (id записей могут быть длиннее лимита callback_data в 64 байта,
                          // поэтому в кнопках передаём короткий индекс, а не сам id)

function saveTgSubs() { saveJson(TG_SUBS_FILE, tgSubscriptions); }

async function tgCall(method, params, timeoutMs) {
  if (!TG_API) return null;
  try {
    const res = await axios.post(`${TG_API}/${method}`, params, { timeout: timeoutMs || 10000 });
    return res.data;
  } catch (err) {
    // getUpdates — единственный метод с реальным долгим ожиданием (long polling,
    // params.timeout=25 сек на стороне Telegram) — если он честно "молчит", не
    // получив новых апдейтов, это НЕ ошибка, а нормальная работа long polling.
    if (method === 'getUpdates' && (err.code === 'ECONNABORTED' || /timeout/i.test(err.message))) {
      return { ok: true, result: [] };
    }
    addLog('error', `Telegram API ошибка (${method}): ` + (err.response?.data?.description || err.message));
    return null;
  }
}

function regionKeyboard(prefix) {
  const rows = [[{ text: '🌍 Вся область', callback_data: `${prefix}:all` }]];
  const regionEntries = Object.entries(REGION_NAMES);
  for (let i = 0; i < regionEntries.length; i += 2) {
    const row = [{ text: regionEntries[i][1], callback_data: `${prefix}:${regionEntries[i][0]}` }];
    if (regionEntries[i + 1]) row.push({ text: regionEntries[i + 1][1], callback_data: `${prefix}:${regionEntries[i + 1][0]}` });
    rows.push(row);
  }
  return { inline_keyboard: rows };
}

function alertTypeKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '🚀 Ракетная опасность', callback_data: 'tga:type:rocket' }],
      [{ text: '🛸 БПЛА', callback_data: 'tga:type:drone' }],
      [{ text: '✅ Отбой', callback_data: 'tga:type:cancel' }]
    ]
  };
}

function alertRegionKeyboard(type) {
  const rows = [[{ text: '🌍 Вся область', callback_data: `tga:go:${type}:all` }]];
  const regionEntries = Object.entries(REGION_NAMES);
  for (let i = 0; i < regionEntries.length; i += 2) {
    const row = [{ text: regionEntries[i][1], callback_data: `tga:go:${type}:${regionEntries[i][0]}` }];
    if (regionEntries[i + 1]) row.push({ text: regionEntries[i + 1][1], callback_data: `tga:go:${type}:${regionEntries[i + 1][0]}` });
    rows.push(row);
  }
  rows.push([{ text: '⬅️ Назад', callback_data: 'tga:back' }]);
  return { inline_keyboard: rows };
}

// Тот же набор данных, что видит админ на сайте (вкладка «Статистика») —
// собран в текстовом виде для Telegram, чтобы не заходить на сайт с телефона.
function buildAdminStatsText() {
  const feedByType = {};
  (Array.isArray(state.feed) ? state.feed : []).forEach((it) => { feedByType[it.t] = (feedByType[it.t] || 0) + 1; });
  const subsByRegion = {};
  (Array.isArray(subscriptions) ? subscriptions : []).forEach((s) => {
    (s.regions || []).forEach((r) => { subsByRegion[r] = (subsByRegion[r] || 0) + 1; });
  });
  const recentErrors = logs.filter((l) => l.level === 'error').slice(-5);
  const lines = [];
  lines.push('📊 *Статистика*');
  lines.push('');
  lines.push(`👁 Визитов всего: *${analytics.totalVisits || 0}*`);
  lines.push(`👤 Уникальных посетителей: *${(analytics.uniqueVisitors || []).length}*`);
  lines.push(`📅 Визитов сегодня: *${(analytics.dailyCounts && analytics.dailyCounts[todayKey()]) || 0}*`);
  lines.push('');
  lines.push(`🔔 Подписчиков push: *${subscriptions.length}*`);
  lines.push(`✈️ Подписчиков Telegram: *${tgSubscriptions.length}*`);
  lines.push('');
  lines.push(`📰 Записей в ленте: *${state.feed.length}*`);
  Object.entries(feedByType).forEach(([t, n]) => lines.push(`  • ${t}: ${n}`));
  lines.push('');
  lines.push(`📡 Парсер каналов: ${lastPollOk ? '✅ работает' : '⚠️ ошибка'}`);
  lines.push(`   Последний опрос: ${lastPollAt ? new Date(lastPollAt).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : '—'}`);
  lines.push(`   Каналы: ${channels.join(', ')}`);
  lines.push('');
  lines.push(`⚙️ Тревога включена: ${alarmConfig.enabled ? 'да' : 'нет'}, типы: ${alarmConfig.types.join(', ')}`);
  if (recentErrors.length) {
    lines.push('');
    lines.push('🔴 Последние ошибки в логах:');
    recentErrors.forEach((l) => lines.push(`  • ${new Date(l.t).toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow' })} — ${l.message}`));
  }
  return lines.join('\n');
}

function mainMenuKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '⚡ Быстрая тревога', callback_data: 'tga:menu:alert' }],
      [{ text: '📊 Статистика', callback_data: 'tga:menu:stats' }, { text: '🧾 Логи', callback_data: 'tga:menu:logs' }],
      [{ text: '📡 Каналы-источники', callback_data: 'tga:menu:channels' }],
      [{ text: '⚙️ Настройки тревоги', callback_data: 'tga:menu:alarmcfg' }],
      [{ text: '🗂 Текущая лента', callback_data: 'tga:menu:feed' }],
      [{ text: '🚪 Выйти из админки', callback_data: 'tga:menu:logout' }]
    ]
  };
}

function logsKeyboard(filter) {
  const mark = (f) => (filter === f ? '• ' : '');
  return {
    inline_keyboard: [
      [{ text: mark('') + 'Все', callback_data: 'tga:logs:' }, { text: mark('error') + 'Ошибки', callback_data: 'tga:logs:error' }],
      [{ text: mark('warn') + 'Предупреждения', callback_data: 'tga:logs:warn' }, { text: mark('info') + 'Инфо', callback_data: 'tga:logs:info' }],
      [{ text: '↻ Обновить', callback_data: 'tga:logs:' + filter }],
      [{ text: '⬅️ Меню', callback_data: 'tga:menu:main' }]
    ]
  };
}

function buildLogsText(filter) {
  const LOG_LABEL = { error: '🔴 Ошибка', warn: '🟡 Предупреждение', info: 'ℹ️ Инфо' };
  const items = (filter ? logs.filter((l) => l.level === filter) : logs).slice(-12).reverse();
  if (!items.length) return '🧾 *Логи*\n\nПока нет записей.';
  const lines = ['🧾 *Логи* ' + (filter ? `(фильтр: ${filter})` : '(все)')];
  items.forEach((l) => {
    const time = new Date(l.t).toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow' });
    lines.push(`\n${LOG_LABEL[l.level] || l.level} · ${time}\n${l.message}`);
  });
  return lines.join('\n');
}

function channelsKeyboard() {
  const rows = channels.map((c) => {
    const health = channelHealth[c];
    const icon = health && health.lastError ? '⚠️' : '✅';
    return [{ text: `${icon} @${c}`, callback_data: 'tga:noop' }, { text: '🗑 удалить', callback_data: 'tga:chan:del:' + c }];
  });
  rows.push([{ text: '➕ Добавить канал', callback_data: 'tga:chan:add' }]);
  rows.push([{ text: '⬅️ Меню', callback_data: 'tga:menu:main' }]);
  return { inline_keyboard: rows };
}

function buildChannelsText() {
  const lines = ['📡 *Каналы-источники*', ''];
  channels.forEach((c) => {
    const health = channelHealth[c];
    const status = health && health.lastError ? `⚠️ ошибка: ${health.lastError}` : '✅ ок';
    lines.push(`@${c} — ${status}`);
  });
  return lines.join('\n');
}

function alarmCfgKeyboard() {
  const on = (v) => (v ? '✅' : '⬜️');
  return {
    inline_keyboard: [
      [{ text: `${on(alarmConfig.enabled)} Тревога включена`, callback_data: 'tga:cfg:enabled' }],
      [{ text: `${on(alarmConfig.types.includes('rocket'))} 🚀 Ракетная опасность`, callback_data: 'tga:cfg:type:rocket' }],
      [{ text: `${on(alarmConfig.types.includes('drone'))} 🛸 БПЛА`, callback_data: 'tga:cfg:type:drone' }],
      [{ text: '⬅️ Меню', callback_data: 'tga:menu:main' }]
    ]
  };
}

function feedKeyboard(chatId) {
  const items = (state.feed || []).slice(0, 8);
  tgFeedCache[chatId] = items.map((it) => it.id);
  const rows = items.map((it, i) => [
    { text: `${it.i} ${it.time} ${it.tag}`.slice(0, 60), callback_data: 'tga:noop' },
    { text: '🗑', callback_data: 'tga:feed:del:' + i }
  ]);
  rows.push([{ text: '⬅️ Меню', callback_data: 'tga:menu:main' }]);
  return { inline_keyboard: rows };
}

function buildFeedText() {
  const items = (state.feed || []).slice(0, 8);
  if (!items.length) return '🗂 *Текущая лента*\n\nПусто.';
  const lines = ['🗂 *Текущая лента* (последние 8)'];
  items.forEach((it) => {
    const regionLabel = it.region === 'all' ? 'вся область' : (REGION_NAMES[it.region] || it.region);
    lines.push(`\n${it.i} *${it.tag}* — ${regionLabel}${it.manual ? ' ✍️' : ''}\n${it.time} · ${it.date}\n${(it.txt || '').slice(0, 150)}`);
  });
  return lines.join('\n');
}

async function tgHandleUpdate(update) {
  try {
    if (update.callback_query) {
      const cq = update.callback_query;
      const chatId = cq.message.chat.id;
      const data = cq.data || '';
      if (data.startsWith('tgregion:')) {
        const region = data.slice('tgregion:'.length);
        const idx = tgSubscriptions.findIndex((s) => s.chatId === chatId);
        const entry = { chatId, regions: [region], joinedAt: idx >= 0 ? tgSubscriptions[idx].joinedAt : Date.now() };
        if (idx >= 0) tgSubscriptions[idx] = entry; else tgSubscriptions.push(entry);
        saveTgSubs();
        const label = region === 'all' ? 'вся область' : (REGION_NAMES[region] || region);
        await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: `Район выбран: ${label}` });
        await tgCall('editMessageText', {
          chat_id: chatId, message_id: cq.message.message_id,
          text: `✅ Подписка настроена — район: *${label}*\n\nТы будешь получать сообщение здесь при каждой ракетной опасности, обнаружении БПЛА и отбое тревоги. Изменить район — /region. Отписаться — /stop.`,
          parse_mode: 'Markdown'
        });
        addLog('info', `Telegram: подписка настроена, район ${region}`);
        return;
      }
      // ===== Админ-панель бота: публикация тревоги/отбоя прямо из Telegram =====
      if (data.startsWith('tga:')) {
        if (!isTgAdmin(chatId)) {
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Доступ только для админов. Наберите /admin <пароль>.', show_alert: true });
          return;
        }
        const parts = data.split(':'); // tga:menu:stats  |  tga:type:rocket  |  tga:go:rocket:belgorod  |  ...
        const mid = cq.message.message_id;
        const edit = (text, reply_markup) => tgCall('editMessageText', { chat_id: chatId, message_id: mid, text, parse_mode: 'Markdown', reply_markup });

        if (data === 'tga:noop') {
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id });
          return;
        }

        if (parts[1] === 'menu') {
          const section = parts[2];
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id });
          if (section === 'main') {
            await edit('🛠 *Админ-меню*\n\nВыбери раздел:', mainMenuKeyboard());
          } else if (section === 'alert') {
            await edit('⚡ Что отправить?', alertTypeKeyboard());
          } else if (section === 'stats') {
            await edit(buildAdminStatsText(), { inline_keyboard: [[{ text: '↻ Обновить', callback_data: 'tga:menu:stats' }], [{ text: '⬅️ Меню', callback_data: 'tga:menu:main' }]] });
          } else if (section === 'logs') {
            await edit(buildLogsText(''), logsKeyboard(''));
          } else if (section === 'channels') {
            await edit(buildChannelsText(), channelsKeyboard());
          } else if (section === 'alarmcfg') {
            await edit('⚙️ *Настройки тревоги*\n\nЧто из этого триггерит громкий звук/вибрацию у пользователей на сайте:', alarmCfgKeyboard());
          } else if (section === 'feed') {
            await edit(buildFeedText(), feedKeyboard(chatId));
          } else if (section === 'logout') {
            tgAdmins = tgAdmins.filter((id) => id !== chatId);
            saveTgAdmins();
            await tgCall('editMessageText', { chat_id: chatId, message_id: mid, text: '👋 Вы вышли из режима администратора. /admin <пароль> — войти снова.' });
          }
          return;
        }

        if (parts[1] === 'type') {
          const type = parts[2];
          const label = type === 'rocket' ? '🚀 Ракетная опасность' : type === 'drone' ? '🛸 БПЛА' : '✅ Отбой';
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id });
          await edit(`${label}\n\nВыбери район:`, alertRegionKeyboard(type));
        } else if (parts[1] === 'go') {
          const type = parts[2], region = parts[3];
          try {
            const item = type === 'cancel' ? await publishCancel(region, null) : await publishQuickAlert(type, region, null);
            const regionLabel = region === 'all' ? 'по всей области' : (REGION_NAMES[region] || region);
            await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Отправлено' });
            await edit(`✅ Отправлено в ленту и всем подписчикам:\n\n${item.i} *${item.tag}* — ${regionLabel}\n${item.txt}`, { inline_keyboard: [[{ text: '⬅️ Меню', callback_data: 'tga:menu:main' }]] });
            addLog('info', `Telegram-админ ${chatId} опубликовал: ${item.tag} (${region})`);
          } catch (err) {
            await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Ошибка: ' + err.message, show_alert: true });
          }
        } else if (parts[1] === 'back') {
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id });
          await edit('⚡ Что отправить?', alertTypeKeyboard());
        } else if (parts[1] === 'logs') {
          const filter = parts[2] || '';
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id });
          await edit(buildLogsText(filter), logsKeyboard(filter));
        } else if (parts[1] === 'cfg') {
          if (parts[2] === 'enabled') {
            alarmConfig.enabled = !alarmConfig.enabled;
          } else if (parts[2] === 'type') {
            const t = parts[3];
            alarmConfig.types = alarmConfig.types.includes(t) ? alarmConfig.types.filter((x) => x !== t) : alarmConfig.types.concat([t]);
          }
          saveJson(ALARM_CONFIG_FILE, alarmConfig);
          addLog('info', `Telegram-админ ${chatId} изменил настройки тревоги`, alarmConfig);
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Сохранено' });
          await edit('⚙️ *Настройки тревоги*\n\nЧто из этого триггерит громкий звук/вибрацию у пользователей на сайте:', alarmCfgKeyboard());
        } else if (parts[1] === 'chan') {
          if (parts[2] === 'add') {
            tgPendingAction[chatId] = 'add_channel';
            await tgCall('answerCallbackQuery', { callback_query_id: cq.id });
            await edit('✏️ Пришли следующим сообщением username канала (например `mchs31` или ссылку `t.me/mchs31`).', { inline_keyboard: [[{ text: '⬅️ Отмена', callback_data: 'tga:menu:channels' }]] });
          } else if (parts[2] === 'del') {
            const parsed = parts.slice(3).join(':'); // на случай ':' в имени канала — маловероятно, но безопасно
            if (channels.length <= 1) {
              await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Нельзя удалить последний оставшийся канал', show_alert: true });
              return;
            }
            channels = channels.filter((c) => c.toLowerCase() !== parsed.toLowerCase());
            delete channelHealth[parsed];
            saveJson(CHANNELS_FILE, channels);
            addLog('info', `Telegram-админ ${chatId} удалил канал @${parsed}`);
            await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Удалено' });
            await edit(buildChannelsText(), channelsKeyboard());
          }
        } else if (parts[1] === 'feed' && parts[2] === 'del') {
          const idx = Number(parts[3]);
          const id = (tgFeedCache[chatId] || [])[idx];
          if (id == null) {
            await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Список устарел, открой раздел заново', show_alert: true });
            return;
          }
          const before = state.feed.length;
          state.feed = (state.feed || []).filter((it) => String(it.id) !== String(id));
          if (state.feed.length !== before) {
            saveJson(STATE_FILE, state);
            addLog('info', `Telegram-админ ${chatId} удалил запись из ленты: ${id}`);
          }
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Удалено' });
          await edit(buildFeedText(), feedKeyboard(chatId));
        }
        return;
      }
      return;
    }
    const msg = update.message;
    if (!msg || !msg.text) return;
    const chatId = msg.chat.id;
    const text = msg.text.trim();

    // Если только что нажали "➕ Добавить канал" — трактуем следующее
    // обычное сообщение как username канала, а не как неизвестную команду.
    if (tgPendingAction[chatId] === 'add_channel' && !text.startsWith('/')) {
      delete tgPendingAction[chatId];
      const parsed = parseChannelInput(text);
      if (!parsed) {
        await tgCall('sendMessage', { chat_id: chatId, text: '❌ Не удалось распознать канал. Пришли просто username, например mchs31.' });
        return;
      }
      if (channels.includes(parsed)) {
        await tgCall('sendMessage', { chat_id: chatId, text: `Канал @${parsed} уже добавлен.`, parse_mode: 'Markdown', reply_markup: channelsKeyboard() });
        return;
      }
      channels.push(parsed);
      saveJson(CHANNELS_FILE, channels);
      addLog('info', `Telegram-админ ${chatId} добавил канал-источник: @${parsed}`);
      pollOnce();
      await tgCall('sendMessage', { chat_id: chatId, text: '✅ Канал добавлен.\n\n' + buildChannelsText(), parse_mode: 'Markdown', reply_markup: channelsKeyboard() });
      return;
    }

    if (text === '/start' || text.startsWith('/start ')) {
      const idx = tgSubscriptions.findIndex((s) => s.chatId === chatId);
      if (idx < 0) { tgSubscriptions.push({ chatId, regions: ['all'], joinedAt: Date.now() }); saveTgSubs(); addLog('info', 'Telegram: новый подписчик ' + chatId); }
      await tgCall('sendMessage', {
        chat_id: chatId,
        text: '🚨 *Тревога Белгород* — оповещения о ракетной опасности и БПЛА.\n\nПо умолчанию включена вся область. Выбери свой район, если нужны только его оповещения:',
        parse_mode: 'Markdown',
        reply_markup: regionKeyboard('tgregion')
      });
    } else if (text === '/region') {
      await tgCall('sendMessage', { chat_id: chatId, text: 'Выбери район:', reply_markup: regionKeyboard('tgregion') });
    } else if (text === '/stop') {
      tgSubscriptions = tgSubscriptions.filter((s) => s.chatId !== chatId);
      saveTgSubs();
      await tgCall('sendMessage', { chat_id: chatId, text: '🔕 Подписка отключена. Вернуться можно командой /start.' });
      addLog('info', 'Telegram: отписка ' + chatId);
    } else if (text.startsWith('/admin')) {
      const password = text.slice('/admin'.length).trim();
      if (!password) {
        await tgCall('sendMessage', { chat_id: chatId, text: 'Использование: /admin ваш_пароль_админки' });
        return;
      }
      // Сразу удаляем сообщение с паролем из чата — не оставляем его
      // открытым текстом в истории переписки дольше, чем необходимо.
      await tgCall('deleteMessage', { chat_id: chatId, message_id: msg.message_id });
      if (password !== ADMIN_PASSWORD) {
        addLog('warn', 'Telegram: неудачная попытка /admin из чата ' + chatId);
        await tgCall('sendMessage', { chat_id: chatId, text: '❌ Неверный пароль.' });
        return;
      }
      if (!isTgAdmin(chatId)) { tgAdmins.push(chatId); saveTgAdmins(); }
      addLog('info', 'Telegram: вход в админку из чата ' + chatId);
      await tgCall('sendMessage', {
        chat_id: chatId,
        text: '✅ Доступ администратора подтверждён.\n\n🛠 *Админ-меню*\n\nВыбери раздел:',
        parse_mode: 'Markdown',
        reply_markup: mainMenuKeyboard()
      });
    } else if (text === '/menu' || text === '/stats' || text === '/alert') {
      if (!isTgAdmin(chatId)) { await tgCall('sendMessage', { chat_id: chatId, text: 'Доступ только для админов. Наберите /admin <пароль>.' }); return; }
      await tgCall('sendMessage', { chat_id: chatId, text: '🛠 *Админ-меню*\n\nВыбери раздел:', parse_mode: 'Markdown', reply_markup: mainMenuKeyboard() });
    } else if (text === '/adminlogout') {
      tgAdmins = tgAdmins.filter((id) => id !== chatId);
      saveTgAdmins();
      await tgCall('sendMessage', { chat_id: chatId, text: '👋 Вышли из режима администратора.' });
    } else {
      const base = 'Команды: /start — подписаться, /region — выбрать район, /stop — отписаться.';
      const adminHint = isTgAdmin(chatId) ? '\n\nАдмин: /menu — открыть меню управления.' : '\n\nВы администратор сайта? Наберите /admin <пароль> для доступа к панели управления.';
      await tgCall('sendMessage', { chat_id: chatId, text: base + adminHint });
    }
  } catch (err) {
    addLog('error', 'Ошибка обработки Telegram-апдейта: ' + err.message, { stack: err.stack });
  }
}

// Long polling — не требует публичного HTTPS-вебхука и лишней настройки,
// работает "из коробки" сразу после того как задан TELEGRAM_BOT_TOKEN.
let tgOffset = 0;
async function tgPollLoop() {
  if (!TG_API) return;
  try {
    const data = await tgCall('getUpdates', { offset: tgOffset, timeout: 25 }, 30000);
    if (data && data.ok && Array.isArray(data.result)) {
      for (const update of data.result) {
        tgOffset = update.update_id + 1;
        await tgHandleUpdate(update);
      }
    }
  } catch (err) {
    addLog('error', 'Ошибка Telegram long polling: ' + err.message);
    await new Promise((r) => setTimeout(r, 3000)); // не долбить API при постоянной ошибке
  }
  setImmediate(tgPollLoop);
}
if (TG_API) {
  tgPollLoop();
  addLog('info', 'Telegram-бот запущен (long polling)');
} else {
  console.log('[i] TELEGRAM_BOT_TOKEN не задан — Telegram-канал оповещений отключён (сайт и push работают как обычно).');
}

async function notifyTelegramSubscribers(item) {
  if (!TG_API || !tgSubscriptions.length) return;
  const isUrgent = isAlarmTriggering(item);
  const regionLabel = item.region === 'all' ? 'по всей области' : (REGION_NAMES[item.region] || item.region);
  const title = isUrgent ? `🚨 *ТРЕВОГА* · ${regionLabel}` : `${item.i} *${item.tag}*`;
  const text = `${title}\n\n${item.txt}\n\n_${item.time} · ${item.date}_`;
  const stillValid = [];
  for (const entry of tgSubscriptions) {
    const matches = item.region === 'all' || (entry.regions && (entry.regions.includes('all') || entry.regions.includes(item.region)));
    if (!matches) { stillValid.push(entry); continue; }
    const result = await tgCall('sendMessage', { chat_id: entry.chatId, text, parse_mode: 'Markdown' });
    // Код 403 = пользователь заблокировал бота — удаляем такого подписчика,
    // как это уже делается для "умерших" web push подписок (404/410).
    if (result === null) { /* сетевая/временная ошибка — не удаляем, оставляем как есть */ stillValid.push(entry); }
    else if (result.ok === false && result.error_code === 403) { addLog('info', `Telegram: подписчик ${entry.chatId} заблокировал бота, удалён из списка`); }
    else stillValid.push(entry);
  }
  tgSubscriptions = stillValid;
  saveTgSubs();
}

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

// Признаки рекламных / нерелевантных постов — такие сообщения отбрасываем ЦЕЛИКОМ
// (это про текстовые маркеры самой рекламы, а не про наличие ссылки — ссылку внутри
// иначе легитимного оповещения вырезаем построчно через stripLinks(), не выкидывая
// оповещение целиком, см. ниже).
const AD_PATTERNS = [
  /реклам/i, /promo/i, /подпис\w+ на канал/i, /erid/i, /18\+.*реклама/i,
  /по вопросам сотрудничества/i
];

// Ссылки внутри иначе нормального сообщения (например «РАКЕТНАЯ ОПАСНОСТЬ» текстом,
// а строкой ниже — рекламная/произвольная ссылка) — вырезаем построчно, само
// оповещение остаётся. Если после вырезания ссылки строка становится пустой или
// состоит только из CTA-обрывка вида «подробнее:», «читать далее» — убираем и её.
const URL_RE = /(https?:\/\/\S+)|(\bwww\.\S+)|(\bt\.me\/\S+)|(\bvk\.cc\/\S+)|(\bclck\.ru\/\S+)|(\bbit\.ly\/\S+)|(\bgoo\.gl\/\S+)/gi;
const LINK_STUB_RE = /^[\s👉➡️\-–—:]*(подробнее(\s*по\s*ссылке)?|читать\s*далее|источник|по\s*ссылке|переходи(те)?|жми(те)?)[\s👉➡️\-–—:]*$/i;
// То же самое, но "приклеенное" к концу строки с обычным текстом — например
// «БПЛА обнаружен в Короче. Источник: <ссылка>» — обрезаем только хвост,
// оставляя содержательную часть строки.
const TRAILING_CTA_RE = /[\s👉➡️\-–—:]*(подробнее(\s*по\s*ссылке)?|читать\s*далее|источник|по\s*ссылке|переходи(те)?|жми(те)?)[\s👉➡️\-–—:]*$/i;

function stripLinks(text) {
  return text
    .split('\n')
    .map((line) => line.replace(URL_RE, '').trim())
    .map((line) => line.replace(TRAILING_CTA_RE, '').trim())
    .filter((line) => line.length > 0 && !LINK_STUB_RE.test(line) && /[a-zA-Zа-яА-ЯёЁ]/.test(line))
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

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
  const cleanText = stripLinks(msg.text);
  const cls = classify(cleanText);
  const region = channelRegionOverride[msg.channel] || detectRegion(cleanText);
  const hasRealTime = !!msg.datetime;
  const dt = hasRealTime ? new Date(msg.datetime) : new Date();
  const time = dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' });
  const date = dt.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', timeZone: 'Europe/Moscow' });
  return {
    id: msg.id,
    t: cls.t, i: cls.i, tag: cls.tag,
    txt: cleanText.length > 400 ? cleanText.slice(0, 400) + '…' : cleanText,
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
  await notifyTelegramSubscribers(item);
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

app.get('/api/telegram-config', (req, res) => {
  res.json({ botUsername: TELEGRAM_BOT_USERNAME || null });
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
        byRegion: subsByRegion,
        telegram: (tgSubscriptions || []).length
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

// Общая логика публикации тревоги/отбоя — используется и HTTP-эндпоинтами
// админки, и админ-командами Telegram-бота, чтобы не дублировать код и не
// разойтись в поведении (оба пути должны одинаково сохранять в ленту и
// одинаково рассылать всем подписчикам).
async function publishQuickAlert(type, region, text) {
  const meta = QUICK_ALERT_META[type];
  if (!meta) throw new Error('type must be rocket or drone');
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
  return item;
}

async function publishCancel(region, text) {
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
  return item;
}

app.post('/api/admin/quick-alert', requireAdmin, async (req, res) => {
  const { type, region, text } = req.body || {};
  try {
    const item = await publishQuickAlert(type, region, text);
    res.json({ ok: true, item });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// Быстрый отбой — по конкретному району или сразу по всей области
app.post('/api/admin/cancel', requireAdmin, async (req, res) => {
  const { region, text } = req.body || {};
  const item = await publishCancel(region, text);
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

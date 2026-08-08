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
const webpush = require('web-push');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { fetchAllChannels } = require('./channel-fetcher');
const { describeError } = require('./error-utils');

// ===== Внешний бэкап данных (Upstash Redis REST) — переживает перезапуск хостинга =====
// НАЙДЕННАЯ ПРИЧИНА, почему каналы-источники и подписки слетали после
// перезапуска сервера: на бесплатном тарифе Render (и большинстве бесплатных
// PaaS) файловая система ЭФЕМЕРНАЯ — по официальной документации Render, все
// изменения на диске стираются при КАЖДОМ редеплое, restart'е и "пересыпании"
// контейнера после простоя, а не только при обновлении кода. Это ограничение
// самого хостинга, а не баг в этом файле — просто писать данные на локальный
// диск было недостаточно.
//
// Решение без платного тарифа: если заданы UPSTASH_REDIS_REST_URL и
// UPSTASH_REDIS_REST_TOKEN (бесплатный Upstash Redis — https://upstash.com,
// без карты, данные не привязаны к диску Render и не пропадают), все те же
// файлы (каналы, подписки push и Telegram, настройки тревоги, лента, логи,
// админ-пароль) при каждом сохранении дополнительно копируются туда, а при
// каждом старте сервера подтягиваются обратно ДО того, как сервер начнёт
// принимать запросы. Без этих двух переменных всё работает как раньше —
// только на локальном диске (и слетает при перезапуске на бесплатном Render).
const UPSTASH_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const REDIS_ENABLED = !!(UPSTASH_URL && UPSTASH_TOKEN);

// Upstash REST API: команда шлётся POST-ом на корневой endpoint с телом вида
// ["SET", key, value] / ["GET", key] — значение в теле запроса, а не в пути
// URL. Это важно, потому что каналы/подписки/лента — это JSON-объекты, и
// первый вариант (значение в самом пути URL) либо упирается в лимит длины
// URL, либо ломается на экранировании кавычек/фигурных скобок в большом JSON.
async function redisCommand(args) {
  if (!REDIS_ENABLED) return null;
  try {
    const res = await axios.post(UPSTASH_URL, args, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, 'Content-Type': 'application/json' }, timeout: 8000
    });
    return res.data || null;
  } catch (err) {
    console.log(`[Redis] команда ${args[0]} ошибка:`, err.message);
    return null;
  }
}

async function redisGet(key) {
  const data = await redisCommand(['GET', key]);
  return (data && data.result != null) ? data.result : null;
}

// Фоновая запись — не блокирует и не ждёт ответа: это резервная копия,
// а не основной путь чтения/записи, сетевая заминка не должна тормозить сайт.
function redisSet(key, value) {
  redisCommand(['SET', key, JSON.stringify(value)]);
}

const PORT = process.env.PORT || 3000;
// частота опроса каналов теперь в EDITABLE_SETTINGS (ключ pollIntervalSec), см. ниже
const DATA_DIR = path.join(__dirname, 'data');

// НАЙДЕННЫЙ ПРИ ТЕСТИРОВАНИИ БАГ (та же природа, что и с логами ранее): если
// заполнять REDIS_KEY_MAP через ссылки на константы вида CHANNELS_FILE, TG_ADMINS_FILE
// и т.п., то это приходится делать уже ПОСЛЕ их объявления — а часть из них (LOGS_FILE,
// TG_SUBS_FILE...) объявляется гораздо ниже по файлу, уже после первой же
// бутстрап-записи каналов при самом первом старте сервера. В результате
// первая запись дефолтных каналов происходила ДО того как карта заполнена, и
// её нечем было бэкапить в Redis. Строим карту сразу по литеральным путям —
// без зависимости от порядка объявления остальных констант.
const REDIS_KEY_MAP = {
  [path.join(DATA_DIR, 'channels.json')]: 'bgdalert:channels',
  [path.join(DATA_DIR, 'subscriptions.json')]: 'bgdalert:subscriptions',
  [path.join(DATA_DIR, 'telegram-subs.json')]: 'bgdalert:telegram-subs',
  [path.join(DATA_DIR, 'telegram-admins.json')]: 'bgdalert:telegram-admins',
  [path.join(DATA_DIR, 'alarm-config.json')]: 'bgdalert:alarm-config',
  [path.join(DATA_DIR, 'channel-regions.json')]: 'bgdalert:channel-regions',
  [path.join(DATA_DIR, 'state.json')]: 'bgdalert:state',
  [path.join(DATA_DIR, 'analytics.json')]: 'bgdalert:analytics',
  [path.join(DATA_DIR, 'logs.json')]: 'bgdalert:logs',
  [path.join(DATA_DIR, 'custom-filters.json')]: 'bgdalert:custom-filters',
  [path.join(DATA_DIR, 'settings.json')]: 'bgdalert:settings'
};
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

function loadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf-8')); } catch (e) { return fallback; }
}
// Пока не завершится восстановление из Redis при старте (см. hydrateFromRedis),
// saveJson НЕ пишет в Redis — иначе бутстрап-записи вида "файла ещё нет,
// запишем дефолт" (например дефолтные каналы при самом первом старте) успевали
// затереть в Redis уже сохранённые ранее реальные данные ДО того, как сервер
// вообще успевал их оттуда прочитать. Найдено тестированием запуска, не
// просто чтением кода.
let redisSyncReady = false;
function saveJson(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf-8');
  const key = REDIS_KEY_MAP[file];
  if (key && redisSyncReady) redisSet(key, data);
}
let subscriptions = loadJson(SUBS_FILE, []); // [{ subscription, regions, sound, vibro }]

let state = loadJson(STATE_FILE, { seenIds: [], feed: [], deletedIds: [] });
// На случай если state загружен из старого файла/Redis, где deletedIds ещё не было —
// без этого удалённые из ленты записи возвращались бы обратно на первом же опросе.
if (!Array.isArray(state.deletedIds)) state.deletedIds = [];
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
function normalizeAnalytics() {
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
}
normalizeAnalytics();
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

// Настройки, редактируемые прямо из веб-админки (вкладка «⚙️ Настройки»)
// БЕЗ редеплоя — в отличие от переменных окружения, которые на большинстве
// хостингов требуют пересборки/ручного перезапуска сервиса. При старте
// используется сохранённое в settings.json значение, если оно есть; иначе —
// переменная окружения (для совместимости с уже настроенными деплоями);
// иначе — дефолт ниже. Чтобы добавить в будущем ещё одну редактируемую из
// админки настройку — достаточно дописать запись в EDITABLE_SETTINGS и
// прочитать её значение через getSetting(key)/getNumberSetting(key, дефолт)
// там, где она нужна по коду; сохранение/API/форма в админке уже общие для
// всех записей этого списка. type:'number' — рендерится как <input type=number>
// с min/max/step, хранится и сравнивается как число, а не строка.
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const EDITABLE_SETTINGS = [
  {
    key: 'telegramGroupChat', envVar: 'TELEGRAM_GROUP_CHAT', fallback: '@BgdAlert', type: 'string',
    label: 'Группа/канал для дублирования оповещений',
    hint: 'Бот должен быть добавлен туда администратором с правом отправки сообщений — иначе рассылка в группу будет падать с ошибкой в логах.'
  },
  {
    key: 'promoChannelUrl', envVar: 'PROMO_CHANNEL_URL', fallback: 'https://t.me/BgdAlert', type: 'string',
    label: 'Ссылка на канал (призыв подписаться)',
    hint: 'Показывается в личных сообщениях бота подписчикам и на карточках ленты сайта — не в самой группе (её участники уже там).'
  },
  {
    key: 'pollIntervalSec', envVar: 'POLL_INTERVAL_SEC', fallback: '10', type: 'number', min: 5, max: 120, step: 1,
    label: 'Частота опроса каналов (сек)',
    hint: 'Как часто проверять каналы на новые сообщения. Меньше — быстрее узнаёте о тревоге, но выше нагрузка на t.me и риск сетевых сбоев. Применяется на следующем цикле опроса, без перезапуска сервера.'
  },
  {
    key: 'groupIntervalMs', envVar: 'GROUP_MIN_INTERVAL_MS', fallback: '1500', type: 'number', min: 500, max: 10000, step: 100,
    label: 'Пауза перед сообщением в группу (мс)',
    hint: 'Минимальный интервал между сообщениями в группу — защита от блокировки Telegram (429), если разом приходит пачка новых событий. Уменьшать осторожно.'
  },
  {
    key: 'alertTextLimit', envVar: 'ALERT_TEXT_LIMIT', fallback: '400', type: 'number', min: 100, max: 2000, step: 50,
    label: 'Длина текста тревоги до обрезки (символов)',
    hint: 'Настоящие тревоги (ракета/БПЛА/укрытие/отбой) обрезаются до этой длины — их читают за секунду, длинный текст только мешает. На новостные посты не влияет — те всегда показываются целиком.'
  },
  {
    key: 'dedupeWindowMin', envVar: 'DEDUPE_WINDOW_MIN', fallback: '15', type: 'number', min: 1, max: 120, step: 1,
    label: 'Окно дедупликации похожих сообщений (минут)',
    hint: 'Сообщения одного типа и района в пределах этого окна, похожие по тексту, считаются повтором и не дублируются в ленте.'
  },
  {
    key: 'cancelDefaultText', envVar: 'CANCEL_DEFAULT_TEXT',
    fallback: 'Отбой ракетной опасности. Угроза обстрела сохраняется. Берегите себя.',
    type: 'string', multiline: true,
    label: 'Текст «✅ Отбой / отмена» по умолчанию',
    hint: 'Подставляется, когда админ жмёт кнопку быстрого отбоя (на сайте или в боте), не вводя свой текст вручную. Свой текст при отправке всегда перекрывает это значение.'
  },
  {
    key: 'promoCtaTemplate', envVar: 'PROMO_CTA_TEMPLATE',
    fallback: '🔥 Узнал первым здесь? В канале ещё быстрее и без задержек — [подписывайся →]({url})',
    type: 'string', multiline: true, validateMarkdown: true,
    label: 'Текст призыва подписаться (в личных сообщениях подписчикам)',
    hint: '{url} автоматически заменяется на ссылку из поля «Ссылка на канал» выше. Добавляется в конец каждого личного сообщения бота подписчику — не в группу (её участники уже там) и не на сайт (там своя кнопка). Здесь работает настоящая Markdown-разметка Telegram (*жирный*, _курсив_, [текст](ссылка)) — следи за тем, чтобы все скобки/звёздочки/подчёркивания были парами. Осторожно с @username ботов/каналов вида @Имя_бот — подчёркивание внутри него тоже считается за разметку и может сломать всё сообщение.'
  },
  {
    key: 'groupCtaTemplate', envVar: 'GROUP_CTA_TEMPLATE', fallback: '',
    type: 'string', multiline: true, validateMarkdown: true,
    label: 'Текст в конце сообщений в группе/канале',
    hint: 'Та же идея, что и текст призыва выше, но для сообщений, которые уходят в группу/канал (см. поле «Группа/канал для дублирования» выше), а не подписчикам в личку. {url} тоже подставляется автоматически. Пусто по умолчанию — сообщения в группе уходят как есть, без добавок. Здесь тоже настоящая Markdown-разметка — следи за парностью скобок/звёздочек/подчёркиваний. Осторожно с @username ботов/каналов вида @Имя_бот — подчёркивание внутри него тоже считается за разметку и может сломать всё сообщение.'
  }
];
const savedSettings = loadJson(SETTINGS_FILE, {});
let runtimeSettings = {};
for (const s of EDITABLE_SETTINGS) {
  const saved = savedSettings && savedSettings[s.key];
  const envVal = process.env[s.envVar];
  runtimeSettings[s.key] = (saved !== undefined && saved !== null && saved !== '') ? saved : (envVal || s.fallback);
}
if (!fs.existsSync(SETTINGS_FILE)) saveJson(SETTINGS_FILE, runtimeSettings);

// Поля с validateMarkdown:true (см. EDITABLE_SETTINGS) содержат НАСТОЯЩУЮ
// Markdown-разметку Telegram — в отличие от cancelDefaultText, который потом
// проходит через escMd() при отправке (см. publishCancel → notifyTelegramSubscribers),
// эти шаблоны вставляются в текст сообщения как есть, без экранирования —
// иначе ссылка [текст](url) просто не работала бы как ссылка. Значит, если
// админ допустит опечатку (забыл закрыть скобку, лишнее подчёркивание),
// Telegram откажется парсить ВСЁ сообщение целиком с ошибкой "can't parse
// entities" — и оповещение о реальной угрозе просто не дойдёт. Ловим такие
// опечатки на сохранении, а не постфактум по логам с ошибками доставки.
function validateMarkdownBalance(text) {
  // Экранированные последовательности (\_, \*, \`, \[, \]) разметку не
  // образуют — исключаем их из подсчёта, иначе легитимный экранированный
  // символ ложно засчитается как "непарный".
  const stripped = String(text).replace(/\\[_*`[\]]/g, '');
  const errors = [];
  for (const ch of ['_', '*', '`']) {
    const count = (stripped.match(new RegExp('\\' + ch, 'g')) || []).length;
    if (count % 2 !== 0) errors.push(`нечётное число символов «${ch}» — Telegram не сможет разобрать разметку`);
  }
  const openBrackets = (stripped.match(/\[/g) || []).length;
  const closeBrackets = (stripped.match(/\]/g) || []).length;
  if (openBrackets !== closeBrackets) errors.push('несовпадающее число «[» и «]»');
  return errors;
}

// Валидация на сохранении (POST /api/admin/settings) ловит НОВЫЕ опечатки,
// но не чинит то, что уже лежит в settings.json — например значение,
// сохранённое ДО того, как эта проверка вообще появилась в коде (именно так
// сюда попал шаблон с "@BgdAlert_Bot" — одиночное подчёркивание в username,
// не парная разметка, и Telegram переставал парсить ВСЁ сообщение целиком).
// Поэтому при каждом старте сервера дополнительно перепроверяем то, что уже
// загружено, и сбрасываем на безопасный дефолт всё, что не проходит
// валидацию — с явной пометкой в логах, что именно и почему сброшено.
for (const s of EDITABLE_SETTINGS) {
  if (!s.validateMarkdown) continue;
  const mdErrors = validateMarkdownBalance(runtimeSettings[s.key]);
  if (mdErrors.length) {
    addLog('error', `Настройка "${s.key}" содержала битую Markdown-разметку (${mdErrors.join('; ')}) — сброшена на значение по умолчанию при старте сервера`);
    runtimeSettings[s.key] = s.fallback;
    saveJson(SETTINGS_FILE, runtimeSettings);
  }
}

// Числовая настройка читается через эту обёртку, а не напрямую — на случай
// если в settings.json окажется мусор (например, кто-то руками поправил
// файл), парсинг всегда отдаёт валидное число, а не NaN, ломающее таймеры.
function getNumberSetting(key, fallback) {
  const raw = runtimeSettings[key];
  const num = Number(raw);
  return Number.isFinite(num) && num > 0 ? num : fallback;
}
function getSetting(key) { return runtimeSettings[key]; }

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

// Вызывается один раз при старте, ДО того как сервер начинает принимать
// запросы — подтягивает последнюю сохранённую копию каждого файла из Redis
// (если он настроен) поверх того, что успело/не успело сохраниться на
// локальном диске, и сразу же кэширует её обратно на диск.
async function hydrateFromRedis() {
  if (!REDIS_ENABLED) { redisSyncReady = true; return; }
  console.log('[Redis] восстанавливаю данные после перезапуска…');
  const targets = [
    { file: CHANNELS_FILE, key: 'bgdalert:channels', apply: (v) => { channels = v; } },
    { file: SUBS_FILE, key: 'bgdalert:subscriptions', apply: (v) => { subscriptions = v; } },
    { file: TG_SUBS_FILE, key: 'bgdalert:telegram-subs', apply: (v) => { tgSubscriptions = v; } },
    { file: TG_ADMINS_FILE, key: 'bgdalert:telegram-admins', apply: (v) => { tgAdmins = v; } },
    { file: ALARM_CONFIG_FILE, key: 'bgdalert:alarm-config', apply: (v) => { alarmConfig = v; } },
    { file: CHANNEL_REGIONS_FILE, key: 'bgdalert:channel-regions', apply: (v) => { channelRegionOverride = v; } },
    { file: STATE_FILE, key: 'bgdalert:state', apply: (v) => { state = v; } },
    { file: ANALYTICS_FILE, key: 'bgdalert:analytics', apply: (v) => { analytics = v; normalizeAnalytics(); } },
    { file: LOGS_FILE, key: 'bgdalert:logs', apply: (v) => { logs = v; } },
    { file: CUSTOM_FILTERS_FILE, key: 'bgdalert:custom-filters', apply: (v) => { customFilterWords = v; } },
    { file: SETTINGS_FILE, key: 'bgdalert:settings', apply: (v) => { runtimeSettings = Object.assign({}, runtimeSettings, v); } }
  ];
  let restored = 0;
  for (const t of targets) {
    const raw = await redisGet(t.key);
    if (raw == null) {
      // В Redis для этого ключа ещё ничего нет (например, самый первый запуск) —
      // сразу отправляем туда то, что уже есть на диске, не дожидаясь случайного
      // следующего вызова saveJson. Но если и на диске файла ещё нет (например,
      // analytics.json появляется только при первом реальном визите на сайт) —
      // loadJson вернёт null, и его в Redis отправлять не нужно: иначе при
      // следующем перезапуске этот null "восстановился" бы как настоящие
      // данные и затёр бы нормальные значения по умолчанию.
      const current = loadJson(t.file, null);
      if (current != null) redisSet(t.key, current);
      continue;
    }
    try {
      const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
      if (parsed == null) continue; // защита от случайно сохранённого null
      t.apply(parsed);
      fs.writeFileSync(t.file, JSON.stringify(parsed, null, 2), 'utf-8');
      restored++;
    } catch (err) {
      console.log(`[Redis] не удалось разобрать сохранённые данные (${t.key}):`, err.message);
    }
  }
  // Пароль администратора — восстанавливаем из Redis, только если он НЕ задан
  // явно через переменную окружения (env всегда в приоритете).
  if (!process.env.ADMIN_PASSWORD) {
    const savedPw = await redisGet('bgdalert:admin-password');
    if (savedPw) {
      ADMIN_PASSWORD = savedPw;
      saveJson(ADMIN_PASSWORD_FILE, { password: ADMIN_PASSWORD });
      console.log('[Redis] ADMIN_PASSWORD восстановлен из Redis.');
    } else {
      redisSet('bgdalert:admin-password', ADMIN_PASSWORD);
    }
  }
  if (!Array.isArray(state.deletedIds)) state.deletedIds = [];
  console.log(`[Redis] готово — восстановлено файлов: ${restored}`);
  addLog('info', `Redis: данные восстановлены после перезапуска (${restored} файлов)`);
  redisSyncReady = true;
}


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
    // editMessageText с текстом и клавиатурой, совпадающими с уже показанными —
    // например, пользователь дважды тапнул одну и ту же кнопку меню, или нажал
    // кнопку раздела, который и так уже открыт. Telegram в этом случае отвечает
    // ошибкой, хотя по сути ничего плохого не произошло — не настоящий сбой,
    // логировать и всплывать как ошибку не нужно.
    if (method === 'editMessageText' && /message is not modified/i.test(err.response?.data?.description || '')) {
      return err.response.data;
    }
    addLog('error', `Telegram API ошибка (${method}): ` + describeError(err));
    // Telegram отвечает конкретным телом с ok:false/error_code (403 — бот
    // заблокирован, 429 — превышен лимит запросов и т.п.) — раньше это тело
    // терялось и вызывающий код получал просто null, из-за чего обработка
    // 403/429 у вызывающих функций фактически никогда не срабатывала.
    // Отдаём null только на настоящий сетевой сбой (нет ответа вообще).
    return err.response ? err.response.data : null;
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
// Экранирование спецсимволов Telegram-Markdown (legacy parse_mode: 'Markdown').
// Без этого любой текст из внешнего источника — имя канала с "_", текст поста
// из канала со звёздочкой/подчёркиванием, сообщение об ошибке со скобкой —
// ломает парсинг ("Can't find end of the entity...") и Telegram просто не
// показывает/не редактирует сообщение. Экранируем везде, где в Markdown-текст
// подставляется что-то не полностью нами контролируемое.
function escMd(s) {
  return String(s == null ? '' : s).replace(/([_*`\[])/g, '\\$1');
}

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
  lines.push(`   Каналы: ${channels.map(escMd).join(', ')}`);
  lines.push('');
  lines.push(`⚙️ Тревога включена: ${alarmConfig.enabled ? 'да' : 'нет'}, типы: ${alarmConfig.types.join(', ')}`);
  if (recentErrors.length) {
    lines.push('');
    lines.push('🔴 Последние ошибки в логах:');
    recentErrors.forEach((l) => lines.push(`  • ${new Date(l.t).toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow' })} — ${escMd(l.message)}`));
  }
  return lines.join('\n');
}

// Главное меню теперь — постоянная клавиатура под полем ввода (Reply Keyboard),
// а не inline-кнопки внутри одного редактируемого сообщения. Она не привязана
// к конкретному сообщению и не может "устареть" или потеряться при прокрутке
// чата — Telegram держит её прикреплённой к чату, пока не пришлют remove_keyboard.
// Разделы с точечными действиями (источники, логи, лента, настройки) по-прежнему
// используют inline-кнопки — там нужен выбор конкретного пункта (удалить канал,
// фильтр логов и т.п.), просто открываются они теперь новым сообщением по тапу
// на постоянную кнопку, а не редактированием одного "главного" сообщения.
const MENU_ALERT = '⚡ Быстрая тревога';
const MENU_STATS = '📊 Статистика';
const MENU_LOGS = '🧾 Логи';
const MENU_CHANNELS = '📡 Каналы-источники';
const MENU_ALARMCFG = '⚙️ Настройки тревоги';
const MENU_FEED = '🗂 Текущая лента';
const MENU_LOGOUT = '🚪 Выйти из админки';

function mainReplyKeyboard() {
  return {
    keyboard: [
      [MENU_ALERT],
      [MENU_STATS, MENU_LOGS],
      [MENU_CHANNELS, MENU_ALARMCFG],
      [MENU_FEED],
      [MENU_LOGOUT]
    ],
    resize_keyboard: true,
    is_persistent: true
  };
}

// Нажатие на кнопку постоянной клавиатуры приходит боту как обычное текстовое
// сообщение с текстом кнопки — в отличие от инлайн-кнопок, тут нет "исходного"
// сообщения, которое можно отредактировать, поэтому для каждого раздела
// отправляем НОВОЕ сообщение с соответствующим текстом и инлайн-клавиатурой
// для точечных действий внутри раздела (обновить, отфильтровать, удалить и т.п.).
// Сама постоянная клавиатура при этом никуда не девается — Telegram держит её
// прикреплённой к чату независимо от того, что происходит в сообщениях выше.
async function sendAdminSection(chatId, label) {
  if (label === MENU_ALERT) {
    await tgCall('sendMessage', { chat_id: chatId, text: '⚡ Что отправить?', reply_markup: alertTypeKeyboard() });
  } else if (label === MENU_STATS) {
    await tgCall('sendMessage', {
      chat_id: chatId, text: buildAdminStatsText(), parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[{ text: '↻ Обновить', callback_data: 'tga:menu:stats' }]] }
    });
  } else if (label === MENU_LOGS) {
    await tgCall('sendMessage', { chat_id: chatId, text: buildLogsText(''), parse_mode: 'Markdown', reply_markup: logsKeyboard('') });
  } else if (label === MENU_CHANNELS) {
    await tgCall('sendMessage', { chat_id: chatId, text: buildChannelsText(), parse_mode: 'Markdown', reply_markup: channelsKeyboard() });
  } else if (label === MENU_ALARMCFG) {
    await tgCall('sendMessage', {
      chat_id: chatId, text: '⚙️ *Настройки тревоги*\n\nЧто из этого триггерит громкий звук/вибрацию у пользователей на сайте:',
      parse_mode: 'Markdown', reply_markup: alarmCfgKeyboard()
    });
  } else if (label === MENU_FEED) {
    await tgCall('sendMessage', { chat_id: chatId, text: buildFeedText(), parse_mode: 'Markdown', reply_markup: feedKeyboard(chatId) });
  } else if (label === MENU_LOGOUT) {
    tgAdmins = tgAdmins.filter((id) => id !== chatId);
    saveTgAdmins();
    addLog('info', `Telegram-админ ${chatId} вышел из режима администратора`);
    await tgCall('sendMessage', {
      chat_id: chatId,
      text: '👋 Вы вышли из режима администратора. Чтобы вернуться, попроси администратора сайта выдать доступ заново.',
      reply_markup: { remove_keyboard: true }
    });
  }
}

function logsKeyboard(filter) {
  const mark = (f) => (filter === f ? '• ' : '');
  return {
    inline_keyboard: [
      [{ text: mark('') + 'Все', callback_data: 'tga:logs:' }, { text: mark('error') + 'Ошибки', callback_data: 'tga:logs:error' }],
      [{ text: mark('warn') + 'Предупреждения', callback_data: 'tga:logs:warn' }, { text: mark('info') + 'Инфо', callback_data: 'tga:logs:info' }],
      [{ text: '↻ Обновить', callback_data: 'tga:logs:' + filter }]
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
    lines.push(`\n${LOG_LABEL[l.level] || l.level} · ${time}\n${escMd(l.message)}`);
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
  return { inline_keyboard: rows };
}

function buildChannelsText() {
  const lines = ['📡 *Каналы-источники*', ''];
  channels.forEach((c) => {
    const health = channelHealth[c];
    const status = health && health.lastError ? `⚠️ ошибка: ${escMd(health.lastError)}` : '✅ ок';
    lines.push(`@${escMd(c)} — ${status}`);
  });
  return lines.join('\n');
}

function alarmCfgKeyboard() {
  const on = (v) => (v ? '✅' : '⬜️');
  return {
    inline_keyboard: [
      [{ text: `${on(alarmConfig.enabled)} Тревога включена`, callback_data: 'tga:cfg:enabled' }],
      [{ text: `${on(alarmConfig.types.includes('rocket'))} 🚀 Ракетная опасность`, callback_data: 'tga:cfg:type:rocket' }],
      [{ text: `${on(alarmConfig.types.includes('drone'))} 🛸 БПЛА`, callback_data: 'tga:cfg:type:drone' }]
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
  if (!rows.length) rows.push([{ text: '↻ Обновить', callback_data: 'tga:menu:feed' }]);
  return { inline_keyboard: rows };
}

function buildFeedText() {
  const items = (state.feed || []).slice(0, 8);
  if (!items.length) return '🗂 *Текущая лента*\n\nПусто.';
  const lines = ['🗂 *Текущая лента* (последние 8)'];
  items.forEach((it) => {
    const regionLabel = it.region === 'all' ? 'вся область' : (REGION_NAMES[it.region] || it.region);
    lines.push(`\n${it.i} *${it.tag}* — ${regionLabel}${it.manual ? ' ✍️' : ''}\n${it.time} · ${it.date}\n${escMd((it.txt || '').slice(0, 150))}`);
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
        const from = cq.from || {};
        const entry = {
          chatId,
          regions: [region],
          joinedAt: idx >= 0 ? tgSubscriptions[idx].joinedAt : Date.now(),
          username: from.username || null,
          firstName: from.first_name || null
        };
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
      if (data.startsWith('tga:')) {
        if (!isTgAdmin(chatId)) {
          await tgCall('answerCallbackQuery', { callback_query_id: cq.id, text: 'Доступ только для админов. Доступ выдаёт администратор сайта.', show_alert: true });
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
          if (section === 'alert') {
            await edit('⚡ Что отправить?', alertTypeKeyboard());
          } else if (section === 'stats') {
            await edit(buildAdminStatsText(), { inline_keyboard: [[{ text: '↻ Обновить', callback_data: 'tga:menu:stats' }]] });
          } else if (section === 'logs') {
            await edit(buildLogsText(''), logsKeyboard(''));
          } else if (section === 'channels') {
            await edit(buildChannelsText(), channelsKeyboard());
          } else if (section === 'alarmcfg') {
            await edit('⚙️ *Настройки тревоги*\n\nЧто из этого триггерит громкий звук/вибрацию у пользователей на сайте:', alarmCfgKeyboard());
          } else if (section === 'feed') {
            await edit(buildFeedText(), feedKeyboard(chatId));
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
            await edit(`✅ Отправлено в ленту и всем подписчикам:\n\n${item.i} *${item.tag}* — ${regionLabel}\n${escMd(item.txt)}`);
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
            // Та же причина, по которой удаление "не работало": без пометки
            // в deletedIds pollOnce возвращал запись обратно на следующем цикле.
            if (!Array.isArray(state.deletedIds)) state.deletedIds = [];
            if (!state.deletedIds.includes(String(id))) state.deletedIds.push(String(id));
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
      if (channels.some((c) => c.toLowerCase() === parsed.toLowerCase())) {
        await tgCall('sendMessage', { chat_id: chatId, text: `Канал @${escMd(parsed)} уже добавлен.`, parse_mode: 'Markdown', reply_markup: channelsKeyboard() });
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
      const from = msg.from || {};
      const subEntry = {
        chatId,
        regions: idx >= 0 ? tgSubscriptions[idx].regions : ['all'],
        joinedAt: idx >= 0 ? tgSubscriptions[idx].joinedAt : Date.now(),
        username: from.username || null,
        firstName: from.first_name || null
      };
      if (idx >= 0) tgSubscriptions[idx] = subEntry; else tgSubscriptions.push(subEntry);
      saveTgSubs();
      if (idx < 0) addLog('info', 'Telegram: новый подписчик ' + chatId);
      if (isTgAdmin(chatId)) {
        await tgCall('sendMessage', { chat_id: chatId, text: '🛠 *Админ-меню*\n\nВыбери раздел на клавиатуре внизу.', parse_mode: 'Markdown', reply_markup: mainReplyKeyboard() });
      } else {
        await tgCall('sendMessage', {
          chat_id: chatId,
          text: '🚨 *Тревога Белгород* — оповещения о ракетной опасности и БПЛА.\n\nПо умолчанию включена вся область. Выбери свой район, если нужны только его оповещения:',
          parse_mode: 'Markdown',
          reply_markup: regionKeyboard('tgregion')
        });
      }
    } else if (text === '/region') {
      await tgCall('sendMessage', { chat_id: chatId, text: 'Выбери район:', reply_markup: regionKeyboard('tgregion') });
    } else if (text === '/stop') {
      tgSubscriptions = tgSubscriptions.filter((s) => s.chatId !== chatId);
      saveTgSubs();
      await tgCall('sendMessage', { chat_id: chatId, text: '🔕 Подписка отключена. Вернуться можно командой /start.' });
      addLog('info', 'Telegram: отписка ' + chatId);
    } else if (text.startsWith('/admin')) {
      // Пароль внутри бота больше не работает — доступ администратора теперь
      // выдаёт администратор сайта из вкладки «Админы Telegram» в веб-панели,
      // по username или chat ID. Подсказываем chat ID на случай, если у
      // пользователя нет @username, по которому его можно найти в списке.
      await tgCall('sendMessage', {
        chat_id: chatId,
        text: `Доступ администратора теперь выдаётся с сайта (вкладка «Админы Telegram»), а не паролем в боте.\n\nТвой chat ID: \`${chatId}\`${msg.from && msg.from.username ? ` (username: @${escMd(msg.from.username)})` : ''} — передай его администратору сайта, чтобы получить доступ.`,
        parse_mode: 'Markdown'
      });
    } else if (text === '/menu' || text === '/stats' || text === '/alert') {
      if (!isTgAdmin(chatId)) { await tgCall('sendMessage', { chat_id: chatId, text: 'Доступ только для админов. Доступ выдаёт администратор сайта — набери /admin, чтобы узнать свой chat ID.' }); return; }
      await tgCall('sendMessage', { chat_id: chatId, text: '🛠 *Админ-меню*\n\nВыбери раздел на клавиатуре внизу.', parse_mode: 'Markdown', reply_markup: mainReplyKeyboard() });
    } else if (text === '/adminlogout') {
      tgAdmins = tgAdmins.filter((id) => id !== chatId);
      saveTgAdmins();
      await tgCall('sendMessage', { chat_id: chatId, text: '👋 Вышли из режима администратора. Чтобы вернуться, попроси администратора сайта выдать доступ заново.', reply_markup: { remove_keyboard: true } });
    } else if (isTgAdmin(chatId) && [MENU_ALERT, MENU_STATS, MENU_LOGS, MENU_CHANNELS, MENU_ALARMCFG, MENU_FEED, MENU_LOGOUT].includes(text)) {
      // Нажатие на кнопку постоянной клавиатуры — Telegram присылает его как
      // обычное текстовое сообщение с текстом кнопки.
      await sendAdminSection(chatId, text);
    } else {
      const base = 'Команды: /start — подписаться, /region — выбрать район, /stop — отписаться.';
      const adminHint = isTgAdmin(chatId) ? '\n\nАдмин: /menu — открыть меню управления.' : '\n\nДоступ администратора выдаётся с сайта. /admin — узнать свой chat ID для этого.';
      await tgCall('sendMessage', { chat_id: chatId, text: base + adminHint });
    }
  } catch (err) {
    addLog('error', 'Ошибка обработки Telegram-апдейта: ' + err.message, { stack: err.stack });
  }
}

// Long polling — не требует публичного HTTPS-вебхука и лишней настройки,
// работает "из коробки" сразу после того как задан TELEGRAM_BOT_TOKEN.
let tgOffset = 0;
let tgPollFailStreak = 0;
async function tgPollLoop() {
  if (!TG_API) return;
  let ok = false;
  try {
    const data = await tgCall('getUpdates', { offset: tgOffset, timeout: 25 }, 30000);
    if (data && data.ok && Array.isArray(data.result)) {
      ok = true;
      for (const update of data.result) {
        tgOffset = update.update_id + 1;
        await tgHandleUpdate(update);
      }
    }
  } catch (err) {
    addLog('error', 'Ошибка Telegram long polling: ' + err.message);
  }
  if (ok) {
    tgPollFailStreak = 0;
  } else {
    // ВАЖНО: tgCall() сам ловит все свои ошибки и никогда не пробрасывает их
    // дальше (см. реализацию выше) — она возвращает {ok:false,...} или null,
    // а не throw. Раньше пауза стояла ТОЛЬКО в catch-блоке, который из-за
    // этого никогда не срабатывал: при любом сбое getUpdates (сетевом, 502 от
    // Telegram и т.п.) цикл долбил API следующей попыткой без единой паузы.
    // Теперь пауза применяется по факту неуспеха запроса, а не по исключению,
    // и растёт при подряд идущих сбоях (3с → 6с → ... → максимум 30с) — если
    // у Telegram/сети затяжной сбой, нет смысла стучаться каждые 3 секунды.
    tgPollFailStreak++;
    const backoffMs = Math.min(3000 * tgPollFailStreak, 30000);
    await new Promise((r) => setTimeout(r, backoffMs));
  }
  setImmediate(tgPollLoop);
}
if (TG_API) {
  tgPollLoop();
  // Список команд в системном меню Telegram (кнопка "☰" рядом с полем ввода) —
  // пользователю не нужно печатать /admin и другие команды руками, можно
  // просто открыть меню и тапнуть нужный пункт.
  tgCall('setMyCommands', {
    commands: [
      { command: 'start', description: 'Подписаться на оповещения' },
      { command: 'region', description: 'Сменить свой район' },
      { command: 'stop', description: 'Отписаться от оповещений' },
      { command: 'admin', description: 'Войти как администратор' },
      { command: 'menu', description: 'Админ-панель (после входа)' },
      { command: 'adminlogout', description: 'Выйти из режима администратора' }
    ]
  });
  addLog('info', 'Telegram-бот запущен (long polling)');
} else {
  console.log('[i] TELEGRAM_BOT_TOKEN не задан — Telegram-канал оповещений отключён (сайт и push работают как обычно).');
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Telegram жёстко режет частую отправку СООБЩЕНИЙ В ОДИН И ТОТ ЖЕ чат —
// в отличие от рассылки подписчикам (там каждое сообщение уходит в РАЗНЫЙ
// chat_id, и общий лимит бота ~30/сек с паузой 40мс между подписчиками
// этого достаточно), группа @BgdAlert — один и тот же получатель на каждый
// новый пункт ленты. Если в одном цикле опроса приходит пачка сообщений
// разом (см. "Новых сообщений в ленте: N" в логах), группа получала бы
// по одному сообщению почти без паузы — именно это разгоняет 429 с растущим
// retry_after (Telegram увеличивает штраф за повторные нарушения). Поэтому
// перед каждой отправкой в группу выдерживаем минимальный интервал от
// предыдущей — не важно, из какого вызова notifyTelegramSubscribers он был.
let lastGroupSendAt = 0;

// Единая отправка текстового сообщения с Markdown-разметкой + аварийный
// откат: если Telegram всё-таки не смог распарсить разметку ("can't parse
// entities" — например опечатка в CTA-шаблоне, отредактированном через
// админку, не отловленная валидацией на сохранении, или админ поправил
// settings.json руками мимо валидации), сообщение не теряется — уходит ещё
// раз уже БЕЗ разметки, обычным текстом. Содержание оповещения важнее
// жирного/курсива, а раньше такие сообщения просто не доходили вовсе.
async function sendTelegramTextSafe(chatId, text) {
  let result = await tgCall('sendMessage', { chat_id: chatId, text, parse_mode: 'Markdown' });
  if (result && result.ok === false && result.error_code === 429) {
    const retryAfterSec = (result.parameters && result.parameters.retry_after) || 2;
    await sleep(retryAfterSec * 1000);
    result = await tgCall('sendMessage', { chat_id: chatId, text, parse_mode: 'Markdown' });
  }
  if (result && result.ok === false && result.error_code === 400 && /parse entities/i.test(result.description || '')) {
    result = await tgCall('sendMessage', { chat_id: chatId, text }); // без parse_mode — гарантированно доставится
  }
  return result;
}

async function notifyTelegramSubscribers(item) {
  if (!TG_API) return;
  const isUrgent = isAlarmTriggering(item);
  const regionLabel = item.region === 'all' ? 'по всей области' : (REGION_NAMES[item.region] || item.region);
  const title = isUrgent ? `🚨 *ТРЕВОГА* · ${regionLabel}` : `${item.i} *${item.tag}*`;
  const text = `${title}\n\n${escMd(item.txt)}\n\n_${item.time} · ${item.date}_`;
  // Призыв подписаться на канал — только для ЛИЧНЫХ сообщений подписчикам,
  // не для группы (её участники и так уже в канале/группе, повторять там
  // нечего). Показывается тем, кто узнаёт об угрозе через бота лично, но
  // ещё не в канале — то есть именно тем, кого имеет смысл заманивать.
  // Текст шаблона редактируется из админки (promoCtaTemplate), {url} —
  // плейсхолдер, подставляется значение promoChannelUrl (тоже редактируемое).
  const ctaTemplate = getSetting('promoCtaTemplate') || '';
  const cta = ctaTemplate.replace('{url}', getSetting('promoChannelUrl') || '');
  const subscriberText = cta ? `${text}\n\n${cta}` : text;

  // Та же самая рассылка, что уходит подписчикам в личку, дублируется в
  // группу (см. EDITABLE_SETTINGS выше, ключ telegramGroupChat) — те же
  // 429/сетевые предосторожности, что и для личных подписчиков, но без
  // удаления "подписки" при ошибке: группа не подписка, отсутствие прав у
  // бота там не повод что-то стирать — просто логируем и пробуем в следующий раз.
  const groupChat = getSetting('telegramGroupChat');
  if (groupChat) {
    // Тот же принцип, что и для личных сообщений подписчикам (см. cta/subscriberText
    // выше), но отдельный шаблон и по умолчанию пустой — сообщения в группе
    // раньше уходили как есть, без добавок, и это поведение не должно
    // поменяться само по себе для тех, кто это поле не заполнял.
    const groupCtaTemplate = getSetting('groupCtaTemplate') || '';
    const groupCta = groupCtaTemplate.replace('{url}', getSetting('promoChannelUrl') || '');
    const groupText = groupCta ? `${text}\n\n${groupCta}` : text;
    const waitMs = getNumberSetting('groupIntervalMs', 1500) - (Date.now() - lastGroupSendAt);
    if (waitMs > 0) await sleep(waitMs);
    lastGroupSendAt = Date.now();
    const groupResult = await sendTelegramTextSafe(groupChat, groupText);
    if (groupResult && groupResult.ok === false) {
      addLog('error', `Не удалось отправить в группу ${groupChat}: ${groupResult.description || 'неизвестная ошибка'}`);
    }
  }

  if (!tgSubscriptions.length) return;
  const stillValid = [];
  for (const entry of tgSubscriptions) {
    const matches = item.region === 'all' || (entry.regions && (entry.regions.includes('all') || entry.regions.includes(item.region)));
    if (!matches) { stillValid.push(entry); continue; }
    let result = await sendTelegramTextSafe(entry.chatId, subscriberText);
    // Код 403 = пользователь заблокировал бота — удаляем такого подписчика,
    // как это уже делается для "умерших" web push подписок (404/410).
    if (result === null) { /* сетевая/временная ошибка — не удаляем, оставляем как есть */ stillValid.push(entry); }
    else if (result.ok === false && result.error_code === 403) { addLog('info', `Telegram: подписчик ${entry.chatId} заблокировал бота, удалён из списка`); }
    else stillValid.push(entry);
    // Небольшая пауза между отправками — Telegram ограничивает примерно
    // 30 сообщений в секунду на бота в целом, при рассылке многим подписчикам
    // подряд без пауз это легко превысить и словить 429 у части из них.
    await sleep(40);
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
  /реклам/i, /promo/i, /подпис\S+ на канал/i, /erid/i, /18\+.*реклама/i,
  /по вопросам сотрудничества/i, /скидк\w+\s*\d/i, /промокод\w*/i,
  /партнёрск\w*\s*(материал|пост|публикаци)/i, /спонсор(ск\w*)?\s*(пост|материал)/i,
  // Сборы на нужды/оборудование, донаты, реквизиты для перевода — не реклама
  // в привычном смысле, но и не оповещение об угрозе: такие посты каналы
  // публикуют вперемешку с настоящими тревогами, и без фильтра они попадали
  // в ленту как обычная новость (см. пример: «Просим не быть равнодушными
  // и проявить активность в сборе на оборудование. Каждые 50-100-200-500
  // рублей...»).
  /сбор\S*\s+(средств\S*\s+)?на\s+(оборудование|дрон\w*|технику|снаряжени\w*|экипировк\w*)/i,
  /не\s+быть\s+равнодушны/i,
  /\d+[\-–—]\d+[\-–—]\d+([\-–—]\d+)?\s*рублей/i,
  /позволя\w+\s+приобрест/i,
  /реквизит\S*\s+(для\s+)?(перевода|сбора|поддержки)/i,
  /поддержа\S+\s+(наш\s+)?сбор/i,
  // Вирусный спам-форвард "найдите канал своего города": типовой цепной
  // форвард с перечислением каналов ДРУГИХ городов (Москва, Питер, Абакан...)
  // без единой конкретики по Белгороду — сеет панику ради накрутки чужих
  // каналов. Раньше слово "атаки" поднимало его до срочной "ТРЕВОГА · по всей
  // области", хотя по сути это не информация, а спам.
  /продолжа\S*\s+массивн\S*\s+атак\S*\s+по\s+всем\s+регион\S*/i,
  /во\s+многих\s+област\S*\s+введ\S*\s+режим\s+чрезвычайн\w*/i,
  /убедительн\S*\s+просьб\S*,?\s*найти\s+свой\s+город/i,
  /каналы\s+будут\s+работать\s+даже\s+без\s+мобильн\S*\s+связи/i,
  // Личные наезды/оскорбления в адрес видеооператоров, журналистов и т.п.
  // ("горе-операторам", "вы с головой дружите", "работаете на врага") — не
  // информация об угрозе, а чья-то ругань, никакой пользы читателю.
  /горе[\s-]?операторам/i,
  /вы\s+с\s+головой\s+дружите/i,
  /работаете\s+получается\s+на\s+врага/i
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

// Продвижение мессенджера MAX ("мы теперь и в MAX", "подписывайтесь в Мах: <ссылка>" —
// каналы часто приклеивают это отдельной строкой прямо под настоящим оповещением).
// Ищем слово целиком (max / мах / макс), а не подстроку — иначе задело бы обычные
// русские слова с этим корнем ("взмах", "размах" и т.п.).
const MAX_WORD_RE = /(^|[^a-zа-яё0-9])(max|мах|макс)([^a-zа-яё0-9]|$)/i;
function lineMentionsMax(line) { return MAX_WORD_RE.test(line); }

function stripLinks(text) {
  return text
    .split('\n')
    .map((line) => line.replace(URL_RE, '').trim())
    .map((line) => line.replace(TRAILING_CTA_RE, '').trim())
    .filter((line) => line.length > 0 && !LINK_STUB_RE.test(line) && !lineMentionsMax(line) && /[a-zA-Zа-яА-ЯёЁ]/.test(line))
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// Рекламные вставки внутри иначе нормального сообщения, которые НЕ содержат
// ссылку (иначе их поймал бы stripLinks) — например отдельная строка вида
// «Скидка 20% в нашем магазине, промокод БПЛА20» под настоящим оповещением,
// или служебные пометки #реклама/erid без остального текста рекламы.
// Вырезаем построчно, само оповещение остаётся.
const AD_LINE_RE = /(^|[\s#])(реклама|ad|erid:\S+)(\s|$)/i;
const PROMO_LINE_RE = /(скидк\S*\s*\d|промокод\S*|только\s+сегодня\s+акци|успей\S*\s+(купить|заказать|оформить)|закажи(те)?\s+(сейчас|сегодня)|партнёрск\S*\s*(материал|пост|публикаци)|спонсор(ск\S*)?\s*(пост|материал)|наш\s+магазин|переходи(те)?\s+в\s+наш\s+чат)/i;

function stripAdLines(text) {
  return text
    .split('\n')
    .filter((line) => !AD_LINE_RE.test(line) && !PROMO_LINE_RE.test(line))
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// ===== Стоп-слова, настраиваемые из админки =====
// В отличие от AD_PATTERNS (который отбрасывает подозрительное сообщение
// целиком) — это точечное вырезание конкретных слов/фраз из ИНАЧЕ нормального
// оповещения: «сообщение парсится, но не всё». Например, если канал вставляет
// название рекламируемого товара прямо в текст оповещения, админ добавляет
// это слово сюда — само оповещение остаётся, конкретное слово вырезается.
const CUSTOM_FILTERS_FILE = path.join(DATA_DIR, 'custom-filters.json');
let customFilterWords = loadJson(CUSTOM_FILTERS_FILE, []); // ['слово или фраза', ...]
function saveCustomFilterWords() { saveJson(CUSTOM_FILTERS_FILE, customFilterWords); }

function stripCustomWords(text) {
  if (!customFilterWords.length) return text;
  return text
    .split('\n')
    .map((line) => {
      let out = line;
      customFilterWords.forEach((w) => {
        if (!w) return;
        const escaped = w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        out = out.replace(new RegExp(escaped, 'gi'), '');
      });
      return out.replace(/[ \t]{2,}/g, ' ').trim();
    })
    .filter((line) => line.length > 0 && /[a-zA-Zа-яА-ЯёЁ]/.test(line))
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
const DIRECT_THREAT_RE = /обнаружен|зафиксирован|курс\S*\s+на|направля(ется|ются)|над\s|заход[ит]*|атак|сбит|поражен|подлета|приближа/i;

// Новостные посты/сводки-пересказы («вчера был объявлен РО», «итоги суток»,
// «статистика за неделю», «оперштаб сообщил») используют ТЕ ЖЕ ключевые слова,
// что и настоящие живые оповещения («ракетная опасность», «обнаружен БПЛА»),
// поэтому раньше могли случайно попадать в ленту как будто это актуальная
// тревога прямо сейчас. Проверяем это ПЕРВЫМ делом, до всех остальных правил —
// если пост говорит о прошлом/итогах, это новость, а не живой сигнал.
const NEWS_RECAP_RE = /(за (истекш|прошедш|отчётн)\S* сутк\w*|за (эту |прошлую |текущую )?недел\w*|итоги (дня|недели|суток|месяца)|сводка|статистик\S+ (за|по)|(^|[^a-zа-яё])вчера([^a-zа-яё]|$)|(^|[^a-zа-яё])позавчера([^a-zа-яё]|$)|на прошлой недел\w*|за (минувш\S* )?сутки было|всего за (сутки|неделю|день)|подвед[ея]м итоги|оперштаб (сообщ|информ)\w*|глава региона (рассказал|сообщил|отчитался))/i;

// Байки/анекдоты про уже случившийся и благополучно закончившийся инцидент —
// пишутся в повествовательном, часто ироничном тоне ("хозяевам повезло",
// "упал без детонации", "никто не пострадал") и не требуют никаких действий
// прямо сейчас, в отличие от настоящего оповещения ("летит БПЛА", "в укрытие").
// Внешне похоже на тревогу (есть слово "БПЛА"/"дрон"), но по сути — новость.
const RESOLVED_STORY_RE = /(жутко повезло|повезло[^.!?\n]{0,20}(упал|не сработал|не деton)|обошлось без|без детонаци\w*|никто не пострадал|к счастью[^.!?\n]{0,20}(никто|обошлось)|запутал\S* в (одежде|белье|проводах|ветвях)|самый важный объект)/i;

// Эмоциональные обращения/воззвания к жителям («Дорогие белгородцы...», с оскорблениями
// в адрес противника, общими советами вроде «не берите с собой детей») — используют те
// же ключевые слова, что настоящие тревоги (БПЛА, «атакует»), но это агитационный или
// эмоциональный пост, а не сигнал «прямо сейчас» с конкретным местом/действием. Поднимать
// по такому тревогу (звук/вибрация/красный статус всем подписчикам) не нужно.
const APPEAL_RANT_RE = /(дорогие белгородц\w*|дорогие жител\w*|гнид[ыа]|свинь\w*[^.!?\n]{0,20}не важно|трус\S+ вою\S*|не подвергайте себя опасност)/i;

// Отчёты о последствиях уже случившейся атаки (кто-то пострадал, скорая везёт
// в больницу, повреждено оборудование) — важная информация, но НЕ сигнал
// «сейчас прилетит, займите укрытие»: событие уже произошло, действовать
// прямо сейчас читателю не нужно. Раньше срабатывала полная тревога (звук,
// вибрация, красный статус) просто из-за слова «беспилотник»/«атак».
// Раньше требовалось, чтобы существительное шло СРАЗУ после "пострадал"
// ("пострадал мужчина") — но реальные посты часто вставляют числительное
// между ними ("Пострадали ТРОЕ детей"), и множественное "дети/детей" не
// было в списке вовсе (было только "ребен*"/"ребён*" в единственном числе).
const CASUALTY_NEWS_RE = /(пострадал\w*[^.!?\n]{0,25}в результате атак|пострадал\w*[^.!?\n]{0,15}(мужчина|женщина|человек\w*|подросток|ребен\w*|ребён\w*|дет(ей|и))|получил\S* осколочн\S* ранени|доставля\w+[^.!?\n]{0,20}в (област\w*|город\w*|районн\w*)?\s*(клиническ\w*)?\s*больниц|бригада скорой|доставили в[^.!?\n]{0,25}(црб|больниц)|на месте атаки повреждены|мирн\S+ жител\S+ пострадал)/i;

// Скорбно-повествовательные посты о УЖЕ СЛУЧИВШЕЙСЯ трагедии ("Страшная
// трагедия в Валуйском округе... Вся область скорбит... Добрый вечер,
// дорогие земляки... этот вечер снова принёс ужасную весть...") — тон
// мемориальный/ретроспективный, а не "действуй прямо сейчас": к моменту
// публикации всё уже произошло. Раньше слово "беспилотником" в таком посте
// само по себе поднимало срочную "🚨 ТРЕВОГА" на всю область.
const TRAGEDY_NARRATIVE_RE = /(страшная трагеди\w*|вся область скорбит|дорогие земляк\w*|невосполнимо\w* го[рб]|принес\S*[^.!?\n]{0,15}(ужасн\S*|больн\S*|скорбн\S*)\s+весть)/i;

// Ежедневная сводка Минобороны РФ о сбитых БПЛА/ракетах по ВСЕЙ России
// ("Средства ПВО за день сбили 245 БПЛА над регионами РФ, а также над
// акваторией Черного моря, — Минобороны РФ") — агрегированная статистика
// за сутки по всей стране, а не сигнал о конкретной угрозе прямо сейчас в
// Белгородской области. Слово «сбили» само по себе совпадало с DIRECT_THREAT_RE
// (используется для распознавания настоящих угроз, см. ниже) и превращало
// такую сводку в срочную "🚨 ТРЕВОГА · по всей области" с сиреной и звуком —
// хотя к Белгороду конкретно она может вообще не иметь отношения.
const MOD_DAILY_DIGEST_RE = /(—\s*минобороны\s+рф|минобороны\s+рф\.?\s*$|над\s+регион\S*\s+рф|за\s+(день|ночь|сутки)\s+сби\S*\s+\d+)/i;

// Новости про суд/штрафы за МОШЕННИЧЕСТВО с инсценировкой атак — например
// «Крупные штрафы — за инсценировку последствий атаки со стороны ВСУ и
// попытку получить компенсации. Летом 2025 года двое жителей... специально
// повредили... и обратились... чтобы компенсировать ущерб от ЯКОБЫ удара
// вражеского беспилотника». Формально текст содержит "атаки"/"беспилотника"
// и раньше матчил DIRECT_THREAT_RE как настоящую угрозу — а по сути это
// ретроспективная (прошедшее время, конкретные даты) судебная новость о
// том, что удара вообще НЕ было, его подделали ради компенсации.
const FRAUD_STAGED_RE = /(инсценировк\S*|инсценирова\S*|специально\s+повредил\S*|обманным\s+путём|якобы\s+удар\S*|мошенничеств\S*[^.!?\n]{0,25}(атак|беспилотник|бпла|дрон))/i;

function classify(text) {
  const lower = text.toLowerCase();
  if (NEWS_RECAP_RE.test(lower)) return { t: 'other', i: '📰', tag: 'Новостная сводка' };
  if (RESOLVED_STORY_RE.test(lower)) return { t: 'other', i: '📰', tag: 'История без действия' };
  if (APPEAL_RANT_RE.test(lower)) return { t: 'other', i: '📰', tag: 'Обращение' };
  if (CASUALTY_NEWS_RE.test(lower)) return { t: 'other', i: '📰', tag: 'Сводка о последствиях' };
  if (TRAGEDY_NARRATIVE_RE.test(lower)) return { t: 'other', i: '📰', tag: 'Сводка о последствиях' };
  if (MOD_DAILY_DIGEST_RE.test(lower)) return { t: 'other', i: '📰', tag: 'Сводка Минобороны' };
  if (FRAUD_STAGED_RE.test(lower)) return { t: 'other', i: '📰', tag: 'Судебная новость' };

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
// fetchChannelMessages() перенесена в отдельный файл channel-fetcher.js —
// см. подробное объяснение почему там (параллельный опрос вместо
// последовательного, без задержки для быстрых каналов из-за медленных).

// Настоящие тревоги (не новости) держим короткими — их читают за секунду,
// принимая решение «в укрытие или нет». Новостные посты (сводки, истории,
// обращения — всё, что classify() относит к типу 'other') показываем целиком:
// это уже не сигнал действовать прямо сейчас, а материал для чтения, и
// обрезка на середине предложения там только мешает. NEWS_SAFETY_CAP —
// просто защита от аномально гигантского поста (не обычный случай), а не
// обычная граница отображения.
const NEWS_SAFETY_CAP = 4000;

function formatFeedItem(msg) {
  const cleanText = stripAdLines(stripCustomWords(stripLinks(msg.text)));
  const cls = classify(cleanText);
  const isNews = cls.t === 'other';
  const region = channelRegionOverride[msg.channel] || detectRegion(cleanText);
  const hasRealTime = !!msg.datetime;
  const dt = hasRealTime ? new Date(msg.datetime) : new Date();
  const time = dt.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' });
  const date = dt.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', timeZone: 'Europe/Moscow' });
  const limit = isNews ? NEWS_SAFETY_CAP : getNumberSetting('alertTextLimit', 400);
  return {
    id: msg.id,
    t: cls.t, i: cls.i, tag: cls.tag,
    txt: cleanText.length > limit ? cleanText.slice(0, limit) + '…' : cleanText,
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
// Короткое повторное сообщение ("❗ РАКЕТНАЯ ОПАСНОСТЬ в Шебекинском МО!") почти
// целиком состоит из слов, уже присутствующих в более подробном сообщении об
// этом же событии, отправленном минутой раньше ("...МО 12:49. Спуститесь в
// подвальное..."). Jaccard в такой паре часто чуть-чуть не дотягивает до порога
// (у длинного сообщения много своих доп. слов, раздувающих объединение) — берём
// ещё и коэффициент вложенности: если почти все слова короткого сообщения есть
// в длинном, это тот же сигнал, просто пересказанный короче.
function containmentRatio(a, b) {
  const minSize = Math.min(a.size, b.size);
  if (minSize === 0) return 0;
  let inter = 0;
  a.forEach((w) => { if (b.has(w)) inter++; });
  return inter / minSize;
}

const DEDUPE_SIMILARITY = 0.55;
const DEDUPE_CONTAINMENT = 0.8;
// Доп. защита: если два сообщения пришли почти одновременно (в пределах
// минуты) и текстуально почти совпадают, считаем их дублем ДАЖЕ если
// классификатор ошибочно присвоил им разный тип/район (например, одно и то
// же событие один канал прислал как «уведомление», другой — как «БПЛА»,
// а третий — с районом, не совпадающим с автоопределённым). Порог схожести
// тут строже, чем для «основного» дедупа выше, именно потому что тип/район
// не обязаны совпадать — не хотим случайно съесть два РАЗНЫХ реальных сигнала.
const SHORT_DEDUPE_WINDOW_MS = 60 * 1000;
const SHORT_DEDUPE_SIMILARITY = 0.75;
const SHORT_DEDUPE_CONTAINMENT = 0.9;

function dedupeItems(items) {
  const dedupeWindowMs = getNumberSetting('dedupeWindowMin', 15) * 60 * 1000;
  const sorted = items.slice().sort((a, b) => a.ts - b.ts); // старые первыми — канонический экземпляр стабилен между опросами
  const result = [];
  for (const it of sorted) {
    const itWords = wordSet(it.txt);
    let dup = null;
    for (const r of result) {
      const dt = Math.abs(r.ts - it.ts);
      // "Отбой" — особый случай: разные каналы формулируют его по-разному
      // ("Внимание, отбой! По ранее объявленным тревогам." vs "ОТБОЙ
      // РАКЕТНОЙ ОПАСНОСТИ в Белгородском и Шебекинском МО") настолько
      // непохоже текстуально, что обычное сравнение по словам их не
      // склеивает — а для читателя это не разные события, а один и тот же
      // факт «опасности больше нет». Здесь не может быть двух РАЗНЫХ
      // самостоятельных "отбоев" в течение нескольких минут (в отличие от
      // rocket/drone, где два разных сигнала подряд — это два разных
      // реальных случая, и склеивать их по типу без сравнения текста
      // опасно), поэтому для cancel сравниваем только тип и время, без
      // текста и района.
      if (r.t === 'cancel' && it.t === 'cancel' && dt <= dedupeWindowMs) {
        dup = r; break;
      }
      const sameBucket = r.t === it.t && r.region === it.region;
      // Вне "своего" типа/района сравниваем, только если оба сообщения
      // попали в короткое окно — иначе просто пропускаем пару.
      if (sameBucket && dt > dedupeWindowMs) continue;
      if (!sameBucket && dt > SHORT_DEDUPE_WINDOW_MS) continue;
      const rWords = wordSet(r.txt);
      const sim = jaccardSimilarity(itWords, rWords);
      const cont = containmentRatio(itWords, rWords);
      const isDup = sameBucket
        ? (sim >= DEDUPE_SIMILARITY || cont >= DEDUPE_CONTAINMENT)
        : (sim >= SHORT_DEDUPE_SIMILARITY || cont >= SHORT_DEDUPE_CONTAINMENT);
      if (isDup) { dup = r; break; }
    }
    if (dup) {
      if (it.source && dup.sources.indexOf(it.source) === -1) dup.sources.push(it.source);
      // Из двух формулировок отбоя оставляем более информативную (длиннее
      // текст обычно означает, что указан конкретный район/повод, а не
      // просто общее "Внимание, отбой!") — иначе в ленте могла бы остаться
      // менее полезная короткая версия только потому, что пришла первой.
      if (dup.t === 'cancel' && it.txt.length > dup.txt.length) {
        dup.txt = it.txt;
      }
    } else {
      result.push(it);
    }
  }
  return result.sort((a, b) => b.ts - a.ts); // новые сверху для отображения
}

// Типы сообщений, которые реально относятся к тревогам/БПЛА/укрытиям —
// всё остальное (общие посты канала не по теме) в ленту не попадает.
const ALERT_TYPES = ['rocket', 'drone', 'cancel', 'shelter', 'repeat', 'notice'];

// Россия (в т.ч. Белгородская область) не переходит на летнее/зимнее время
// с 2014 года — смещение Москвы всегда UTC+3, поэтому можно посчитать без
// обращения к Intl/локали.
const MOSCOW_OFFSET_MS = 3 * 60 * 60 * 1000;
function startOfTodayMoscowMs() {
  const shifted = Date.now() + MOSCOW_OFFSET_MS;
  const shiftedMidnight = Math.floor(shifted / 86400000) * 86400000;
  return shiftedMidnight - MOSCOW_OFFSET_MS;
}

// ===== Основной цикл опроса (сразу по всем каналам-источникам) =====
// bootGraceActive: при каждом перезапуске сервера state.seenIds теряется,
// если Redis не настроен (на бесплатных VPS/Render это обычный случай —
// диск не переживает деплой/рестарт). Без этой защиты первый же опрос после
// рестарта видит все сегодняшние сообщения как "новые" и рассылает их всем
// подписчикам ещё раз — то самое "куча сообщений, которые уже были".
// Поэтому на первом опросе после старта: лента и seenIds заполняются как
// обычно (сайт сразу видит сегодняшние сообщения), а рассылка (push +
// Telegram) — нет. Правда, есть и обратная сторона: если сервер перезапустится
// ровно в момент настоящей новой тревоги, именно это первое сообщение тоже не
// разошлётся — плата за то, чтобы рестарт не превращался в спам-рассылку.
// Чтобы вообще не выбирать между этими двумя сценариями — см. README про
// подключение Upstash Redis, тогда seenIds переживает рестарт по-честному.
let bootGraceActive = true;

// Раньше здесь стоял setInterval(pollOnce, POLL_MS), который запускал
// следующий вызов каждые 10 сек по часам, НЕЗАВИСИМО от того, завершился ли предыдущий вызов. Если рассылка
// подписчикам+группе (с учётом пауз между сообщениями и повторов при 429)
// в какой-то момент не укладывается в 10 секунд, второй цикл опроса и
// рассылки стартует ПОВЕРХ первого — оба параллельно шлют сообщения в
// Telegram, суммарная частота отправки взлетает выше лимита, и именно
// это разгоняет каскад 429 с растущим retry_after, а не сама по себе
// рассылка в группу (та уже была ограничена по частоте — но только
// САМА С СОБОЙ, а не с параллельным вторым циклом). Флаг ниже гарантирует,
// что новый цикл опроса просто не начнётся, пока предыдущий не закончил
// ВСЮ свою работу, включая рассылку.
let pollInProgress = false;

async function pollOnce() {
  if (pollInProgress) {
    addLog('warn', 'Пропущен цикл опроса — предыдущий ещё не завершился (идёт долгая рассылка/опрос)');
    return;
  }
  pollInProgress = true;
  try {
    // Параллельный опрос всех каналов (см. channel-fetcher.js) — время
    // цикла ограничено самым медленным ОДНИМ каналом, а не суммой по всем.
    // Успешно спарсенные сообщения не ждут упавшие/зависшие каналы.
    const results = await fetchAllChannels(channels);
    let raw = [];
    const failedChannels = new Set();
    for (const r of results) {
      if (r.ok) {
        raw = raw.concat(r.msgs);
        channelHealth[r.channel] = { ok: true, lastPollAt: Date.now(), lastError: null, count: r.msgs.length };
      } else if (r.pending) {
        // Канал (со своими внутренними повторами) не уложился в бюджет ЭТОГО
        // цикла — запрос не прерван, продолжает выполняться в фоне и будет
        // учтён следующим циклом (см. channel-fetcher.js). Это ожидаемая
        // ситуация при затяжном сетевом сбое, не сбой самого бота — не
        // засоряем логи как error, и (как и для настоящих ошибок) не теряем
        // его seenIds, раз новых данных в этом цикле от него нет.
        failedChannels.add(r.channel);
        channelHealth[r.channel] = { ok: false, lastPollAt: Date.now(), lastError: 'не уложился в бюджет цикла опроса, повтор продолжается в фоне', count: 0 };
      } else {
        failedChannels.add(r.channel);
        channelHealth[r.channel] = { ok: false, lastPollAt: Date.now(), lastError: r.error, count: 0 };
        addLog('error', `Ошибка опроса канала @${r.channel}: ${r.error}`);
      }
    }

    const fresh = raw.filter((m) => !isAd(m.text));
    const allItems = fresh.map(formatFeedItem).sort((a, b) => b.ts - a.ts);

    // Только сегодняшние сообщения (по времени Белгорода/Москвы) — старые посты
    // (вчера, неделю назад и т.п.), которые публичная страница t.me/s/канал всё
    // ещё показывает, не должны попадать в ленту и уж тем более не должны слать
    // уведомления в бот/push, как будто это свежая тревога.
    const todayStart = startOfTodayMoscowMs();
    const deletedSet = new Set((state.deletedIds || []).map(String));
    const alertItems = allItems.filter((it) =>
      ALERT_TYPES.includes(it.t) &&
      it.ts >= todayStart &&
      !deletedSet.has(String(it.id))
    );
    const items = dedupeItems(alertItems);

    const seen = new Set(state.seenIds);
    const newItems = items.filter((it) => !seen.has(String(it.id)));

    if (newItems.length) {
      newItems.forEach((it) => { it.isNew = true; });
      addLog('info', `Новых сообщений в ленте: ${newItems.length}`, { types: newItems.map((it) => it.t) });
      if (bootGraceActive) {
        addLog('info', `Первый опрос после запуска сервера — ${newItems.length} сообщений тихо приняты как известные, без рассылки (защита от повторной рассылки уже опубликованного после рестарта)`);
      } else {
        // помечаем как новые только первые несколько минут — на фронте это условно,
        // здесь просто фиксируем факт появления для push-рассылки
        for (const it of newItems) {
          await notifySubscribers(it);
        }
      }
    }
    bootGraceActive = false;

    // Ручные сообщения (отправленные из админки) не приходят из парсера каналов —
    // сохраняем их поверх свежей выборки, иначе следующий же цикл опроса их сотрёт.
    const manualItems = (state.feed || []).filter((it) => it.manual);
    state.feed = manualItems.concat(items)
      .sort((a, b) => b.ts - a.ts)
      .slice(0, 60); // храним последние 60 сообщений
    // seenIds строим по ВСЕМ сообщениям канала (включая нерелевантные),
    // чтобы off-topic посты не пересчитывались и не «просачивались» после правок фильтра.
    // ВАЖНО: если какой-то канал в этом цикле не опросился (таймаут/ошибка сети —
    // см. failedChannels выше), его сообщений нет в allItems, и полная перезапись
    // seenIds стёрла бы память о том, что его посты уже видели. Тогда при следующем
    // удачном опросе этот канал присылает СТАРЫЕ (уже показанные и разосланные)
    // сообщения как будто они новые — та самая повторная рассылка после сбоя.
    // Поэтому id-шники упавших каналов переносим из предыдущего state.seenIds как есть
    // (у всех id префикс "канал/...", см. fetchChannelMessages), остальные — обновляем.
    const freshSeenIds = allItems.slice(0, 200).map((it) => String(it.id));
    const preservedSeenIds = failedChannels.size
      ? (state.seenIds || []).filter((id) =>
          Array.from(failedChannels).some((ch) => id.startsWith(ch + '/'))
        )
      : [];
    state.seenIds = Array.from(new Set([...freshSeenIds, ...preservedSeenIds])).slice(0, 400);
    // deletedIds храним по тому же принципу, что и seenIds — не бесконечно,
    // достаточно последних записей, чтобы удалённое сегодня не вернулось.
    if (Array.isArray(state.deletedIds) && state.deletedIds.length > 500) {
      state.deletedIds = state.deletedIds.slice(-500);
    }
    saveJson(STATE_FILE, state);
    lastPollOk = true;
    lastPollAt = Date.now();
  } catch (err) {
    lastPollOk = false;
    addLog('error', 'Ошибка опроса канала: ' + err.message, { stack: err.stack });
  } finally {
    pollInProgress = false;
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
  res.json({ botUsername: TELEGRAM_BOT_USERNAME || null, promoChannelUrl: getSetting('promoChannelUrl') || null });
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
// ===== Админы Telegram-бота (выдаются с сайта, не паролем внутри бота) =====
// Список всех, кто хоть раз писал /start боту — по нему сайт-админ находит
// нужного человека (по username, если он есть, иначе по chat ID) и решает,
// давать ли ему доступ к панели управления внутри Telegram.
app.get('/api/admin/telegram-users', requireAdmin, (req, res) => {
  const adminSet = new Set(tgAdmins);
  const users = (tgSubscriptions || [])
    .map((s) => ({
      chatId: s.chatId,
      username: s.username || null,
      firstName: s.firstName || null,
      regions: s.regions || [],
      joinedAt: s.joinedAt || null,
      isAdmin: adminSet.has(s.chatId)
    }))
    .sort((a, b) => (b.joinedAt || 0) - (a.joinedAt || 0));
  res.json({ users });
});

app.post('/api/admin/telegram-admins', requireAdmin, async (req, res) => {
  const chatId = Number((req.body || {}).chatId);
  if (!chatId) return res.status(400).json({ error: 'chatId required' });
  if (!tgAdmins.includes(chatId)) {
    tgAdmins.push(chatId);
    saveTgAdmins();
    addLog('info', `Сайт-админ выдал доступ администратора Telegram-чату ${chatId}`);
    try {
      await tgCall('sendMessage', {
        chat_id: chatId,
        text: '✅ Тебе выдан доступ администратора. Набери /menu, чтобы открыть панель управления.'
      });
    } catch (err) { /* пользователь мог заблокировать бота — не критично */ }
  }
  res.json({ ok: true, tgAdmins });
});

app.delete('/api/admin/telegram-admins', requireAdmin, async (req, res) => {
  const chatId = Number((req.body || {}).chatId);
  if (!chatId) return res.status(400).json({ error: 'chatId required' });
  const before = tgAdmins.length;
  tgAdmins = tgAdmins.filter((id) => id !== chatId);
  if (tgAdmins.length !== before) {
    saveTgAdmins();
    addLog('info', `Сайт-админ забрал доступ администратора у Telegram-чата ${chatId}`);
    try {
      await tgCall('sendMessage', { chat_id: chatId, text: '🚪 Доступ администратора отозван администратором сайта.' });
    } catch (err) { /* пользователь мог заблокировать бота — не критично */ }
  }
  res.json({ ok: true, tgAdmins });
});

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
  if (!channels.some((c) => c.toLowerCase() === parsed.toLowerCase())) return res.status(404).json({ error: 'unknown channel' });
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
  if (channels.some((c) => c.toLowerCase() === parsed.toLowerCase())) return res.status(409).json({ error: 'already added' });
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

// ===== Стоп-слова (точечное вырезание слов/фраз из текста оповещений) =====
app.get('/api/admin/filter-words', requireAdmin, (req, res) => {
  res.json({ words: customFilterWords });
});

app.post('/api/admin/filter-words', requireAdmin, (req, res) => {
  const word = ((req.body || {}).word || '').trim();
  if (!word) return res.status(400).json({ error: 'empty word' });
  if (customFilterWords.some((w) => w.toLowerCase() === word.toLowerCase())) {
    return res.status(409).json({ error: 'already added' });
  }
  customFilterWords.push(word);
  saveCustomFilterWords();
  addLog('info', `Добавлено стоп-слово: «${word}»`);
  res.json({ ok: true, words: customFilterWords });
});

app.delete('/api/admin/filter-words', requireAdmin, (req, res) => {
  const word = ((req.body || {}).word || '').trim();
  if (!word) return res.status(400).json({ error: 'empty word' });
  customFilterWords = customFilterWords.filter((w) => w.toLowerCase() !== word.toLowerCase());
  saveCustomFilterWords();
  addLog('info', `Удалено стоп-слово: «${word}»`);
  res.json({ ok: true, words: customFilterWords });
});

// ===== Настройки звуковой тревоги (какие типы/районы дают громкий push) =====
// Отдаём и текущие значения, и метаданные (подпись/подсказку) из
// EDITABLE_SETTINGS — так админка может отрисовать форму настроек, просто
// пройдясь по списку, без хардкода конкретных полей на фронте. Добавление
// новой настройки в будущем требует правки только EDITABLE_SETTINGS выше —
// ни это API, ни форма в admin.html переписывать не придётся.
app.get('/api/admin/settings', requireAdmin, (req, res) => {
  const fields = EDITABLE_SETTINGS.map((s) => ({
    key: s.key, label: s.label, hint: s.hint, value: runtimeSettings[s.key],
    type: s.type || 'string', min: s.min, max: s.max, step: s.step, multiline: !!s.multiline
  }));
  res.json({ fields });
});

app.post('/api/admin/settings', requireAdmin, (req, res) => {
  const body = req.body || {};
  const registry = new Map(EDITABLE_SETTINGS.map((s) => [s.key, s]));
  let changed = false;
  const rejected = [];
  for (const key of Object.keys(body)) {
    const spec = registry.get(key);
    if (!spec) continue; // неизвестный ключ — игнорируем, а не падаем с ошибкой
    let value = typeof body[key] === 'string' ? body[key].trim() : body[key];
    if (value === '' || value === null || value === undefined) continue; // пусто — оставляем прежнее значение
    if (spec.type === 'number') {
      const num = Number(value);
      if (!Number.isFinite(num)) { rejected.push(`${key}: не число`); continue; }
      // min/max — не просто подсказка в UI, а реальная граница: значение вне
      // диапазона могло бы, например, увести таймаут запроса в 0 (вечный цикл
      // ошибок) или окно дедупликации в часы (склеит разные реальные тревоги).
      if (typeof spec.min === 'number' && num < spec.min) { rejected.push(`${key}: меньше минимума (${spec.min})`); continue; }
      if (typeof spec.max === 'number' && num > spec.max) { rejected.push(`${key}: больше максимума (${spec.max})`); continue; }
      value = num;
    }
    if (spec.validateMarkdown) {
      const mdErrors = validateMarkdownBalance(value);
      if (mdErrors.length) { rejected.push(`${key}: ${mdErrors.join('; ')}`); continue; }
    }
    runtimeSettings[key] = value;
    changed = true;
  }
  if (changed) {
    saveJson(SETTINGS_FILE, runtimeSettings);
    addLog('info', 'Настройки изменены из админки', runtimeSettings);
  }
  const fields = EDITABLE_SETTINGS.map((s) => ({
    key: s.key, label: s.label, hint: s.hint, value: runtimeSettings[s.key],
    type: s.type || 'string', min: s.min, max: s.max, step: s.step, multiline: !!s.multiline
  }));
  res.json({ ok: rejected.length === 0, rejected: rejected.length ? rejected : undefined, fields });
});

// Сам процесс себя "перезапустить" не может — рестарт делает внешний
// супервизор (systemd-сервис из install.sh: Restart=always, поднимает через
// 5 сек после любого выхода процесса; то же самое на Render/Railway и
// подобных хостингах — падение процесса = автоматический перезапуск).
// Поэтому здесь просто: ответить админке успехом, дать чуть-чуть времени,
// чтобы ответ реально ушёл по сети, и завершить процесс — дальше это уже
// не наша забота, супервизор поднимет заново.
app.post('/api/admin/restart', requireAdmin, (req, res) => {
  addLog('info', 'Перезапуск сервера запрошен из админки');
  res.json({ ok: true, message: 'Перезапускаюсь…' });
  setTimeout(() => process.exit(0), 300);
});

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
    txt: text && text.trim() ? text.trim() : getSetting('cancelDefaultText'),
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
  // Без этого следующий же опрос канала (pollOnce) снова находил это сообщение
  // на публичной странице t.me/s/канал и добавлял его обратно в ленту —
  // удаление "не работало" именно поэтому.
  if (!Array.isArray(state.deletedIds)) state.deletedIds = [];
  if (!state.deletedIds.includes(String(id))) state.deletedIds.push(String(id));
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

(async () => {
  await hydrateFromRedis();
  app.listen(PORT, () => {
    console.log(`Тревога · Белгород — сервер запущен на порту ${PORT}`);
    addLog('info', `Сервер запущен на порту ${PORT}`);
    pollLoop();
  });
})();

// Частота опроса каналов теперь настраивается из веб-админки
// (pollIntervalSec) и может меняться "на лету", без перезапуска сервера —
// setInterval так не умеет, он фиксирует интервал один раз при старте и
// не замечает изменений в settings.json. Вместо него — self-rescheduling
// setTimeout: следующий вызов планируется ТОЛЬКО после того как текущий
// цикл (опрос + вся рассылка) полностью завершился, и каждый раз заново
// читает актуальное значение настройки.
async function pollLoop() {
  await pollOnce();
  const intervalMs = getNumberSetting('pollIntervalSec', 10) * 1000;
  setTimeout(pollLoop, intervalMs);
}

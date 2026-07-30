// ============================================================
// channel-fetcher.js — отдельный модуль опроса каналов-источников
// ============================================================
// ПОЧЕМУ этот файл выделен отдельно:
// Раньше pollOnce() в server.js опрашивал каналы ПОСЛЕДОВАТЕЛЬНО —
// простой for..of с await на каждой итерации. Если один канал зависал
// или отвечал с задержкой (таймаут), это переносилось "лесенкой" на
// ВСЕ каналы после него в списке: суммарное время одного цикла опроса
// могло вырасти до (таймаут × число проблемных каналов), а сообщения
// из уже успешно опрошенных каналов просто ждали своей очереди на
// публикацию в ленту — хотя реально ждать было незачем.
//
// Для оповещений о ракетной опасности/БПЛА такая задержка недопустима:
// секунды имеют значение. Здесь опрос всех каналов идёт ПАРАЛЛЕЛЬНО
// (Promise.allSettled) — общее время цикла ограничено самым медленным
// ОДНИМ каналом (максимум CHANNEL_TIMEOUT_MS), а не суммой по всем
// каналам. Результат каждого канала (успех/ошибка) независим от
// остальных, поэтому упавший канал не задерживает и не блокирует
// сообщения, которые успешно спарсились из других источников —
// они уходят в ленту сразу же, в этом же цикле, без ожидания.
//
// Таймаут одного запроса — 10 секунд (как и частота самого опроса
// в server.js, POLL_MS): при параллельном опросе поднимать его больше
// уже не нужно, т.к. медленный канал больше не тянет за собой очередь.

const axios = require('axios');
const https = require('https');
const cheerio = require('cheerio');
const { describeError } = require('./error-utils');

const CHANNEL_TIMEOUT_MS = 10000;
// Небольшой разброс старта запроса на канал — чтобы не долбить t.me
// буквально всеми каналами в один и тот же миллисекунд (это повышало шанс
// сетевых сбоев вроде AggregateError при параллельных подключениях к одному
// хосту). На общее время цикла почти не влияет: максимум (число каналов-1)×
// STAGGER_STEP_MS добавки к старту последнего канала, а не к длине опроса.
const STAGGER_STEP_MS = 120;

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

// Постоянное (keep-alive) HTTPS-соединение вместо нового TCP+TLS хендшейка
// на КАЖДЫЙ запрос к t.me — само по себе устанавливать TLS-сессию заново
// каждые 10 секунд к одному и тому же хосту дольше и менее надёжно, чем
// переиспользовать уже открытое соединение. family: 4 — принудительно
// IPv4: часть VPS-хостингов имеют плохо работающий/более медленный IPv6
// маршрут до Telegram, и попытка Node достучаться и по IPv6, и по IPv4
// одновременно — как раз то, что порождает AggregateError с "пустым"
// сообщением об ошибке (см. error-utils.js).
const keepAliveAgent = new https.Agent({ keepAlive: true, maxSockets: 20, family: 4 });

// До 3 попыток на канал (1 основная + 2 повтора) с нарастающей паузой между
// ними — большинство ETIMEDOUT/ECONNABORTED разовые сетевые сбои, но иногда
// один повтор недостаточен, если сбой длится несколько секунд подряд.
const RETRY_DELAYS_MS = [500, 1500];

// Один HTTP-запрос + разбор страницы канала. Логика разбора не менялась —
// просто вынесена в отдельную функцию, чтобы можно было повторить попытку
// при сбое (см. fetchChannelMessages ниже) не дублируя код разбора.
async function fetchChannelMessagesOnce(channel) {
  const url = `https://t.me/s/${channel}`;
  const res = await axios.get(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; TrevogaBelgorodBot/1.0)' },
    timeout: CHANNEL_TIMEOUT_MS,
    httpsAgent: keepAliveAgent
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

// ETIMEDOUT/ECONNABORTED/AggregateError на t.me в подавляющем большинстве —
// разовый сетевой сбой (обрыв соединения, мигнувший DNS), а не системная
// проблема именно с этим каналом: секунду-другую спустя тот же запрос почти
// всегда проходит нормально. Поэтому при сбое пробуем ещё раз (до 2 повторов
// с нарастающей паузой), прежде чем сдаться и пометить канал как упавший в
// этом цикле опроса. Это не отменяет защиту seenIds для реально упавших
// каналов (см. server.js) — это её дополняет, снижая сам шанс дойти до той
// ветки.
// Если опрос канала (с повторами) всё ещё выполняется, когда стартует
// следующий 10-секундный цикл pollOnce(), новый запрос к ТОМУ ЖЕ каналу не
// запускается поверх старого — иначе при затяжном сбое запросы к одному
// проблемному каналу копились бы друг на друга. Вместо этого новый цикл
// просто дожидается уже идущего запроса и получает тот же результат.
const inFlight = new Map(); // channel -> Promise

async function fetchChannelMessages(channel) {
  if (inFlight.has(channel)) return inFlight.get(channel);
  const promise = (async () => {
    let lastErr;
    for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
      if (attempt > 0) await sleep(RETRY_DELAYS_MS[attempt - 1]);
      try {
        return await fetchChannelMessagesOnce(channel);
      } catch (err) {
        lastErr = err;
      }
    }
    throw lastErr;
  })();
  inFlight.set(channel, promise);
  try {
    return await promise;
  } finally {
    inFlight.delete(channel);
  }
}

// Опрашивает ВСЕ каналы параллельно и возвращает результат по каждому
// отдельно — вызывающий код (pollOnce в server.js) сам решает, что делать
// с успехами и ошибками, не дожидаясь друг друга.
// Возвращает: [{ channel, ok, msgs, error }, ...] — в том же порядке, что и channels.
async function fetchAllChannels(channels) {
  const settled = await Promise.allSettled(
    channels.map((ch, i) => sleep(i * STAGGER_STEP_MS).then(() => fetchChannelMessages(ch)))
  );
  return channels.map((channel, i) => {
    const r = settled[i];
    if (r.status === 'fulfilled') {
      return { channel, ok: true, msgs: r.value, error: null };
    }
    return { channel, ok: false, msgs: [], error: describeError(r.reason) };
  });
}

module.exports = { fetchChannelMessages, fetchAllChannels, CHANNEL_TIMEOUT_MS };

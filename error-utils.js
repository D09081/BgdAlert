// ============================================================
// error-utils.js — извлечение читаемого текста из любой ошибки
// ============================================================
// Почему это нужно отдельным хелпером: при ПАРАЛЛЕЛЬНЫХ сетевых запросах
// (несколько соединений к одному хосту почти в один момент) Node иногда
// выбрасывает AggregateError — например, когда DNS вернул и IPv4, и IPv6
// адрес, и оба не смогли подключиться. У AggregateError по умолчанию
// err.message === '' (пустая строка) — реальные причины лежат внутри
// err.errors[]. Код, который брал только err.message, в этом случае
// получал пустоту и логировал бесполезное "unknown error" — то, что было
// видно в логах бота: "Ошибка опроса канала @X: unknown error" и
// "Telegram API ошибка (sendMessage): " без текста вообще.

function describeError(err) {
  if (!err) return 'unknown error';

  // AggregateError — несколько причин сразу, вытаскиваем каждую
  if (Array.isArray(err.errors) && err.errors.length) {
    const parts = err.errors.map((e) => {
      if (!e) return String(e);
      return e.code ? `${e.code}${e.message ? ' ' + e.message : ''}` : (e.message || String(e));
    });
    return `${err.name || 'AggregateError'}: ${parts.join('; ')}`;
  }

  // Ответ от сервера есть, но это не 2xx (ошибка Telegram API / HTTP-ошибка)
  if (err.response) {
    const status = err.response.status;
    const data = err.response.data;
    let desc = '';
    if (data && typeof data === 'object') {
      desc = data.description || JSON.stringify(data);
    } else if (typeof data === 'string' && data) {
      // Ответ иногда НЕ JSON, а голая HTML-страница ошибки (типично для
      // 502/503/504 от nginx/прокси перед самим Telegram) — раньше такая
      // страница целиком (сотни символов разметки) улетала в лог как есть.
      // Вырезаем теги, схлопываем пробелы, обрезаем до разумной длины —
      // достаточно сути ("502 Bad Gateway nginx/1.30.1"), а не всей вёрстки.
      const stripped = data.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
      desc = stripped.length > 150 ? stripped.slice(0, 150) + '…' : stripped;
    }
    return `HTTP ${status}${desc ? ' — ' + desc : ''}`;
  }

  // Обычная сетевая ошибка Node (ECONNRESET, ENOTFOUND, ETIMEDOUT и т.п.)
  if (err.code && err.message) return `${err.code}: ${err.message}`;
  if (err.message) return err.message;
  if (err.code) return err.code;

  try { return JSON.stringify(err); } catch (_) { return String(err); }
}

module.exports = { describeError };

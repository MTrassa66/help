// Cloudflare Worker мойки: входная дверь для заявок и быстрая доставка вложений в Telegram.
//
// ЗАЯВКА (v6): POST /claim — номер выдаёт Durable Object (единый счётчик, без повторов, защита от дублей
// по clientId), клиент получает ответ сразу. Сообщение в группу и строку в таблицу Google Durable Object
// отправляет следом, а при сбое повторяет по будильнику (alarm) до 24 часов.
// Если Worker недоступен, страница отправляет заявку в Google; Google сам берёт номер у этого же счётчика
// (POST /next), а если не может — из запасного диапазона 9001+.
//
// ВЛОЖЕНИЯ:
//  1) ПРЕДЗАГРУЗКА (основной): пока клиент заполняет форму, файлы уходят в служебную
//     группу и сохраняются как file_id. После номера заявки /commit пересылает их в
//     основную группу по file_id — без повторной загрузки, за доли секунды.
//  2) ПОСЛЕ НОМЕРА (запасной): /file и /album с ключом заявки.
//
// Настройки (wrangler.toml — привязки, Cron, миграция Durable Object; секреты — в Cloudflare):
//   BOT_TOKEN        (Secret) — токен бота
//   CHAT_ID                   — основная группа, например -5287464297
//   STAGING_CHAT_ID           — служебная группа для предзагрузки (бот — администратор)
//   SIGN_KEY         (Secret) — значение SIGN_KEY из журнала функции setup() в Google
//   GAS_URL                   — адрес веб-приложения Google (…/exec): туда уходят строки таблицы и события
//   DRAFTS_PER_HOUR           — необязательно: черновиков с одного IP в час (по умолчанию 5)
//   CLAIMS_PER_IP, CLAIMS_PER_PHONE — необязательно: заявок в час с одного IP (20) и на один телефон (3)
//   DEDUPE  (KV)              — черновики, защита от дублей вложений, лимиты
//   DESK    (Durable Object)  — счётчик номеров и очередь «Telegram + таблица»
//   Cron 0 3 * * *            — раз в сутки удаляет старые сообщения служебной группы
//
// Эндпоинты:
//   POST /claim                                   — заявка (тело — JSON, text/plain), ответ: { id, uploadKey, msgId }
//   POST /event                                   — событие статистики (пересылается в Google после ответа)
//   POST /next                                    — для Google: номер из общего счётчика (подпись SIGN_KEY)
//   POST /admin/sync                              — для Google setup(): счётчик не меньше LAST_ID таблицы
//   POST /draft                                   — черновой ключ (при выборе проблемы)
//   POST /pre?token=&part=&kind=&name=[&as=]      — предзагрузка одного файла (тело — файл)
//   POST /pre-del?token=&part=                    — клиент удалил файл до отправки
//   POST /commit?token=&parts=[&id=&key=&cid=&msg=] — переслать предзагруженное в заявку
//   POST /file?id=&key=&cid=&part=&msg=&kind=&name=  — запасной путь: файл после номера
//   POST /album?id=&key=&cid=&part=&msg=          — запасной путь: фото после номера
//   GET  /sent?id=&key=&cid=&part=                — для Google: часть уже доставлена?
//   v8:
//   POST /change-phone                            — «Не работает размен»: телефон первого сообщившего (ключ заявки)
//   POST /resolved                                — для Google: заявка «размен» решена — окно «первого» закрыто
//   POST /review-prep                             — для Google: готовый текст просьбы об отзыве (подпись SIGN_KEY)
//   GET  /w/<id>.<подпись>                        — владелец нажал «Попросить отзыв» → WhatsApp с текстом, отметка «запрошен»
//   GET  /r/<id>.<подпись>                        — клиент открыл ссылку на отзыв → Яндекс Карты, отметка «перешли»
//   REVIEW_URL (wrangler.toml)                    — куда ведёт ссылка на отзыв
//   v8.1 — одна заявка = одно сообщение:
//   POST /ticket-update                           — для Google: что изменилось в заявке (подпись SIGN_KEY) → правка сообщения
//   GET  /g/<id>.<подпись>                        — страница «Все вложения» заявки
//   GET  /f/<id>.<часть>.<подпись>                — файл вложения (Worker берёт у Telegram, токен наружу не уходит)
//   POST /gsend/<id>.<часть>.<подпись>            — «Прислать в группу» (видео > 20 МБ)
//   GET  /c/<id>.<подпись>?a=msg|call&t=hello|bonus|review[&via=wa|viber|sms] — связь с клиентом по его выбору
//   PUBLIC_URL (wrangler.toml)                    — адрес этого Worker для кнопок в Telegram

import { DurableObject } from 'cloudflare:workers';

const MB = 1024 * 1024;
const DRAFT_TTL = 7200;            // черновик живёт 2 часа
const DEDUPE_TTL = 21600;          // 6 часов
const STAGING_KEEP = 40 * 3600;    // служебные сообщения: удалять в окне 3–40 ч (Telegram разрешает до 48 ч)
const DRAFTS_PER_HOUR = 5;         // черновиков с одного IP в час
// заявок в час: по IP — щедро (у мобильных операторов много абонентов за одним адресом), по телефону — строго
const CLAIMS_PER_IP = 20;
const CLAIMS_PER_PHONE = 3;
// v8: ссылка на форму отзыва (настройка — REVIEW_URL в wrangler.toml; здесь — запасное значение)
const REVIEW_URL = 'https://yandex.by/maps/org/avtomoyka/153062315248/reviews/?add-review=true';
const CHANGE_WINDOW = 48 * 3600e3;   // «размен»: если заявку не закрыли «Решено», окно «первого» закроется само

// ---------- v8.1: настройки ----------
const ATT_WAIT = 10e3;               // ждать вложения перед отправкой заявки в группу, не дольше
const SIGNAL_TTL = 15 * 60e3;        // сигнал 🔔 удаляется сам через 15 минут
const LINK_DAYS = 30;                // срок ссылок /g, /c и хранения file_id
const PUBLIC_URL = 'https://mtrassa.dowtnohot.workers.dev';   // запасное значение; настройка — PUBLIC_URL в wrangler.toml
// тексты клиенту (Вы/Вам/Вас — с большой буквы)
const HELLO_TEXT = id => 'Здравствуйте! Это Автомойка «Трасса 66» на ул. Минской, 10А, по Вашей заявке #' + id + '.';
const STATUS_ICON = { 'Новая': '🔴', 'Связались': '🟡', 'Решено': '✅' };
const CAPTION_MAX = 1024, TEXT_MAX = 4096;
const CUT_TAIL = '… (полностью — в таблице)';
const DRAFT_MAX_BYTES = 100 * MB;  // всего на один черновик
// номера частей: фото 1–30, видео 100–109, голос 200–209 (одинаковы для всех путей — так работает защита от дублей)
const KINDS = {
  voice: { from: 200, to: 209, max: 5 * MB, commitMax: 1 },
  photo: { from: 1, to: 30, max: 15 * MB, commitMax: 10 },
  video: { from: 100, to: 109, max: 48 * MB, commitMax: 3 }
};
const kindOfPart = part => Object.keys(KINDS).find(k => part >= KINDS[k].from && part <= KINDS[k].to) || null;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400'
};
const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), { status, headers: { ...CORS, 'Content-Type': 'application/json' } });

export default {
  async fetch(req, env, ctx) {
    if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, '');
    try {
      if (req.method === 'GET' && path.endsWith('/sent')) return json(await sentCheck(env, url.searchParams));
      const short = req.method === 'GET' && path.match(/\/([rw])\/(\d+)\.([0-9a-f]{10})$/);
      if (short) return await shortLink(env, ctx, short[1], Number(short[2]), short[3]);
      // v8.1: страницы для владельца
      let m;
      if (req.method === 'GET' && (m = path.match(/\/g\/(\d+)\.([0-9a-f]{10})$/))) return await galleryPage(env, Number(m[1]), m[2]);
      if (req.method === 'GET' && (m = path.match(/\/f\/(\d+)\.(\d+)\.([0-9a-f]{10})$/))) return await fileProxy(req, env, Number(m[1]), Number(m[2]), m[3]);
      if (req.method === 'POST' && (m = path.match(/\/gsend\/(\d+)\.(\d+)\.([0-9a-f]{10})$/))) return await gallerySend(env, Number(m[1]), Number(m[2]), m[3]);
      if (req.method === 'GET' && (m = path.match(/\/c\/(\d+)\.([0-9a-f]{10})$/))) return await contactRoute(env, ctx, Number(m[1]), m[2], url.searchParams);
      if (req.method !== 'POST') return new Response('ok', { headers: CORS });   // прогрев соединения
      if (path.endsWith('/claim')) return json(await claimRoute(req, env, ctx));
      if (path.endsWith('/change-phone')) return json(await changePhoneRoute(req, env, ctx));
      if (path.endsWith('/resolved')) return json(await resolvedRoute(req, env));
      if (path.endsWith('/review-prep')) return json(await reviewPrepRoute(req, env));
      if (path.endsWith('/ticket-update')) return json(await ticketUpdateRoute(req, env));
      if (path.endsWith('/event')) return await eventRoute(req, env, ctx);
      if (path.endsWith('/next')) return json(await nextRoute(req, env));
      if (path.endsWith('/admin/sync')) return json(await syncRoute(req, env));
      if (path.endsWith('/draft')) return json(await draft(req, env));
      if (path.endsWith('/pre')) return json(await preupload(req, env, url.searchParams));
      if (path.endsWith('/pre-del')) return json(await preDelete(env, url.searchParams));
      if (path.endsWith('/commit')) return json(await commit(env, url.searchParams));
      if (path.endsWith('/album')) return json(await album(req, env, url.searchParams));
      if (path.endsWith('/file')) return json(await file(req, env, url.searchParams));
      return json({ ok: false, error: 'not found' }, 404);
    } catch (e) {
      return json({ ok: false, error: String((e && e.message) || e) });
    }
  },

  // раз в сутки: удаляем из служебной группы сообщения старше 3 часов
  async scheduled(event, env, ctx) {
    ctx.waitUntil(cleanStaging(env));
  }
};

// ================= ПОДПИСИ =================

async function hmac(env, text) {
  const raw = new TextEncoder().encode(env.SIGN_KEY || '');
  const k = await crypto.subtle.importKey('raw', raw, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', k, new TextEncoder().encode(text));
  return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 24);
}

// ключ заявки (выдаёт Google): nonce.срок.подпись(id.nonce.срок)
async function keyOk(env, id, key) {
  const [nonce, exp, mac] = String(key || '').split('.');
  if (!id || !nonce || !exp || !mac) return false;
  if (Number(exp) < Date.now() / 1000) return false;
  return mac === await hmac(env, id + '.' + nonce + '.' + exp);
}

// черновой ключ (выдаёт Worker): draftId.срок.подпись('draft.'+draftId.срок)
async function draftOk(env, token) {
  const [id, exp, mac] = String(token || '').split('.');
  if (!id || !/^[0-9a-f]{16,32}$/.test(id) || !exp || !mac) return null;
  if (Number(exp) < Date.now() / 1000) return null;
  return mac === await hmac(env, 'draft.' + id + '.' + exp) ? id : null;
}

// ================= KV =================

const kv = env => env.DEDUPE;
async function kget(env, key) {
  if (!kv(env)) return null;
  try { return await kv(env).get(key); } catch (_) { return null; }
}
async function kgetJson(env, key) {
  const v = await kget(env, key);
  try { return v ? JSON.parse(v) : null; } catch (_) { return null; }
}
async function kput(env, key, value, ttl, metadata) {
  if (!kv(env)) return;
  try { await kv(env).put(key, value, metadata ? { expirationTtl: ttl, metadata } : { expirationTtl: ttl }); } catch (_) {}
}

const sentKey = (cid, part) => 'sent:' + cid + ':' + part;
const partKey = (draft, part) => 'dr:' + draft + ':p:' + part;
const delKey = (draft, part) => 'dr:' + draft + ':del:' + part;
const claimKey = draft => 'dr:' + draft + ':claim';

// ================= TELEGRAM =================

function api(env, method) {
  return (env.TG_API || 'https://api.telegram.org') + '/bot' + env.BOT_TOKEN + '/' + method;
}
// Ограничение времени: зависший запрос к Telegram не должен держать очередь заявок.
// Текст — 15 с, файл — 120 с. По истечении — { ok: false, timeout: true }.
async function tg(env, method, body) {
  const file = body instanceof FormData;
  const opts = file
    ? { method: 'POST', body }
    : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
  opts.signal = AbortSignal.timeout(file ? 120e3 : 15e3);
  let r;
  try { r = await fetch(api(env, method), opts); } catch (e) { return { ok: false, timeout: true, description: String((e && e.message) || e) }; }
  try { return await r.json(); } catch (_) { return { ok: false }; }
}
const methodFor = field => 'send' + field[0].toUpperCase() + field.slice(1);
const replyTo = msg => JSON.stringify({ message_id: msg, allow_sending_without_reply: true });
const isPlainPhoto = type => /^image\/(jpeg|png|webp)$/i.test(type || '');

// file_id из ответа Telegram: для фото — самый большой размер
function fileIdOf(result, field) {
  if (!result) return null;
  if (field === 'photo' && Array.isArray(result.photo)) return result.photo[result.photo.length - 1].file_id;
  const obj = result[field] || result.document || result.video || result.voice || result.audio || result.animation;
  return obj ? obj.file_id : null;
}

// пробует по очереди способы отправки (в конце — документом); возвращает ответ и способ
async function sendChain(env, chat, blob, name, ways, caption, msg) {
  for (const w of ways.concat(['document'])) {
    const f = new FormData();
    f.set('chat_id', chat);
    if (caption) f.set('caption', caption);
    if (msg) f.set('reply_parameters', replyTo(msg));
    if (w === 'video') f.set('supports_streaming', 'true');
    f.set(w, blob, name);
    const r = await tg(env, methodFor(w), f);
    if (r.ok) return { field: w, result: r.result };
  }
  return null;
}

// большой файл — потоком, не загружая его целиком в память
async function streamUpload(env, req, chat, field, name, mime, len, fields) {
  const boundary = '----moika' + crypto.randomUUID().replace(/-/g, '');
  const enc = new TextEncoder();
  let head = '';
  for (const [k, v] of Object.entries(Object.assign({ chat_id: chat }, fields))) {
    head += `--${boundary}\r\nContent-Disposition: form-data; name="${k}"\r\n\r\n${v}\r\n`;
  }
  head += `--${boundary}\r\nContent-Disposition: form-data; name="${field}"; filename="${name}"\r\n` +
          `Content-Type: ${mime}\r\n\r\n`;
  const h = enc.encode(head);
  const t = enc.encode(`\r\n--${boundary}--\r\n`);
  const { readable, writable } = new FixedLengthStream(h.byteLength + len + t.byteLength);
  const pump = (async () => {
    const w = writable.getWriter();
    await w.write(h);
    const r = req.body.getReader();
    for (;;) {
      const { done, value } = await r.read();
      if (done) break;
      await w.write(value);
    }
    await w.write(t);
    await w.close();
  })();
  const [res] = await Promise.all([
    fetch(api(env, methodFor(field)), {
      method: 'POST',
      headers: { 'Content-Type': 'multipart/form-data; boundary=' + boundary },
      body: readable
    }),
    pump
  ]);
  try { return await res.json(); } catch (_) { return { ok: false }; }
}

// ================= ЧЕРНОВИК =================

async function draft(req, env) {
  // не больше 5 черновиков в час с одного IP
  const ip = req.headers.get('CF-Connecting-IP') || 'local';
  const hour = Math.floor(Date.now() / 3600000);
  const ipKey = 'ip:' + ip + ':' + hour;
  const used = Number(await kget(env, ipKey)) || 0;
  const perHour = env.DRAFTS_PER_HOUR ? Number(env.DRAFTS_PER_HOUR) : DRAFTS_PER_HOUR;
  if (used >= perHour) return { ok: false, error: 'limit' };
  await kput(env, ipKey, String(used + 1), 3700);

  const id = [...crypto.getRandomValues(new Uint8Array(8))].map(b => b.toString(16).padStart(2, '0')).join('');
  const exp = Math.floor(Date.now() / 1000) + DRAFT_TTL;
  return { ok: true, draft: id, token: id + '.' + exp + '.' + await hmac(env, 'draft.' + id + '.' + exp), exp };
}

// ================= ПРЕДЗАГРУЗКА =================

async function preupload(req, env, q) {
  // всё проверяем ДО чтения тела
  const draftId = await draftOk(env, q.get('token'));
  if (!draftId) return { ok: false, error: 'token' };
  const part = Math.floor(Number(q.get('part')));
  const kind = Number.isFinite(part) ? kindOfPart(part) : null;
  if (!kind || q.get('kind') !== kind) return { ok: false, error: 'part' };
  const len = Number(req.headers.get('Content-Length') || 0);
  if (!len) return { ok: false, error: 'length' };
  if (len > KINDS[kind].max) return { ok: false, error: 'size' };
  if (!env.STAGING_CHAT_ID) return { ok: false, error: 'staging' };

  const [existing, deleted, list] = await Promise.all([
    kgetJson(env, partKey(draftId, part)),
    kget(env, delKey(draftId, part)),
    kv(env) ? kv(env).list({ prefix: 'dr:' + draftId + ':p:' }).catch(() => ({ keys: [] })) : { keys: [] }
  ]);
  if (deleted) return { ok: false, error: 'deleted' };
  if (existing) return { ok: true, repeat: true };                  // уже предзагружено
  const usedBytes = list.keys.reduce((s, k) => s + ((k.metadata && k.metadata.s) || 0), 0);
  if (usedBytes + len > DRAFT_MAX_BYTES) return { ok: false, error: 'total' };

  const name = (q.get('name') || 'file').replace(/[^\w.\-]/g, '_').slice(0, 60);
  const mime = (req.headers.get('Content-Type') || 'application/octet-stream').split(';')[0];
  const chat = env.STAGING_CHAT_ID;
  let sent = null;

  if (len < 5 * MB) {
    const body = await req.blob();
    const blob = body.type === mime ? body : body.slice(0, body.size, mime);
    let ways = [];
    if (kind === 'photo') ways = isPlainPhoto(mime) ? ['photo'] : [];        // HEIC и редкие форматы — документом
    if (kind === 'voice') ways = /webm/.test(mime) ? [] : ['voice', 'audio'];
    if (kind === 'video') ways = ['video'];
    sent = await sendChain(env, chat, blob, name, ways, '', 0);
  } else {
    const field = q.get('as') === 'document' || kind !== 'video' ? 'document' : 'video';
    const r = await streamUpload(env, req, chat, field, name, mime, len, field === 'video' ? { supports_streaming: 'true' } : {});
    if (!r.ok) return { ok: false, fallback: field === 'video' ? 'document' : null, error: r.description || '' };
    sent = { field, result: r.result };
  }
  if (!sent) return { ok: false, error: 'telegram' };

  const fid = fileIdOf(sent.result, sent.field);
  if (!fid) return { ok: false, error: 'file_id' };
  const msgId = sent.result.message_id;
  await Promise.all([
    kput(env, partKey(draftId, part), JSON.stringify({ kind, method: sent.field, fid, stg: msgId, s: len }), DRAFT_TTL, { s: len }),
    kput(env, 'stg:' + msgId, '1', STAGING_KEEP, { t: Date.now() })
  ]);
  return { ok: true };
}

async function preDelete(env, q) {
  const draftId = await draftOk(env, q.get('token'));
  if (!draftId) return { ok: false, error: 'token' };
  const part = Math.floor(Number(q.get('part')));
  if (!Number.isFinite(part) || !kindOfPart(part)) return { ok: false, error: 'part' };
  await kput(env, delKey(draftId, part), '1', DRAFT_TTL);
  const rec = await kgetJson(env, partKey(draftId, part));
  if (rec && rec.stg && env.STAGING_CHAT_ID) {
    await tg(env, 'deleteMessage', { chat_id: env.STAGING_CHAT_ID, message_id: rec.stg }).catch(() => {});
  }
  return { ok: true };
}

// ================= COMMIT: пересылка по file_id в заявку =================

async function commit(env, q) {
  const draftId = await draftOk(env, q.get('token'));
  if (!draftId) return { ok: false, error: 'token' };

  // первый commit приносит номер заявки и её ключ; дальше Worker помнит их по черновику
  let claim;
  if (q.get('id')) {
    const id = Number(q.get('id')) || 0;
    if (!await keyOk(env, id, q.get('key'))) return { ok: false, error: 'key' };
    claim = { id, msg: Number(q.get('msg')) || 0, cid: String(q.get('cid') || '').replace(/[^\w\-]/g, '').slice(0, 64) };
    if (!claim.msg) claim.msg = await deskMsg(env, id);
    await kput(env, claimKey(draftId), JSON.stringify(claim), DEDUPE_TTL);
  } else {
    claim = await kgetJson(env, claimKey(draftId));
    if (!claim) return { ok: false, error: 'noclaim' };
    if (!claim.msg) claim.msg = await deskMsg(env, claim.id);
  }

  const parts = String(q.get('parts') || '').split(',').filter(Boolean)
    .map(Number).filter(p => Number.isFinite(p) && kindOfPart(p));
  if (!parts.length) return { ok: true, sent: [], missing: [] };

  const rows = await Promise.all(parts.map(async part => ({
    part,
    done: claim.cid ? await kget(env, sentKey(claim.cid, part)) : null,
    del: await kget(env, delKey(draftId, part)),
    rec: await kgetJson(env, partKey(draftId, part))
  })));

  const sent = [], missing = [], failed = [];
  const todo = [];
  const count = { photo: 0, video: 0, voice: 0 };
  for (const r of rows) {
    if (r.done) { sent.push(r.part); continue; }                    // уже переслано (повторный commit)
    if (r.del) continue;                                             // клиент удалил
    if (!r.rec) { missing.push(r.part); continue; }                  // ещё не предзагружено
    if (++count[r.rec.kind] > KINDS[r.rec.kind].commitMax) continue; // лимит 10 фото / 3 видео / 1 голос
    todo.push(r);
  }

  // v8.1: заявку ведёт Worker — вложения не шлём в группу, а кладём в заявку (одно сообщение)
  const D = desk(env);
  if (todo.length && D && await D.hasTicket(claim.id)) {
    await D.addAtts(claim.id, todo.map(r => ({ part: r.part, kind: r.rec.kind, method: r.rec.method, fid: r.rec.fid, size: r.rec.s || 0 })));
    if (claim.cid) await Promise.all(todo.map(r => kput(env, sentKey(claim.cid, r.part), '1', DEDUPE_TTL)));
    // эти сообщения служебной группы больше не чистим — file_id нужен заявке 30 дней
    if (kv(env)) await Promise.all(todo.map(r => r.rec.stg ? kv(env).delete('stg:' + r.rec.stg).catch(() => {}) : null));
    sent.push(...todo.map(r => r.part));
    return { ok: true, sent, missing, failed };
  }

  const cap = 'Заявка #' + claim.id;
  const mark = async ps => {
    sent.push(...ps);
    if (claim.cid) await Promise.all(ps.map(p => kput(env, sentKey(claim.cid, p), '1', DEDUPE_TTL)));
  };

  // фото — альбомами по file_id
  const photos = todo.filter(r => r.rec.method === 'photo');
  for (let i = 0; i < photos.length; i += 10) {
    const group = photos.slice(i, i + 10);
    let res;
    if (group.length === 1) {
      res = await tg(env, 'sendPhoto', { chat_id: env.CHAT_ID, photo: group[0].rec.fid, caption: cap, reply_parameters: { message_id: claim.msg, allow_sending_without_reply: true } });
    } else {
      res = await tg(env, 'sendMediaGroup', {
        chat_id: env.CHAT_ID,
        reply_parameters: { message_id: claim.msg, allow_sending_without_reply: true },
        media: group.map((r, j) => j ? { type: 'photo', media: r.rec.fid } : { type: 'photo', media: r.rec.fid, caption: cap })
      });
    }
    if (res.ok) await mark(group.map(r => r.part)); else failed.push(...group.map(r => r.part));
  }

  // видео, голос, документы — по одному, тем же способом, что при предзагрузке
  for (const r of todo.filter(r => r.rec.method !== 'photo')) {
    const body = { chat_id: env.CHAT_ID, caption: cap + (r.rec.kind === 'voice' ? ', голосовое' : ''),
      reply_parameters: { message_id: claim.msg, allow_sending_without_reply: true } };
    body[r.rec.method] = r.rec.fid;
    if (r.rec.method === 'video') body.supports_streaming = true;
    const res = await tg(env, methodFor(r.rec.method), body);
    if (res.ok) await mark([r.part]); else failed.push(r.part);
  }

  return { ok: !failed.length, sent, missing, failed };
}

// ================= ЗАПАСНОЙ ПУТЬ: ЗАГРУЗКА ПОСЛЕ НОМЕРА =================

async function authorize(env, q) {
  const id = Number(q.get('id')) || 0;
  const part = Math.floor(Number(q.get('part')));
  const c = {
    id,
    cid: String(q.get('cid') || '').replace(/[^\w\-]/g, '').slice(0, 64),
    part: Number.isFinite(part) && kindOfPart(part) ? part : -1,
    msg: Number(q.get('msg')) || 0
  };
  if (c.part < 0 || !await keyOk(env, id, q.get('key'))) return { error: 'key' };
  if (c.cid && await kget(env, sentKey(c.cid, c.part))) return { repeat: true };
  if (!c.msg) c.msg = await deskMsg(env, id);
  return c;
}

async function sentCheck(env, q) {
  const c = await authorize(env, q);
  return { sent: !!c.repeat };
}

async function album(req, env, q) {
  const c = await authorize(env, q);
  if (c.error) return { ok: false, error: c.error };
  if (c.repeat) return { ok: true, repeat: true };

  const form = await req.formData();
  const files = [];
  for (const [k, v] of form) if (k[0] === 'f' && typeof v !== 'string') files.push(v);
  if (!files.length) return { ok: false, error: 'empty' };
  const list = files.slice(0, 10);
  const D = desk(env);
  if (D && env.STAGING_CHAT_ID && await D.hasTicket(c.id)) {           // v8.1: в заявку, а не ответами в группу
    const add = [];
    let ok = true;
    for (let i = 0; i < list.length; i++) {
      const b = list[i];
      const sent = await sendChain(env, env.STAGING_CHAT_ID, b, b.name || 'photo.jpg', isPlainPhoto(b.type) ? ['photo'] : [], '', 0);
      const fid = sent && fileIdOf(sent.result, sent.field);
      if (fid) add.push({ part: c.part + i, kind: 'photo', method: sent.field, fid, size: b.size || 0 }); else ok = false;
    }
    if (add.length) await D.addAtts(c.id, add);
    if (ok && c.cid) await kput(env, sentKey(c.cid, c.part), '1', DEDUPE_TTL);
    return { ok };
  }
  const cap = 'Заявка #' + c.id;
  const normal = list.filter(b => isPlainPhoto(b.type));
  const other = list.filter(b => !isPlainPhoto(b.type));

  let ok = true;
  if (normal.length > 1) {
    const f = new FormData();
    f.set('chat_id', env.CHAT_ID);
    if (c.msg) f.set('reply_parameters', replyTo(c.msg));
    f.set('media', JSON.stringify(normal.map((b, i) =>
      i ? { type: 'photo', media: 'attach://p' + i } : { type: 'photo', media: 'attach://p' + i, caption: cap })));
    normal.forEach((b, i) => f.set('p' + i, b, b.name || 'photo' + i + '.jpg'));
    if (!(await tg(env, 'sendMediaGroup', f)).ok) {
      for (const b of normal) ok = !!(await sendChain(env, env.CHAT_ID, b, b.name || 'photo.jpg', ['photo'], cap, c.msg)) && ok;
    }
  } else if (normal.length === 1) {
    ok = !!await sendChain(env, env.CHAT_ID, normal[0], normal[0].name || 'photo.jpg', ['photo'], cap, c.msg);
  }
  for (const b of other) ok = !!(await sendChain(env, env.CHAT_ID, b, b.name || 'photo', [], cap, c.msg)) && ok;

  if (ok && c.cid) await kput(env, sentKey(c.cid, c.part), '1', DEDUPE_TTL);
  return { ok };
}

async function file(req, env, q) {
  const c = await authorize(env, q);
  if (c.error) return { ok: false, error: c.error };
  if (c.repeat) return { ok: true, repeat: true };

  const kind = q.get('kind');
  const name = (q.get('name') || 'file').replace(/[^\w.\-]/g, '_').slice(0, 60);
  const mime = (req.headers.get('Content-Type') || 'application/octet-stream').split(';')[0];
  const len = Number(req.headers.get('Content-Length') || 0);
  const cap = 'Заявка #' + c.id + (kind === 'voice' ? ', голосовое' : '');
  const D = desk(env);
  const toTicket = !!(D && env.STAGING_CHAT_ID && await D.hasTicket(c.id));   // v8.1: в заявку через служебную группу
  const chat = toTicket ? env.STAGING_CHAT_ID : env.CHAT_ID;

  let ok, got = null;
  if (!len || len < 5 * MB) {
    const body = await req.blob();
    const blob = body.type === mime ? body : body.slice(0, body.size, mime);
    let ways = [];
    if (kind === 'voice') ways = /webm/.test(mime) ? [] : ['voice', 'audio'];
    if (kind === 'video') ways = ['video'];
    if (kind === 'photo') ways = isPlainPhoto(mime) ? ['photo'] : [];
    got = await sendChain(env, chat, blob, name, ways, toTicket ? '' : cap, toTicket ? 0 : c.msg);
    ok = !!got;
  } else {
    const field = q.get('as') === 'document' || kind !== 'video' ? 'document' : 'video';
    const fields = toTicket ? {} : { caption: cap };
    if (c.msg && !toTicket) fields.reply_parameters = replyTo(c.msg);
    if (field === 'video') fields.supports_streaming = 'true';
    const r = await streamUpload(env, req, chat, field, name, mime, len, fields);
    ok = !!r.ok;
    if (!ok) return { ok: false, fallback: field === 'video' ? 'document' : null, error: r.description || '' };
    got = { field, result: r.result };
  }
  if (ok && toTicket) {
    const fid = fileIdOf(got.result, got.field);
    if (fid) await D.addAtts(c.id, [{ part: c.part, kind: kindOfPart(c.part) || kind, method: got.field, fid, size: len }]);
  }
  if (ok && c.cid) await kput(env, sentKey(c.cid, c.part), '1', DEDUPE_TTL);
  return { ok };
}

// ================= ЧИСТКА СЛУЖЕБНОЙ ГРУППЫ (Cron) =================

async function cleanStaging(env) {
  if (!kv(env) || !env.STAGING_CHAT_ID) return;
  const minAge = env.STAGING_MIN_AGE_MS != null ? Number(env.STAGING_MIN_AGE_MS) : 3 * 3600 * 1000;
  const cutoff = Date.now() - minAge;                     // моложе 3 часов не трогаем: черновик может быть ещё жив
  const ids = [];
  let cursor;
  do {
    const page = await kv(env).list({ prefix: 'stg:', cursor });
    for (const k of page.keys) {
      if (k.metadata && k.metadata.t < cutoff) ids.push(Number(k.name.slice(4)));
    }
    cursor = page.list_complete ? null : page.cursor;
  } while (cursor && ids.length < 5000);
  // deleteMessages — до 100 сообщений за один запрос; уже удалённые Telegram пропускает
  for (let i = 0; i < ids.length && i < 4000; i += 100) {
    await tg(env, 'deleteMessages', { chat_id: env.STAGING_CHAT_ID, message_ids: ids.slice(i, i + 100) });
  }
}


// ================= ЗАЯВКА: ВХОД ЧЕРЕЗ WORKER (v6) =================

// locationHint: счётчик создаётся там, где к нему обратились впервые, и больше не переезжает.
// Первым его вызовет setup() с серверов Google в США — без подсказки счётчик оказался бы за океаном.
// 'eeur' — Восточная Европа, ближе к Витебску (подсказка действует только при самом первом создании).
const desk = env => env.DESK ? env.DESK.get(env.DESK.idFromName('main'), { locationHint: 'eeur' }) : null;
const cleanCid = v => String(v || '').replace(/[^\w\-]/g, '').slice(0, 64);

// msgId сообщения о заявке: Durable Object отправляет его в фоне — подождём до 4 с
async function deskMsg(env, id) {
  const d = desk(env);
  if (!d || !id) return 0;
  try {
    if (await d.hasTicket(Number(id))) return 0;                     // v8.1: вложения идут в саму заявку — ждать нечего
    return Number(await d.getMsg(Number(id), 4000)) || 0;
  } catch (_) { return 0; }
}

async function readJson(req, max) {
  if (Number(req.headers.get('Content-Length') || 0) > max) throw new Error('size');
  const text = await req.text();
  if (text.length > max) throw new Error('size');
  return JSON.parse(text);
}

async function claimRoute(req, env, ctx) {
  const d = await readJson(req, 64 * 1024);
  if (d.website) return { ok: true, id: 0 };                         // ловушка для ботов
  const t = cleanTicket(d);
  if (!t.category || (t.code !== 'change' && t.phone.length < 10)) return { ok: false, error: 'empty' };   // размен — без телефона
  const D = desk(env);
  if (!D) return { ok: false, error: 'desk' };
  const cid = cleanCid(d.clientId) || crypto.randomUUID().replace(/-/g, '');
  const limits = {
    ip: req.headers.get('CF-Connecting-IP') || 'local',
    phone: t.phone,
    perIp: Number(env.CLAIMS_PER_IP) || CLAIMS_PER_IP,
    perPhone: Number(env.CLAIMS_PER_PHONE) || CLAIMS_PER_PHONE
  };
  const r = await D.claim(cid, d, limits);
  if (r.error === 'limit') return { ok: false, error: 'limit' };   // в Google не уходим: там тоже лимит
  if (r.error) return { ok: false, error: r.error };                // страница уйдёт в Google
  if (r.dup) return { ok: true, id: r.id, dup: true };              // «размен» уже сообщили — в группу не дублируем
  ctx.waitUntil(D.process(r.id));                                   // Telegram и таблица — после ответа
  return { ok: true, id: r.id, uploadKey: r.uploadKey, msgId: r.msgId || null, repeat: r.repeat || undefined, first: r.first || undefined };
}

// ---------- v8: «Не работает размен» и просьба об отзыве ----------

const cleanPhone = v => {
  let d = String(v || '').replace(/\D/g, '').slice(0, 15);
  if (/^80\d{9}$/.test(d)) d = '375' + d.slice(2);
  if (/^\d{9}$/.test(d)) d = '375' + d;
  return d.length >= 10 ? '+' + d : '';
};
const shortSig = async (env, kind, id) => (await hmac(env, kind + '.' + id)).slice(0, 10);

// телефон первого сообщившего о размене: в строку заявки (через очередь «таблица») и ответом в группу
async function changePhoneRoute(req, env, ctx) {
  const d = await readJson(req, 4096);
  const id = Math.floor(Number(d.id)) || 0;
  if (!await keyOk(env, id, d.uploadKey)) return { ok: false, error: 'key' };
  const phone = cleanPhone(d.phone);
  if (!phone) return { ok: false, error: 'phone' };
  const D = desk(env);
  const r = D ? await D.changePhone(id, phone) : { error: 'nojob' };
  if (r.error) return { ok: false, error: r.error };                // nojob — заявку вёл Google, страница отправит туда
  if (!r.repeat) {
    ctx.waitUntil(D.refresh(id));                                    // v8.1: телефон и кнопки связи — правкой сообщения
    ctx.waitUntil(D.process(id));                                    // строка в таблице получит телефон
  }
  return { ok: true };
}

async function resolvedRoute(req, env) {
  const d = await readJson(req, 4096);
  const id = Math.floor(Number(d.id)) || 0;
  if (!id || d.sig !== await hmac(env, 'resolved.' + id)) return { ok: false, error: 'sig' };
  await desk(env).resolveChange(id);
  return { ok: true };
}

// Google готовит просьбу об отзыве: текст и телефон храним, чтобы /w открыл WhatsApp сразу (без запроса в Google)
async function reviewPrepRoute(req, env) {
  const d = await readJson(req, 8192);
  const id = Math.floor(Number(d.id)) || 0;
  if (!id || d.sig !== await hmac(env, 'rp.' + id)) return { ok: false, error: 'sig' };
  const phone = String(d.phone || '').replace(/\D/g, '');
  const text = String(d.text || '').slice(0, 3000);
  if (phone.length < 10 || !text) return { ok: false, error: 'empty' };
  await desk(env).reviewPrep(id, phone, text);
  return { ok: true };
}

// короткие ссылки: /w — владелец открывает WhatsApp с просьбой; /r — клиент идёт оставлять отзыв
async function gasNote(env, action, s, id) {
  if (!env.GAS_URL) return;
  await fetch(env.GAS_URL, { method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
    body: JSON.stringify({ action, id, sig: await hmac(env, s + '.' + id) }), signal: AbortSignal.timeout(30e3) }).catch(() => {});
}
async function shortLink(env, ctx, kind, id, sig) {
  const good = sig === await shortSig(env, kind, id);
  const note = (action, s) => gasNote(env, action, s, id);
  if (kind === 'r') {                                                 // клиента ведём на отзыв в любом случае
    if (good) ctx.waitUntil(note('review_click', 'rc'));
    return Response.redirect(env.REVIEW_URL || REVIEW_URL, 302);
  }
  const rec = good ? await desk(env).reviewGet(id) : null;
  if (!rec) return new Response('Ссылка устарела — попросите отзыв из таблицы ещё раз.', { status: 404, headers: { 'Content-Type': 'text/plain;charset=utf-8' } });
  ctx.waitUntil(note('review_asked', 'ra'));
  return Response.redirect('https://wa.me/' + rec.phone + '?text=' + encodeURIComponent(rec.text), 302);
}

// ================= v8.1: ОДНА ЗАЯВКА = ОДНО СООБЩЕНИЕ =================

const selfUrl = env => (env.PUBLIC_URL || PUBLIC_URL).replace(/\/+$/, '');
const sig10 = async (env, text) => (await hmac(env, text)).slice(0, 10);
const htmlEsc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const NO_STORE = { 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex, nofollow' };
function page(title, body, status = 200) {
  return new Response(`<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex, nofollow">
<title>${htmlEsc(title)}</title><style>
:root{color-scheme:dark}body{margin:0;background:#1a1c1e;color:#f2f2f2;font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:560px;margin:0 auto;padding:16px}h1{font-size:20px;margin:4px 0 14px}
.btn{display:block;box-sizing:border-box;width:100%;margin:10px 0;padding:15px 14px;border:0;border-radius:8px;background:#f5e11c;color:#111;
 font:700 17px system-ui,sans-serif;text-align:center;text-decoration:none;cursor:pointer}
.btn.alt{background:#2b2e32;color:#f2f2f2;border:1px solid #444}
.txt{white-space:pre-wrap;background:#232528;border:1px solid #3a3d41;border-radius:8px;padding:12px;font-size:16px}
.item{margin:0 0 18px}.item img,.item video{display:block;width:100%;border-radius:8px;background:#000}.item audio{width:100%}
.lbl{color:#9aa0a6;font-size:13px;margin:0 0 6px}.ok{color:#7ddc8c}
</style></head><body><main>${body}</main></body></html>`, { status, headers: { 'Content-Type': 'text/html;charset=utf-8', ...NO_STORE } });
}

// Google: что изменилось в заявке → Worker правит одно сообщение (и при необходимости даёт сигнал 🔔)
async function ticketUpdateRoute(req, env) {
  const d = await readJson(req, 16384);
  const id = Math.floor(Number(d.id)) || 0;
  if (!id || d.sig !== await hmac(env, 'tu.' + id)) return { ok: false, error: 'sig' };
  const D = desk(env);
  if (!D || !await D.hasTicket(id)) return { ok: false, error: 'noticket' };   // заявку вёл Google — он и пишет по-старому
  return await D.update(id, d.set && typeof d.set === 'object' ? d.set : {});
}

// «📎 Все вложения»
async function galleryPage(env, id, sig) {
  if (sig !== await sig10(env, 'g.' + id)) return page('Ссылка устарела', '<h1>Ссылка устарела</h1>', 404);
  const D = desk(env);
  const atts = D ? await D.attsOf(id) : [];
  if (!atts.length) return page('Вложений нет', '<h1>Вложений нет или ссылка устарела</h1><p>Вложения хранятся ' + LINK_DAYS + ' дней.</p>', 404);
  const items = [];
  for (const a of atts) {
    const f = selfUrl(env) + '/f/' + id + '.' + a.part + '.' + await sig10(env, 'f.' + id + '.' + a.part);
    const big = a.size > 20 * MB;
    const send = `<form method="post" action="${selfUrl(env)}/gsend/${id}.${a.part}.${await sig10(env, 's.' + id + '.' + a.part)}">` +
      `<button class="btn alt" type="submit">Прислать в группу</button></form>`;
    const name = a.kind === 'photo' ? 'Фото' : a.kind === 'video' ? 'Видео' : 'Голосовое';
    let body;
    if (big) body = `<p>${name} больше 20 МБ — открыть здесь нельзя.</p>` + send;
    else if (a.kind === 'photo' && a.method === 'photo') body = `<a href="${f}"><img loading="lazy" src="${f}" alt="${name}"></a>`;
    else if (a.kind === 'video') body = `<video controls playsinline preload="metadata" src="${f}"></video>` + (a.size ? '' : send);
    else if (a.kind === 'voice') body = `<audio controls preload="none" src="${f}"></audio>`;
    else body = `<a class="btn alt" href="${f}">Открыть файл</a>`;
    items.push(`<div class="item"><p class="lbl">${name} ${items.length + 1} из ${atts.length}</p>${body}</div>`);
  }
  return page('Заявка #' + id + ' — вложения', `<h1>Заявка #${id} — вложения (${atts.length})</h1>` + items.join(''));
}

// файл вложения: getFile → поток с сервера Telegram (Range — чтобы видео играло на iPhone)
async function fileProxy(req, env, id, part, sig) {
  if (sig !== await sig10(env, 'f.' + id + '.' + part)) return new Response('Ссылка устарела', { status: 404, headers: NO_STORE });
  const a = await desk(env).attGet(id, part);
  if (!a) return new Response('Нет файла', { status: 404, headers: NO_STORE });
  const g = await tg(env, 'getFile', { file_id: a.fid });
  if (!g.ok || !g.result || !g.result.file_path) return new Response('Файл больше 20 МБ — нажмите «Прислать в группу»', { status: 404, headers: { 'Content-Type': 'text/plain;charset=utf-8', ...NO_STORE } });
  const base = (env.TG_API || 'https://api.telegram.org');
  const h = {};
  if (req.headers.get('Range')) h.Range = req.headers.get('Range');
  const r = await fetch(base + '/file/bot' + env.BOT_TOKEN + '/' + g.result.file_path, { headers: h });
  const out = new Headers(NO_STORE);
  ['Content-Type', 'Content-Length', 'Content-Range', 'Accept-Ranges'].forEach(k => { if (r.headers.get(k)) out.set(k, r.headers.get(k)); });
  if (!out.get('Content-Type') || out.get('Content-Type') === 'application/octet-stream') {
    const t = { photo: 'image/jpeg', video: 'video/mp4', voice: 'audio/ogg' }[a.kind];
    if (t && a.method !== 'document') out.set('Content-Type', t);
  }
  return new Response(r.body, { status: r.status, headers: out });
}

// «Прислать в группу» — единственный случай отдельного сообщения, и только по нажатию
async function gallerySend(env, id, part, sig) {
  if (sig !== await sig10(env, 's.' + id + '.' + part)) return page('Ссылка устарела', '<h1>Ссылка устарела</h1>', 404);
  const D = desk(env);
  const a = await D.attGet(id, part), msg = await D.ticketMsg(id);
  if (!a) return page('Нет файла', '<h1>Файл не найден</h1>', 404);
  const body = { chat_id: env.CHAT_ID, caption: 'Заявка #' + id, reply_parameters: { message_id: msg, allow_sending_without_reply: true } };
  body[a.method] = a.fid;
  if (a.method === 'video') body.supports_streaming = true;
  const r = await tg(env, methodFor(a.method), body);
  return page('Готово', r.ok ? '<h1 class="ok">Отправлено в группу ✓</h1><p>Видео пришло ответом на заявку.</p>'
    : '<h1>Не отправилось</h1><p>Попробуйте ещё раз.</p>');
}

// связь с клиентом — по способу, который он выбрал
async function contactRoute(env, ctx, id, sig, q) {
  if (sig !== await sig10(env, 'c.' + id)) return page('Ссылка устарела', '<h1>Ссылка устарела</h1>', 404);
  const D = desk(env);
  const t = await D.contactInfo(id);
  if (!t || !t.phone) return page('Нет телефона', '<h1>У заявки нет телефона</h1>', 404);
  const digits = t.phone.replace(/\D/g, ''), plus = '+' + digits;
  const kind = ['hello', 'bonus', 'review'].includes(q.get('t')) ? q.get('t') : 'hello';
  const text = (t.texts && t.texts[kind]) || HELLO_TEXT(id);
  if (q.get('a') === 'call') {
    return page('Позвонить', `<h1>Заявка #${id}</h1><a class="btn" href="tel:${htmlEsc(plus)}">📞 Позвонить ${htmlEsc(plus)}</a>`);
  }
  if (kind === 'review') ctx.waitUntil(D.reviewAsked(id).then(n => n && gasNote(env, 'review_asked', 'ra', id)));   // «Отзыв запрошен» — при нажатии
  const pref = { 'WhatsApp': 'wa', 'Viber': 'viber', 'Звонок': 'sms' }[t.contact] || '';
  const via = ['wa', 'viber', 'sms'].includes(q.get('via')) ? q.get('via') : pref;
  const wa = 'https://wa.me/' + digits + '?text=' + encodeURIComponent(text);
  if (via === 'wa') return Response.redirect(wa, 302);
  const viber = 'viber://chat?number=' + encodeURIComponent(plus);
  const smsSep = '__SMS__';
  const btns = {
    viber: `<button class="btn" type="button" onclick="go('${viber}')">💬 Скопировать и открыть Viber</button>`,
    sms: `<a class="btn" id="sms" href="sms:${htmlEsc(plus)}${smsSep}body=${encodeURIComponent(text)}">💬 Отправить SMS</a>`,
    wa: `<a class="btn" href="${htmlEsc(wa)}">💬 WhatsApp</a>`,
    call: `<a class="btn" href="tel:${htmlEsc(plus)}">📞 Позвонить</a>`
  };
  const first = via === 'viber' ? btns.viber : via === 'sms' ? btns.sms : '';
  const others = ['wa', 'viber', 'sms', 'call'].filter(k => k !== via).map(k => btns[k].replace('class="btn"', 'class="btn alt"')).join('');
  return page('Написать клиенту', `<h1>Заявка #${id} · ${htmlEsc(plus)}</h1>
<div class="txt" id="t">${htmlEsc(text)}</div>
<button class="btn alt" type="button" onclick="copy()">Скопировать текст</button>${first}
<p class="lbl" style="margin-top:18px">${first ? 'Другой способ:' : 'Способ связи:'}</p>${others}
<p class="lbl" id="msg"></p>
<script>
const T = document.getElementById('t').textContent;
// SMS: на iPhone текст передаётся через «&body=», на Android — через «?body=»
document.querySelectorAll('a[href^="sms:"]').forEach(a => { a.href = a.getAttribute('href').replace('${smsSep}', /iPhone|iPad|iPod/.test(navigator.userAgent) ? '&' : '?'); });
async function copy(){
  try { await navigator.clipboard.writeText(T); }
  catch (_) { const r = document.createRange(); r.selectNodeContents(document.getElementById('t')); const s = getSelection(); s.removeAllRanges(); s.addRange(r); document.execCommand('copy'); }
  document.getElementById('msg').textContent = 'Текст скопирован — вставьте его в чат.';
}
async function go(u){ await copy(); location.href = u; }
</script>`);
}

// событие статистики: тело читаем сразу (после ответа оно уже недоступно), в Google — после ответа
async function eventRoute(req, env, ctx) {
  let body = '';
  try { body = await req.text(); } catch (_) {}
  let action = '';
  try { action = JSON.parse(body).action; } catch (_) {}
  if ((action === 'event' || action === 'promo' || action === 'plate') && body.length < 4096 && env.GAS_URL) {
    ctx.waitUntil(fetch(env.GAS_URL, { method: 'POST', body, headers: { 'Content-Type': 'text/plain;charset=utf-8' }, signal: AbortSignal.timeout(30e3) }).catch(() => {}));
  }
  return new Response(null, { status: 204, headers: CORS });
}

// Google (запасной путь или старые страницы) берёт номер у общего счётчика
async function nextRoute(req, env) {
  const d = await readJson(req, 4096);
  const cid = cleanCid(d.cid);
  if (!cid || d.sig !== await hmac(env, 'next.' + cid)) return { ok: false, error: 'sig' };
  return Object.assign({ ok: true }, await desk(env).next(cid));   // nosync → { ok: false }
}

async function syncRoute(req, env) {
  const d = await readJson(req, 4096);
  const last = Math.floor(Number(d.last) || 0);
  if (d.sig !== await hmac(env, 'sync.' + last)) return { ok: false, error: 'sig' };
  const n = await desk(env).sync(last);
  return { ok: true, last: n, gas: await probeGas(env), gasUrl: env.GAS_URL || '' };
}

// для setup(): задан ли GAS_URL и отвечает ли по нему новая версия Code.gs
//   none — переменной нет; ok — отвечает v6; old — отвечает, но старая версия развертывания;
//   error — не отвечает или ответ не JSON
async function probeGas(env) {
  if (!env.GAS_URL) return 'none';
  try {
    const r = await fetch(env.GAS_URL, { method: 'POST', body: JSON.stringify({ action: 'ping' }),
      headers: { 'Content-Type': 'text/plain;charset=utf-8' }, signal: AbortSignal.timeout(20e3) });
    const j = await r.json();
    return j && j.ok === true && j.pong === true ? 'ok' : 'old';
  } catch (_) { return 'error'; }
}

// ключ заявки — в том же формате, что выдаёт Google: nonce.срок.подпись(id.nonce.срок)
async function makeKey(env, id) {
  const nonce = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
  const exp = Math.floor(Date.now() / 1000) + DEDUPE_TTL;
  return nonce + '.' + exp + '.' + await hmac(env, id + '.' + nonce + '.' + exp);
}

// ---------- текст заявки — как в Code.gs (buildText_), чтобы сообщения выглядели одинаково ----------
const CATEGORIES = {
  'money':            { sheet: 'Терминал не зачислил деньги',                     tg: 'Не зачислил деньги' },
  'program.nostart':  { sheet: 'Проблема с программой: Не включается',            tg: 'Программа — не включается' },
  'program.nosupply': { sheet: 'Проблема с программой: Нет воды, пены или воска', tg: 'Программа — нет воды, пены или воска' },
  'program.pressure': { sheet: 'Проблема с программой: Слабый напор',             tg: 'Программа — слабый напор' },
  'change':           { sheet: 'Не работает размен',                              tg: 'Не работает размен' },
  'other':            { sheet: 'Другое',                                          tg: 'Другое' }
};
const PROGRAMS = { 1: 'Предварительная мойка', 2: 'Пена', 3: 'Ополаскивание', 4: 'Воск', 5: 'Блеск + сушка' };
const COINS = [0.5, 1, 2], NOTES = [5, 10], MAX_COUNT = 20, MAX_SUM = 500, POSTS = 6;
const cut = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '').trim().slice(0, n);
const escH = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const nominal = v => v < 1 ? Math.round(v * 100) + ' к.' : v + ' р.';
const rub = x => Number(x).toFixed(2).replace('.', ',') + ' р.';

function cleanTicket(d) {
  let digits = String(d.phone || '').replace(/\D/g, '').slice(0, 15);
  if (/^80\d{9}$/.test(digits)) digits = '375' + digits.slice(2);
  const raw = cut(d.category, 100);
  const progs = [...new Set((Array.isArray(d.programs) ? d.programs : []).map(x => Math.floor(Number(x))).filter(n => PROGRAMS[n]))].sort((a, b) => a - b);
  const src = d.money && typeof d.money === 'object' ? d.money : {};
  const cnt = v => { const n = Math.floor(Number(v)); return isFinite(n) ? Math.max(0, Math.min(MAX_COUNT, n)) : 0; };
  const sum = v => { const n = Math.round(Number(v) * 100) / 100; return isFinite(n) ? Math.max(0, Math.min(MAX_SUM, n)) : 0; };
  let payText = '', total = '';
  if (d.payMethod === 'coins' || d.payMethod === 'notes') {
    const parts = []; let t = 0;
    (d.payMethod === 'coins' ? COINS : NOTES).forEach(v => { const n = cnt(src[String(v)]); if (n) { parts.push(nominal(v) + ' ×' + n); t += v * n; } });
    if (d.payMethod === 'notes') { const o = sum(src.other); if (o) { parts.push('другая ' + rub(o)); t += o; } }
    payText = (d.payMethod === 'coins' ? 'Монеты' : 'Купюры') + (parts.length ? ': ' + parts.join(', ') : '');
    total = Math.round(t * 100) / 100;
  } else if (d.payMethod === 'card') { payText = 'Карта'; total = sum(src.card); }
  const count = v => Math.max(0, Math.min(20, Math.floor(Number(v) || 0)));
  const attach = [];
  if (count(d.photos)) attach.push(count(d.photos) + ' фото');
  if (count(d.videos)) attach.push(count(d.videos) + ' видео');
  if (d.voice === true) attach.push('голосовое');
  if (count(d.skipped)) attach.push('большое видео пришлёт в Viber/WhatsApp');
  const post = /^\d+$/.test(String(d.post || '').trim()) && +d.post >= 1 && +d.post <= POSTS ? String(+d.post) : '';
  return {
    code: CATEGORIES[raw] ? raw : '',
    post, category: CATEGORIES[raw] ? CATEGORIES[raw].sheet : raw, tgCategory: CATEGORIES[raw] ? CATEGORIES[raw].tg : raw,
    programsText: progs.map(n => n + ' ' + PROGRAMS[n]).join(', '), payText, total,
    selfhelp: d.selfhelpTried === true, phone: digits ? '+' + digits : '', contact: cut(d.contact, 100),
    comment: cut(d.comment, 2000), attach: attach.join(', '), promo: d.promo === true
  };
}

// кнопка под сообщением о заявке: написать клиенту в WhatsApp с готовым началом (Viber так не умеет)
const WA_HELLO = HELLO_TEXT;
function waButton(id, phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.length < 10) return undefined;
  return { inline_keyboard: [[{ text: '💬 WhatsApp клиенту', url: 'https://wa.me/' + digits + '?text=' + encodeURIComponent(WA_HELLO(id)) }]] };
}

function ticketText(id, d) {
  if (d.code === 'change') {                                         // v8: размен — один на мойку, пост — где сканировали
    const L = ['🪙 <b>Не работает размен</b>' + (d.post ? ' (сканировали с поста ' + escH(d.post) + ')' : ''), 'Заявка #' + id];
    if (d.phone) L.push('<b>Телефон:</b> ' + escH(d.phone) + ' · 🎁 бонус при подтверждении');
    return L.join('\n');
  }
  const make = comment => {
    const L = ['🔴 <b>Заявка #' + id + '</b>' + (d.post ? ' / Пост ' + escH(d.post) : '')];
    L.push('<b>Проблема:</b> ' + escH(d.tgCategory || d.category));
    if (d.payText) {
      const i = d.payText.indexOf(':'), t = d.total !== '' && d.total != null ? rub(d.total) : '';
      L.push(i >= 0 ? '<b>' + escH(d.payText.slice(0, i)) + ':</b> ' + escH(d.payText.slice(i + 2)) + (t ? ' = ' + t : '')
                    : '<b>' + escH(d.payText) + ':</b> ' + (t || '-'));
    }
    if (d.programsText) L.push('<b>Программы:</b> ' + escH(d.programsText));
    if (d.selfhelp) L.push('<b>Пробовал самопомощь:</b> да');
    L.push('<b>Телефон:</b> ' + escH(d.phone));
    if (d.contact) L.push('<b>Связь:</b> ' + escH(d.contact));
    if (comment) L.push('<b>Текст:</b> ' + escH(comment));
    if (d.attach) L.push('<b>Вложения:</b> ' + escH(d.attach));
    if (d.promo) L.push('Согласен на акции');
    return L.join('\n');
  };
  const c = String(d.comment || '');
  let text = make(c);
  if (text.length > 4096) {                                          // укорачиваем только комментарий
    let lo = 0, hi = c.length;
    while (lo < hi) { const mid = Math.ceil((lo + hi) / 2); if (make(c.slice(0, mid) + '…').length <= 4096) lo = mid; else hi = mid - 1; }
    text = make(lo ? c.slice(0, lo) + '…' : '');
  }
  return text.slice(0, 4096);
}

// ---------- v8.1: текст/подпись и кнопки одного сообщения заявки ----------
// сколько вложений обещала страница (большое видео «пришлю в Viber» не считается)
function expectedAtts(d) {
  const n = v => Math.max(0, Math.min(20, Math.floor(Number(v) || 0)));
  return Math.min(n(d.photos), KINDS.photo.commitMax) + Math.min(n(d.videos), KINDS.video.commitMax) + (d.voice === true ? 1 : 0);
}
const MINSK = 3 * 3600e3;                                             // Минск — UTC+3 круглый год
const fmtDay = ms => { const d = new Date(ms + MINSK); return String(d.getUTCDate()).padStart(2, '0') + '.' + String(d.getUTCMonth() + 1).padStart(2, '0'); };

// строки, которые приходят потом (авто, подсказки, бонус, отзыв)
function extraLines(ex) {
  const L = [];
  if (ex.plate) L.push('<b>Авто:</b> ' + escH(ex.plate));
  (ex.hints || []).forEach(h => L.push(escH(h)));
  const b = ex.bonus;
  if (b && b.code) {
    let s = '🎁 Бонус ' + escH(rub(b.sum || 0)) + ' · код ' + escH(b.code) + ' · ';
    if (b.given) s += 'выдан ' + escH(b.given);
    else if (b.claimed) s += '<b>клиент на посту ' + escH(b.claimed) + ' — зачислите</b>';
    else s += 'не выдан';
    L.push(s);
  }
  const r = ex.review;
  if (r && r.asked) L.push('⭐ Отзыв запрошен ' + escH(r.asked));
  else if (r && r.due) L.push('⭐ <b>Пора попросить отзыв</b>');
  return L;
}

function renderTicket(id, d, ex, max) {
  const icon = STATUS_ICON[ex.status] || STATUS_ICON['Новая'];
  const head = icon + ' ' + escH(STATUS_ICON[ex.status] ? ex.status : 'Новая') + ' · ';
  const more = extraLines(ex);
  const make = comment => {
    const L = [];
    if (d.code === 'change') {
      L.push(head + '🪙 <b>Не работает размен</b>' + (d.post ? ' (сканировали с поста ' + escH(d.post) + ')' : ''), 'Заявка #' + id);
      if (d.phone) L.push('<b>Телефон:</b> ' + escH(d.phone) + ' · 🎁 бонус при подтверждении');
      if (d.contact && d.phone) L.push('<b>Связь:</b> ' + escH(d.contact));
    } else {
      L.push(head + '<b>Заявка #' + id + '</b>' + (d.post ? ' / Пост ' + escH(d.post) : ''));
      L.push('<b>Проблема:</b> ' + escH(d.tgCategory || d.category));
      if (d.payText) {
        const i = d.payText.indexOf(':'), t = d.total !== '' && d.total != null ? rub(d.total) : '';
        L.push(i >= 0 ? '<b>' + escH(d.payText.slice(0, i)) + ':</b> ' + escH(d.payText.slice(i + 2)) + (t ? ' = ' + t : '')
                      : '<b>' + escH(d.payText) + ':</b> ' + (t || '-'));
      }
      if (d.programsText) L.push('<b>Программы:</b> ' + escH(d.programsText));
      if (d.selfhelp) L.push('<b>Пробовал самопомощь:</b> да');
      L.push('<b>Телефон:</b> ' + escH(d.phone));
      if (d.contact) L.push('<b>Связь:</b> ' + escH(d.contact));
      if (comment) L.push('<b>Текст:</b> ' + escH(comment));
      if (d.attach) L.push('<b>Вложения:</b> ' + escH(d.attach));
      if (d.promo) L.push('Согласен на акции');
    }
    return L.concat(more).join('\n');
  };
  // Telegram считает длину без HTML-разметки
  const len = h => h.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&').length;
  const c = String(d.comment || '');
  let text = make(c);
  if (len(text) > max) {                                             // укорачиваем только комментарий клиента
    let lo = 0, hi = c.length;
    while (lo < hi) { const mid = Math.ceil((lo + hi) / 2); if (len(make(c.slice(0, mid).trimEnd() + CUT_TAIL)) <= max) lo = mid; else hi = mid - 1; }
    text = make(c.slice(0, lo).trimEnd() + CUT_TAIL);
    if (len(text) > max) text = make('');                              // совсем не влезает — без комментария (он есть в таблице)
  }
  return text;
}

// кнопки: связь по способу, который выбрал клиент; «📎 Все вложения»; «Отправить код»; «Попросить отзыв»
function ticketKeyboard(d, ex, c, g, attN, showAtt) {
  const rows = [];
  if (String(d.phone || '').replace(/\D/g, '').length >= 10) {
    const msg = (t, via) => c + '?a=msg&t=' + t + (via ? '&via=' + via : '');
    const pref = { 'WhatsApp': 'wa', 'Viber': 'viber', 'Звонок': 'sms' }[d.contact] || '';
    if (pref === 'wa') rows.push([{ text: '💬 WhatsApp', url: msg('hello', 'wa') }]);
    else if (pref === 'viber') rows.push([{ text: '💬 Viber', url: msg('hello', 'viber') }]);
    else if (pref === 'sms') rows.push([{ text: '💬 SMS', url: msg('hello', 'sms') }, { text: '📞 Позвонить', url: c + '?a=call' }]);
    else rows.push([{ text: '💬 WhatsApp', url: msg('hello', 'wa') }, { text: '💬 Viber', url: msg('hello', 'viber') }, { text: '📞 Позвонить', url: c + '?a=call' }]);
    if (ex.bonus && ex.bonus.code && ex.bonus.text && !ex.bonus.given) rows.push([{ text: '🎁 Отправить код', url: msg('bonus') }]);
    if (ex.review && ex.review.due && ex.review.text && !ex.review.asked) rows.push([{ text: '⭐ Попросить отзыв', url: msg('review') }]);
  }
  if (showAtt) rows.push([{ text: '📎 Все вложения (' + attN + ')', url: g }]);
  return rows.length ? { inline_keyboard: rows } : { inline_keyboard: [] };
}

// ================= DURABLE OBJECT: счётчик номеров и очередь «Telegram + таблица» =================
// Один экземпляр («main»). Номер выдаётся синхронной операцией SQLite — без await внутри, поэтому
// две одновременные заявки никогда не получат один номер.
//
// Сроки повторов:
//   Telegram — до 24 ч; дальше сообщение досылает Google (строки с отметкой «Worker» старше 25 ч);
//   строка в таблицу — до 7 дней (например, забыли GAS_URL: после исправления строки дойдут сами);
//   через 24 ч без строки в таблице — одно предупреждение в группу ответом на заявку.
const DAY = 86400e3;
const TG_WINDOW = DAY, GS_WINDOW = 7 * DAY;
const FB_FIRST = 9001, FB_LAST = 9999;
const FIRST_WINDOW = 3 * 3600e3;    // «первым сообщили»: такой же проблемы на этом посту не было 3 часа
const backoff = n => [15e3, 30e3, 60e3, 120e3, 300e3, 600e3, 1800e3][Math.max(0, n)] || 3600e3;

export class TicketDesk extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
      CREATE TABLE IF NOT EXISTS claims(cid TEXT PRIMARY KEY, id INTEGER NOT NULL, ukey TEXT, ts INTEGER);
      CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY, cid TEXT, body TEXT, msg INTEGER,
        tg INTEGER DEFAULT 0, gs INTEGER DEFAULT 0, ttries INTEGER DEFAULT 0, gtries INTEGER DEFAULT 0,
        tbusy INTEGER DEFAULT 0, gbusy INTEGER DEFAULT 0, next_at INTEGER DEFAULT 0, ts INTEGER,
        warned INTEGER DEFAULT 0);
      CREATE TABLE IF NOT EXISTS hits(k TEXT NOT NULL, ts INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS recent(post TEXT, cat TEXT, ts INTEGER);
      CREATE TABLE IF NOT EXISTS reviews(id INTEGER PRIMARY KEY, phone TEXT, text TEXT, ts INTEGER);
      CREATE TABLE IF NOT EXISTS tix(id INTEGER PRIMARY KEY, body TEXT, msg INTEGER DEFAULT 0, media TEXT DEFAULT '',
        mpart INTEGER DEFAULT 0, ex TEXT DEFAULT '{}', ts INTEGER);
      CREATE TABLE IF NOT EXISTS atts(id INTEGER NOT NULL, part INTEGER NOT NULL, kind TEXT, method TEXT, fid TEXT,
        size INTEGER DEFAULT 0, ts INTEGER, PRIMARY KEY(id, part));
      CREATE TABLE IF NOT EXISTS signals(msg INTEGER PRIMARY KEY, del_at INTEGER);
      CREATE INDEX IF NOT EXISTS hits_k ON hits(k, ts);`);
    // счётчик, созданный до этой версии: досоздаём колонку предупреждения
    try { this.sql.exec('ALTER TABLE jobs ADD COLUMN warned INTEGER DEFAULT 0'); } catch (_) {}
    try { this.sql.exec('ALTER TABLE claims ADD COLUMN first INTEGER DEFAULT 0'); } catch (_) {}
  }
  one(q, ...a) { return this.sql.exec(q, ...a).toArray()[0] || null; }
  last() { const r = this.one("SELECT v FROM meta WHERE k='last'"); return r ? Number(r.v) : 0; }
  setLast(n) { this.sql.exec("INSERT INTO meta(k, v) VALUES('last', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v", String(n)); }

  // номер по clientId: повтор — тот же номер; новый — следующий (всё синхронно, атомарно)
  take(cid) {
    const r = this.one('SELECT id, ukey FROM claims WHERE cid = ?', cid);
    if (r) return { id: r.id, ukey: r.ukey, repeat: true };
    let id = this.last() + 1;
    if (id >= FB_FIRST && id <= FB_LAST) id = FB_LAST + 1;           // 9001–9999 — запасные номера Google
    this.setLast(id);
    this.sql.exec('INSERT INTO claims(cid, id, ukey, ts) VALUES(?, ?, NULL, ?)', cid, id, Date.now());
    return { id, ukey: null, repeat: false };
  }

  // пока счётчик не сверен с таблицей (setup() в Google), номера не выдаём — иначе возможны повторы
  synced() { return !!this.one("SELECT v FROM meta WHERE k='last'"); }

  // лимит за час по IP и по телефону (синхронно: проверка и учёт без await между ними)
  limited(l) {
    if (!l) return false;
    const now = Date.now(), from = now - 3600e3;
    this.sql.exec('DELETE FROM hits WHERE ts < ?', now - 2 * 3600e3);
    const n = k => this.one('SELECT COUNT(*) AS n FROM hits WHERE k = ? AND ts > ?', k, from).n;
    const ipK = 'ip:' + l.ip, phK = 'ph:' + l.phone;
    if (n(ipK) >= l.perIp || (l.phone && n(phK) >= l.perPhone)) return true;
    this.sql.exec('INSERT INTO hits(k, ts) VALUES(?, ?)', ipK, now);
    if (l.phone) this.sql.exec('INSERT INTO hits(k, ts) VALUES(?, ?)', phK, now);
    return false;
  }

  // v8: открытая заявка «Не работает размен» (пока не «Решено» и не дольше CHANGE_WINDOW)
  chgOpen() {
    const r = this.one("SELECT v FROM meta WHERE k='chg'");
    let o = null;
    try { o = r ? JSON.parse(r.v) : null; } catch (_) {}
    return o && Date.now() - o.ts < CHANGE_WINDOW ? o : null;
  }
  resolveChange(id) {
    const o = this.chgOpen();
    if (o && o.id === Number(id)) this.sql.exec("DELETE FROM meta WHERE k='chg'");
  }
  // телефон первого сообщившего: в тело заявки и заново в таблицу
  changePhone(id, phone) {
    const j = this.one('SELECT body, msg FROM jobs WHERE id = ?', id);
    if (!j) return { error: 'nojob' };
    const d = JSON.parse(j.body);
    if (cleanTicket(d).code !== 'change') return { error: 'nojob' };
    if (d.phone) return { repeat: true, msg: j.msg };
    d.phone = phone;
    this.sql.exec('UPDATE jobs SET body = ?, gs = 0, gtries = 0 WHERE id = ?', JSON.stringify(d), id);
    const tk = this.one('SELECT body FROM tix WHERE id = ?', id);
    if (tk) { const b = JSON.parse(tk.body); b.phone = phone; this.sql.exec('UPDATE tix SET body = ? WHERE id = ?', JSON.stringify(b), id); }
    return { msg: j.msg };
  }
  reviewPrep(id, phone, text) {
    this.sql.exec('INSERT INTO reviews(id, phone, text, ts) VALUES(?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET phone = excluded.phone, text = excluded.text, ts = excluded.ts',
      id, phone, text, Date.now());
  }
  reviewGet(id) { return this.one('SELECT phone, text FROM reviews WHERE id = ?', id); }

  async claim(cid, payload, limits) {
    if (!this.synced()) return { error: 'nosync' };
    const isChange = cleanTicket(payload).code === 'change';
    if (isChange && !this.one('SELECT 1 AS x FROM claims WHERE cid = ?', cid)) {
      const o = this.chgOpen();
      if (o) return { id: o.id, dup: true };                         // о размене уже сообщили — ни номера, ни сообщения
    }
    // повтор той же заявки лимитом не считается
    if (!this.one('SELECT 1 AS x FROM claims WHERE cid = ?', cid) && this.limited(limits)) return { error: 'limit' };
    const t = this.take(cid);
    if (!t.repeat) {
      this.sql.exec('INSERT OR IGNORE INTO jobs(id, cid, body, ts) VALUES(?, ?, ?, ?)', t.id, cid, JSON.stringify(payload), Date.now());
      this.sql.exec("INSERT OR IGNORE INTO tix(id, body, ex, ts) VALUES(?, ?, '{}', ?)", t.id, JSON.stringify(payload), Date.now());
      if (isChange) {
        this.sql.exec("INSERT INTO meta(k, v) VALUES('chg', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v", JSON.stringify({ id: t.id, ts: Date.now() }));
        this.sql.exec('UPDATE claims SET first = 1 WHERE cid = ?', cid);
      } else if (this.isFirst(payload)) this.sql.exec('UPDATE claims SET first = 1 WHERE cid = ?', cid);
    }
    const first = !!(this.one('SELECT first FROM claims WHERE cid = ?', cid) || {}).first;
    let ukey = t.ukey;
    if (!ukey) {
      ukey = await makeKey(this.env, t.id);
      this.sql.exec('UPDATE claims SET ukey = ? WHERE cid = ? AND ukey IS NULL', ukey, cid);
      ukey = this.one('SELECT ukey FROM claims WHERE cid = ?', cid).ukey;
    }
    const j = this.one('SELECT msg FROM jobs WHERE id = ?', t.id);
    if (!t.repeat) await this.arm(Date.now() + 20e3);                 // страховка: если фоновая отправка оборвётся
    return { id: t.id, uploadKey: ukey, msgId: j && j.msg, repeat: t.repeat, first };
  }

  // «Вы первым сообщили»: по этому посту и этой проблеме за последние FIRST_WINDOW заявок не было
  isFirst(payload) {
    const t = cleanTicket(payload);
    if (!t.post || !t.category) return false;
    const now = Date.now();
    const seen = this.one('SELECT 1 AS x FROM recent WHERE post = ? AND cat = ? AND ts > ?', t.post, t.category, now - FIRST_WINDOW);
    this.sql.exec('INSERT INTO recent(post, cat, ts) VALUES(?, ?, ?)', t.post, t.category, now);
    return !seen;
  }

  // для Google: номер из общего счётчика; заявку Google ведёт сам (Telegram и таблица — на его стороне)
  next(cid) {
    if (!this.synced()) return { ok: false, error: 'nosync' };
    const t = this.take(cid);
    if (!t.repeat) return { id: t.id };
    const j = this.one('SELECT msg FROM jobs WHERE id = ?', t.id);
    return { id: t.id, repeat: true, uploadKey: t.ukey, msgId: j && j.msg, viaWorker: !!j };
  }

  sync(last) {
    if (last > this.last() || !this.synced()) this.setLast(Math.max(last, this.last()));
    return this.last();
  }

  async getMsg(id, waitMs) {
    const until = Date.now() + (waitMs || 0);
    for (;;) {
      const j = this.one('SELECT msg, tg, ttries, tbusy FROM jobs WHERE id = ?', id);
      if (!j) return 0;                                               // заявку вёл Google
      if (j.msg) return j.msg;
      if (Date.now() >= until || (!j.tbusy && j.ttries >= 2)) return 0;
      await new Promise(r => setTimeout(r, 150));
    }
  }

  // отправить в Telegram и в таблицу то, что ещё не отправлено; при сбое — будильник
  async process(id) {
    let j = this.one('SELECT * FROM jobs WHERE id = ?', id);
    if (!j) return;
    const now = Date.now();
    const age = now - j.ts;
    const d = JSON.parse(j.body);
    const tk = this.ticket(id);
    // v8.1: ждём вложения (не дольше ATT_WAIT), чтобы заявка ушла одним сообщением
    const waiting = tk && !j.ttries && age < ATT_WAIT && this.attCount(id) < expectedAtts(d);
    if (!j.tg && !waiting && age < TG_WINDOW && !(j.tbusy && now - j.tbusy < 60e3)) {
      this.sql.exec('UPDATE jobs SET tbusy = ? WHERE id = ?', now, id);
      let msg = 0;
      try {
        if (tk) msg = await this.sendTicket(id);
        else {
          const t = cleanTicket(d);
          const r = await tg(this.env, 'sendMessage', { chat_id: this.env.CHAT_ID, parse_mode: 'HTML', disable_web_page_preview: true,
            text: ticketText(id, t), reply_markup: waButton(id, t.phone) });
          if (r.ok) msg = r.result.message_id;
        }
      } catch (_) {}
      // строка в таблице уже есть без номера сообщения — отправим её ещё раз (Google обновит отметку)
      if (msg) this.sql.exec('UPDATE jobs SET tg = 1, msg = ?, tbusy = 0, gs = 0, gtries = 0 WHERE id = ?', msg, id);
      else this.sql.exec('UPDATE jobs SET tbusy = 0, ttries = ttries + 1 WHERE id = ?', id);
    }
    j = this.one('SELECT * FROM jobs WHERE id = ?', id);
    // в таблицу — сразу после попытки Telegram; если он не ответил, строка уйдёт с отметкой «Worker»,
    // а когда сообщение дойдёт, Worker пришлёт строку ещё раз — Google допишет номер сообщения
    if (!j.gs && age < GS_WINDOW && (j.tg || j.ttries >= 1 || age >= TG_WINDOW) && !(j.gbusy && now - j.gbusy < 60e3)) {
      this.sql.exec('UPDATE jobs SET gbusy = ? WHERE id = ?', Date.now(), id);
      let ok = false;
      try {
        const body = JSON.stringify({ action: 'ingest', id, clientId: j.cid, msgId: j.msg || null,
          sig: await hmac(this.env, 'ingest.' + id + '.' + j.cid), payload: d });
        const r = await fetch(this.env.GAS_URL, { method: 'POST', body, headers: { 'Content-Type': 'text/plain;charset=utf-8' },
          signal: AbortSignal.timeout(30e3) });
        ok = (await r.json()).ok === true;
      } catch (_) {}
      if (ok) this.sql.exec('UPDATE jobs SET gs = 1, gbusy = 0 WHERE id = ?', id);
      else this.sql.exec('UPDATE jobs SET gbusy = 0, gtries = gtries + 1 WHERE id = ?', id);
    }
    j = this.one('SELECT * FROM jobs WHERE id = ?', id);
    // сутки без строки в таблице — одно предупреждение в группу (ответом на заявку)
    if (!j.gs && j.tg && age >= TG_WINDOW && !j.warned) {
      const r = await tg(this.env, 'sendMessage', { chat_id: this.env.CHAT_ID,
        text: '⚠️ Заявка #' + id + ' за сутки не попала в таблицу. Проверьте GAS_URL в Cloudflare и развертывание Google. Worker продолжит попытки до 7 дней.',
        reply_parameters: { message_id: j.msg, allow_sending_without_reply: true } });
      if (r.ok) this.sql.exec('UPDATE jobs SET warned = 1 WHERE id = ?', id);
    }
    this.plan(id);
  }

  // ---------- v8.1: одна заявка = одно сообщение ----------
  ticket(id) {
    const r = this.one('SELECT * FROM tix WHERE id = ?', id);
    if (!r) return null;
    let ex = {};
    try { ex = JSON.parse(r.ex || '{}') || {}; } catch (_) {}
    return { id: r.id, d: JSON.parse(r.body), msg: r.msg, media: r.media, mpart: r.mpart, ex };
  }
  hasTicket(id) { return !!this.one('SELECT 1 AS x FROM tix WHERE id = ?', id); }
  ticketMsg(id) { const r = this.one('SELECT msg FROM tix WHERE id = ?', id); return r ? r.msg : 0; }
  attCount(id) { return this.one('SELECT COUNT(*) AS n FROM atts WHERE id = ?', id).n; }
  attsOf(id) { return this.sql.exec('SELECT part, kind, method, size FROM atts WHERE id = ? ORDER BY part', id).toArray(); }
  attGet(id, part) { return this.one('SELECT * FROM atts WHERE id = ? AND part = ?', id, part); }
  later(p) { p = Promise.resolve(p).catch(() => {}); if (this.ctx.waitUntil) this.ctx.waitUntil(p); return p; }

  // вложения заявки (из /commit или запасного пути): в таблицу вложений; сообщение — отправить или поправить
  async addAtts(id, list) {
    const now = Date.now();
    for (const a of list) this.sql.exec('INSERT OR IGNORE INTO atts(id, part, kind, method, fid, size, ts) VALUES(?, ?, ?, ?, ?, ?, ?)',
      id, a.part, a.kind, a.method, a.fid, a.size || 0, now);
    const tk = this.ticket(id);
    if (!tk) return;
    if (tk.msg) this.later(this.refresh(id));
    else if (this.attCount(id) >= expectedAtts(tk.d)) {             // всё пришло — отправляем, не дожидаясь 10 с
      this.sql.exec('UPDATE jobs SET next_at = ? WHERE id = ?', now, id);
      await this.arm(now);
    }
  }

  async parts(id, tk) {
    const d = cleanTicket(tk.d);
    const base = selfUrl(this.env);
    const c = base + '/c/' + id + '.' + await sig10(this.env, 'c.' + id);
    const g = base + '/g/' + id + '.' + await sig10(this.env, 'g.' + id);
    const n = this.attCount(id);
    const showAtt = n > 0 && !(tk.media && n === 1);
    return { d, text: renderTicket(id, d, tk.ex, tk.media ? CAPTION_MAX : TEXT_MAX), markup: ticketKeyboard(d, tk.ex, c, g, n, showAtt) };
  }

  // первое сообщение: главное вложение с подписью (первое фото; нет — видео; иначе голосовое) или текст
  async sendTicket(id) {
    const tk = this.ticket(id);
    const atts = this.sql.exec('SELECT * FROM atts WHERE id = ? ORDER BY part', id).toArray();
    const main = atts.find(a => a.kind === 'photo') || atts.find(a => a.kind === 'video') || atts.find(a => a.kind === 'voice');
    let msg = 0, media = '', mpart = 0, key = '';
    if (main) {
      const p = await this.parts(id, Object.assign({}, tk, { media: main.method }));
      const body = { chat_id: this.env.CHAT_ID, caption: p.text, parse_mode: 'HTML', reply_markup: p.markup };
      body[main.method] = main.fid;
      if (main.method === 'video') body.supports_streaming = true;
      const r = await tg(this.env, methodFor(main.method), body);
      if (r.ok) { msg = r.result.message_id; media = main.method; mpart = main.part; key = JSON.stringify([p.text, p.markup]); }
    }
    if (!msg) {                                                       // без вложений или вложение не ушло — текстом
      const p = await this.parts(id, Object.assign({}, tk, { media: '' }));
      const r = await tg(this.env, 'sendMessage', { chat_id: this.env.CHAT_ID, parse_mode: 'HTML', disable_web_page_preview: true,
        text: p.text, reply_markup: p.markup });
      if (r.ok) { msg = r.result.message_id; key = JSON.stringify([p.text, p.markup]); }
    }
    if (msg) {
      this.shown = this.shown || new Map(); this.shown.set(id, key);  // то, что уже показано, второй раз не правим
      this.sql.exec('UPDATE tix SET msg = ?, media = ?, mpart = ? WHERE id = ?', msg, media, mpart, id);
      this.later(this.refresh(id));                                   // пока отправляли, могло прийти что-то ещё
    }
    return msg;
  }

  // правка сообщения заявки; одновременные правки — по очереди, последняя всегда с актуальным состоянием
  async refresh(id) {
    this.dirty = this.dirty || new Set(); this.running = this.running || new Set();
    this.dirty.add(id);
    if (this.running.has(id)) return;
    this.running.add(id);
    try {
      while (this.dirty.has(id)) {
        this.dirty.delete(id);
        const tk = this.ticket(id);
        if (!tk || !tk.msg) return;
        const p = await this.parts(id, tk);
        const key = JSON.stringify([p.text, p.markup]);
        if (this.shown && this.shown.get(id) === key) continue;
        const body = { chat_id: this.env.CHAT_ID, message_id: tk.msg, parse_mode: 'HTML', reply_markup: p.markup };
        let r;
        if (tk.media) r = await tg(this.env, 'editMessageCaption', Object.assign(body, { caption: p.text }));
        else r = await tg(this.env, 'editMessageText', Object.assign(body, { text: p.text, disable_web_page_preview: true }));
        if (r.ok || /not modified/i.test(r.description || '')) { this.shown = this.shown || new Map(); this.shown.set(id, key); }
      }
    } finally { this.running.delete(id); }
  }

  // Google: что изменилось (авто, статус, подсказки, бонус, отзыв)
  async update(id, set) {
    const tk = this.ticket(id);
    if (!tk) return { ok: false, error: 'noticket' };
    const ex = tk.ex, sig = [];
    const str = (v, n) => cut(v == null ? '' : String(v), n);
    if ('plate' in set) ex.plate = str(set.plate, 20);
    if ('status' in set && STATUS_ICON[set.status]) ex.status = set.status;
    if ('hints' in set) ex.hints = (Array.isArray(set.hints) ? set.hints : []).slice(0, 5).map(h => str(h, 200)).filter(Boolean);
    if ('bonus' in set) {
      const b = set.bonus;
      if (!b || !str(b.code, 8)) ex.bonus = null;
      else {
        const was = ex.bonus && ex.bonus.code === str(b.code, 8) ? ex.bonus : {};
        ex.bonus = { sum: Math.max(0, Number(b.sum) || 0), code: str(b.code, 8), text: str(b.text, 2000) || was.text || '',
          claimed: was.claimed || '', given: was.given || '' };
      }
    }
    if ('bonus_claimed' in set && ex.bonus) {
      const p = str(set.bonus_claimed, 4);
      if (p && !ex.bonus.claimed && !ex.bonus.given) sig.push('🔔 Клиент на посту ' + p + ' ждёт бонус — заявка #' + id);
      ex.bonus.claimed = p;
    }
    if ('bonus_given' in set && ex.bonus) ex.bonus.given = str(set.bonus_given, 20);
    if ('review_due' in set) {
      const r = set.review_due;
      if (r && str(r.text, 3000)) {
        if (!(ex.review && ex.review.due)) sig.push('🔔 Пора попросить отзыв — заявка #' + id);
        ex.review = { due: 1, text: str(r.text, 3000), asked: (ex.review && ex.review.asked) || '' };
      } else if (!r) ex.review = null;
    }
    if ('review_asked' in set) { ex.review = ex.review || {}; ex.review.asked = str(set.review_asked, 20); }
    this.sql.exec('UPDATE tix SET ex = ? WHERE id = ?', JSON.stringify(ex), id);
    await this.refresh(id);
    for (const text of sig) await this.signal(id, text);
    return { ok: true, msg: this.ticketMsg(id) || null };
  }

  // короткий ответ-сигнал со звуком; бот удалит его сам через SIGNAL_TTL
  async signal(id, text) {
    const msg = this.ticketMsg(id);
    const body = { chat_id: this.env.CHAT_ID, text };
    if (msg) body.reply_parameters = { message_id: msg, allow_sending_without_reply: true };
    const r = await tg(this.env, 'sendMessage', body);
    if (!r.ok) return;
    const ttl = Number(this.env.SIGNAL_TTL_MS) || SIGNAL_TTL;
    this.sql.exec('INSERT OR REPLACE INTO signals(msg, del_at) VALUES(?, ?)', r.result.message_id, Date.now() + ttl);
    await this.arm(Date.now() + ttl);
  }

  contactInfo(id) {
    const tk = this.ticket(id);
    if (!tk) return null;
    const d = cleanTicket(tk.d);
    return { phone: d.phone, contact: d.contact, texts: { hello: HELLO_TEXT(id), bonus: tk.ex.bonus && tk.ex.bonus.text, review: tk.ex.review && tk.ex.review.text } };
  }

  // «⭐ Попросить отзыв» нажата: отметка в сообщении; true — первый раз (Google поставит «Отзыв запрошен»)
  async reviewAsked(id) {
    const tk = this.ticket(id);
    if (!tk || !tk.ex.review || tk.ex.review.asked) return false;
    tk.ex.review.asked = fmtDay(Date.now());
    this.sql.exec('UPDATE tix SET ex = ? WHERE id = ?', JSON.stringify(tk.ex), id);
    await this.refresh(id);
    return true;
  }

  // следующий будильник для заявки: по самому «отстающему» из незавершённых дел
  plan(id) {
    const j = this.one('SELECT * FROM jobs WHERE id = ?', id);
    if (!j) return;
    const now = Date.now(), age = now - j.ts;
    const tgDue = !j.tg && age < TG_WINDOW;
    const gsDue = !j.gs && age < GS_WINDOW;
    if (!tgDue && !gsDue) return;
    let at = now + backoff(Math.max(tgDue ? j.ttries : 0, gsDue ? j.gtries : 0) - 1);
    if (tgDue && age + (at - now) > TG_WINDOW) at = j.ts + TG_WINDOW + 1e3;   // успеть к суточной отметке
    if (tgDue && !j.ttries && age < ATT_WAIT && this.ticket(id)) at = Math.min(at, j.ts + ATT_WAIT + 100);   // v8.1: срок ожидания вложений
    if (j.tbusy && now - j.tbusy < 60e3) at = Math.min(at, j.tbusy + 61e3);   // занятость истечёт — проверим
    this.sql.exec('UPDATE jobs SET next_at = ? WHERE id = ?', at, id);
    return this.arm(at);
  }

  async arm(at) {
    const cur = await this.ctx.storage.getAlarm();
    // будильник давно просрочен (например, сорвался при выкладке новой версии) — ставим заново
    if (!cur || cur > at || cur < Date.now() - 60e3) await this.ctx.storage.setAlarm(at);
  }

  pending(now) {
    return '((tg = 0 AND ts > ' + (now - TG_WINDOW) + ') OR (gs = 0 AND ts > ' + (now - GS_WINDOW) + '))';
  }

  async alarm() {
    const now = Date.now();
    const due = this.sql.exec('SELECT id FROM jobs WHERE ' + this.pending(now) + ' AND next_at <= ?', now).toArray();
    for (const r of due) await this.process(r.id);
    this.sql.exec('DELETE FROM claims WHERE ts < ?', now - 7 * DAY);
    this.sql.exec('DELETE FROM jobs WHERE ts < ? AND ((tg = 1 AND gs = 1) OR ts < ?)', now - 7 * DAY, now - 8 * DAY);
    this.sql.exec('DELETE FROM hits WHERE ts < ?', now - 2 * 3600e3);
    this.sql.exec('DELETE FROM recent WHERE ts < ?', now - DAY);
    this.sql.exec('DELETE FROM reviews WHERE ts < ?', now - 30 * DAY);
    // v8.1: сигналы 🔔 — удалить через SIGNAL_TTL; заявки и вложения хранить LINK_DAYS дней
    const old = this.sql.exec('SELECT msg FROM signals WHERE del_at <= ?', now).toArray().map(r => r.msg);
    for (let i = 0; i < old.length; i += 100) {
      const r = await tg(this.env, 'deleteMessages', { chat_id: this.env.CHAT_ID, message_ids: old.slice(i, i + 100) });
      if (r.ok || !r.timeout) this.sql.exec('DELETE FROM signals WHERE msg IN (' + old.slice(i, i + 100).map(Number).join(',') + ')');
    }
    this.sql.exec('DELETE FROM signals WHERE del_at < ?', now - DAY);   // Telegram не удаляет старше 48 ч — не копим
    this.sql.exec('DELETE FROM tix WHERE ts < ?', now - LINK_DAYS * DAY);
    this.sql.exec('DELETE FROM atts WHERE ts < ?', now - LINK_DAYS * DAY);
    const t = Date.now();
    const nxt = this.one('SELECT MIN(next_at) AS t FROM jobs WHERE ' + this.pending(t));
    const sg = this.one('SELECT MIN(del_at) AS t FROM signals');
    const at = Math.min(nxt && nxt.t ? Math.max(nxt.t, t + 5e3) : Infinity, sg && sg.t ? Math.max(sg.t, t + 1e3) : Infinity);
    if (at < Infinity) await this.ctx.storage.setAlarm(at);
  }
}

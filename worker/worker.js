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

import { DurableObject } from 'cloudflare:workers';

const MB = 1024 * 1024;
const DRAFT_TTL = 7200;            // черновик живёт 2 часа
const DEDUPE_TTL = 21600;          // 6 часов
const STAGING_KEEP = 40 * 3600;    // служебные сообщения: удалять в окне 3–40 ч (Telegram разрешает до 48 ч)
const DRAFTS_PER_HOUR = 5;         // черновиков с одного IP в час
// заявок в час: по IP — щедро (у мобильных операторов много абонентов за одним адресом), по телефону — строго
const CLAIMS_PER_IP = 20;
const CLAIMS_PER_PHONE = 3;
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
      if (req.method !== 'POST') return new Response('ok', { headers: CORS });   // прогрев соединения
      if (path.endsWith('/claim')) return json(await claimRoute(req, env, ctx));
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
    kput(env, partKey(draftId, part), JSON.stringify({ kind, method: sent.field, fid, stg: msgId }), DRAFT_TTL, { s: len }),
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

  let ok;
  if (!len || len < 5 * MB) {
    const body = await req.blob();
    const blob = body.type === mime ? body : body.slice(0, body.size, mime);
    let ways = [];
    if (kind === 'voice') ways = /webm/.test(mime) ? [] : ['voice', 'audio'];
    if (kind === 'video') ways = ['video'];
    if (kind === 'photo') ways = isPlainPhoto(mime) ? ['photo'] : [];
    ok = !!await sendChain(env, env.CHAT_ID, blob, name, ways, cap, c.msg);
  } else {
    const field = q.get('as') === 'document' || kind !== 'video' ? 'document' : 'video';
    const fields = { caption: cap };
    if (c.msg) fields.reply_parameters = replyTo(c.msg);
    if (field === 'video') fields.supports_streaming = 'true';
    const r = await streamUpload(env, req, env.CHAT_ID, field, name, mime, len, fields);
    ok = !!r.ok;
    if (!ok) return { ok: false, fallback: field === 'video' ? 'document' : null, error: r.description || '' };
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
  try { return Number(await d.getMsg(Number(id), 4000)) || 0; } catch (_) { return 0; }
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
  if (!t.category || t.phone.length < 10) return { ok: false, error: 'empty' };
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
  ctx.waitUntil(D.process(r.id));                                   // Telegram и таблица — после ответа
  return { ok: true, id: r.id, uploadKey: r.uploadKey, msgId: r.msgId || null, repeat: r.repeat || undefined };
}

// событие статистики: тело читаем сразу (после ответа оно уже недоступно), в Google — после ответа
async function eventRoute(req, env, ctx) {
  let body = '';
  try { body = await req.text(); } catch (_) {}
  let action = '';
  try { action = JSON.parse(body).action; } catch (_) {}
  if ((action === 'event' || action === 'promo') && body.length < 4096 && env.GAS_URL) {
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
    post, category: CATEGORIES[raw] ? CATEGORIES[raw].sheet : raw, tgCategory: CATEGORIES[raw] ? CATEGORIES[raw].tg : raw,
    programsText: progs.map(n => n + ' ' + PROGRAMS[n]).join(', '), payText, total,
    selfhelp: d.selfhelpTried === true, phone: digits ? '+' + digits : '', contact: cut(d.contact, 100),
    comment: cut(d.comment, 2000), attach: attach.join(', '), promo: d.promo === true
  };
}

function ticketText(id, d) {
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
      CREATE INDEX IF NOT EXISTS hits_k ON hits(k, ts);`);
    // счётчик, созданный до этой версии: досоздаём колонку предупреждения
    try { this.sql.exec('ALTER TABLE jobs ADD COLUMN warned INTEGER DEFAULT 0'); } catch (_) {}
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

  async claim(cid, payload, limits) {
    if (!this.synced()) return { error: 'nosync' };
    // повтор той же заявки лимитом не считается
    if (!this.one('SELECT 1 AS x FROM claims WHERE cid = ?', cid) && this.limited(limits)) return { error: 'limit' };
    const t = this.take(cid);
    if (!t.repeat) this.sql.exec('INSERT OR IGNORE INTO jobs(id, cid, body, ts) VALUES(?, ?, ?, ?)', t.id, cid, JSON.stringify(payload), Date.now());
    let ukey = t.ukey;
    if (!ukey) {
      ukey = await makeKey(this.env, t.id);
      this.sql.exec('UPDATE claims SET ukey = ? WHERE cid = ? AND ukey IS NULL', ukey, cid);
      ukey = this.one('SELECT ukey FROM claims WHERE cid = ?', cid).ukey;
    }
    const j = this.one('SELECT msg FROM jobs WHERE id = ?', t.id);
    if (!t.repeat) await this.arm(Date.now() + 20e3);                 // страховка: если фоновая отправка оборвётся
    return { id: t.id, uploadKey: ukey, msgId: j && j.msg, repeat: t.repeat };
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
    if (!j.tg && age < TG_WINDOW && !(j.tbusy && now - j.tbusy < 60e3)) {
      this.sql.exec('UPDATE jobs SET tbusy = ? WHERE id = ?', now, id);
      let msg = 0;
      try {
        const r = await tg(this.env, 'sendMessage', { chat_id: this.env.CHAT_ID, parse_mode: 'HTML', disable_web_page_preview: true, text: ticketText(id, cleanTicket(d)) });
        if (r.ok) msg = r.result.message_id;
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
    const t = Date.now();
    const nxt = this.one('SELECT MIN(next_at) AS t FROM jobs WHERE ' + this.pending(t));
    if (nxt && nxt.t) await this.ctx.storage.setAlarm(Math.max(nxt.t, Date.now() + 5e3));
  }
}

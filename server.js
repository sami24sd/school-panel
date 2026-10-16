// server.js
// ------------------------------------------------------------------
// این فایل «مترجم» است: کد اصلی ورکر (worker-src.js) دست‌نخورده می‌ماند
// و همینجا شبیه‌سازهای لازم برای اجرای آن روی یک هاست Node.js ساده
// (Railway، RunFlare، یا هر جای دیگری) ساخته می‌شود:
//   - KV کلودفلر (EXAM_KV)      -> Redis (یا حافظه‌ی موقت اگر REDIS_URL نباشد)
//   - Durable Object (CLASSROOM) -> یک نمونه‌ی درون‌حافظه‌ای به‌ازای هر اتاق
//   - WebSocketPair              -> کتابخانه‌ی ws روی همان سرور HTTP
//   - Response با status:101     -> جایگزین سبک که این status را رد نمی‌کند
//
// نکته‌ی مهم: چون Durable Object اینجا فقط یک Map درون‌حافظه‌ی همین
// پردازه است (نه واقعاً توزیع‌شده)، تمام اتاق‌ها (کلاس آنلاین/وبینار/
// تماس تعاملی/تخته) باید روی یک instance واحد اجرا شوند، نه چند replica.
// روی Railway/RunFlare این یعنی: تعداد instance را روی 1 نگه دارید.
// ------------------------------------------------------------------

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import nodeCrypto from "node:crypto";
import { WebSocketServer } from "ws";
import { AsyncLocalStorage } from "node:async_hooks";

// ---------- polyfill های global لازم (قبل از import کردن ورکر) ----------
if (!globalThis.crypto) globalThis.crypto = nodeCrypto.webcrypto;
if (!globalThis.crypto.randomUUID) globalThis.crypto.randomUUID = nodeCrypto.randomUUID.bind(nodeCrypto);
// fetch / Blob / FormData / TextEncoder در Node 18+ به‌صورت global موجودند.

// ---------- Response/Headers سبک (اجازه‌ی status:101 را می‌دهد) ----------
class SimpleHeaders {
  constructor(init) {
    this.map = new Map();
    if (!init) return;
    if (init instanceof SimpleHeaders) {
      for (const [k, v] of init.map) this.map.set(k, v);
    } else if (Array.isArray(init)) {
      for (const [k, v] of init) this.set(k, v);
    } else {
      for (const k in init) this.set(k, init[k]);
    }
  }
  set(k, v) { this.map.set(String(k).toLowerCase(), String(v)); }
  get(k) { const kk = String(k).toLowerCase(); return this.map.has(kk) ? this.map.get(kk) : null; }
  has(k) { return this.map.has(String(k).toLowerCase()); }
  delete(k) { this.map.delete(String(k).toLowerCase()); }
  append(k, v) {
    const kk = String(k).toLowerCase();
    if (this.map.has(kk)) this.map.set(kk, this.map.get(kk) + ", " + v);
    else this.map.set(kk, String(v));
  }
  entries() { return this.map.entries(); }
  forEach(cb) { this.map.forEach((v, k) => cb(v, k, this)); }
  [Symbol.iterator]() { return this.map.entries(); }
}

class SimpleResponse {
  constructor(body, init = {}) {
    this.body = body === undefined ? null : body;
    this.status = init.status === undefined ? 200 : init.status;
    this.statusText = init.statusText || "";
    this.headers = new SimpleHeaders(init.headers);
    this.webSocket = init.webSocket || null; // فقط برای مسیرهای WebSocket استفاده می‌شود
    this.ok = this.status >= 200 && this.status < 300;
  }
  static redirect(url, status = 302) {
    return new SimpleResponse(null, { status, headers: { location: String(url) } });
  }
  static error() { return new SimpleResponse(null, { status: 500 }); }
  async text() {
    if (this.body == null) return "";
    if (typeof this.body === "string") return this.body;
    if (this.body instanceof Uint8Array) return Buffer.from(this.body).toString("utf8");
    return String(this.body);
  }
  async json() { return JSON.parse(await this.text()); }
  async arrayBuffer() {
    if (this.body instanceof Uint8Array) return this.body.buffer.slice(this.body.byteOffset, this.body.byteOffset + this.body.byteLength);
    if (typeof this.body === "string") { const b = Buffer.from(this.body, "utf8"); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); }
    return new ArrayBuffer(0);
  }
}
globalThis.Response = SimpleResponse;
// توجه: fetch() بومی Node همچنان Response واقعی خودش را برمی‌گرداند (این override فقط
// روی new Response(...) که خود کد ورکر می‌سازد اثر دارد، نه روی نتیجه‌ی fetch به بیرون).

// ---------- WebSocketPair (برای کلاس‌آنلاین/وبینار/تماس‌تعاملی/تخته) ----------
const wsALS = new AsyncLocalStorage();

function wrapRawWs(rawWs) {
  if (rawWs.__wrapper) return rawWs.__wrapper;
  const listeners = { message: [], close: [], error: [] };
  const wrapper = {
    accept() {}, // اتصال از قبل توسط کتابخانه‌ی ws برقرار شده
    send(data) { try { rawWs.send(data); } catch {} },
    close(code, reason) { try { rawWs.close(code, reason); } catch {} },
    addEventListener(type, cb) { if (listeners[type]) listeners[type].push(cb); },
    removeEventListener(type, cb) {
      if (!listeners[type]) return;
      const i = listeners[type].indexOf(cb);
      if (i > -1) listeners[type].splice(i, 1);
    },
  };
  Object.defineProperty(wrapper, "bufferedAmount", { get: () => rawWs.bufferedAmount || 0 });
  rawWs.on("message", (data, isBinary) => {
    const payload = isBinary ? data : data.toString("utf8");
    listeners.message.forEach((cb) => { try { cb({ data: payload }); } catch {} });
  });
  rawWs.on("close", (code, reasonBuf) => {
    listeners.close.forEach((cb) => { try { cb({ code, reason: reasonBuf ? reasonBuf.toString() : "" }); } catch {} });
  });
  rawWs.on("error", (err) => {
    listeners.error.forEach((cb) => { try { cb({ error: err }); } catch {} });
  });
  rawWs.__wrapper = wrapper;
  return wrapper;
}

class WebSocketPair {
  constructor() {
    const rawWs = wsALS.getStore();
    if (!rawWs) throw new Error("WebSocketPair used outside of a WebSocket upgrade");
    const wrapper = wrapRawWs(rawWs);
    this[0] = wrapper; // client
    this[1] = wrapper; // server (تنها چیزی که کد ورکر واقعاً استفاده می‌کند)
  }
}
globalThis.WebSocketPair = WebSocketPair;

// ---------- KV (EXAM_KV) روی Redis، با fallback به حافظه‌ی موقت ----------
function makeMemoryKv() {
  const store = new Map();
  
  return {
    async get(key) { return store.has(key) ? store.get(key) : null; },
    async put(key, value) { store.set(key, String(value)); },
    async delete(key) { store.delete(key); },
    async list({ prefix = "", cursor, limit = 1000 } = {}) {
      const all = Array.from(store.keys()).filter((k) => k.startsWith(prefix)).sort();
      const start = cursor ? Number(cursor) : 0;
      const slice = all.slice(start, start + limit);
      const done = start + limit >= all.length;
      return { keys: slice.map((name) => ({ name })), cursor: done ? "" : String(start + limit), list_complete: done };
    },
  };
}

async function makeRedisKv(redisUrl) {
  const { default: Redis } = await import("ioredis");
  const redis = new Redis(redisUrl, { lazyConnect: false, maxRetriesPerRequest: 3 });
  redis.on("error", (e) => console.error("Redis error:", e.message));
  const PFX = "kv:";
  return {
    async get(key) { return await redis.get(PFX + key); },
    async put(key, value) { await redis.set(PFX + key, String(value)); },
    async delete(key) { await redis.del(PFX + key); },
    async list({ prefix = "", cursor, limit = 1000 } = {}) {
      let cur = cursor || "0";
      const found = [];
      do {
        const [next, batch] = await redis.scan(cur, "MATCH", PFX + prefix + "*", "COUNT", 300);
        cur = next;
        found.push(...batch);
      } while (cur !== "0" && found.length < limit);
      const done = cur === "0";
      const names = found.slice(0, limit).map((k) => ({ name: k.slice(PFX.length) }));
      return { keys: names, cursor: done ? "" : cur, list_complete: done };
    },
  };
}

// ---------- ذخیره‌ی دائمی روی دیسک (برای دیسک پایدار رانفلر/Railway Volume) ----------
// هر کلید = یک فایل در DATA_DIR (نام فایل = encodeURIComponent(key)). نوشتن اتمیک (فایل موقت + rename).
// اگر DATA_DIR روی یک «دیسک پایدار» ساخته شده باشد، با ری‌استارت یا استقرار دوباره (دیپلوی از گیت‌هاب) پاک نمی‌شود.
async function makeDiskKv(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
  // خودآزمایی: آیا واقعاً می‌توان نوشت و خواند؟
  const probe = path.join(dir, ".write_test");
  await fs.promises.writeFile(probe, String(Date.now()));
  await fs.promises.readFile(probe);
  await fs.promises.unlink(probe);
  const keys = new Set();
  for (const f of await fs.promises.readdir(dir)) {
    if (f.endsWith(".tmp") || f.startsWith(".")) continue;
    try { keys.add(decodeURIComponent(f)); } catch {}
  }
  const fileOf = (key) => path.join(dir, encodeURIComponent(key));
  console.log(`[STORAGE] disk dir=${dir} keys=${keys.size}`);
  return {
    async get(key) {
      if (!keys.has(key)) return null;
      try { return await fs.promises.readFile(fileOf(key), "utf8"); } catch { return null; }
    },
    async put(key, value) {
      const f = fileOf(key);
      const tmp = f + "." + process.pid + "." + Date.now() + ".tmp";
      await fs.promises.writeFile(tmp, String(value));
      await fs.promises.rename(tmp, f);
      keys.add(key);
    },
    async delete(key) {
      keys.delete(key);
      try { await fs.promises.unlink(fileOf(key)); } catch {}
    },
    async list({ prefix = "", cursor, limit = 1000 } = {}) {
      const all = Array.from(keys).filter((k) => k.startsWith(prefix)).sort();
      const start = cursor ? Number(cursor) : 0;
      const slice = all.slice(start, start + limit);
      const done = start + limit >= all.length;
      return { keys: slice.map((name) => ({ name })), cursor: done ? "" : String(start + limit), list_complete: done };
    },
  };
}

// ---------- Durable Object (CLASSROOM) به‌صورت نمونه‌ی درون‌حافظه‌ای ----------
function makeClassroomNamespace(ClassRoomClass, env) {
  const rooms = new Map();
  return {
    idFromName(name) { return { name }; },
    get(id) {
      let inst = rooms.get(id.name);
      if (!inst) {
        inst = new ClassRoomClass({ id }, env);
        rooms.set(id.name, inst);
      }
      return { fetch: (req) => inst.fetch(req) };
    },
  };
}

// ---------- ساخت یک شیء Request سبک (بدون محدودیت‌های Headers استاندارد) ----------
function buildFetchRequest(nodeReq, bodyBuffer) {
  const proto = (nodeReq.headers["x-forwarded-proto"] || "http").split(",")[0].trim();
  const host = nodeReq.headers["x-forwarded-host"] || nodeReq.headers.host || "localhost";
  const url = `${proto}://${host}${nodeReq.url}`;
  const rawHeaders = nodeReq.headers;
  return {
    method: nodeReq.method,
    url,
    headers: {
      get(name) {
        const v = rawHeaders[String(name).toLowerCase()];
        if (v === undefined) return null;
        return Array.isArray(v) ? v.join(", ") : v;
      },
    },
    async json() { return bodyBuffer && bodyBuffer.length ? JSON.parse(bodyBuffer.toString("utf8")) : {}; },
    async text() { return bodyBuffer ? bodyBuffer.toString("utf8") : ""; },
    async arrayBuffer() { return bodyBuffer ? bodyBuffer.buffer.slice(bodyBuffer.byteOffset, bodyBuffer.byteOffset + bodyBuffer.byteLength) : new ArrayBuffer(0); },
  };
}

function readBody(nodeReq, maxBytes = 60 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    if (nodeReq.method === "GET" || nodeReq.method === "HEAD") return resolve(Buffer.alloc(0));
    const chunks = [];
    let size = 0;
    nodeReq.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) { reject(new Error("Request body too large")); nodeReq.destroy(); return; }
      chunks.push(chunk);
    });
    nodeReq.on("end", () => resolve(Buffer.concat(chunks)));
    nodeReq.on("error", reject);
  });
}

function sendNodeResponse(nodeRes, response) {
  nodeRes.statusCode = response.status || 200;
  for (const [k, v] of response.headers.entries()) {
    nodeRes.setHeader(k, v);
  }
  const body = response.body;
  if (body == null) { nodeRes.end(); return; }
  if (body instanceof Uint8Array) { nodeRes.end(Buffer.from(body)); return; }
  nodeRes.end(String(body));
}

// ------------------------------------------------------------------
// اجرای اصلی
// ------------------------------------------------------------------
async function main() {
  const redisUrl = process.env.REDIS_URL || process.env.UPSTASH_REDIS_URL || "";
  const dataDir = process.env.DATA_DIR || "";
  let kv, storageMode;
  if (redisUrl) {
    kv = await makeRedisKv(redisUrl); storageMode = "redis";
  } else if (dataDir) {
    try {
      // اگر مسیر دیسک مشکل داشته باشد (قفل/هنگ)، سرور نباید برای همیشه منتظر بماند
      kv = await Promise.race([makeDiskKv(dataDir), new Promise((_, rej) => setTimeout(() => rej(new Error("timeout 8s")), 8000))]);
      storageMode = "disk";
    }
    catch (e) {
      console.error(`[STORAGE] ❌ نوشتن در DATA_DIR=${dataDir} ممکن نیست (${e.message}) — موقتاً از رم استفاده می‌شود و داده‌ها پایدار نیستند!`);
      kv = makeMemoryKv(); storageMode = "ram";
    }
  } else {
    kv = makeMemoryKv(); storageMode = "ram";
  }
  globalThis.__storageMode = storageMode;
  if (storageMode === "ram") console.error("[STORAGE] ⚠️ ذخیره‌سازی موقت (RAM): با هر ری‌استارت یا استقرار دوباره همه‌ی اطلاعات پاک می‌شود. متغیر REDIS_URL (Redis رانفلر) یا DATA_DIR (دیسک پایدار) را تنظیم کنید.");

  // import ورکر اصلی *بعد* از نصب همه‌ی polyfill های بالا انجام می‌شود
  const workerModule = await import("./worker-src.js");
  const worker = workerModule.default;
  const ClassRoom = workerModule.ClassRoom;

  const env = {
    EXAM_KV: kv,
    GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
    GROQ_API_KEY: process.env.GROQ_API_KEY || "",
    GROQ_MODEL: process.env.GROQ_MODEL || "",
    TOKENHARBOR_API_KEY: process.env.TOKENHARBOR_API_KEY || "",
    TOKENHARBOR_MODEL: process.env.TOKENHARBOR_MODEL || "",
    // env.AI (Cloudflare Workers AI binding) عمداً تنظیم نمی‌شود؛ کد خودش این حالت را مدیریت می‌کند.
  };
  env.CLASSROOM = ClassRoom ? makeClassroomNamespace(ClassRoom, env) : null;

  const server = http.createServer(async (nodeReq, nodeRes) => {
    try {
      const bodyBuffer = await readBody(nodeReq);
      const fetchReq = buildFetchRequest(nodeReq, bodyBuffer);
      const response = await worker.fetch(fetchReq, env);
      sendNodeResponse(nodeRes, response);
    } catch (err) {
      console.error(err);
      nodeRes.statusCode = 500;
      nodeRes.end("Internal Server Error");
    }
  });

  // پشت پروکسی/لودبالانسر، keep-alive پیش‌فرض Node (۵ ثانیه) باعث خطاهای تصادفی (502/ECONNRESET) می‌شود
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;

  const wss = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, (rawWs) => {
      rawWs.isAlive = true;
      rawWs.on("pong", () => { rawWs.isAlive = true; });
      wsALS.run(rawWs, async () => {
        try {
          const fetchReq = buildFetchRequest(req, Buffer.alloc(0));
          await worker.fetch(fetchReq, env); // نتیجه دور ریخته می‌شود؛ ارتباط واقعی همین الان توسط ws برقرار شده
        } catch (err) {
          console.error("WS upgrade error:", err);
          try { rawWs.close(); } catch {}
        }
      });
    });
  });

  // ضربان سطح پروتکل: هر ۲۵ ثانیه ping؛ اتصالی که pong نداد (گوشی خاموش/اینترنت قطع) بسته می‌شود تا در فهرست حاضران نماند
  const diag = (globalThis.__wsDiag = globalThis.__wsDiag || { deadTerminated: 0 });
  setInterval(() => {
    for (const c of wss.clients) {
      if (c.isAlive === false) {
        diag.deadTerminated = (diag.deadTerminated || 0) + 1;
        console.log("[WS] terminate dead socket (no pong for ~25-50s)");
        try { c.terminate(); } catch {}
        continue;
      }
      c.isAlive = false;
      try { c.ping(); } catch {}
    }
  }, 25000).unref();

  // آمار دوره‌ای در لاگ: اگر وسط کلاس سرور ری‌استارت شود، uptime دوباره از صفر شروع می‌شود
  let maxLag = 0, lastTick = Date.now();
  setInterval(() => { const now = Date.now(); maxLag = Math.max(maxLag, now - lastTick - 1000); lastTick = now; }, 1000).unref();
  let quietTicks = 0;
  setInterval(() => {
    const n = wss.clients.size;
    quietTicks = n > 0 ? 0 : quietTicks + 1;
    if (n === 0 && quietTicks % 20 !== 1) return; // بدون کاربر: هر ~۱۰ دقیقه یک خط
    const m = process.memoryUsage();
    const rooms = {};
    for (const r of globalThis.__roomSet || []) { if (r && r.sessions && r.sessions.size) rooms[r.kind || "?"] = r.sessions.size; }
    const d = globalThis.__wsDiag || {};
    console.log(`[STATS] storage=${globalThis.__storageMode} up=${Math.round(process.uptime())}s rss=${Math.round(m.rss / 1048576)}MB heap=${Math.round(m.heapUsed / 1048576)}MB lagMax=${Math.max(0, maxLag)}ms ws=${n} rooms=${JSON.stringify(rooms)} open=${d.open || 0} close=${d.close || 0} abnormal1006=${d.abnormal || 0} err=${d.error || 0} busy=${d.busy || 0} dropSlow=${d.dropBuf || 0} dropFlow=${d.dropFlow || 0} deadKilled=${d.deadTerminated || 0}`);
    maxLag = 0;
  }, 30000).unref();

  // وقتی پلتفرم سرویس را متوقف/ری‌استارت می‌کند، سیگنال SIGTERM می‌فرستد؛ این خط در لاگ دلیل ری‌استارت را نشان می‌دهد
  let shuttingDown = false;
  const shutdown = (sig) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[SIGNAL] ${sig} received after up=${Math.round(process.uptime())}s — پلتفرم در حال متوقف/ری‌استارت کردن سرویس است. اتصال‌های فعال: ${wss.clients.size}`);
    for (const c of wss.clients) { try { c.close(1012, "service restart"); } catch {} }
    setTimeout(() => process.exit(0), 800);
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  // یک خطای پیش‌بینی‌نشده نباید کل کلاس را از کار بیندازد؛ فقط لاگ می‌شود
  // اگر سرور اصلاً نتواند پورت را بگیرد (مثلاً پورت اشغال است)، ادامه‌دادن بی‌فایده است؛ باید خارج شود تا پلتفرم دوباره اجرایش کند
  server.on("error", (e) => { console.error("[FATAL] خطای سرور HTTP:", e && e.message ? e.message : e); process.exit(1); });
  process.on("uncaughtException", (err) => {
    console.error("[ERROR] uncaughtException:", err && err.stack ? err.stack : err);
    if (err && (err.code === "EADDRINUSE" || err.code === "EACCES")) process.exit(1);
  });
  process.on("unhandledRejection", (err) => { console.error("[ERROR] unhandledRejection:", err && err.stack ? err.stack : err); });

  const port = process.env.PORT || 3000;
  server.listen(port, () => {
    console.log(`✅ پنل آموزشی روی پورت ${port} در حال اجراست`);
    console.log(`[BOOT] start pid=${process.pid} node=${process.version} port=${port} storage=${globalThis.__storageMode} at=${new Date().toISOString()}`);
  });
}

main().catch((err) => {
  console.error("خطای راه‌اندازی سرور:", err);
  process.exit(1);
});

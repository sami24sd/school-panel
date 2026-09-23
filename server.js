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
  console.warn("⚠️  REDIS_URL تنظیم نشده؛ از حافظه‌ی موقت (RAM) استفاده می‌شود — با هر ری‌استارت همه‌چیز پاک می‌شود. برای production حتماً یک Redis (مثلاً افزونه‌ی Redis در Railway یا Upstash) وصل کنید.");
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
  const kv = redisUrl ? await makeRedisKv(redisUrl) : makeMemoryKv();

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

  const wss = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    wss.handleUpgrade(req, socket, head, (rawWs) => {
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

  const port = process.env.PORT || 3000;
  server.listen(port, () => {
    console.log(`✅ پنل آموزشی روی پورت ${port} در حال اجراست`);
  });
}

main().catch((err) => {
  console.error("خطای راه‌اندازی سرور:", err);
  process.exit(1);
});

// اجرای Cloudflare Worker (worker-src.js، بدون تغییر) روی Node.js برای Runflare
import "./shims.mjs";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import { bridge } from "./shims.mjs";
import { FileKV } from "./kv-store.mjs";
import * as worker from "./worker-src.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, "data"));
const PUBLIC_DIR = path.join(__dirname, "public");
const MAX_BODY = 80 * 1024 * 1024;

const kv = new FileKV(path.join(DATA_DIR, "kv"));

// Durable Object معادل: برای هر نام اتاق (main/webinar/interactive/board) فقط یک نمونه
const rooms = new Map();
const env = {
  ...process.env,
  EXAM_KV: kv,
  CLASSROOM: {
    idFromName: (name) => String(name),
    get: (id) => ({
      fetch: (req) => {
        let r = rooms.get(id);
        if (!r) { r = new worker.ClassRoom({}, env); rooms.set(id, r); }
        return r.fetch(req);
      },
    }),
  },
};
const ctx = { waitUntil: (p) => { Promise.resolve(p).catch((e) => console.error("[waitUntil]", e)); }, passThroughOnException() {} };

/* ---- جایگزینی آدرس‌های CDN با فایل‌های محلی (برای کار روی شبکه‌ی ملی) ---- */
const REWRITES = [
  ['<link rel="preconnect" href="https://cdn.jsdelivr.net">', ""],
  ['<link rel="preconnect" href="https://fonts.googleapis.com">', ""],
  ['<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>', ""],
  ["https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css", "/_lib/fonts/vazirmatn/Vazirmatn-font-face.css"],
  ["https://cdn.jsdelivr.net/gh/rastikerdar/shabnam-font@v5.0.1/dist/font-face.css", "/_lib/fonts/shabnam/font-face.css"],
  ["https://cdn.jsdelivr.net/gh/rastikerdar/sahel-font@v3.4.0/dist/font-face.css", "/_lib/fonts/sahel/font-face.css"],
  ["https://cdn.jsdelivr.net/gh/intuxicated/css-persian@master/fonts/", "/_lib/fonts/b/"],
  ["https://cdn.jsdelivr.net/gh/naderuser/bnazanin@main/BNazanin.ttf", "/_lib/fonts/b/BNazanin.ttf"],
  ["https://fonts.googleapis.com/css2?family=Noto+Nastaliq+Urdu:wght@400..700&display=swap", "/_lib/fonts/nastaliq/nastaliq.css"],
  ["https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js", "/_lib/pdfjs/pdf.worker.min.js"],
  ["https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js", "/_lib/pdfjs/pdf.min.js"],
  ["https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js", "/_lib/jszip/jszip.min.js"],
  ["https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js", "/_lib/jspdf/jspdf.umd.min.js"],
  ["https://cdnjs.cloudflare.com/ajax/libs/exceljs/4.3.0/exceljs.min.js", "/_lib/exceljs/exceljs.min.js"],
  ["https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js", "/_lib/tesseract/tesseract.min.js"],
  ["Tesseract.createWorker('fas')", "Tesseract.createWorker('fas',1,{workerPath:'/_lib/tesseract/worker.min.js',corePath:'/_lib/tesseract/',langPath:'/_lib/tesseract/lang'})"],
];
const REWRITE_TYPES = /^(text\/html|text\/css|application\/javascript|text\/javascript)/i;
function rewriteText(s) { for (const [a, b] of REWRITES) if (s.includes(a)) s = s.split(a).join(b); return s; }

const MIME = {
  ".js": "application/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".woff2": "font/woff2", ".woff": "font/woff",
  ".ttf": "font/ttf", ".wasm": "application/wasm", ".gz": "application/gzip", ".json": "application/json", ".map": "application/json",
};

function serveStatic(req, res, pathname) {
  let rel;
  try { rel = decodeURIComponent(pathname); } catch { res.writeHead(400).end(); return true; }
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403).end(); return true; }
  let st; try { st = fs.statSync(file); } catch { res.writeHead(404).end("not found"); return true; }
  if (!st.isFile()) { res.writeHead(404).end("not found"); return true; }
  const ext = path.extname(file).toLowerCase();
  const headers = { "content-type": MIME[ext] || "application/octet-stream", "cache-control": "public, max-age=2592000, immutable", "content-length": st.size, "access-control-allow-origin": "*" };
  res.writeHead(200, headers);
  if (req.method === "HEAD") return res.end(), true;
  fs.createReadStream(file).pipe(res);
  return true;
}

function publicOrigin(req) {
  if (process.env.PUBLIC_URL) return process.env.PUBLIC_URL.replace(/\/+$/, "");
  const proto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim() || (req.socket.encrypted ? "https" : "http");
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "localhost").split(",")[0].trim();
  return proto + "://" + host;
}

function toHeaders(nodeHeaders) {
  const h = new Headers();
  for (const [k, v] of Object.entries(nodeHeaders)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) for (const x of v) h.append(k, x); else h.set(k, v);
  }
  return h;
}

async function readBody(req) {
  const chunks = []; let size = 0;
  for await (const c of req) { size += c.length; if (size > MAX_BODY) throw Object.assign(new Error("body too large"), { status: 413 }); chunks.push(c); }
  return Buffer.concat(chunks);
}

function buildRequest(req, body) {
  const url = publicOrigin(req) + req.url;
  const init = { method: req.method, headers: toHeaders(req.headers) };
  if (body && body.length && req.method !== "GET" && req.method !== "HEAD") init.body = body;
  return new Request(url, init);
}

async function sendResponse(req, res, response) {
  const headers = {};
  for (const [k, v] of response.headers) if (k !== "set-cookie") headers[k] = v;
  const cookies = typeof response.headers.getSetCookie === "function" ? response.headers.getSetCookie() : [];
  if (cookies.length) headers["set-cookie"] = cookies;
  delete headers["content-length"]; delete headers["content-encoding"]; delete headers["transfer-encoding"];
  const type = response.headers.get("content-type") || "";
  let buf = Buffer.from(await response.arrayBuffer());
  if (REWRITE_TYPES.test(type) && buf.length) buf = Buffer.from(rewriteText(buf.toString("utf8")), "utf8");
  const ae = String(req.headers["accept-encoding"] || "");
  if (buf.length > 1024 && /^(text\/|application\/(json|javascript))/i.test(type) && /\bgzip\b/.test(ae)) {
    buf = zlib.gzipSync(buf, { level: 6 });
    headers["content-encoding"] = "gzip";
    headers["vary"] = "Accept-Encoding";
  }
  headers["content-length"] = buf.length;
  res.writeHead(response.status, headers);
  res.end(req.method === "HEAD" ? undefined : buf);
}

const server = http.createServer(async (req, res) => {
  try {
    const pathname = (req.url || "/").split("?")[0];
    if (pathname === "/healthz") return void res.writeHead(200, { "content-type": "text/plain" }).end("ok");
    if (pathname.startsWith("/_lib/") && (req.method === "GET" || req.method === "HEAD")) return void serveStatic(req, res, pathname);
    const body = await readBody(req);
    const response = await worker.default.fetch(buildRequest(req, body), env, ctx);
    await sendResponse(req, res, response);
  } catch (e) {
    console.error("[request]", req.method, req.url, e);
    if (!res.headersSent) res.writeHead(e.status || 500, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ ok: false, error: e.status === 413 ? "حجم درخواست زیاد است" : "خطای داخلی سرور" }));
  }
});

/* ---- WebSocket ---- */
const wss = new WebSocketServer({ noServer: true, maxPayload: 6 * 1024 * 1024, perMessageDeflate: process.env.WS_DEFLATE === "1" });
server.on("upgrade", async (req, socket, head) => {
  const fail = (code, msg) => { try { socket.write("HTTP/1.1 " + code + " Error\r\nConnection: close\r\nContent-Length: " + Buffer.byteLength(msg) + "\r\n\r\n" + msg); } catch {} socket.destroy(); };
  try {
    const response = await worker.default.fetch(buildRequest(req, null), env, ctx);
    if (response.status === 101 && response.webSocket) {
      wss.handleUpgrade(req, socket, head, (ws) => {
        ws.isAlive = true;
        ws.on("pong", () => { ws.isAlive = true; });
        bridge(ws, response.webSocket);
      });
    } else {
      fail(response.status || 400, await response.text().catch(() => "error"));
    }
  } catch (e) { console.error("[upgrade]", e); fail(500, "error"); }
});
// جلوگیری از قطع شدن اتصال‌های بیکار توسط پراکسی و پاکسازی اتصال‌های مرده
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 25000).unref();

process.on("unhandledRejection", (e) => console.error("[unhandledRejection]", e));
process.on("uncaughtException", (e) => console.error("[uncaughtException]", e));
server.keepAliveTimeout = 65000;
server.listen(PORT, "0.0.0.0", () => console.log("Teacher panel listening on :" + PORT + " | data: " + DATA_DIR + " | keys: " + kv.count));

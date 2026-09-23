// اجرای همان Worker کلودفلر (فایل index.js) روی Node.js، بدون هیچ تغییری در index.js
// ─ داده‌ها (KV و Durable Object) روی دیسک، داخل DATA_DIR ذخیره می‌شوند.
// ─ فونت‌ها و کتابخانه‌هایی که پنل از CDN خارجی می‌گیرد، از پوشه‌ی assets/ همین سرور داده می‌شوند
//   تا با «شبکه‌ی ملی» (بدون اینترنت بین‌الملل) هم کار کنند.
// ─ اتصال‌های WebSocket (کلاس آنلاین، وبینار، تخته) هم پشتیبانی می‌شوند.
import { Miniflare } from "miniflare";
import { WebSocketServer } from "ws";
import { createServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { networkInterfaces } from "node:os";
import selfsigned from "selfsigned";
import { readFile, stat } from "node:fs/promises";
import { gzipSync } from "node:zlib";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || "./data"; // در رانفلر: مسیر دیسکِ متصل‌شده به پروژه
const ASSET_DIR = path.join(HERE, "assets");
const LOCAL_ASSETS = process.env.LOCAL_ASSETS !== "0"; // برای خاموش کردن جایگزینی آدرس‌ها: LOCAL_ASSETS=0
const PUBLIC_ORIGIN = process.env.PUBLIC_ORIGIN || ""; // مثلاً https://panel.example.ir (اختیاری)
// HTTPS=1: با گواهی خودامضا (برای شبکه‌ی محلی) تا دوربین و میکروفون کلاس آنلاین روی آدرس IP هم کار کند.
// مرورگرها یک بار هشدار می‌دهند؛ «Advanced ← Proceed» را بزنید. (روی سرور واقعی به‌جای این از گواهی معتبر و پروکسی استفاده کنید.)
const HTTPS = process.env.HTTPS === "1";

const mf = new Miniflare({
  modules: true,
  scriptPath: path.join(HERE, "index.js"),
  compatibilityDate: "2024-09-23",
  kvNamespaces: ["EXAM_KV"],
  durableObjects: { CLASSROOM: "ClassRoom" },
  kvPersist: DATA_DIR + "/kv",
  durableObjectsPersist: DATA_DIR + "/do",
  // کلیدهای هوش مصنوعی اختیاری‌اند و بدون اینترنت بین‌الملل کار نمی‌کنند
  bindings: {
    GROQ_API_KEY: process.env.GROQ_API_KEY || "",
    GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
  },
});
await mf.ready;

/* ---------- جایگزینی آدرس‌های CDN با فایل‌های محلی ---------- */
const JSD = "https://cdn.jsdelivr.net";
const CDNJS = "https://cdnjs.cloudflare.com/ajax/libs";
// فونت‌های B Nazanin/Titr/Mitra/Koodak از قبل داخل خود Worker هستند (مسیر /fonts/*.ttf)
const REWRITES = [
  [JSD + "/gh/intuxicated/css-persian@master/fonts/BNazanin.ttf", "/fonts/nazanin.ttf"],
  [JSD + "/gh/intuxicated/css-persian@master/fonts/BTitrBold.ttf", "/fonts/titr.ttf"],
  [JSD + "/gh/intuxicated/css-persian@master/fonts/BMitra.ttf", "/fonts/mitra.ttf"],
  [JSD + "/gh/intuxicated/css-persian@master/fonts/BKoodakBold.ttf", "/fonts/koodak.ttf"],
  [JSD + "/gh/naderuser/bnazanin@main/BNazanin.ttf", "/fonts/nazanin.ttf"],
  [JSD + "/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css", "/_a/vazirmatn/Vazirmatn-font-face.css"],
  [JSD + "/gh/rastikerdar/sahel-font@v3.4.0/dist/font-face.css", "/_a/sahel/font-face.css"],
  [JSD + "/gh/rastikerdar/shabnam-font@v5.0.1/dist/font-face.css", "/_a/shabnam/font-face.css"],
  ["https://fonts.googleapis.com/css2?family=Noto+Nastaliq+Urdu:wght@400..700&display=swap", "/_a/noto-nastaliq/index.css"],
  [JSD + "/npm/tesseract.js@5/dist/tesseract.min.js", "/_a/lib/tesseract.min.js"],
  [CDNJS + "/exceljs/4.3.0/exceljs.min.js", "/_a/lib/exceljs.min.js"],
  [CDNJS + "/jspdf/2.5.1/jspdf.umd.min.js", "/_a/lib/jspdf.umd.min.js"],
  [CDNJS + "/jszip/3.10.1/jszip.min.js", "/_a/lib/jszip.min.js"],
  [CDNJS + "/pdf.js/3.11.174/pdf.min.js", "/_a/lib/pdf.min.js"],
  [CDNJS + "/pdf.js/3.11.174/pdf.worker.min.js", "/_a/lib/pdf.worker.min.js"],
];
const PRECONNECT = /<link rel="preconnect" href="https:\/\/(cdn\.jsdelivr\.net|fonts\.googleapis\.com|fonts\.gstatic\.com)"[^>]*>/g;
const OCR_CALL = "Tesseract.createWorker('fas')";

function rewriteText(text, host) {
  const base = "//" + host;
  for (const [from, to] of REWRITES) text = text.split(from).join(base + to);
  text = text.replace(PRECONNECT, "");
  text = text.split(OCR_CALL).join(
    "Tesseract.createWorker('fas',1,{workerPath:'" + base + "/_a/tesseract/worker.min.js',corePath:'" + base +
      "/_a/tesseract/core',langPath:'" + base + "/_a/tesseract/lang'})"
  );
  return text;
}

const MIME = {
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".ttf": "font/ttf",
  ".wasm": "application/wasm",
  ".gz": "application/octet-stream", // خودِ Tesseract آن را باز می‌کند؛ نباید Content-Encoding داشته باشد
};
const COMPRESSIBLE = /^(text\/|application\/(javascript|json))/;
const wantsGzip = (req) => /\bgzip\b/.test(req.headers["accept-encoding"] || "");

async function serveAsset(req, res, pathname) {
  const rel = decodeURIComponent(pathname.slice("/_a/".length));
  const file = path.join(ASSET_DIR, rel);
  if (!file.startsWith(ASSET_DIR + path.sep)) {
    res.writeHead(403);
    return res.end();
  }
  try {
    const st = await stat(file);
    if (!st.isFile()) throw new Error("not a file");
    let data = await readFile(file);
    const type = MIME[path.extname(file)] || "application/octet-stream";
    const headers = {
      "content-type": type,
      "cache-control": "public, max-age=31536000, immutable",
      "access-control-allow-origin": "*",
    };
    if (COMPRESSIBLE.test(type) && wantsGzip(req)) {
      data = gzipSync(data);
      headers["content-encoding"] = "gzip";
    }
    headers["content-length"] = data.length;
    res.writeHead(200, headers);
    res.end(req.method === "HEAD" ? undefined : data);
  } catch {
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  }
}

function lanAddresses() {
  const out = [];
  for (const list of Object.values(networkInterfaces())) {
    for (const i of list || []) if (i.family === "IPv4" && !i.internal) out.push(i.address);
  }
  return out;
}

// گواهی خودامضا برای localhost و IPهای فعلی کامپیوتر؛ اگر IPها عوض شوند دوباره ساخته می‌شود
function loadOrCreateCert() {
  const dir = path.join(DATA_DIR, "https");
  const ips = lanAddresses().sort();
  const stamp = ips.join(",");
  const stampFile = path.join(dir, "ips.txt");
  const certFile = path.join(dir, "cert.pem");
  const keyFile = path.join(dir, "key.pem");
  if (existsSync(certFile) && existsSync(keyFile) && existsSync(stampFile) && readFileSync(stampFile, "utf8") === stamp) {
    return { cert: readFileSync(certFile), key: readFileSync(keyFile) };
  }
  mkdirSync(dir, { recursive: true });
  const altNames = [{ type: 2, value: "localhost" }, { type: 7, ip: "127.0.0.1" }, ...ips.map((ip) => ({ type: 7, ip }))];
  const pems = selfsigned.generate([{ name: "commonName", value: "panel.local" }], {
    days: 3650,
    keySize: 2048,
    algorithm: "sha256",
    extensions: [{ name: "subjectAltName", altNames }],
  });
  writeFileSync(certFile, pems.cert);
  writeFileSync(keyFile, pems.private);
  writeFileSync(stampFile, stamp);
  return { cert: Buffer.from(pems.cert), key: Buffer.from(pems.private) };
}

function originOf(req) {
  if (PUBLIC_ORIGIN) return PUBLIC_ORIGIN.replace(/\/$/, "");
  const fwd = req.headers["x-forwarded-proto"];
  const proto = String(fwd || (req.socket && req.socket.encrypted ? "https" : "http")).split(",")[0].trim();
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "localhost").split(",")[0].trim();
  return proto + "://" + host;
}

const handler = async (req, res) => {
  try {
    const origin = originOf(req);
    const host = new URL(origin).host;
    const pathname = new URL(req.url, "http://x").pathname;
    if (LOCAL_ASSETS && pathname.startsWith("/_a/")) return await serveAsset(req, res, pathname);

    const body = ["GET", "HEAD"].includes(req.method)
      ? undefined
      : await new Promise((resolve) => {
          const chunks = [];
          req.on("data", (d) => chunks.push(d));
          req.on("end", () => resolve(Buffer.concat(chunks)));
        });
    const resp = await mf.dispatchFetch(origin + req.url, { method: req.method, headers: req.headers, body });

    // هدرهای «hop-by-hop» را از Worker کپی نمی‌کنیم؛ وجود همزمان transfer-encoding و content-length
    // برای بعضی کلاینت‌ها (Node/fetch، پروکسی‌ها، Apps Script) خطاست
    const HOP = new Set(["set-cookie", "transfer-encoding", "connection", "keep-alive", "content-length"]);
    const headers = {};
    for (const [k, v] of resp.headers) if (!HOP.has(k)) headers[k] = v;
    const cookies = resp.headers.getSetCookie ? resp.headers.getSetCookie() : [];
    if (cookies.length) headers["set-cookie"] = cookies;

    const type = resp.headers.get("content-type") || "";
    let out = Buffer.from(await resp.arrayBuffer());
    if (LOCAL_ASSETS && /^(text\/html|application\/javascript|text\/javascript|text\/css)/.test(type)) {
      out = Buffer.from(rewriteText(out.toString("utf8"), host), "utf8");
    }
    delete headers["content-encoding"];
    if (COMPRESSIBLE.test(type) && out.length > 1024 && wantsGzip(req)) {
      out = gzipSync(out);
      headers["content-encoding"] = "gzip";
    }
    headers["content-length"] = out.length;
    res.writeHead(resp.status, headers);
    res.end(req.method === "HEAD" ? undefined : out);
  } catch (e) {
    res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
    res.end("خطای سرور: " + e);
  }
};
const server = HTTPS ? createHttpsServer(loadOrCreateCert(), handler) : createServer(handler);

/* ---------- WebSocket (کلاس آنلاین / وبینار / تخته): پل بین مرورگر و Durable Object ---------- */
const wss = new WebSocketServer({ noServer: true });
server.on("upgrade", async (req, socket, head) => {
  try {
    const resp = await mf.dispatchFetch(originOf(req) + req.url, { headers: req.headers });
    const upstream = resp.webSocket;
    if (!upstream) {
      // Worker اتصال را رد کرده (مثلاً ۴۰۱): همان پاسخ HTTP را به مرورگر برگردان
      const text = Buffer.from(await resp.arrayBuffer());
      socket.write(
        "HTTP/1.1 " + resp.status + " " + (resp.statusText || "Error") + "\r\n" +
          "content-type: " + (resp.headers.get("content-type") || "text/plain") + "\r\n" +
          "content-length: " + text.length + "\r\nconnection: close\r\n\r\n"
      );
      socket.end(text);
      return;
    }
    // پیام‌هایی که Worker بلافاصله می‌فرستد (مثل init) قبل از آماده‌شدن سوکت مرورگر نباید گم شوند
    const queue = [];
    let client = null;
    let closed = false;
    upstream.addEventListener("message", (e) => {
      if (client) { try { client.send(e.data); } catch {} } else queue.push(e.data);
    });
    upstream.addEventListener("close", () => { closed = true; if (client) { try { client.close(); } catch {} } });
    upstream.addEventListener("error", () => { closed = true; if (client) { try { client.close(); } catch {} } });
    upstream.accept();
    wss.handleUpgrade(req, socket, head, (c) => {
      client = c;
      for (const m of queue) { try { c.send(m); } catch {} }
      queue.length = 0;
      if (closed) { try { c.close(); } catch {} return; }
      c.on("message", (data, isBinary) => { try { upstream.send(isBinary ? data : data.toString()); } catch {} });
      c.on("close", () => { try { upstream.close(); } catch {} });
      c.on("error", () => { try { upstream.close(); } catch {} });
    });
  } catch {
    try { socket.destroy(); } catch {}
  }
});

server.listen(PORT, () => {
  const scheme = HTTPS ? "https" : "http";
  console.log("Panel is running on port " + PORT + " (" + scheme + "). Data folder: " + DATA_DIR);
  console.log("پنل روی پورت " + PORT + " بالا آمد؛ داده‌ها در " + DATA_DIR);
  for (const ip of lanAddresses()) console.log("  " + scheme + "://" + ip + ":" + PORT + "/teacher");
});
const stop = async () => {
  server.close();
  await mf.dispose();
  process.exit(0);
};
process.on("SIGTERM", stop);
process.on("SIGINT", stop);

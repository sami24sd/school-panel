// جایگزین Cloudflare KV روی دیسک: هر کلید یک فایل (خط اول: سربرگ JSON، بقیه: مقدار).
// نوشتن اتمیک است (فایل موقت + rename)، پس قطع برق/ری‌استارت وسط نوشتن فایل نصفه نمی‌گذارد.
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export class FileKV {
  constructor(dir) {
    this.dir = dir;
    fs.mkdirSync(dir, { recursive: true });
    this.index = new Map(); // key -> { file, exp }
    this._sorted = null;
    this._load();
  }

  _fileFor(key) {
    const h = crypto.createHash("sha256").update(key).digest("hex");
    return path.join(this.dir, h.slice(0, 2), h + ".kv");
  }

  _load() {
    let n = 0;
    const buf = Buffer.alloc(8192);
    for (const sub of fs.readdirSync(this.dir)) {
      const sd = path.join(this.dir, sub);
      let st; try { st = fs.statSync(sd); } catch { continue; }
      if (!st.isDirectory()) continue;
      for (const f of fs.readdirSync(sd)) {
        const fp = path.join(sd, f);
        if (f.endsWith(".tmp")) { try { fs.unlinkSync(fp); } catch {} continue; }
        if (!f.endsWith(".kv")) continue;
        try {
          const fd = fs.openSync(fp, "r");
          const len = fs.readSync(fd, buf, 0, buf.length, 0);
          fs.closeSync(fd);
          const nl = buf.indexOf(10);
          if (nl < 0 || nl >= len) continue;
          const head = JSON.parse(buf.slice(0, nl).toString("utf8"));
          this.index.set(head.k, { file: fp, exp: head.e || 0 });
          n++;
        } catch { /* فایل ناقص/خراب نادیده گرفته می‌شود */ }
      }
    }
    this.count = n;
  }

  _expired(meta) { return meta.exp && meta.exp * 1000 < Date.now(); }

  async get(key, opts) {
    key = String(key);
    const meta = this.index.get(key);
    if (!meta) return null;
    if (this._expired(meta)) { await this.delete(key); return null; }
    let raw;
    try { raw = fs.readFileSync(meta.file); } catch { this.index.delete(key); this._sorted = null; return null; }
    const nl = raw.indexOf(10);
    const value = raw.slice(nl + 1);
    const type = typeof opts === "string" ? opts : (opts && opts.type) || "text";
    if (type === "arrayBuffer") return value.buffer.slice(value.byteOffset, value.byteOffset + value.length);
    const text = value.toString("utf8");
    if (type === "json") { try { return JSON.parse(text); } catch { return null; } }
    return text;
  }

  async getWithMetadata(key, opts) { return { value: await this.get(key, opts), metadata: null }; }

  async put(key, value, opts) {
    key = String(key);
    if (Buffer.byteLength(key) > 512) throw new Error("KV key too long");
    let body;
    if (typeof value === "string") body = Buffer.from(value, "utf8");
    else if (value instanceof ArrayBuffer) body = Buffer.from(value);
    else if (ArrayBuffer.isView(value)) body = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    else body = Buffer.from(String(value), "utf8");
    if (body.length > 25 * 1024 * 1024) throw new Error("KV value too large (max 25MB)");
    let exp = 0;
    if (opts && opts.expiration) exp = Math.floor(Number(opts.expiration));
    else if (opts && opts.expirationTtl) exp = Math.floor(Date.now() / 1000 + Number(opts.expirationTtl));
    const file = this._fileFor(key);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = file + "." + process.pid + "." + Date.now() + ".tmp";
    fs.writeFileSync(tmp, Buffer.concat([Buffer.from(JSON.stringify({ k: key, e: exp }) + "\n", "utf8"), body]));
    fs.renameSync(tmp, file);
    if (!this.index.has(key)) this._sorted = null;
    this.index.set(key, { file, exp });
  }

  async delete(key) {
    key = String(key);
    const meta = this.index.get(key);
    if (!meta) return;
    try { fs.unlinkSync(meta.file); } catch {}
    this.index.delete(key);
    this._sorted = null;
  }

  async list(opts) {
    opts = opts || {};
    const prefix = opts.prefix || "";
    const limit = Math.max(1, Math.min(1000, Number(opts.limit) || 1000));
    if (!this._sorted) this._sorted = Array.from(this.index.keys()).sort();
    let after = null;
    if (opts.cursor) { try { after = Buffer.from(String(opts.cursor), "base64").toString("utf8"); } catch {} }
    const out = [];
    let complete = true;
    for (const k of this._sorted) {
      if (after !== null && k <= after) continue;
      if (prefix && !k.startsWith(prefix)) continue;
      const meta = this.index.get(k);
      if (!meta || this._expired(meta)) continue;
      if (out.length >= limit) { complete = false; break; }
      const item = { name: k };
      if (meta.exp) item.expiration = meta.exp;
      out.push(item);
    }
    const res = { keys: out, list_complete: complete };
    if (!complete) res.cursor = Buffer.from(out[out.length - 1].name, "utf8").toString("base64");
    return res;
  }
}

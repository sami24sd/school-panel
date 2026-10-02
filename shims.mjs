// لایه‌ی سازگاری: اجرای بدون تغییر Cloudflare Worker روی Node (Response با کد ۱۰۱، WebSocketPair)
const NativeResponse = globalThis.Response;

class WorkerResponse extends NativeResponse {
  constructor(body, init) {
    if (init && init.status === 101) {
      super(null, { headers: init.headers, status: 200 });
      Object.defineProperty(this, "status", { value: 101 });
      this.webSocket = init.webSocket;
    } else {
      super(body, init);
    }
  }
}
globalThis.Response = WorkerResponse;

class SockHalf {
  constructor() {
    this.listeners = { message: [], close: [], error: [] };
    this.real = null;      // سوکت واقعی (ws) بعد از پذیرش اتصال
    this.queue = [];       // پیام‌های ارسال‌شده قبل از اتصال واقعی (مثل پیام init)
    this.readyState = 1;
    this.closedEarly = null;
  }
  accept() {}
  addEventListener(type, fn) { (this.listeners[type] || (this.listeners[type] = [])).push(fn); }
  removeEventListener(type, fn) { const a = this.listeners[type]; if (a) { const i = a.indexOf(fn); if (i >= 0) a.splice(i, 1); } }
  dispatch(type, evt) { for (const fn of (this.listeners[type] || []).slice()) { try { fn(evt); } catch (e) { console.error("[ws handler]", e); } } }
  send(data) {
    if (this.readyState !== 1) throw new Error("WebSocket is closed");
    if (this.real) this.real.send(data); else this.queue.push(data);
  }
  close(code, reason) {
    if (this.readyState === 3) return;
    this.readyState = 3;
    if (this.real) { try { this.real.close(code && code >= 1000 && code < 5000 && code !== 1005 && code !== 1006 ? code : 1000, reason ? String(reason).slice(0, 100) : undefined); } catch {} }
    else this.closedEarly = { code, reason };
  }
  get bufferedAmount() { return this.real ? this.real.bufferedAmount : 0; }
}

globalThis.WebSocketPair = function WebSocketPair() {
  const client = new SockHalf();
  const server = new SockHalf();
  client.peer = server; server.peer = client;
  this[0] = client; this[1] = server;
};

// اتصال نیمه‌ی «سرور» (که Worker/ClassRoom نگه می‌دارد) به سوکت واقعی ws
export function bridge(realWs, clientHalf) {
  const server = clientHalf.peer;
  server.real = realWs;
  for (const d of server.queue.splice(0)) { try { realWs.send(d); } catch {} }
  realWs.on("message", (data, isBinary) => {
    server.dispatch("message", { data: isBinary ? (Array.isArray(data) ? Buffer.concat(data) : data) : data.toString("utf8") });
  });
  realWs.on("close", (code, reason) => { server.readyState = 3; server.dispatch("close", { code, reason: String(reason || ""), wasClean: true }); });
  realWs.on("error", (err) => { server.dispatch("error", { error: err }); });
}

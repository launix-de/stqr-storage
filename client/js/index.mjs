// SPDX-License-Identifier: GPL-3.0-only
import {
  Writer,
  Reader,
  newID,
  hex,
  unhex,
  sha256,
  SHA256,
  equal,
  bytes,
} from "./codec.mjs";
export { Writer, Reader, newID, hex, unhex, sha256, SHA256 } from "./codec.mjs";
const ZERO = "0".repeat(32),
  OZERO = "0".repeat(64),
  MAX_BUFFER = 16 << 20,
  CHUNK = 1 << 20;
export const OP = Object.freeze({
  HELLO: 1,
  PING: 2,
  GET: 0x100,
  GET_RANGE: 0x101,
  PUT: 0x102,
  PUT_RANGE: 0x103,
  APPEND: 0x104,
  TRUNCATE: 0x105,
  DELETE: 0x106,
  STAT: 0x107,
  CAS: 0x108,
  MAP_GET: 0x10b,
  BUCKET_INFO: 0x10c,
  SUBSCRIBE: 0x110,
  UNSUBSCRIBE: 0x111,
  UPDATE: 0x112,
  MOVED: 0x113,
  ACK: 0x114,
  MAINTENANCE: 0x115,
  SCAN: 0x116,
  LIST: 0x117,
  ACCEPTED: 0x118,
  SYNC: 0x119,
  DURABLE: 0x11a,
  SPACE: 0x11b,
  LEASE: 0x11c,
  END_LEASE: 0x11d,
  REFERENCE: 0x121,
  CONNECTION_SYNC: 0x122,
});
export class StatusError extends Error {
  constructor(status, message, requestId, headers = new Map()) {
    super(message || `STQR status ${status}`);
    this.name = "StatusError";
    Object.assign(this, { status, requestId, headers });
  }
}
export class UnknownOutcomeError extends Error {
  constructor(requestId, cause) {
    super(`Outcome unknown; retry with requestId ${requestId}`, { cause });
    this.name = "UnknownOutcomeError";
    this.requestId = requestId;
  }
}
export class ProtocolError extends Error {
  constructor(message = "Invalid STQR message") {
    super(message);
    this.name = "ProtocolError";
  }
}
function invariant(ok, message) {
  if (!ok) throw new ProtocolError(message);
}
function b64(s) {
  const b = new TextEncoder().encode(s);
  let raw = "";
  for (const v of b) raw += String.fromCharCode(v);
  return btoa(raw);
}
const token = (s) =>
  b64(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const envelope = (op, body, id = newID(), kind = 1) =>
  new Writer().u8(1).u8(kind).u16(op).id(id).raw(body).bytes();
function checkSignal(signal) {
  signal?.throwIfAborted();
}
function range(v) {
  if (typeof v === "number" && !Number.isSafeInteger(v))
    throw RangeError("Unsafe integer");
  v = BigInt(v);
  if (v < 0n || v > 0xffffffffffffffffn) throw RangeError("u64 overflow");
  return v;
}
function precondition(w, o) {
  w.bool(o.expectedLength != null);
  if (o.expectedLength != null) w.u64(range(o.expectedLength));
  w.bool(!!o.absent).bool(o.compare != null);
  if (o.compare != null)
    w.u64(range(o.compareOffset ?? 0n)).blob(bytes(o.compare));
  return w;
}
export function readInfo(r) {
  const info = {
    length: r.u64(),
    payloadDigest: r.bool() ? r.take(32).slice() : null,
    flags: r.u32(),
    runtimeId: r.id(),
    compression: r.u8(),
    layoutVersion: r.u16(),
  };
  invariant(info.compression <= 3);
  return info;
}
function transfer(r) {
  const t = {
    mode: r.u8(),
    length: r.u64(),
    digest: r.take(32).slice(),
    data: r.blob().slice(),
  };
  invariant(t.mode <= 1);
  return t;
}
function endpointURL(value) {
  const u = new URL(value);
  if (
    !["http:", "https:", "ws:", "wss:"].includes(u.protocol) ||
    u.username ||
    u.password ||
    u.search ||
    u.hash ||
    !["", "/", "/api/ws"].includes(u.pathname)
  )
    throw RangeError("Expected a node URL without credentials/query");
  u.protocol =
    u.protocol === "ws:"
      ? "http:"
      : u.protocol === "wss:"
        ? "https:"
        : u.protocol;
  u.pathname = "/";
  return u;
}
const abortError = () => new DOMException("Connection closed", "AbortError");
async function delay(ms, signal) {
  checkSignal(signal);
  await new Promise((resolve, reject) => {
    const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", abort);
      },
      abort = () => {
        done();
        reject(signal.reason);
      },
      timer = setTimeout(() => {
        done();
        resolve();
      }, ms);
    signal?.addEventListener("abort", abort, { once: true });
  });
}

class Transport {
  constructor(ws, client) {
    this.ws = ws;
    this.client = client;
    this.pending = new Map();
    this.durability = new Map();
    this.noAck = new Map();
    this.closed = false;
    this.receiving = Promise.resolve();
    ws.binaryType = "arraybuffer";
    ws.onmessage = (e) => {
      this.receiving = this.receiving
        .then(() => this.receive(e.data))
        .catch((err) => this.fail(err));
    };
    ws.onclose = () => this.fail(abortError());
    ws.onerror = () => this.fail(Error("WebSocket failed"));
  }
  fail(error) {
    if (this.closed) return;
    this.closed = true;
    this.ws.close();
    for (const p of this.pending.values()) {
      p.cleanup();
      p.reject(error);
    }
    this.pending.clear();
    for (const sub of this.client.subscriptions.values())
      if (sub.transport === this) sub.fail(error);
  }
  async send(op, body, id, kind = 1, signal) {
    checkSignal(signal);
    while (this.ws.bufferedAmount > CHUNK) {
      if (this.closed) throw abortError();
      await delay(5, signal);
    }
    if (this.closed) throw abortError();
    this.ws.send(envelope(op, body, id, kind));
  }
  request(
    op,
    body,
    {
      requestId = newID(),
      signal,
      upload,
      sink,
      maxBuffered = MAX_BUFFER,
    } = {},
  ) {
    checkSignal(signal);
    if (this.closed) return Promise.reject(abortError());
    if (this.pending.size >= 12)
      return Promise.reject(
        new StatusError(429, "WOULD_BLOCK: request queue full", requestId),
      );
    if (this.pending.has(requestId))
      return Promise.reject(
        new StatusError(409, "Request ID already in flight", requestId),
      );
    return new Promise((resolve, reject) => {
      const abort = () => {
          this.pending.delete(requestId);
          cleanup();
          reject(signal.reason);
        },
        cleanup = () => signal?.removeEventListener("abort", abort);
      signal?.addEventListener("abort", abort, { once: true });
      this.pending.set(requestId, {
        op,
        resolve,
        reject,
        cleanup,
        stream: null,
        sink,
        maxBuffered,
      });
      this.send(op, body, requestId, 1, signal)
        .then(() => upload?.(this, requestId, signal))
        .catch((error) => {
          const p = this.pending.get(requestId);
          if (p) {
            this.pending.delete(requestId);
            cleanup();
            reject(error);
          }
        });
    });
  }
  async receive(raw) {
    const data =
        raw instanceof ArrayBuffer
          ? new Uint8Array(raw)
          : bytes(await raw.arrayBuffer()),
      r = new Reader(data);
    invariant(data.length <= MAX_BUFFER && r.u8() === 1);
    const kind = r.u8(),
      op = r.u16(),
      id = r.id(),
      body = r.take(r.b.length - r.at);
    if (kind === 3) {
      if ([13, 5, 6].includes(op)) return this.stream(op, body);
      if (op === OP.UPDATE) return this.client.update(this, body);
      if (op === OP.MOVED) {
        const q = new Reader(body),
          sid = q.id();
        this.client.subscriptions
          .get(sid)
          ?.fail(
            new ProtocolError(
              "Subscription moved; resubscribe for a fresh snapshot",
            ),
          );
        return;
      }
      if (op === OP.DURABLE) return;
      if (op === 3) {
        this.client.goAway(this, body);
        this.fail(abortError());
        return;
      }
      return;
    }
    invariant(kind === 2);
    let p = this.pending.get(id);
    if (!p) {
      const submission = this.noAck.get(id);
      if (!submission) return;
      p = {
        op: OP.ACCEPTED,
        cleanup() {},
        resolve() {
          throw new ProtocolError("Unexpected no-ACK success");
        },
        reject: (error) => {
          submission.error = error;
          if (
            (error.status === 301 || error.status === 302) &&
            error.headers?.has(2)
          )
            this.client.learn(error.headers.get(2), submission.oid);
        },
      };
    } else this.pending.delete(id);
    p.cleanup();
    try {
      invariant(op === p.op, "Response opcode mismatch");
      const q = new Reader(body),
        status = Number(q.v()),
        h = new Reader(q.blob()),
        headers = new Map();
      for (let i = 0, n = h.count(100); i < n; i++) {
        const tag = Number(h.v());
        invariant(!headers.has(tag));
        headers.set(tag, h.blob().slice());
      }
      h.done();
      const result = q.take(q.b.length - q.at);
      if (status >= 300) {
        let message = "";
        if (headers.has(3)) {
          const e = new Reader(headers.get(3));
          message = e.text();
          e.done();
        }
        throw new StatusError(status, message, id, headers);
      }
      if (headers.has(48)) {
        const d = new Reader(headers.get(48)),
          boot = d.id(),
          sequence = d.u64();
        d.done();
        invariant(boot !== ZERO && sequence > 0n);
        const prev = this.durability.get(boot) ?? 0n;
        if (sequence > prev) this.durability.set(boot, sequence);
      }
      p.resolve({ result, headers, stream: p.stream, transport: this, status });
    } catch (error) {
      p.reject(error);
    }
  }
  async stream(op, body) {
    const r = new Reader(body),
      id = r.id(),
      p = this.pending.get(id);
    if (!p) return;
    if (op === 13) {
      invariant(!p.stream);
      const length = r.u64(),
        digest = r.take(32).slice();
      if (r.bool()) readInfo(r);
      if (r.bool()) {
        r.u64();
        r.u64();
        r.u64();
      }
      r.done();
      invariant(length <= BigInt(Number.MAX_SAFE_INTEGER));
      invariant(
        p.sink || length <= BigInt(p.maxBuffered),
        "Use a streaming sink for large transfers",
      );
      p.stream = {
        length,
        digest,
        chunks: [],
        hash: new SHA256(),
        next: 0n,
        part: 0n,
        ended: false,
      };
      return;
    }
    const s = p.stream;
    invariant(s && !s.ended);
    if (op === 5) {
      const part = r.u64(),
        offset = r.u64(),
        data = r.blob().slice();
      r.done();
      invariant(
        part === s.part &&
          offset === s.next &&
          data.length <= CHUNK &&
          s.next + BigInt(data.length) <= s.length,
      );
      s.hash.update(data);
      if (p.sink) await p.sink(data);
      else s.chunks.push(data);
      s.next += BigInt(data.length);
      s.part++;
      return;
    }
    const length = r.u64(),
      digest = r.take(32);
    r.done();
    invariant(
      length === s.length && s.next === s.length && equal(digest, s.digest),
    );
    invariant(equal(s.hash.digest(), s.digest));
    if (!p.sink) {
      s.data = new Uint8Array(Number(s.length));
      let at = 0;
      for (const chunk of s.chunks) {
        s.data.set(chunk, at);
        at += chunk.length;
      }
    }
    s.chunks = [];
    s.ended = true;
  }
}

export class Client {
  static async connect(options) {
    const c = new Client(options);
    try {
      await c.establish(c.endpoint, { signal: options.signal });
      return c;
    } catch (error) {
      c.close();
      throw error;
    }
  }
  constructor({
    endpoint,
    username,
    password,
    credentials,
    csrf,
    bucket,
    format,
    routing = "gateway",
    signal,
    WebSocket: WebSocketClass = globalThis.WebSocket,
    fetch: fetchFn = globalThis.fetch,
    requestTimeout = 30000,
  }) {
    this.endpoint = endpointURL(endpoint).href;
    this.selector = bucket;
    this.format = format;
    this.routing = routing;
    this.WebSocket = WebSocketClass;
    this.fetch = (...args) => fetchFn(...args);
    this.csrf = csrf;
    this.credentials =
      credentials ??
      (username && password ? async () => ({ username, password }) : null);
    this.requestTimeout = requestTimeout;
    this.transports = new Map();
    this.allTransports = new Set();
    this.connecting = new Map();
    this.routes = [];
    this.routers = new Map();
    this.subscriptions = new Map();
    this.closed = false;
    this.runtime = ZERO;
    this.cluster = null;
    this.lease = null;
    this.life = new AbortController();
    if (
      typeof bucket !== "string" ||
      !bucket ||
      new TextEncoder().encode(bucket).length > 260 ||
      bucket.trim() !== bucket ||
      /[\x00-\x1f\x7f]/.test(bucket)
    )
      throw RangeError("A bucket name or ID is required");
    if (
      typeof format !== "string" ||
      !format ||
      new TextEncoder().encode(format).length > 255 ||
      format.trim() !== format ||
      /[\x00-\x1f\x7f]/.test(format)
    )
      throw RangeError("An opaque format is required");
    if (!["direct", "gateway"].includes(routing))
      throw RangeError("Unknown routing mode");
    if (!WebSocketClass)
      throw Error("Provide a browser-compatible WebSocket implementation");
    signal?.addEventListener("abort", () => this.close(), { once: true });
    checkSignal(signal);
  }
  async authorization() {
    if (!this.credentials) return "";
    const { username, password } = await this.credentials();
    if (!username || username.includes(":") || !password)
      throw RangeError("Invalid credentials");
    return "Basic " + b64(username + ":" + password);
  }
  async establish(endpoint, { node, signal } = {}) {
    if (this.closed) throw abortError();
    const key = this.routing === "gateway" ? node ?? "seed" : endpoint;
    if (this.transports.get(key) && !this.transports.get(key).closed)
      return this.transports.get(key);
    if (this.connecting.has(key)) return this.connecting.get(key);
    const promise = this.open(endpoint, { node, signal });
    this.connecting.set(key, promise);
    try {
      const t = await promise;
      this.transports.set(key, t);
      return t;
    } finally {
      this.connecting.delete(key);
    }
  }
  async open(endpoint, { node, signal }) {
    signal = AbortSignal.any([
      this.life.signal,
      ...(signal ? [signal] : []),
      AbortSignal.timeout(this.requestTimeout),
    ]);
    checkSignal(signal);
    const authorization = await this.authorization(),
      csrf = typeof this.csrf === "function" ? this.csrf() : this.csrf;
    if (!this.bucket) {
      const u = new URL("/api/bucket-connect", this.endpoint),
        headers = {
          "X-STQR-Bucket": this.selector,
          "X-STQR-Format": this.format,
        };
      if (authorization) headers.Authorization = authorization;
      if (csrf) headers["X-CSRF-Token"] = csrf;
      const response = await this.fetch(u, {
        method: "POST",
        headers,
        credentials: "same-origin",
        signal,
        redirect: "error",
      });
      if (!response.ok)
        throw new StatusError(response.status, (await response.json()).error);
      const result = await response.json();
      unhex(result.bucket, 16);
      invariant(result.bucket !== ZERO);
      this.bucket = result.bucket;
    }
    const u = new URL(this.routing === "gateway" ? this.endpoint : endpoint);
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
    u.pathname =
      this.routing === "gateway" && node ? "/api/bucket-tunnel" : "/api/ws";
    u.search = "";
    if (this.routing === "gateway" && node) u.searchParams.set("node", node);
    const protocols = [
      "stqr.v2",
      "bucket." + token(this.bucket),
      "format." + token(this.format),
    ];
    if (authorization) protocols.push("auth." + token(authorization));
    if (csrf) protocols.push("csrf." + csrf);
    if (this.lease && this.lease !== "local")
      protocols.push("maintenance." + token(this.lease));
    const ws = new this.WebSocket(u.href, protocols);
    await new Promise((resolve, reject) => {
      const abort = () => {
          ws.close();
          reject(signal.reason);
        },
        cleanup = () => signal.removeEventListener("abort", abort);
      signal.addEventListener("abort", abort, { once: true });
      ws.onopen = () => {
        cleanup();
        resolve();
      };
      ws.onerror = () => {
        cleanup();
        reject(Error("WebSocket upgrade failed"));
      };
      ws.onclose = () => {
        cleanup();
        reject(Error("WebSocket upgrade rejected"));
      };
    });
    invariant(ws.protocol === "stqr.v2");
    const t = new Transport(ws, this);
    t.endpoint = endpoint;
    t.proxyNode = node;
    for (const old of this.allTransports)
      if (old.closed && !old.durability.size && !old.noAck.size)
        this.allTransports.delete(old);
    if (this.allTransports.size >= 1024) {
      t.fail(new StatusError(429, "Unconfirmed transport history full"));
      throw new StatusError(429, "Unconfirmed transport history full");
    }
    this.allTransports.add(t);
    if (this.closed) {
      t.fail(abortError());
      throw abortError();
    }
    try {
      const reply = await t.request(
          OP.HELLO,
          new Writer()
            .u16(2)
            .u16(2)
            .u64(8192n | 262144n | 2097152n | 524288n | 512n)
            .text("stqr-js")
            .bytes(),
          { signal },
        ),
        r = new Reader(reply.result);
      invariant(r.u16() === 2);
      t.capabilities = r.u64();
      t.node = r.id();
      const cluster = r.id();
      t.membership = r.u64();
      t.revision = r.u64();
      r.done();
      invariant(!node || node === t.node, "Node identity mismatch");
      invariant(
        !this.cluster || this.cluster === cluster,
        "Cluster identity mismatch",
      );
      this.cluster = cluster;
      this.capabilities = t.capabilities;
      this.clusterRouting = !!(t.capabilities & 8n);
      this.routers.set(t.node, endpoint);
      return t;
    } catch (error) {
      t.fail(error);
      throw error;
    }
  }
  ref(oid) {
    unhex(oid);
    return new Writer()
      .id(this.bucket)
      .raw(unhex(oid))
      .u64(0)
      .u64(0)
      .id(this.clusterRouting ? ZERO : this.runtime);
  }
  async call(op, body, o = {}) {
    if (this.closed) throw abortError();
    if (this.acquiringLease && op !== OP.LEASE && op !== OP.MAINTENANCE)
      throw new StatusError(409, "Exclusive lease acquisition in progress");
    const signal = AbortSignal.any([
        this.life.signal,
        ...(o.signal ? [o.signal] : []),
        AbortSignal.timeout(this.requestTimeout),
      ]),
      requestId = o.requestId ?? newID();
    let endpoint = this.endpoint,
      node;
    let routed =
        body.length >= 80 &&
        ((op >= 0x100 && op <= 0x110) ||
          op === OP.LIST ||
          op === OP.SCAN ||
          op === OP.REFERENCE ||
          op === OP.ACCEPTED),
      oid = routed ? hex(body.subarray(16, 48)) : null;
    for (let attempt = 0; attempt < 16; attempt++) {
      checkSignal(signal);
      if (this.leaseTransport?.closed)
        throw new StatusError(
          410,
          "Exclusive lease connection was lost",
          requestId,
        );
      let outgoing = body.slice();
      if (routed) {
        if (this.clusterRouting && !this.route(oid))
          await this.ensureRoute(oid, signal);
        const route = this.route(oid);
        if (route) {
          endpoint = route.endpoint;
          node = route.node;
          outgoing.set(
            new Writer()
              .u64(route.revision)
              .u64(route.epoch)
              .id(route.runtime)
              .bytes(),
            48,
          );
        }
      }
      try {
        const t = await this.establish(endpoint, { node, signal });
        const reply = await t.request(op, outgoing, {
          requestId,
          signal,
          upload: o.upload,
          sink: o.sink,
          maxBuffered: o.maxBuffered,
        });
        return reply;
      } catch (error) {
        if (
          error instanceof StatusError &&
          (error.status === 301 || error.status === 302) &&
          routed
        ) {
          this.learn(error.headers.get(2), oid);
          continue;
        }
        if (
          error instanceof StatusError &&
          error.status === 503 &&
          attempt < 15
        ) {
          await delay(Math.min(100 * 2 ** attempt, 1000), signal);
          continue;
        }
        if (
          !(error instanceof StatusError) &&
          !(error instanceof ProtocolError) &&
          !signal.aborted &&
          !this.leaseTransport &&
          attempt < 15
        ) {
          this.routes = this.routes.filter((route) => route.node !== node);
          await delay(Math.min(100 * 2 ** attempt, 1000), signal);
          continue;
        }
        if (!(error instanceof StatusError) && o.mutation)
          throw new UnknownOutcomeError(requestId, error);
        throw error;
      }
    }
    throw new StatusError(503, "Routing did not converge", requestId);
  }
  async ensureRoute(oid, signal) {
    let last;
    for (const [node, endpoint] of this.routers) {
      try {
        const t = await this.establish(endpoint, { node, signal });
        const reply = await t.request(
          OP.MAP_GET,
          new Writer().id(this.bucket).raw(unhex(oid)).bytes(),
          { signal },
        );
        this.learn(reply.result, oid);
        return;
      } catch (error) {
        last = error;
        if (error instanceof ProtocolError) throw error;
      }
    }
    throw last ?? new ProtocolError("No route source");
  }
  goAway(t, body) {
    const r = new Reader(body),
      reason = r.u8(),
      operation = r.id(),
      n = r.count(10000),
      alternatives = [];
    for (let i = 0; i < n; i++) {
      const node = r.id(),
        addresses = [];
      for (let j = 0, m = r.count(32); j < m; j++)
        addresses.push(endpointURL(r.text()).href);
      invariant(addresses.length > 0);
      alternatives.push({ node, addresses });
    }
    const membership = r.u64(),
      revision = r.u64();
    r.done();
    invariant(
      reason <= 3 && operation !== ZERO && membership > 0n && revision > 0n,
    );
    this.routes = this.routes.filter((route) => route.node !== t.node);
    this.routers.delete(t.node);
    for (const { node, addresses } of alternatives)
      if (node !== t.node) this.routers.set(node, addresses[0]);
  }
  route(oid) {
    return this.routes.find((r) => oid >= r.begin && (!r.end || oid < r.end));
  }
  learn(raw, oid) {
    invariant(raw, "Missing routing map");
    const r = new Reader(raw),
      membership = r.u64(),
      generation = r.u64(),
      revision = r.u64(),
      bucket = r.id(),
      begin = hex(r.take(32)),
      toEnd = r.bool(),
      end = hex(r.take(32));
    invariant(bucket === this.bucket && oid >= begin && (toEnd || oid < end));
    const entries = [];
    for (let i = 0, n = r.count(200000); i < n; i++) {
      const start = hex(r.take(32)),
        bits = r.u16(),
        policy = r.u64(),
        epoch = r.u64(),
        state = r.u8(),
        runtime = r.id(),
        locations = [];
      invariant(bits <= 256 && state <= 4);
      const startInt = BigInt("0x" + start),
        width = 1n << BigInt(256 - bits);
      invariant(startInt % width === 0n);
      for (let j = 0, m = r.count(256); j < m; j++)
        locations.push({
          node: r.id(),
          volume: r.id(),
          role: r.u8(),
          class: r.u8(),
          fragment: r.u16(),
          flags: r.u32(),
        });
      entries.push({
        begin: start,
        end:
          startInt + width === 1n << 256n
            ? null
            : (startInt + width).toString(16).padStart(64, "0"),
        epoch,
        runtime,
        revision,
        locations,
        policy,
      });
    }
    const addresses = new Map();
    for (let i = 0, n = r.count(10000); i < n; i++) {
      const node = r.id(),
        urls = [];
      for (let j = 0, m = r.count(32); j < m; j++)
        urls.push(endpointURL(r.text()).href);
      addresses.set(node, urls);
      if (urls.length) this.routers.set(node, urls[0]);
    }
    for (let i = 0, n = r.count(10000); i < n; i++) {
      r.id();
      r.u64();
      r.text();
      r.take(5);
      r.take(6);
      r.bool();
      r.u16();
      r.u64();
      r.u8();
      r.u64();
      r.bool();
      r.u64();
      r.u32();
    }
    r.done();
    const matching = entries.find(
      (e) => oid >= e.begin && (!e.end || oid < e.end),
    );
    invariant(matching, "No routing entry for object");
    const primary = matching.locations.find((l) => l.role === 0);
    invariant(
      primary && addresses.get(primary.node)?.length,
      "Missing owner address",
    );
    const existing = this.route(oid);
    if (existing && existing.revision > revision)
      throw new ProtocolError("Stale routing map");
    this.routes = this.routes.filter(
      (e) => !(oid >= e.begin && (!e.end || oid < e.end)),
    );
    matching.begin = matching.begin < begin ? begin : matching.begin;
    if (!toEnd && (!matching.end || matching.end > end)) matching.end = end;
    this.routes.unshift({
      ...matching,
      node: primary.node,
      endpoint: addresses.get(primary.node)[0],
      membership,
      generation,
    });
    this.clusterRouting = true;
  }
  acceptInfo(info) {
    if (!this.clusterRouting) {
      if (this.runtime !== ZERO && this.runtime !== info.runtimeId)
        throw new StatusError(410, "Bucket runtime changed");
      this.runtime = info.runtimeId;
    }
    return info;
  }
  async read(oid, { offset, length, condition, sink, ...o } = {}) {
    const ranged = length != null,
      w = this.ref(oid);
    if (ranged) {
      offset = range(offset ?? 0n);
      length = range(length);
      if (offset + length > 0xffffffffffffffffn)
        throw RangeError("Range overflow");
      w.u64(offset).u64(length);
    }
    w.bool(!!condition);
    if (condition) {
      w.bool(condition.length != null);
      if (condition.length != null) w.u64(range(condition.length));
      w.u64(range(condition.offset ?? 0n)).blob(bytes(condition.bytes));
    }
    const reply = await this.call(ranged ? OP.GET_RANGE : OP.GET, w.bytes(), {
        ...o,
        sink,
      }),
      r = new Reader(reply.result),
      info = this.acceptInfo(readInfo(r));
    let expected = info.length;
    if (ranged) {
      const returnedOffset = r.u64();
      expected = r.u64();
      invariant(r.u64() === info.length && returnedOffset === offset);
    }
    const t = transfer(r);
    r.done();
    invariant(t.length === expected);
    let data = t.data;
    if (t.mode === 1) {
      invariant(
        reply.stream?.ended &&
          reply.stream.length === t.length &&
          equal(reply.stream.digest, t.digest),
      );
      if (sink) return info;
      data = reply.stream.data;
    } else
      invariant(
        BigInt(data.length) === t.length && equal(sha256(data), t.digest),
      );
    if (!sink && data.length > MAX_BUFFER)
      throw RangeError("Use getTo for payloads above 16 MiB");
    if (sink) {
      for (let at = 0; at < data.length; at += CHUNK) {
        checkSignal(o.signal);
        await sink(data.subarray(at, at + CHUNK));
      }
      return info;
    }
    return { data, info };
  }
  get(oid, o = {}) {
    return this.read(oid, o);
  }
  getRange(oid, offset, length, o = {}) {
    return this.read(oid, { ...o, offset, length });
  }
  getTo(oid, sink, o = {}) {
    return this.read(oid, { ...o, sink });
  }
  getRangeTo(oid, offset, length, sink, o = {}) {
    return this.read(oid, { ...o, offset, length, sink });
  }
  getRangeToMatching(oid, offset, length, condition, sink, o = {}) {
    return this.read(oid, { ...o, offset, length, condition, sink });
  }
  async stat(oid, o = {}) {
    const reply = await this.call(OP.STAT, this.ref(oid).bytes(), o),
      r = new Reader(reply.result),
      info = this.acceptInfo(readInfo(r));
    r.done();
    return info;
  }
  async write(op, oid, data, offset, o = {}) {
    const blob = typeof Blob !== "undefined" && data instanceof Blob;
    if (!blob) data = bytes(data);
    const length = blob ? data.size : data.length,
      hash = new SHA256();
    let inline;
    if (blob) {
      for (let at = 0; at < length; at += CHUNK) {
        checkSignal(o.signal);
        hash.update(
          new Uint8Array(await data.slice(at, at + CHUNK).arrayBuffer()),
        );
      }
      if (length <= CHUNK) inline = new Uint8Array(await data.arrayBuffer());
    } else {
      hash.update(data);
      inline = data;
    }
    const w = precondition(
        op === OP.PUT_RANGE ? this.ref(oid).u64(range(offset)) : this.ref(oid),
        o,
      ),
      digest = hash.digest(),
      streamed = length > CHUNK;
    w.u8(streamed ? 1 : 0)
      .u64(length)
      .raw(digest)
      .blob(streamed ? new Uint8Array() : inline);
    let opcode = op;
    if (o.accepted) {
      const base = w.bytes();
      opcode = OP.ACCEPTED;
      w.a = [...base.subarray(0, 80), op & 255, op >> 8, ...base.subarray(80)];
    }
    const upload = streamed
      ? async (t, parent, signal) => {
          for (let at = 0, part = 0; at < length; at += CHUNK, part++) {
            if (!t.pending.has(parent)) return;
            const chunk = blob
              ? new Uint8Array(await data.slice(at, at + CHUNK).arrayBuffer())
              : data.subarray(at, at + CHUNK);
            await t.send(
              5,
              new Writer().id(parent).u64(part).u64(at).blob(chunk).bytes(),
              newID(),
              0,
              signal,
            );
          }
          if (t.pending.has(parent))
            await t.send(
              6,
              new Writer().id(parent).u64(length).raw(digest).bytes(),
              newID(),
              0,
              signal,
            );
        }
      : null;
    const reply = await this.call(opcode, w.bytes(), {
        ...o,
        mutation: true,
        upload,
      }),
      r = new Reader(reply.result),
      info = this.acceptInfo(readInfo(r)),
      startOffset = op === OP.APPEND ? r.u64() : undefined;
    if (op === OP.PUT_RANGE) {
      invariant(
        r.u64() === range(offset) &&
          r.u64() === BigInt(length) &&
          r.u64() === info.length,
      );
    }
    r.done();
    return op === OP.APPEND ? { info, startOffset } : info;
  }
  put(oid, data, o = {}) {
    return this.write(OP.PUT, oid, data, 0n, o);
  }
  putRange(oid, offset, data, o = {}) {
    if (range(offset) + BigInt(bytes(data).length) > 0xffffffffffffffffn)
      throw RangeError("Range overflow");
    return this.write(OP.PUT_RANGE, oid, data, offset, o);
  }
  append(oid, data, o = {}) {
    return this.write(OP.APPEND, oid, data, 0n, o);
  }
  async putAsync(oid, data, o = {}) {
    return this.submitWrite(OP.PUT, oid, 0n, data, o);
  }
  async putRangeAsync(oid, offset, data, o = {}) {
    return this.submitWrite(OP.PUT_RANGE, oid, offset, data, o);
  }
  async submitWrite(op, oid, offset, data, o) {
    data = bytes(data).slice();
    offset = range(offset);
    if (offset + BigInt(data.length) > 0xffffffffffffffffn)
      throw RangeError("Range overflow");
    if (this.lease || data.length > CHUNK)
      return this.write(op, oid, data, offset, { ...o, accepted: true });
    const signal = AbortSignal.any([
      this.life.signal,
      ...(o.signal ? [o.signal] : []),
      AbortSignal.timeout(this.requestTimeout),
    ]);
    if (this.clusterRouting && !this.route(oid))
      await this.ensureRoute(oid, signal);
    const route = this.route(oid),
      t = await this.establish(route?.endpoint ?? this.endpoint, {
        node: route?.node,
        signal,
      });
    if (!(t.capabilities & 524288n))
      return this.write(op, oid, data, offset, { ...o, accepted: true });
    const w = precondition(
        op === OP.PUT_RANGE ? this.ref(oid).u64(offset) : this.ref(oid),
        o,
      )
        .u8(0)
        .u64(data.length)
        .raw(sha256(data))
        .blob(data),
      body = w.bytes();
    if (route)
      body.set(
        new Writer()
          .u64(route.revision)
          .u64(route.epoch)
          .id(route.runtime)
          .bytes(),
        48,
      );
    const accepted = new Writer()
        .raw(body.subarray(0, 80))
        .u16(op)
        .raw(body.subarray(80))
        .bytes(),
      requestId = o.requestId ?? newID();
    if (t.noAck.has(requestId))
      throw new StatusError(409, "Request ID already submitted", requestId);
    if (t.noAck.size >= 1024)
      throw new StatusError(429, "WOULD_BLOCK: outstanding no-ACK queue full");
    t.noAck.set(requestId, { oid, error: null });
    try {
      await t.send(OP.ACCEPTED, accepted, requestId, 0, signal);
    } catch (error) {
      t.noAck.get(requestId).error = new UnknownOutcomeError(requestId, error);
      throw error;
    }
  }
  concurrentObjectRequests() {
    return true;
  }
  async reconnect(o = {}) {
    if (this.leaseTransport)
      throw new StatusError(410, "Cannot reconnect an exclusive lease");
    for (const t of this.transports.values()) t.fail(abortError());
    await this.establish(this.endpoint, o);
  }
  async syncConnection(t, o = {}, cut = [...t.noAck.keys()]) {
    if (t.closed && cut.length)
      throw new UnknownOutcomeError(cut[0], abortError());
    if (!cut.length) return;
    let reply;
    try {
      reply = await t.request(OP.CONNECTION_SYNC, new Uint8Array(), o);
    } catch (error) {
      const prior = cut.map((id) => t.noAck.get(id)?.error).find(Boolean);
      for (const id of cut) if (t.noAck.get(id)?.error) t.noAck.delete(id);
      throw prior ?? error;
    }
    const r = new Reader(reply.result),
      boot = r.id();
    r.u64();
    r.done();
    invariant(boot !== ZERO);
    const failure = cut.map((id) => t.noAck.get(id)?.error).find(Boolean);
    for (const id of cut) t.noAck.delete(id);
    if (failure) throw failure;
  }
  async from(op, oid, source, length, offset, o) {
    length = range(length);
    if (length > BigInt(Number.MAX_SAFE_INTEGER))
      throw RangeError("Source too large");
    if (typeof Blob !== "undefined" && source instanceof Blob) {
      if (BigInt(source.size) !== length)
        throw RangeError("Source length mismatch");
      return this.write(op, oid, source, offset, o);
    }
    const parts = [];
    let staged = 0n;
    for await (const part of source) {
      checkSignal(o.signal);
      const b = bytes(part);
      staged += BigInt(b.length);
      if (staged > length) throw RangeError("Source exceeds declared length");
      parts.push(new Blob([b]));
    }
    if (staged !== length)
      throw RangeError("Source shorter than declared length");
    return this.write(op, oid, new Blob(parts), offset, o);
  }
  putFrom(oid, source, length, o = {}) {
    return this.from(OP.PUT, oid, source, length, 0n, o);
  }
  putRangeFrom(oid, offset, source, length, o = {}) {
    return this.from(OP.PUT_RANGE, oid, source, length, offset, o);
  }
  appendFrom(oid, source, length, o = {}) {
    return this.from(OP.APPEND, oid, source, length, 0n, o);
  }
  async mutateInfo(op, w, o) {
    let body = w.bytes();
    if (o.accepted) {
      body = new Writer()
        .raw(body.subarray(0, 80))
        .u16(op)
        .raw(body.subarray(80))
        .bytes();
      op = OP.ACCEPTED;
    }
    const reply = await this.call(op, body, { ...o, mutation: true });
    if (!reply.result.length) return;
    const r = new Reader(reply.result),
      info = this.acceptInfo(readInfo(r));
    r.done();
    return info;
  }
  truncate(oid, length, o = {}) {
    return this.mutateInfo(
      OP.TRUNCATE,
      precondition(this.ref(oid).u64(range(length)), o),
      o,
    );
  }
  trunc(oid, length, o = {}) {
    return this.truncate(oid, length, o);
  }
  delete(oid, o = {}) {
    return this.mutateInfo(OP.DELETE, precondition(this.ref(oid), o), o);
  }
  cas(oid, offset, expected, replacement, o = {}) {
    expected = bytes(expected);
    replacement = bytes(replacement);
    offset = range(offset);
    const scope = o.scope === "object" || o.scope === 1 ? 1 : 0;
    if (
      ![undefined, "range", "object", 0, 1].includes(o.scope) ||
      expected.length > CHUNK ||
      replacement.length > CHUNK ||
      (scope === 0 && expected.length !== replacement.length) ||
      (scope === 1 && offset !== 0n) ||
      offset + BigInt(replacement.length) > 0xffffffffffffffffn
    )
      throw RangeError("Invalid CAS range");
    return this.mutateInfo(
      OP.CAS,
      precondition(this.ref(oid).u8(scope).u64(offset), o)
        .blob(expected)
        .blob(replacement),
      o,
    );
  }
  async scanObjects(after = "", limit = 100, o = {}) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000)
      throw RangeError("Page limit must be 1..1000");
    const w = this.ref(OZERO).bool(!!after);
    if (after) w.raw(unhex(after));
    w.u32(limit);
    const reply = await this.call(
        o.maintenance ? OP.SCAN : OP.LIST,
        w.bytes(),
        o,
      ),
      r = new Reader(reply.result),
      n = r.u32(),
      objects = [];
    invariant(n <= limit);
    let previous = after;
    for (let i = 0; i < n; i++) {
      const id = hex(r.take(32)),
        info = this.acceptInfo(readInfo(r));
      invariant(!previous || id > previous);
      previous = id;
      objects.push({ id, info });
    }
    const next = r.bool() ? hex(r.take(32)) : "";
    r.done();
    invariant(
      !next || ((!after || next > after) && (!previous || next >= previous)),
    );
    return { objects, next };
  }
  async space(o = {}) {
    const reply = await this.call(
        OP.SPACE,
        new Writer().id(this.bucket).bytes(),
        o,
      ),
      r = new Reader(reply.result),
      result = {
        used: r.u64(),
        total: r.u64(),
        free: r.u64(),
        available: r.u64(),
        quota: r.u64(),
        quotaLimited: r.bool(),
      };
    r.done();
    invariant(
      result.used + result.free === result.total &&
        result.available <= result.free,
    );
    return result;
  }
  async bucketAccess(o = {}) {
    const reply = await this.call(
        OP.BUCKET_INFO,
        new Writer().text(this.bucket).bytes(),
        o,
      ),
      r = new Reader(reply.result);
    invariant(r.id() === this.bucket);
    r.u64();
    r.text();
    const state = r.u8();
    r.take(4);
    r.take(6);
    r.bool();
    r.u16();
    r.u64();
    r.u8();
    r.u64();
    r.bool();
    r.u64();
    r.u32();
    const permissions = r.u32();
    r.done();
    invariant(state <= 2 && (permissions & ~15) === 0);
    return permissions;
  }
  pendingMark() {
    const marks = new Map();
    for (const t of this.allTransports)
      for (const [boot, sequence] of t.durability) {
        const old = marks.get(boot);
        if (!old || sequence > old.sequence)
          marks.set(boot, {
            bucket: this.bucket,
            boot,
            sequence,
            node: t.node,
            endpoint: t.endpoint,
            proxyNode: t.proxyNode,
          });
      }
    if (marks.size > 1)
      throw new StatusError(
        501,
        "Multiple owner incarnations pending; use captureFence()",
      );
    return marks.values().next().value ?? null;
  }
  async syncThrough(mark, o = {}) {
    if (this.closed) throw abortError();
    if (mark.bucket && mark.bucket !== this.bucket)
      throw RangeError("Durability mark belongs to another bucket");
    unhex(mark.boot, 16);
    const sequence = range(mark.sequence);
    if (mark.boot === ZERO || sequence === 0n)
      throw RangeError("Invalid durability mark");
    let source = [...this.allTransports].find((t) =>
      t.durability.has(mark.boot),
    );
    if (!source) {
      source = [...this.allTransports].find((t) => t.node === mark.node);
      if (!source) throw new StatusError(410, "Unknown durability owner");
    }
    const t = source.closed
      ? await this.establish(source.endpoint, {
          node: source.proxyNode,
          signal: o.signal,
        })
      : source;
    const reply = await t.request(
        OP.SYNC,
        new Writer().id(this.bucket).id(mark.boot).u64(sequence).bytes(),
        o,
      ),
      r = new Reader(reply.result),
      boot = r.id(),
      confirmed = r.u64();
    r.done();
    if (boot !== mark.boot || confirmed < sequence)
      throw new StatusError(410, "Durability incarnation changed");
    const result = { ...mark, bucket: this.bucket, boot, sequence: confirmed };
    if (o.observe !== false) this.noteDurable(result);
    return result;
  }
  noteDurable(mark) {
    if (mark.bucket && mark.bucket !== this.bucket)
      throw RangeError("Durability mark belongs to another bucket");
    unhex(mark.boot, 16);
    const sequence = range(mark.sequence);
    for (const t of this.allTransports)
      if ((t.durability.get(mark.boot) ?? sequence + 1n) <= sequence)
        t.durability.delete(mark.boot);
  }
  captureFence() {
    return new Fence().add(this);
  }
  async sync(o = {}) {
    for (const t of this.allTransports) {
      await this.syncConnection(t, o);
      for (const [boot, sequence] of t.durability) {
        const destination = t.closed
          ? await this.establish(t.endpoint, {
              node: t.proxyNode,
              signal: o.signal,
            })
          : t;
        const reply = await destination.request(
            OP.SYNC,
            new Writer().id(this.bucket).id(boot).u64(sequence).bytes(),
            o,
          ),
          r = new Reader(reply.result);
        invariant(r.id() === boot && r.u64() >= sequence);
        r.done();
        if (t.durability.get(boot) <= sequence) t.durability.delete(boot);
      }
    }
  }
  async prepareLease(o) {
    if (this.lease || this.acquiringLease) throw Error("Lease already active");
    await this.sync(o);
    this.acquiringLease = true;
    try {
      const seed = await this.establish(this.endpoint, { signal: o.signal });
      for (const t of this.transports.values()) {
        if (t === seed) continue;
        const closed =
          t.ws.readyState === 3
            ? Promise.resolve()
            : new Promise((resolve) =>
                t.ws.addEventListener("close", resolve, { once: true }),
              );
        t.fail(abortError());
        await closed;
      }
      for (const [key, t] of this.transports)
        if (t !== seed) this.transports.delete(key);
    } catch (error) {
      this.acquiringLease = false;
      throw error;
    }
  }
  async beginObjectLease(readonly = false, o = {}) {
    await this.prepareLease(o);
    try {
      const reply = await this.call(
          OP.LEASE,
          this.ref(OZERO).bool(readonly).bytes(),
          { ...o, requestId: o.requestId ?? newID() },
        ),
        r = new Reader(reply.result);
      if (reply.result.length) {
        invariant(r.u8() === 1);
        this.lease = r.id();
        r.done();
      } else this.lease = "local";
      this.leaseTransport = reply.transport;
    } finally {
      this.acquiringLease = false;
    }
  }
  async beginMaintenance(o = {}) {
    await this.prepareLease(o);
    try {
      const reply = await this.call(OP.MAINTENANCE, this.ref(OZERO).bytes(), o),
        r = new Reader(reply.result);
      this.lease = "local";
      if (reply.result.length) {
        invariant(r.u8() === 1);
        this.lease = r.id();
        r.done();
      }
      this.leaseTransport = reply.transport;
    } finally {
      this.acquiringLease = false;
    }
  }
  async endMaintenance(o = {}) {
    const owner = this.leaseTransport;
    if (!owner) return;
    await this.sync(o);
    if (this.lease !== "local")
      await owner.request(OP.END_LEASE, this.ref(OZERO).bytes(), o);
    this.lease = null;
    this.leaseTransport = null;
    for (const t of this.transports.values()) {
      const closed =
        t.ws.readyState === 3
          ? Promise.resolve()
          : new Promise((resolve) =>
              t.ws.addEventListener("close", resolve, { once: true }),
            );
      t.fail(abortError());
      await closed;
    }
    this.transports.clear();
  }

  async reference(oid, tokenId, action, o = {}) {
    unhex(oid);
    unhex(tokenId, 16);
    if (
      tokenId === ZERO ||
      !Number.isInteger(action) ||
      action < 0 ||
      action > 8
    )
      throw RangeError("Invalid reference action");
    const root = hex(
      sha256(new TextEncoder().encode("STQR object references v1")),
    );
    if (oid === root)
      throw RangeError("Reference root cannot reference itself");
    const reply = await this.call(
        OP.REFERENCE,
        this.ref(root).raw(unhex(oid)).u8(action).id(tokenId).bytes(),
        o,
      ),
      r = new Reader(reply.result),
      granted = r.bool();
    r.done();
    if (!granted) throw new StatusError(409, "WOULD_BLOCK");
    return granted;
  }
  pinObject(id, token, o = {}) {
    return this.reference(id, token, 0, o);
  }
  unpinObject(id, token, o = {}) {
    return this.reference(id, token, 1, o);
  }
  claimObjectRetirement(id, token, o = {}) {
    return this.reference(id, token, 2, o);
  }
  releaseObjectRetirement(id, token, o = {}) {
    return this.reference(id, token, 3, o);
  }
  pinObjectDependency(id, token, o = {}) {
    return this.reference(id, token, 4, o);
  }
  unpinObjectDependency(id, token, o = {}) {
    return this.reference(id, token, 5, o);
  }
  resetAbandonedObjectReferences(o = {}) {
    return this.reference(OZERO, newID(), 6, o);
  }
  reclaimBucketMetadata(o = {}) {
    return this.reference(OZERO, newID(), 7, o);
  }
  async canReclaimDependencies(o = {}) {
    try {
      return await this.reference(OZERO, newID(), 8, o);
    } catch (error) {
      if (error.status === 409) return false;
      throw error;
    }
  }
  objectReferencesSupported() {
    return !!(this.capabilities & 262144n);
  }
  async readObjectDependencies(o = {}) {
    const root = hex(
      sha256(new TextEncoder().encode("STQR object references v1")),
    );
    let data;
    try {
      ({ data } = await this.get(root, o));
    } catch (error) {
      if (error.status === 404) return [];
      throw error;
    }
    const r = new Reader(data);
    invariant(new TextDecoder().decode(r.take(4)) === "ORF1");
    const bucket = r.id(),
      n = r.u32();
    invariant(n <= 8192 && data.length === 24 + n * 81);
    const ids = new Set();
    for (let i = 0; i < n; i++) {
      const id = hex(r.take(32));
      r.take(32);
      r.id();
      const flags = r.u8();
      invariant(flags <= 2);
      if (flags === 2) ids.add(id);
    }
    r.done();
    return bucket === this.bucket ? [...ids] : [];
  }
  async subscribe(oid, o = {}) {
    const id = o.id ?? newID();
    if (this.subscriptions.has(id))
      throw RangeError("Duplicate subscription ID");
    const sub = new Subscription(this, id, o);
    this.subscriptions.set(id, sub);
    const w = this.ref(oid)
      .id(id)
      .bool(!!o.ranged)
      .u64(range(o.offset ?? 0n))
      .u64(range(o.length ?? 0n))
      .bool(o.resume != null);
    if (o.resume != null) w.u64(range(o.resume));
    try {
      const reply = await this.call(OP.SUBSCRIBE, w.bytes(), o),
        r = new Reader(reply.result);
      invariant(r.id() === id);
      sub.cursor = r.u64();
      sub.info = this.acceptInfo(readInfo(r));
      const t = transfer(r);
      r.done();
      invariant(sub.cursor > 0n && t.length <= BigInt(MAX_BUFFER));
      sub.data = t.mode === 0 ? t.data : reply.stream?.data;
      invariant(
        sub.data &&
          BigInt(sub.data.length) === t.length &&
          equal(sha256(sub.data), t.digest),
      );
      sub.transport = reply.transport;
      sub.ready = true;
      for (const update of sub.early) sub.receive(update);
      sub.early = [];
      return sub;
    } catch (error) {
      this.subscriptions.delete(id);
      throw error;
    }
  }
  update(t, body) {
    const r = new Reader(body),
      id = r.id(),
      cursor = r.u64(),
      part = r.u32(),
      parts = r.u32(),
      change = r.u8(),
      offset = r.u64(),
      newLength = r.u64(),
      tr = transfer(r);
    r.done();
    invariant(
      cursor > 0n &&
        parts > 0 &&
        part < parts &&
        change <= 5 &&
        tr.mode === 0 &&
        BigInt(tr.data.length) === tr.length &&
        equal(sha256(tr.data), tr.digest),
    );
    const sub = this.subscriptions.get(id);
    if (!sub) return;
    sub.transport ??= t;
    const update = {
      cursor,
      part,
      parts,
      change,
      offset,
      newLength,
      data: tr.data,
    };
    if (!sub.ready) {
      invariant(sub.early.length < 64);
      sub.early.push(update);
    } else sub.receive(update);
  }
  close() {
    if (this.closing) return this.closing;
    if (this.closed) return Promise.resolve();
    const finish = () => {
      this.closed = true;
      this.life.abort();
      for (const t of this.allTransports) t.fail(abortError());
      this.transports.clear();
      this.allTransports.clear();
      this.credentials = null;
      for (const sub of this.subscriptions.values()) sub.fail(abortError());
      this.subscriptions.clear();
    };
    if (this.leaseTransport && !this.leaseTransport.closed) {
      this.closing = this.endMaintenance({ signal: AbortSignal.timeout(5000) })
        .catch(() => {})
        .finally(finish);
      return this.closing;
    }
    finish();
    return Promise.resolve();
  }
}

class Subscription {
  constructor(client, id, options) {
    Object.assign(this, {
      client,
      id,
      options,
      cursor: 0n,
      data: null,
      info: null,
      early: [],
      queue: [],
      waiters: [],
      ready: false,
      closed: false,
      parts: null,
    });
  }
  receive(update) {
    if (update.cursor <= this.cursor) return;
    if (update.change === 5) {
      this.fail(new ProtocolError("Subscription resync required"));
      return;
    }
    if (!this.parts) {
      invariant(update.cursor === this.cursor + 1n && update.part === 0);
      this.parts = { cursor: update.cursor, parts: update.parts, updates: [] };
    }
    const p = this.parts;
    invariant(
      update.cursor === p.cursor &&
        update.parts === p.parts &&
        update.part === p.updates.length,
    );
    p.updates.push(update);
    if (p.updates.length !== p.parts) return;
    this.parts = null;
    this.cursor = update.cursor;
    for (const u of p.updates) {
      if (u.change === 0 && p.parts === 1) {
        this.data = u.data.slice();
        this.info = { ...this.info, length: u.newLength };
      } else if (u.change === 4) {
        this.data = null;
        this.snapshotError = new StatusError(404, "Object deleted");
      } else {
        this.data = null;
        this.snapshotError = new ProtocolError(
          "Subscription snapshot requires resync",
        );
      }
      if (this.queue.length >= 64) {
        this.fail(new StatusError(429, "Subscription consumer too slow"));
        return;
      }
      if (this.waiters.length)
        this.waiters.shift().resolve({ value: u, done: false });
      else this.queue.push(u);
    }
    this.transport
      .send(
        OP.ACK,
        new Writer().id(this.id).u64(this.cursor).bytes(),
        newID(),
        0,
      )
      .catch((error) => this.fail(error));
  }
  fail(error) {
    if (this.closed) return;
    this.closed = true;
    this.error = error;
    for (const waiter of this.waiters) waiter.reject(error);
    this.waiters = [];
  }
  snapshot() {
    if (this.error) throw this.error;
    if (this.snapshotError) throw this.snapshotError;
    return {
      data: this.data.slice(),
      info: {
        ...this.info,
        proof: { subscription: this.id, cursor: this.cursor },
      },
      cursor: this.cursor,
    };
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  next() {
    if (this.queue.length)
      return Promise.resolve({ value: this.queue.shift(), done: false });
    if (this.error) return Promise.reject(this.error);
    if (this.closed) return Promise.resolve({ done: true });
    return new Promise((resolve, reject) =>
      this.waiters.push({ resolve, reject }),
    );
  }
  async close() {
    this.client.subscriptions.delete(this.id);
    if (!this.transport?.closed)
      await this.transport.request(
        OP.UNSUBSCRIBE,
        new Writer().id(this.id).bytes(),
      );
    this.closed = true;
    for (const w of this.waiters) w.resolve({ done: true });
    this.waiters = [];
  }
}

// Split durability barrier: later marks remain pending when confirmations apply.
export class Fence {
  constructor() {
    this.targets = [];
    this.waiting = false;
    this.closed = false;
  }
  add(client) {
    if (this.closed || this.waiting) throw Error("Fence is closed or waiting");
    for (const t of client.allTransports) {
      if (t.noAck.size)
        this.targets.push({ client, transport: t, cut: [...t.noAck.keys()] });
      for (const [boot, sequence] of t.durability)
        this.targets.push({
          client,
          mark: {
            bucket: client.bucket,
            boot,
            sequence,
            node: t.node,
            endpoint: t.endpoint,
            proxyNode: t.proxyNode,
          },
        });
    }
    return this;
  }
  get count() {
    return this.targets.length;
  }
  async waitOne(index, o = {}) {
    if (
      this.closed ||
      !Number.isInteger(index) ||
      index < 0 ||
      index >= this.targets.length
    )
      throw RangeError("Invalid fence target");
    const target = this.targets[index];
    if (target.mark)
      target.confirmed = await target.client.syncThrough(target.mark, {
        ...o,
        observe: false,
      });
    else await target.client.syncConnection(target.transport, o, target.cut);
    target.complete = true;
  }
  async wait(o = {}) {
    if (this.closed || this.waiting) throw Error("Fence is closed or waiting");
    this.waiting = true;
    try {
      for (let i = 0; i < this.targets.length; i++) await this.waitOne(i, o);
    } finally {
      this.waiting = false;
    }
    return this;
  }
  apply() {
    if (this.waiting || this.targets.some((target) => !target.complete))
      throw Error("Fence has unconfirmed targets");
    for (const target of this.targets)
      if (target.confirmed) target.client.noteDurable(target.confirmed);
    return this;
  }
  reset() {
    if (this.waiting) throw Error("Fence is waiting");
    this.targets = [];
    return this;
  }
  close() {
    if (this.waiting) throw Error("Fence is waiting");
    this.closed = true;
    this.targets = [];
  }
}

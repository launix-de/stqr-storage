// SPDX-License-Identifier: GPL-3.0-only
import test from "node:test";
import assert from "node:assert/strict";
import { Client, Reader, Writer } from "./index.mjs";
const BUCKET = "1".repeat(32),
  BOOT = "2".repeat(32);
class FakeWebSocket extends EventTarget {
  static hangClose = false;
  constructor() {
    super();
    this.protocol = "stqr.v2";
    this.readyState = 0;
    this.bufferedAmount = 0;
    queueMicrotask(() => {
      this.readyState = 1;
      this.onopen?.();
    });
  }
  send(data) {
    const r = new Reader(data);
    r.u8();
    r.u8();
    const op = r.u16(),
      id = r.id();
    if (op !== 1) return;
    const result = new Writer()
        .u16(2)
        .u64(8192)
        .id("3".repeat(32))
        .id("4".repeat(32))
        .u64(1)
        .u64(1)
        .bytes(),
      reply = new Writer()
        .u8(1)
        .u8(2)
        .u16(op)
        .id(id)
        .v(200)
        .blob(new Writer().v(0).bytes())
        .raw(result)
        .bytes();
    queueMicrotask(() => this.onmessage?.({ data: reply.buffer }));
  }
  close() {
    this.readyState = 2;
    if (FakeWebSocket.hangClose) return;
    queueMicrotask(() => {
      this.readyState = 3;
      this.onclose?.();
      this.dispatchEvent(new Event("close"));
    });
  }
}
async function fixture(timeout = 40) {
  const c = await Client.connect({
    endpoint: "http://localhost",
    bucket: BUCKET,
    format: "opaque",
    requestTimeout: timeout,
    WebSocket: FakeWebSocket,
    fetch: async () => ({ ok: true, json: async () => ({ bucket: BUCKET }) }),
  });
  return { c, t: [...c.transports.values()][0] };
}
async function alive(fn) {
  const timer = setInterval(() => {}, 1000);
  try {
    return await fn();
  } finally {
    clearInterval(timer);
  }
}
test("raw sync requests obey default timeout and caller abort", () =>
  alive(async () => {
    const { c, t } = await fixture();
    try {
      t.durability.set(BOOT, 1n);
      await assert.rejects(c.sync(), { name: "TimeoutError" });
      const abort = new AbortController(),
        promise = c.sync({ signal: abort.signal });
      abort.abort();
      await assert.rejects(promise, { name: "AbortError" });
      assert.equal(t.pending.size, 0);
    } finally {
      await c.close();
    }
  }));
test("syncThrough and native unsubscribe obey central timeout", () =>
  alive(async () => {
    const { c, t } = await fixture();
    try {
      t.durability.set(BOOT, 1n);
      await assert.rejects(c.syncThrough(c.pendingMark()), {
        name: "TimeoutError",
      });
      await assert.rejects(t.request(0x111, new Writer().id(BOOT).bytes()), {
        name: "TimeoutError",
      });
      assert.equal(t.pending.size, 0);
    } finally {
      await c.close();
    }
  }));
test("lease teardown close wait observes abort and default deadline", () =>
  alive(async () => {
    FakeWebSocket.hangClose = true;
    try {
      for (const callerAbort of [false, true]) {
        const { c, t } = await fixture();
        c.lease = "local";
        c.leaseTransport = t;
        const abort = new AbortController(),
          promise = c.endMaintenance({
            signal: callerAbort ? abort.signal : undefined,
          });
        if (callerAbort) abort.abort();
        await assert.rejects(promise, {
          name: callerAbort ? "AbortError" : "TimeoutError",
        });
        assert.equal(t.closed, true);
        await c.close();
      }
    } finally {
      FakeWebSocket.hangClose = false;
    }
  }));

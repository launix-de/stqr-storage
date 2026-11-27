// SPDX-License-Identifier: GPL-3.0-only
import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { Writer, Reader, hex, unhex, sha256, SHA256 } from "./codec.mjs";
import { Client, StatusError } from "./index.mjs";
test("canonical unsigned codec preserves full u64 and rejects invalid encodings", () => {
  for (const value of [
    0n,
    127n,
    128n,
    65535n,
    9007199254740993n,
    18446744073709551615n,
  ]) {
    const r = new Reader(new Writer().u64(value).v(value).bytes());
    assert.equal(r.u64(), value);
    assert.equal(r.v(), value);
    r.done();
  }
  assert.throws(() => new Writer().u64(9007199254740992));
  assert.throws(() => new Writer().u64(-1n));
  assert.throws(() => new Reader(new Uint8Array([128, 0])).v());
  assert.throws(() => new Reader(new Uint8Array([2])).bool());
  assert.throws(() => new Reader(new Uint8Array([1])).u64());
  assert.throws(() => unhex("x".repeat(64)));
  assert.throws(() => new Reader(new Uint8Array([1, 255])).text());
});
test("hash matches SHA-256 for padding boundaries and native transfer-size data", () => {
  for (const n of [0, 1, 55, 56, 63, 64, 65, 1000, 1048576]) {
    const data = Uint8Array.from({ length: n }, (_, i) => i % 251);
    assert.equal(
      hex(sha256(data)),
      createHash("sha256").update(data).digest("hex"),
    );
    const hash = new SHA256();
    for (let at = 0; at < data.length; at += 17)
      hash.update(data.subarray(at, at + 17));
    assert.equal(hex(hash.digest()), hex(sha256(data)));
  }
});
test("client rejects invalid selectors and unsafe offsets without RPC", () => {
  const config = {
    endpoint: "http://localhost:7419",
    bucket: "sample",
    format: "opaque<v2>",
  };
  for (const bucket of ["", " bad", "x\n"])
    assert.throws(() => new Client({ ...config, bucket }));
  for (const format of ["", " bad", "x\n"])
    assert.throws(() => new Client({ ...config, format }));
  assert.throws(
    () => new Client({ ...config, endpoint: "http://user:pass@localhost" }),
  );
  const c = new Client(config);
  c.bucket = "1".repeat(32);
  assert.throws(() =>
    c.cas("1".repeat(64), 0n, new Uint8Array(1), new Uint8Array(2)),
  );
  assert.throws(() =>
    c.cas("1".repeat(64), 1n, new Uint8Array(), new Uint8Array(), {
      scope: "object",
    }),
  );
  assert.throws(() =>
    c.putRange("1".repeat(64), 9007199254740992, new Uint8Array()),
  );
  c.close();
});
test("routing map is scoped to its bucket and affected range", () => {
  const c = new Client({
    endpoint: "http://localhost:7419",
    bucket: "sample",
    format: "opaque",
  });
  c.bucket = "1".repeat(32);
  const writer = new Writer()
    .u64(1)
    .u64(1)
    .u64(2)
    .id(c.bucket)
    .raw(new Uint8Array(32))
    .bool(true)
    .raw(new Uint8Array(32))
    .v(1)
    .raw(new Uint8Array(32))
    .u16(0)
    .u64(1)
    .u64(3)
    .u8(0)
    .id("2".repeat(32))
    .v(1)
    .id("3".repeat(32))
    .id("4".repeat(32))
    .u8(0)
    .u8(0)
    .u16(0)
    .u32(0)
    .v(1)
    .id("3".repeat(32))
    .v(1)
    .text("http://localhost:7420")
    .v(0);
  c.learn(writer.bytes(), "5".repeat(64));
  assert.equal(c.route("5".repeat(64)).endpoint, "http://localhost:7420/");
  assert.equal(c.route("5".repeat(64)).epoch, 3n);
  const other = new Client({
    endpoint: "http://localhost",
    bucket: "sample",
    format: "opaque",
  });
  other.bucket = "9".repeat(32);
  assert.throws(() => other.learn(writer.bytes(), "5".repeat(64)));
  c.close();
  other.close();
});

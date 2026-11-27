// SPDX-License-Identifier: GPL-3.0-only
import test from "node:test";
import assert from "node:assert/strict";
import {
  decodeNode,
  decodeDirectory,
  decodeIndex,
  readFile,
} from "./format-plugins/fs.mjs";
import { decodeObject, setPublicRead } from "./format-plugins/s3.mjs";
import {
  decodeMetadata,
  encodeMetadata,
  resize,
  validate,
} from "./format-plugins/bd.mjs";
const utf8 = new TextEncoder();
class Writer {
  constructor() {
    this.a = [];
  }
  raw(b) {
    this.a.push(...b);
    return this;
  }
  u8(n) {
    this.a.push(n);
    return this;
  }
  u16(n) {
    return this.u8(n & 255).u8(n >>> 8);
  }
  u32(n) {
    for (let i = 0; i < 4; i++) this.u8((n >>> (8 * i)) & 255);
    return this;
  }
  u64(n) {
    n = BigInt(n);
    for (let i = 0; i < 8; i++) {
      this.u8(Number(n & 255n));
      n >>= 8n;
    }
    return this;
  }
  blob(b) {
    let n = b.length;
    do {
      this.u8((n & 127) | (n > 127 ? 128 : 0));
      n >>>= 7;
    } while (n);
    return this.raw(b);
  }
  text(s) {
    return this.blob(utf8.encode(s));
  }
  bytes() {
    return new Uint8Array(this.a);
  }
}
test("FS legacy v2 indexes preserve original 64-bit offsets and reject historical references", () => {
  const fixture = (token = 0n) => new Writer()
    .raw(utf8.encode("STIX")).u16(2).u32(2)
    .u64(16384n).raw(new Uint8Array(32).fill(7)).u64(9n).u64(token)
    .u64(16384n).raw(new Uint8Array(32)).u64(0n).u64(0n).bytes();
  const raw = fixture();
  const parts = decodeIndex(raw, 32768n, 1);
  assert.equal(parts[0].offset, 9n);
  assert.equal(parts[0].length, 16384n);
  assert.equal(parts[1].id, "0".repeat(64));
  assert.equal(parts[1].length, 16384n);
  assert.throws(() => decodeIndex(fixture(123n), 32768n, 1));
  assert.throws(() => decodeIndex(raw.slice(0, -1), 32768n, 1));
  const extra = new Uint8Array(raw.length + 1);
  extra.set(raw);
  assert.throws(() => decodeIndex(extra, 32768n, 1));
});
function inode(
  data,
  {
    kind = 1,
    layout = 0,
    root = new Uint8Array(32).fill(7),
    length = BigInt(data.length),
  } = {},
) {
  const w = new Writer()
    .raw(utf8.encode("STFS"))
    .u16(7)
    .u8(kind)
    .u32(0o644)
    .u32(0)
    .u32(0)
    .u64(0)
    .u64(length)
    .u8(layout);
  if (layout === 0) w.blob(data);
  else w.u8(1).raw(root);
  return w
    .u8(0)
    .text("")
    .u64(0)
    .u64(0)
    .u32(1)
    .u32(0)
    .u8(1)
    .raw(new Uint8Array(32))
    .u8(0)
    .bytes();
}
test("FS reads actual v7 inode layout and rejects truncated/corrupt metadata", () => {
  const raw = inode(utf8.encode("hello")),
    node = decodeNode(raw);
  assert.equal(node.length, 5n);
  assert.equal(new TextDecoder().decode(node.inline), "hello");
  assert.equal(node.mutable, true);
  assert.throws(() => decodeNode(raw.subarray(0, raw.length - 1)));
  const invalid = raw.slice();
  invalid[6] = 255;
  assert.throws(() => decodeNode(invalid));
});
test("FS directory names and segment partitions are strictly validated", () => {
  const id = new Uint8Array(32).fill(1),
    dir = new Writer()
      .raw(utf8.encode("STDR"))
      .u16(1)
      .u64(1)
      .text("file")
      .raw(id)
      .bytes();
  assert.equal(decodeDirectory(dir)[0].name, "file");
  const bad = new Writer()
    .raw(utf8.encode("STDR"))
    .u16(1)
    .u64(1)
    .text("../bad")
    .raw(id)
    .bytes();
  assert.throws(() => decodeDirectory(bad));
  const index = new Writer()
    .raw(utf8.encode("STIX"))
    .u16(3)
    .u32(1)
    .u64(8)
    .raw(id)
    .u64(0)
    .u8(0)
    .bytes();
  assert.equal(decodeIndex(index, 8n, 1)[0].length, 8n);
  assert.throws(() => decodeIndex(index, 9n, 1));
});
test("FS refuses a download when inode changes during read", async () => {
  const initial = inode(utf8.encode("one")),
    updated = inode(utf8.encode("two"));
  let reads = 0;
  const store = {
    get: async () => ({ data: ++reads === 1 ? initial : updated }),
  };
  await assert.rejects(readFile(store, "1".repeat(64)), /geändert/);
});
test("FS resolves a committed namespace intent without issuing mutations", async () => {
  const id = "1".repeat(64),
    txid = "2".repeat(64),
    before = inode(utf8.encode("old")),
    after = inode(utf8.encode("new"));
  const unhex = (s) => Uint8Array.from(s.match(/../g), (b) => parseInt(b, 16));
  const lock = new Writer()
      .raw(utf8.encode("STFL"))
      .u16(1)
      .raw(unhex(txid))
      .bytes(),
    tx = new Writer()
      .raw(utf8.encode("STFT"))
      .u16(3)
      .u8(1)
      .u32(1)
      .raw(unhex(id))
      .blob(before)
      .blob(after)
      .u32(0)
      .bytes();
  const store = { get: async (oid) => ({ data: oid === id ? lock : tx }) };
  const result = await readFile(store, id);
  assert.equal(await result.blob.text(), "new");
});
function s3(flags = 0) {
  const id = "1".repeat(64),
    metadata = new Writer()
      .text("key.txt")
      .u8(1)
      .text("content-type")
      .text("text/plain")
      .u8(0)
      .bytes();
  const raw = new Writer()
    .raw(utf8.encode("STS3"))
    .u16(2)
    .u16(0)
    .u32(flags)
    .raw(new Uint8Array(32).fill(0x11))
    .u64(4)
    .u64(0)
    .raw(new Uint8Array(16))
    .u32(metadata.length)
    .raw(metadata)
    .bytes();
  return { id, raw, length: BigInt(raw.length + 4) };
}
test("S3 metadata decodes key/maps and validates payload bounds/flags", () => {
  const fixture = s3(),
    o = decodeObject(fixture.raw, fixture.id, fixture.length);
  assert.equal(o.key, "key.txt");
  assert.equal(o.headers["content-type"], "text/plain");
  assert.throws(() =>
    decodeObject(fixture.raw, fixture.id, fixture.length + 1n),
  );
  const bad = s3(8);
  assert.throws(() => decodeObject(bad.raw, bad.id, bad.length));
});
test("S3 ACL changes compare only the exact four flag bytes and sync", async () => {
  const fixture = s3(),
    object = decodeObject(fixture.raw, fixture.id, fixture.length),
    calls = [];
  await setPublicRead(
    {
      cas: async (...args) => calls.push(args),
      sync: async () => calls.push("sync"),
    },
    object,
    true,
  );
  assert.equal(calls[0][1], 8n);
  assert.deepEqual([...calls[0][2]], [0, 0, 0, 0]);
  assert.deepEqual([...calls[0][3]], [2, 0, 0, 0]);
  assert.equal(calls[0][4].scope, "range");
  assert.equal(calls[1], "sync");
});
test("BD descriptors retain native layout, identity and exact u64 sizes", () => {
  const m = {
    id: "12".repeat(16),
    name: "disk",
    size: 9007199254741504n,
    manufacturer: "STQR",
    model: "Striped block device",
    serial: "serial",
  };
  const raw = encodeMetadata(m),
    decoded = decodeMetadata(raw);
  assert.equal(decoded.size, m.size);
  assert.equal(decoded.id, m.id);
  assert.equal(decoded.name, "disk");
  assert.throws(() => validate({ ...m, size: 513n }));
  assert.throws(() => decodeMetadata(raw.subarray(0, raw.length - 1)));
});
test("BD rejects shrinking before contacting storage", async () => {
  await assert.rejects(resize({}, { size: 2048n }, 1024n), /Verkleinern/);
});

test("FS validates directory ordering by UTF-8 bytes like the Go codec", () => {
  const id = new Uint8Array(32).fill(1),
    dir = new Writer()
      .raw(utf8.encode("STDR"))
      .u16(1)
      .u64(2)
      .text("\ue000")
      .raw(id)
      .text("\u{10000}")
      .raw(id)
      .bytes();
  assert.equal(decodeDirectory(dir).length, 2);
});

test("FS bootstrap encodes a v7 directory root with an empty v1 directory", async () => {
  const { Writer } = await import("../client/js/codec.mjs");
  const { encodeEmptyRoot } = await import("./format-plugins/fs.mjs");
  const node = decodeNode(encodeEmptyRoot(123456789n, Writer));
  assert.equal(node.kind, 2);
  assert.equal(node.links, 2);
  assert.equal(node.mode, 0o755);
  assert.equal(node.mtime, 123456789n);
  assert.deepEqual(decodeDirectory(node.inline), []);
});

// SPDX-License-Identifier: GPL-3.0-only
import test from "node:test";
import assert from "node:assert/strict";
import { Client, Writer } from "./index.mjs";
const bucket = "1".repeat(32);
function table(count) {
  const data = new Uint8Array(24 + count * 81);
  data.set(new Writer().raw(new TextEncoder().encode("ORF1")).id(bucket).u32(count).bytes());
  const view = new DataView(data.buffer);
  for (let i = 0; i < count; i++) {
    view.setUint32(24 + i * 81 + 28, i, true);
    data[24 + i * 81 + 80] = 2;
  }
  return data;
}
const dependencies = (data, selectedBucket = bucket) =>
  Client.prototype.readObjectDependencies.call({
    bucket: selectedBucket,
    get: async () => ({ data }),
  });
test("dependency tables grow beyond the former 8192-entry ceiling", async () => {
  const data = table(8193);
  const ids = await dependencies(data);
  assert.equal(ids.length, 8193);
  assert.equal(new Set(ids).size, 8193);
  assert.deepEqual(await dependencies(data, "2".repeat(32)), []);
});
test("dependency counts must match actual object bytes exactly", async () => {
  const data = table(2);
  await assert.rejects(dependencies(data.subarray(0, data.length - 1)));
  await assert.rejects(dependencies(new Uint8Array([...data, 0])));
  new DataView(data.buffer).setUint32(20, 0xffffffff, true);
  await assert.rejects(dependencies(data));
  await assert.rejects(dependencies(new Uint8Array(23)));
});
test("dependency flags and empty tables remain validated", async () => {
  assert.deepEqual(await dependencies(table(0)), []);
  const data = table(1);
  data[104] = 3;
  await assert.rejects(dependencies(data));
});

// SPDX-License-Identifier: GPL-3.0-only
import assert from "node:assert/strict";
import { Client, hex, sha256, newID } from "../client/js/index.mjs";
let input = "";
for await (const chunk of process.stdin) input += chunk;
const cfg = JSON.parse(input),
  data = (s) => new TextEncoder().encode(s),
  text = (b) => new TextDecoder().decode(b),
  oid = (s) => hex(sha256(data(s)));
const client = await Client.connect({
  ...cfg,
  format: "js-native",
  requestTimeout: 10000,
});
try {
  assert.equal(client.bucket, cfg.bucketID);
  assert.equal(await client.bucketAccess(), 15);
  const id = oid("operations"),
    requestId = newID();
  await client.put(id, data("hello"), { absent: true, requestId });
  console.log("STEP put");
  assert.equal(text((await client.get(id)).data), "hello");
  assert.equal((await client.stat(id)).length, 5n);
  await client.putRange(id, 1n, data("A"));
  assert.equal(text((await client.getRange(id, 1n, 2n)).data), "Al");
  await client.cas(id, 1n, data("A"), data("e"));
  const appended = await client.append(id, data("!"));
  assert.equal(appended.startOffset, 5n);
  assert.equal(appended.info.length, 6n);
  await client.truncate(id, 5n);
  await assert.rejects(client.put(id, data("bad"), { expectedLength: 99n }), {
    status: 412,
  });
  await assert.rejects(
    client.getRange(id, 0n, 1n, {
      condition: { length: 5n, offset: 0n, bytes: data("bad") },
    }),
    { status: 412 },
  );
  assert.equal(
    text(
      (
        await client.getRange(id, 0n, 1n, {
          condition: { length: 5n, offset: 0n, bytes: data("hello") },
        })
      ).data,
    ),
    "h",
  );
  const sub = await client.subscribe(id);
  assert.equal(text(sub.data), "hello");
  await client.append(id, data("s"));
  const update = await sub.next();
  assert.equal(update.value.change, 0);
  assert.equal(text(update.value.data), "hellos");
  await sub.close();
  console.log("STEP subscription");
  const token = newID();
  await client.pinObject(id, token);
  await client.unpinObject(id, token);
  await client.pinObjectDependency(id, token);
  assert.deepEqual(await client.readObjectDependencies(), [id]);
  await client.unpinObjectDependency(id, token);
  console.log("STEP references");
  const large = new Uint8Array(2 * 1024 * 1024 + 17);
  for (let i = 0; i < large.length; i++) large[i] = i % 251;
  await client.put(oid("stream"), large);
  assert.deepEqual((await client.get(oid("stream"))).data, large);
  let read = 0;
  await client.getTo(oid("stream"), (chunk) => {
    assert.deepEqual(chunk, large.subarray(read, read + chunk.length));
    read += chunk.length;
  });
  assert.equal(read, large.length);
  await client.putFrom(
    oid("source"),
    [large.subarray(0, 100), large.subarray(100)],
    BigInt(large.length),
  );
  assert.deepEqual((await client.get(oid("source"))).data, large);
  console.log("STEP streams");
  await client.put(oid("accepted"), data("accepted"), { accepted: true });
  const mark = client.pendingMark();
  assert(mark && mark.sequence > 0n);
  const fence = client.captureFence();
  await fence.wait();
  fence.apply();
  fence.close();
  await client.sync();
  assert.equal(text((await client.get(oid("accepted"))).data), "accepted");
  await client.putAsync(oid("async"), data("async"));
  await client.sync();
  assert.equal(text((await client.get(oid("async"))).data), "async");
  await client.putRangeAsync(oid("async"), 1n, data("Z"));
  await client.sync();
  assert.equal(text((await client.get(oid("async"))).data), "aZync");
  await client.putAsync(oid("async"), data("bad"), { absent: true });
  await assert.rejects(client.sync(), { status: 412 });
  const concurrent = Array.from({ length: 6 }, (_, i) => ({
    id: oid("concurrent-" + i),
    value: data("value-" + i),
  }));
  await Promise.all(concurrent.map((e) => client.put(e.id, e.value)));
  await Promise.all(
    concurrent.map(async (e) =>
      assert.deepEqual((await client.get(e.id)).data, e.value),
    ),
  );
  let after = "",
    all = [];
  do {
    const page = await client.scanObjects(after, 2);
    all.push(...page.objects);
    after = page.next;
  } while (after);
  assert(all.length >= 10);
  assert(all.some((e) => e.id === id));
  const space = await client.space();
  assert.equal(space.used + space.free, space.total);
  await client.delete(id);
  await assert.rejects(client.get(id), { status: 404 });
  console.log("STEP lease acquire");
  await client.beginObjectLease(false);
  console.log("STEP lease acquired");
  await client.put(oid("lease"), data("leased"));
  console.log("STEP lease release");
  await client.endMaintenance();
  console.log("STEP lease reacquire");
  await client.beginObjectLease(false);
  console.log("STEP lease reacquired");
  assert.equal(text((await client.get(oid("lease"))).data), "leased");
  console.log("STEP final lease release");
  await client.endMaintenance();
  console.log("JS_NATIVE_OK " + cfg.routing);
} finally {
  await client.close();
}

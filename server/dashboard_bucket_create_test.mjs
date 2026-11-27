// SPDX-License-Identifier: GPL-3.0-only
import test from "node:test";
import assert from "node:assert/strict";
import { finishBucketCreation } from "./dashboard/bucket-create.js";
import { FormatPlugins } from "./dashboard/format-plugins.js";
function fixture(format = "custom") {
  const calls = [],
    progress = {
      bucket: { id: "1".repeat(32), name: "new", format },
      owner: "2".repeat(32),
      id: "create-id",
      grantID: "grant-id",
      initializationID: "init-id",
      initializationState: {},
    };
  const connection = {
    createBucket: async (...args) => calls.push(["create", ...args]),
    grantBucket: async (...args) => calls.push(["grant", ...args]),
  };
  const plugins = {
    module: async (...args) => calls.push(["module", ...args]),
    initialize: async (...args) => calls.push(["initialize", ...args]),
  };
  return { calls, progress, connection, plugins };
}
test("empty selection creates a bare bucket without loading or binding any plugin", async () => {
  const f = fixture("");
  await finishBucketCreation(f);
  assert.deepEqual(
    f.calls.map((c) => c[0]),
    ["create", "grant"],
  );
  assert.equal(f.progress.initialized, true);
});
test("optional initialization runs after creation and before granting a user access", async () => {
  const f = fixture();
  await finishBucketCreation(f);
  assert.deepEqual(
    f.calls.map((c) => c[0]),
    ["module", "create", "initialize", "grant"],
  );
  assert.equal(f.calls[2][2].initializationID, "init-id");
  assert.equal(f.calls[2][2].state, f.progress.initializationState);
});
test("an initializer failure resumes the same bucket and preserves initializer state", async () => {
  const f = fixture();
  let tries = 0;
  f.plugins.initialize = async (bucket, options) => {
    assert.equal(bucket.id, f.progress.bucket.id);
    options.state.attempts = ++tries;
    if (tries === 1) throw Error("temporary initialization failure");
  };
  await assert.rejects(finishBucketCreation(f), /temporary/);
  assert.equal(f.progress.created, true);
  assert.equal(f.progress.initialized, undefined);
  assert.equal(f.calls.filter((c) => c[0] === "grant").length, 0);
  await finishBucketCreation(f);
  assert.equal(f.calls.filter((c) => c[0] === "create").length, 1);
  assert.equal(f.progress.initializationState.attempts, 2);
  assert.equal(f.calls.find((c) => c[0] === "grant")[4], "grant-id");
  await finishBucketCreation(f);
  assert.equal(tries, 2);
});
test("failed grants retry without initializing or creating again", async () => {
  const f = fixture();
  let tries = 0;
  f.connection.grantBucket = async (...args) => {
    assert.equal(args[3], "grant-id");
    if (++tries === 1) throw Error("grant failed");
  };
  await assert.rejects(finishBucketCreation(f), /grant failed/);
  assert.equal(f.progress.initialized, true);
  await finishBucketCreation(f);
  assert.equal(f.calls.filter((c) => c[0] === "create").length, 1);
  assert.equal(f.calls.filter((c) => c[0] === "initialize").length, 1);
});
test("a broken plugin fails before the administrative creation", async () => {
  const f = fixture();
  f.plugins.module = async () => {
    throw Error("invalid module");
  };
  await assert.rejects(finishBucketCreation(f), /invalid/);
  assert.equal(f.calls.length, 0);
});
function host(plugin) {
  const calls = [],
    client = {
      sync: async () => calls.push("sync"),
      close: async () => calls.push("close"),
    };
  const h = Object.assign(Object.create(FormatPlugins.prototype), {
    entries: new Map([["custom", "/plugin.mjs"]]),
    modules: new Map(),
    controller: new AbortController(),
    importModule: async () => ({ apiVersion: 1, open() {}, ...plugin }),
    connect: async (bucket) => {
      calls.push(["connect", bucket.format]);
      return client;
    },
  });
  return { h, calls, client };
}
test("host binds format even without an initializer and awaits sync and close", async () => {
  const { h, calls } = host();
  await h.initialize({ format: "custom" });
  assert.deepEqual(calls, [["connect", "custom"], "sync", "close"]);
});
test("host exposes one cached native client to the optional initializer", async () => {
  let seen;
  const { h, calls, client } = host({
    initialize: async (context) => {
      seen = context;
      assert.equal(await context.connect(), await context.connect());
    },
  });
  const state = {};
  await h.initialize({ format: "custom" }, { initializationID: "id", state });
  assert.equal(await seen.connect(), client);
  assert.equal(seen.state, state);
  assert.equal(seen.initializationID, "id");
  assert.deepEqual(calls, [["connect", "custom"], "sync", "close"]);
});
test("failed initializers close their client and invalid hooks are rejected", async () => {
  const { h, calls } = host({
    initialize: async () => {
      throw Error("failed");
    },
  });
  await assert.rejects(h.initialize({ format: "custom" }), /failed/);
  assert.deepEqual(calls, [["connect", "custom"], "close"]);
  const invalid = host({ initialize: true });
  await assert.rejects(invalid.h.module("custom"), /Initializer/);
});

test("an initializer cannot connect after its originating session ends", async () => {
  let release;
  const delayed = new Promise((resolve) => (release = resolve));
  const { h, calls } = host();
  h.importModule = () => delayed;
  const initializing = h.initialize({ format: "custom" });
  h.controller.abort();
  h.controller = new AbortController();
  release({ apiVersion: 1, open() {} });
  await assert.rejects(initializing, { name: "AbortError" });
  assert.deepEqual(calls, []);
});

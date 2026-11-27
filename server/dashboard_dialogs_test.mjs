// SPDX-License-Identifier: GPL-3.0-only
import test from "node:test";
import assert from "node:assert/strict";
import { configureDialog, requestDialogClose } from "./dashboard/dialogs.js";
class Observer {
  constructor() {
    this.records = [];
  }
  observe(root) {
    root.observer = this;
  }
  takeRecords() {
    return this.records.splice(0);
  }
  disconnect() {}
}
globalThis.MutationObserver = Observer;
class Dialog extends EventTarget {
  constructor() {
    super();
    this.tagName = "DIALOG";
    this.ownerDocument = new EventTarget();
    this.ownerDocument.createElement = () => ({
      dataset: {},
      setAttribute() {},
      textContent: "",
    });
    this.open = true;
    this.dataset = {};
  }
  querySelector(selector) {
    return selector === "[data-dialog-close-error]"
      ? this.error
      : this.disabledClose
        ? {}
        : null;
  }
  append(field) {
    this.error = field;
  }
  close() {
    this.open = false;
    this.dispatchEvent(new Event("close"));
  }
}
const deferred = () => {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
};
test("close attempts return one shared Promise and await an asynchronous decision", async () => {
  const dialog = new Dialog(),
    decision = deferred();
  let calls = 0;
  configureDialog(dialog, {
    canClose: async () => {
      calls++;
      return decision.promise;
    },
  });
  const first = requestDialogClose(dialog),
    second = requestDialogClose(dialog);
  assert.equal(first, second);
  await Promise.resolve();
  assert.equal(dialog.open, true);
  assert.equal(calls, 1);
  decision.resolve(true);
  assert.equal(await first, true);
  assert.equal(dialog.open, false);
});
test("denied decisions, modal policy and disabled close controls keep dialogs open", async () => {
  for (const options of [
    { canClose: async () => false },
    { dismissible: false },
  ]) {
    const dialog = new Dialog();
    configureDialog(dialog, options);
    assert.equal(await requestDialogClose(dialog), false);
    assert.equal(dialog.open, true);
  }
  const dialog = new Dialog();
  dialog.disabledClose = true;
  configureDialog(dialog);
  assert.equal(await requestDialogClose(dialog), false);
});
test("legacy cancel listeners retain veto and see the close reason", async () => {
  const dialog = new Dialog();
  let reason,
    cancels = 0;
  configureDialog(dialog);
  dialog.addEventListener("cancel", (event) => {
    reason = event.closeReason;
    cancels++;
    event.preventDefault();
  });
  assert.equal(await requestDialogClose(dialog, "backdrop"), false);
  assert.equal(reason, "backdrop");
  assert.equal(cancels, 1);
  assert.equal(dialog.open, true);
});
test("forced close settles a pending dismissal even if its condition never resolves", async () => {
  const dialog = new Dialog();
  configureDialog(dialog, { canClose: () => new Promise(() => {}) });
  const result = requestDialogClose(dialog);
  await Promise.resolve();
  dialog.close();
  assert.equal(await result, false);
});
test("an old decision cannot dismiss a reopened instance or replace a new policy", async () => {
  const dialog = new Dialog(),
    decision = deferred();
  configureDialog(dialog, { canClose: () => decision.promise });
  const result = requestDialogClose(dialog);
  await Promise.resolve();
  dialog.ownerDocument.observer.records.push({ target: dialog });
  decision.resolve(true);
  assert.equal(await result, false);
  assert.equal(dialog.open, true);
});
test("a rejected condition is shown as an error without dismissing the popup", async () => {
  const dialog = new Dialog();
  configureDialog(dialog, {
    canClose: async () => {
      throw Error("confirmation failed");
    },
  });
  assert.equal(await requestDialogClose(dialog), false);
  assert.equal(dialog.open, true);
  assert.equal(dialog.error.textContent, "confirmation failed");
});

test("replacing a pending condition cancels the old attempt without hanging", async () => {
  const dialog = new Dialog();
  configureDialog(dialog, { canClose: () => new Promise(() => {}) });
  const old = requestDialogClose(dialog);
  await Promise.resolve();
  configureDialog(dialog, { canClose: async () => true });
  assert.equal(await old, false);
  assert.equal(await requestDialogClose(dialog), true);
});

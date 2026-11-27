// SPDX-License-Identifier: GPL-3.0-only
import assert from "node:assert/strict";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url),
  { chromium } = require(process.env.STQR_TEST_BROWSER_MODULE);
let input = "";
for await (const chunk of process.stdin) input += chunk;
const cfg = JSON.parse(input);
const browser = await chromium.launch({
  executablePath:
    process.env.STQR_TEST_BROWSER_EXECUTABLE ?? "/usr/bin/google-chrome",
  headless: true,
  args: ["--no-sandbox"],
});
try {
  const page = await browser.newPage(),
    errors = [];
  page.setDefaultTimeout(10000);
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(cfg.url);
  await page.evaluate(async () => {
    window.dialogs = await import("/assets/dialogs.js");
    const lib = await import("/assets/plugin-ui.js");
    window.windows = lib.createWindows();
    window.ui = lib.ui;
    window.openView = (options) =>
      (window.view = window.windows.open({ title: "Test popup", ...options }));
  });
  await page.evaluate(() => window.openView());
  const root = page.locator(".plugin-window-scroll");
  await root.click();
  assert.equal(await page.locator(".plugin-window").count(), 1);
  const box = await root.boundingBox();
  await page.mouse.move(box.x + 20, box.y + 20);
  await page.mouse.down();
  await page.mouse.move(1, 1);
  await page.mouse.up();
  assert.equal(await page.locator(".plugin-window").count(), 1);
  await page.mouse.click(1, 1);
  await page.waitForFunction(() => window.view.signal.aborted);
  assert.equal(await page.evaluate(() => window.view.closed), "");
  await page.evaluate(() => {
    window.calls = 0;
    window.openView({
      canClose: async () => {
        window.calls++;
        return new Promise((resolve) => (window.decide = resolve));
      },
    });
  });
  await page.keyboard.press("Escape");
  await page.waitForFunction(() => window.calls === 1);
  await page.mouse.click(1, 1);
  await page.locator(".plugin-window > .dialog-heading button").click();
  assert.equal(await page.evaluate(() => window.calls), 1);
  await page.evaluate(() => window.decide(false));
  await page.waitForTimeout(30);
  assert.equal(await page.locator(".plugin-window").count(), 1);
  await page.mouse.click(1, 1);
  await page.waitForFunction(() => window.calls === 2);
  await page.evaluate(() => window.decide(true));
  await page.waitForFunction(() => window.view.signal.aborted);
  await page.evaluate(() => window.openView({ dismissible: false }));
  await page.mouse.click(1, 1);
  await page.keyboard.press("Escape");
  await page.locator(".plugin-window > .dialog-heading button").click();
  assert.equal(await page.locator(".plugin-window").count(), 1);
  assert.equal(
    await page.evaluate(async () => {
      await window.view.close("done");
      return window.view.closed;
    }),
    "done",
  );
  await page.evaluate(() => {
    window.parentView = window.windows.open({ title: "Parent" });
    window.childView = window.windows.open({
      title: "Child",
      signal: window.parentView.signal,
    });
  });
  await page.mouse.click(1, 1);
  await page.waitForFunction(() => window.childView.signal.aborted);
  assert.equal(await page.evaluate(() => window.parentView.signal.aborted), false);
  await page.evaluate(() => window.parentView.close());
  await page.evaluate(() =>
    window.openView({
      canClose: ({ signal }) =>
        window.ui.confirm({ message: "Änderungen verwerfen?", signal }),
    }),
  );
  await page.mouse.click(1, 1);
  await page.locator(".confirmation-dialog").waitFor();
  assert.equal(await page.locator(".plugin-window").count(), 1);
  await page.mouse.click(1, 1);
  await page.locator(".confirmation-dialog").waitFor({ state: "detached" });
  assert.equal(await page.locator(".plugin-window").count(), 1);
  await page.locator(".plugin-window > .dialog-heading button").click();
  await page.locator(".confirmation-dialog button.primary").click();
  await page.waitForFunction(() => window.view.signal.aborted);
  await page.evaluate(() => {
    window.legacy = document.createElement("dialog");
    legacy.innerHTML =
      '<form method="dialog"><p>Legacy popup</p><button>Schließen</button></form>';
    window.block = true;
    window.cancels = 0;
    legacy.addEventListener("cancel", (e) => {
      window.cancels++;
      if (window.block) e.preventDefault();
    });
    document.body.append(legacy);
    legacy.showModal();
  });
  await page.mouse.click(1, 1);
  await page.waitForFunction(() => window.cancels === 1);
  assert.equal(await page.evaluate(() => legacy.open), true);
  await page.locator("dialog[open] button").click();
  await page.waitForFunction(() => window.cancels === 2);
  assert.equal(await page.evaluate(() => legacy.open), true);
  await page.evaluate(() => (window.block = false));
  await page.locator("dialog[open] button").click();
  await page.waitForFunction(() => !legacy.open);
  await page.evaluate(() => legacy.remove());
  await page.evaluate(() => {
    window.decide=null;
    window.legacy = document.createElement("dialog");
    legacy.textContent = "Reopened dialog";
    document.body.append(legacy);
    legacy.showModal();
    window.dialogs.configureDialog(legacy, {
      canClose: () => new Promise((resolve) => (window.decide = resolve)),
    });
    window.oldAttempt = window.dialogs.requestDialogClose(legacy);
  });
  await page.waitForFunction(() => typeof window.decide === "function");
  await page.evaluate(() => {
    legacy.close();
    legacy.showModal();
    window.decide(true);
  });
  assert.equal(await page.evaluate(() => window.oldAttempt), false);
  assert.equal(await page.evaluate(() => legacy.open), true);
  await page.evaluate(() => {
    legacy.close();
    legacy.remove();
  });
  assert.deepEqual(errors, []);
  console.log(
    "DIALOGS_OK: backdrop/drag/content, async guarded and modal dismissal, nested windows, confirmation promises, legacy cancel/form hooks, stale attempts and lifecycle.",
  );
} finally {
  await browser.close();
}

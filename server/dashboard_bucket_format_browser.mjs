// SPDX-License-Identifier: GPL-3.0-only
import assert from "node:assert/strict";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url),
  { chromium } = require(process.env.STQR_TEST_BROWSER_MODULE);
let input = "";
for await (const chunk of process.stdin) input += chunk;
const cfg = JSON.parse(input),
  browser = await chromium.launch({
    executablePath:
      process.env.STQR_TEST_BROWSER_EXECUTABLE ?? "/usr/bin/google-chrome",
    headless: true,
    args: ["--no-sandbox"],
  });
try {
  const page = await browser.newPage(),
    errors = [];
  page.setDefaultTimeout(15000);
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(cfg.url);
  await page.locator("#username").fill(cfg.username);
  await page.locator("#password").fill(cfg.password);
  await page.locator("#login-form button[type=submit]").click();
  await page.locator('[data-view="buckets"]').click();
  const lookup = (name) =>
    page.evaluate(async (name) => {
      const { NodeConnection } = await import("/assets/protocol.js"),
        session = await (await fetch("/api/session")).json(),
        connection = new NodeConnection(
          session.csrf_token,
          () => {},
          () => {},
        );
      try {
        await new Promise((resolve) => (connection.onOpen = resolve));
        const page = await connection.listBucketsPage("", 100, name);
        return page.buckets.filter((b) => b.name === name);
      } finally {
        connection.stop();
      }
    }, name);
  const create = async (name, format) => {
    await page.locator("#add-bucket").click();
    await page.locator("#bucket-name").fill(name);
    await page.waitForFunction(
      () => !document.getElementById("bucket-create-submit").disabled,
    );
    assert.equal(await page.locator("#bucket-format").inputValue(), "");
    const choices = await page
        .locator("#bucket-format option")
        .evaluateAll((options) => options.map((o) => o.value)),
      index = await (
        await page.request.get(new URL("/api/format-plugins", cfg.url).href)
      ).json();
    assert.deepEqual(
      [...choices].sort(),
      ["", ...index.plugins.map((p) => p.format)].sort(),
    );
    await page.locator("#bucket-format").selectOption(format);
  };
  for (const format of ["", "bd", "s3", "fs"]) {
    const name = "created-" + (format || "bare");
    await create(name, format);
    await page.locator("#bucket-create-submit").click();
    await page.locator("#bucket-create-dialog").waitFor({ state: "hidden" });
    const matches = await lookup(name);
    assert.equal(matches.length, 1);
    assert.equal(matches[0].format ?? "", format);
    const row = page.locator(`[data-bucket="${matches[0].id}"]`);
    await row.waitFor();
    assert.equal(
      await row.locator("[data-format-bucket]").count(),
      format ? 1 : 0,
    );
    if (format === "fs") {
      await row.locator("[data-format-bucket]").click();
      await page
        .locator('[data-stqr-plugin="fs"] .fs-tree')
        .waitFor({ state: "attached" });
      assert.equal(
        await page.locator('[data-stqr-plugin="fs"] .fs-tree li').count(),
        0,
      );
      await page.locator(".plugin-window > .dialog-heading button").click();
    }
  }
  await create("created-custom-init", "custom-init");
  await page.locator("#bucket-owner").selectOption(cfg.ownerID);
  await page.locator("#bucket-create-submit").click();
  await page
    .locator("#bucket-create-error")
    .filter({ hasText: "Bitte Initialisierung wiederholen" })
    .waitFor();
  assert.equal(
    await page.locator("#bucket-create-submit").textContent(),
    "Initialisierung erneut versuchen",
  );
  const first = await lookup("created-custom-init");
  assert.equal(first.length, 1);
  assert.equal(
    (first[0].grants ?? []).some((g) => g.user_id === cfg.ownerID),
    false,
  );
  assert.equal(await page.locator("#bucket-format").isDisabled(), true);
  assert.equal(await page.locator("#bucket-name").isDisabled(), true);
  // Partial creation needs an explicit answer before abandoning its initializer.
  await page.mouse.click(1, 1);
  await page.locator(".confirmation-dialog").waitFor();
  await page.locator(".confirmation-dialog .secondary").click();
  assert.equal(await page.locator("#bucket-create-dialog").isVisible(), true);
  await page.locator("#bucket-create-submit").click();
  await page.locator("#bucket-create-dialog").waitFor({ state: "hidden" });
  const second = await lookup("created-custom-init");
  assert.equal(second.length, 1);
  assert.equal(second[0].id, first[0].id);
  assert.equal(second[0].format, "custom-init");
  assert.equal(
    second[0].grants.some((g) => g.user_id === cfg.ownerID),
    true,
  );
  await page.locator(`[data-bucket-edit="${second[0].id}"]`).click();
  assert.equal(await page.locator("#bucket-format-field").isVisible(), false);
  await page.locator('[data-close="bucket-create-dialog"]').first().click();
  await page.locator("#bucket-create-dialog").waitFor({ state: "hidden" });
  await create("created-broken", "broken");
  await page.locator("#bucket-create-submit").click();
  await page
    .locator("#bucket-create-error")
    .filter({ hasText: "Ungültiger Plugin-Initializer" })
    .waitFor();
  assert.deepEqual(await lookup("created-broken"), []);
  await page.locator('[data-close="bucket-create-dialog"]').first().click();
  assert.deepEqual(errors, []);
  console.log(
    "BUCKET_FORMAT_OK: directory choices, optional blank format, BD/S3 empty binding, FS root bootstrap, arbitrary initializer retry, grant ordering and no duplicate creation.",
  );
} finally {
  await browser.close();
}

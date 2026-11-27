// SPDX-License-Identifier: GPL-3.0-only
import { ui, escape, createWindows } from "./plugin-ui.js";
// This host treats format identifiers as opaque lookup keys. All format logic
// (including whether an operation is available) belongs to the loaded module.
export class FormatPlugins {
  constructor({
    connect,
    toast,
    onChange = () => {},
    importModule = (url) => import(url),
  }) {
    Object.assign(this, { connect, toast, onChange, importModule });
    this.entries = new Map();
    this.modules = new Map();
    this.windows = createWindows();
    this.generation = 0;
    this.controller = new AbortController();
    this.styles = new Map();
    this.opening = new Set();
  }
  async refresh() {
    const generation = this.generation,
      response = await fetch("/api/format-plugins", {
        credentials: "same-origin",
        signal: this.controller.signal,
      });
    if (!response.ok)
      throw Error("Format-Plugins konnten nicht geladen werden.");
    const { plugins } = await response.json();
    if (generation !== this.generation) return;
    this.entries = new Map(plugins.map((p) => [p.format, p.url]));
    this.onChange();
  }
  has(format) {
    return this.entries.has(format);
  }
  cell(bucket) {
    return this.has(bucket.format)
      ? `<button type="button" class="row-action bucket-format-open" data-format-bucket="${escape(bucket.id)}" ${bucket.state === "deleting" ? "disabled" : ""}>${escape(bucket.format)}</button>`
      : escape(bucket.format || "-");
  }
  async module(format) {
    if (!this.modules.has(format)) {
      const url = this.entries.get(format);
      if (!url) throw Error("Format-Plugin ist nicht installiert.");
      const pending = this.importModule(url).then((plugin) => {
        if (plugin.apiVersion !== 1 || typeof plugin.open !== "function")
          throw Error("Nicht unterstützte Plugin-API.");
        return plugin;
      });
      this.modules.set(format, pending);
      pending.catch(() => {
        if (this.modules.get(format) === pending) this.modules.delete(format);
      });
    }
    return this.modules.get(format);
  }
  style(format, css) {
    if (!css || this.styles.has(format)) return;
    // Prefixing is a plugin ABI, not a sandbox for executable plugin code.
    // Parse first so malformed CSS cannot accidentally discard a scope boundary.
    const scope = `[data-stqr-plugin=${JSON.stringify(format)}]`,
      sheet = new CSSStyleSheet();
    sheet.replaceSync(css);
    const check = (rules) => {
      for (const rule of rules) {
        if (rule.type === CSSRule.STYLE_RULE) {
          for (const selector of rule.selectorText.split(",")) {
            const s = selector.trim();
            if (
              !s.startsWith(scope) ||
              (s.length > scope.length && !/[\s>]/.test(s[scope.length])) ||
              /[+~]/.test(s) ||
              rule.cssRules?.length
            )
              throw Error("Plugin-CSS benötigt den Prefix " + scope);
          }
        } else if (
          rule.type === CSSRule.MEDIA_RULE ||
          rule.type === CSSRule.SUPPORTS_RULE
        )
          check(rule.cssRules);
        else
          throw Error(
            "Plugin-CSS unterstützt nur prefixed Regeln, @media und @supports.",
          );
      }
    };
    check(sheet.cssRules);
    const style = document.createElement("style");
    style.nonce =
      document.querySelector('meta[name="stqr-style-nonce"]')?.content ?? "";
    style.textContent = css;
    document.head.append(style);
    this.styles.set(format, style);
  }
  async open(bucket) {
    const token = bucket.id;
    if (this.opening.has(token)) return;
    this.opening.add(token);
    const generation = this.generation;
    try {
      const plugin = await this.module(bucket.format);
      if (generation !== this.generation) return;
      this.style(bucket.format, plugin.css);
      const lifetime = new AbortController(),
        parent = this.controller.signal,
        abort = () => lifetime.abort();
      parent.addEventListener("abort", abort, { once: true });
      let windows = 0;
      const scopedWindows = {
        open: (options) => {
          windows++;
          const window = this.windows.open({
            ...options,
            signal: options?.signal
              ? AbortSignal.any([options.signal, lifetime.signal])
              : lifetime.signal,
          });
          window.root.dataset.stqrPlugin = bucket.format;
          window.signal.addEventListener(
            "abort",
            () => {
              if (--windows === 0) lifetime.abort();
            },
            { once: true },
          );
          return window;
        },
      };
      lifetime.signal.addEventListener(
        "abort",
        () => parent.removeEventListener("abort", abort),
        { once: true },
      );
      let clientPromise;
      const connect = () =>
        (clientPromise ??= this.connect(bucket, {
          signal: lifetime.signal,
        }).then((client) => {
          if (lifetime.signal.aborted) {
            client.close();
            throw new DOMException("Fenster geschlossen.", "AbortError");
          }
          lifetime.signal.addEventListener("abort", () => client.close(), {
            once: true,
          });
          return client;
        }));
      try {
        const cleanup = await plugin.open({
          bucket: Object.freeze(structuredClone(bucket)),
          connect,
          windows: scopedWindows,
          ui,
          signal: lifetime.signal,
          escape,
          toast: this.toast,
        });
        if (typeof cleanup === "function") {
          if (lifetime.signal.aborted) cleanup();
          else
            lifetime.signal.addEventListener("abort", cleanup, { once: true });
        }
      } catch (e) {
        lifetime.abort();
        throw e;
      }
    } catch (e) {
      if (generation === this.generation && e.name !== "AbortError")
        this.toast(e.message, true);
    } finally {
      this.opening.delete(token);
    }
  }
  clear() {
    this.generation++;
    this.controller.abort();
    this.controller = new AbortController();
    this.windows.closeAll();
    this.entries.clear();
    this.modules.clear();
    this.opening.clear();
    for (const style of this.styles.values()) style.remove();
    this.styles.clear();
  }
}

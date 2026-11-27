// SPDX-License-Identifier: GPL-3.0-only
import {
  configureDialog,
  requestDialogClose,
  installDialogDismissal,
  confirmDialog,
} from "./dialogs.js";
import { EndlessList, bindSearch } from "./endless.js";
export const escape = (s) =>
  String(s ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
export function button(
  label,
  action,
  { kind = "secondary", signal, onError } = {},
) {
  const b = document.createElement("button");
  b.type = "button";
  b.className = kind;
  b.textContent = label;
  b.addEventListener(
    "click",
    async () => {
      b.disabled = true;
      try {
        await action();
      } catch (e) {
        if (!signal?.aborted) {
          if (onError) onError(e);
          else {
            let error = b.parentElement?.querySelector(
              ":scope > .plugin-action-error",
            );
            if (!error) {
              error = document.createElement("p");
              error.className = "error plugin-action-error";
              error.setAttribute("role", "alert");
              b.parentElement?.append(error);
            }
            error.textContent = e.message;
          }
        }
      } finally {
        if (!signal?.aborted) b.disabled = false;
      }
    },
    { signal },
  );
  return b;
}
export function createList({
  container,
  root = null,
  columns,
  fetchPage,
  key,
  renderRow,
  signal,
  placeholder = "Suchen …",
  empty = "Keine passenden Einträge.",
  size = 50,
}) {
  const panel = document.createElement("section");
  panel.className = "storage-panel plugin-list";
  panel.innerHTML = `<div class="panel-heading"><label class="search"><span aria-hidden="true">⌕</span><input type="search" placeholder="${escape(placeholder)}" aria-label="${escape(placeholder)}"></label><button type="button" class="secondary">Aktualisieren</button></div><p class="error" role="alert"></p><div class="table-scroll"><table class="volume-table"><thead><tr>${columns.map((c) => `<th>${escape(c)}</th>`).join("")}</tr></thead><tbody></tbody></table></div>`;
  container.append(panel);
  const rows = panel.querySelector("tbody"),
    error = panel.querySelector(".error"),
    input = panel.querySelector("input"),
    refresh = panel.querySelector("button");
  const pager = new EndlessList({
    container: panel,
    root,
    fetchPage,
    key,
    size,
    visible: () => !signal?.aborted && panel.isConnected,
    onLoading: (busy) => {
      refresh.disabled = busy;
    },
    onError: (e) => {
      error.textContent = e.message;
    },
    onUpdate: (items, state) => {
      if (state.loading) {
        rows.replaceChildren();
        return;
      }
      if (items.length) rows.querySelector(".empty")?.closest("tr").remove();
      const fragment = document.createDocumentFragment();
      for (const item of state.append ? state.added : items) {
        const row = renderRow(item);
        fragment.append(row);
      }
      if (state.append) rows.append(fragment);
      else rows.replaceChildren(fragment);
      if (!items.length) {
        const row = document.createElement("tr"),
          cell = document.createElement("td");
        cell.colSpan = columns.length;
        cell.className = "empty";
        cell.textContent = empty;
        row.append(cell);
        rows.append(row);
      }
    },
  });
  const load = () => {
      error.textContent = "";
      return pager.reset(input.value.trim());
    },
    search = bindSearch(input, load, { onChange: () => pager.cancel() });
  refresh.onclick = load;
  let destroyed = false;
  const destroy = () => {
    if (destroyed) return;
    destroyed = true;
    search.destroy();
    pager.destroy();
    panel.remove();
  };
  signal?.addEventListener("abort", destroy, { once: true });
  return { element: panel, pager, load, destroy, input };
}
export function createWindows() {
  installDialogDismissal();
  const active = new Set();
  return {
    open({ title, signal, canClose = () => true, dismissible = true } = {}) {
      const controller = new AbortController(),
        dialog = document.createElement("dialog");
      dialog.className = "plugin-window";
      dialog.innerHTML =
        '<div class="dialog-heading"><h2></h2><button type="button" class="icon-button" aria-label="Schließen">×</button></div><div class="plugin-window-scroll"></div>';
      dialog.querySelector("h2").textContent = title ?? "";
      document.body.append(dialog);
      const root = dialog.querySelector(".plugin-window-scroll");
      const releaseGuard = configureDialog(dialog, { canClose, dismissible });
      let resolveClosed;
      const closed = new Promise((resolve) => {
        resolveClosed = resolve;
      });
      const finish = () => {
        if (controller.signal.aborted) return;
        controller.abort();
        releaseGuard();
        dialog.remove();
        resolveClosed(dialog.returnValue);
        active.delete(close);
        signal?.removeEventListener("abort", abort);
      };
      const close = (value = "") => {
          dialog.close(String(value));
          finish();
          return closed;
        },
        abort = () => close();
      dialog.querySelector("button").onclick = () =>
        requestDialogClose(dialog, "button");
      dialog.addEventListener("close", finish);
      signal?.addEventListener("abort", abort, { once: true });
      active.add(close);
      if (signal?.aborted) close();
      else dialog.showModal();
      return {
        root,
        scrollRoot: root,
        signal: controller.signal,
        close,
        closed,
        requestClose: (reason) => requestDialogClose(dialog, reason),
      };
    },
    closeAll() {
      for (const close of [...active]) close();
    },
  };
}
// Blob URLs remain alive until the consumer's window closes; downloads survive
// asynchronous browser handoff. Formats choose MIME and payload interpretation.
export function download(
  data,
  name,
  { type = "application/octet-stream", signal } = {},
) {
  if (signal?.aborted) return;
  const url = URL.createObjectURL(new Blob([data], { type })),
    a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.hidden = true;
  document.body.append(a);
  a.click();
  a.remove();
  let timer;
  const release = () => {
    clearTimeout(timer);
    URL.revokeObjectURL(url);
    signal?.removeEventListener("abort", release);
  };
  signal?.addEventListener("abort", release, { once: true });
  timer = setTimeout(release, 60000);
}
export const ui = Object.freeze({
  createList,
  button,
  download,
  escape,
  confirm: confirmDialog,
});

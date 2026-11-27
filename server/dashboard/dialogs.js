// SPDX-License-Identifier: GPL-3.0-only
// All user dismissals pass through the same asynchronous guard and cancel event.
// dialog.close() remains the explicit completion/forced-lifecycle operation.
const policies = new WeakMap(),
  pending = new WeakMap(),
  versions = new WeakMap(),
  roots = new WeakMap(),
  internalCancels = new WeakSet();
function isDialog(node) {
  return node?.tagName === "DIALOG";
}
function permitted(dialog) {
  return (
    dialog.dataset.dialogDismissible !== "false" &&
    policies.get(dialog)?.dismissible !== false &&
    !dialog.querySelector("[data-close]:disabled,[data-grow-close]:disabled")
  );
}
function errorMessage(dialog, error) {
  if (!dialog.open || error?.name === "AbortError") return;
  let field = dialog.querySelector("[data-dialog-close-error]");
  if (!field) {
    field = dialog.ownerDocument.createElement("p");
    field.dataset.dialogCloseError = "";
    field.className = "error";
    field.setAttribute("role", "alert");
    dialog.append(field);
  }
  field.textContent = error.message ?? String(error);
}
function abortable(value, signal) {
  if (signal.aborted) return Promise.resolve(false);
  return new Promise((resolve, reject) => {
    const abort = () => resolve(false);
    signal.addEventListener("abort", abort, { once: true });
    Promise.resolve(value)
      .then(resolve, reject)
      .finally(() => signal.removeEventListener("abort", abort));
  });
}
export function configureDialog(
  dialog,
  { canClose = () => true, dismissible = true } = {},
) {
  if (typeof canClose !== "function")
    throw TypeError("canClose must be a function");
  installDialogDismissal(dialog.ownerDocument);
  pending.get(dialog)?.controller.abort();
  const policy = { canClose, dismissible };
  policies.set(dialog, policy);
  return () => {
    if (policies.get(dialog) === policy) {
      pending.get(dialog)?.controller.abort();
      policies.delete(dialog);
    }
  };
}
export function requestDialogClose(dialog, reason = "dismiss") {
  if (!dialog?.open) return Promise.resolve(false);
  const root = roots.get(dialog.ownerDocument);
  root?.flush();
  if (pending.has(dialog)) return pending.get(dialog).promise;
  const controller = new AbortController(),
    version = versions.get(dialog) ?? 0,
    policy = policies.get(dialog),
    attempt = { controller };
  const closed = () => controller.abort();
  dialog.addEventListener("close", closed, { once: true });
  attempt.promise = Promise.resolve()
    .then(async () => {
      if (!dialog.open || !permitted(dialog)) return false;
      const allowed = policy
        ? await abortable(
            policy.canClose({ reason, dialog, signal: controller.signal }),
            controller.signal,
          )
        : true;
      root?.flush();
      if (
        allowed !== true ||
        controller.signal.aborted ||
        !dialog.open ||
        (versions.get(dialog) ?? 0) !== version ||
        policies.get(dialog) !== policy ||
        !permitted(dialog)
      )
        return false;
      const event = new Event("cancel", { cancelable: true });
      Object.defineProperty(event, "closeReason", { value: reason });
      internalCancels.add(event);
      if (!dialog.dispatchEvent(event) || !dialog.open) return false;
      dialog.close();
      return true;
    })
    .catch((error) => {
      errorMessage(dialog, error);
      return false;
    })
    .finally(() => {
      dialog.removeEventListener("close", closed);
      if (pending.get(dialog) === attempt) pending.delete(dialog);
    });
  pending.set(dialog, attempt);
  return attempt.promise;
}
export function installDialogDismissal(root = document) {
  if (roots.has(root)) return roots.get(root).dispose;
  let pressed = null;
  const outside = (dialog, event) => {
    const r = dialog.getBoundingClientRect();
    return (
      event.clientX < r.left ||
      event.clientX >= r.right ||
      event.clientY < r.top ||
      event.clientY >= r.bottom
    );
  };
  const down = (event) => {
    pressed = null;
    if (event.button !== 0 || event.isPrimary === false) return;
    const dialog = event.target;
    if (isDialog(dialog) && dialog.open && outside(dialog, event))
      pressed = dialog;
  };
  const click = (event) => {
    const dialog = pressed;
    pressed = null;
    if (
      event.button === 0 &&
      event.detail !== 0 &&
      dialog === event.target &&
      dialog?.open &&
      outside(dialog, event)
    )
      void requestDialogClose(dialog, "backdrop");
  };
  const closeButton = (event) => {
    if (event.defaultPrevented) return;
    const button = event.target.closest?.("[data-close]");
    if (!button || button.matches(":disabled")) return;
    const dialog = root.getElementById(button.dataset.close);
    if (isDialog(dialog)) {
      event.preventDefault();
      void requestDialogClose(dialog, "button");
    }
  };
  const cancel = (event) => {
    if (
      !isDialog(event.target) ||
      internalCancels.has(event) ||
      !event.isTrusted
    )
      return;
    event.preventDefault();
    event.stopImmediatePropagation();
    void requestDialogClose(event.target, "escape");
  };
  const submit = (event) => {
    const form = event.target,
      method = event.submitter?.getAttribute("formmethod") ?? form.method;
    if (event.defaultPrevented || method !== "dialog") return;
    const dialog = form.closest("dialog");
    if (dialog) {
      event.preventDefault();
      void requestDialogClose(dialog, "button");
    }
  };
  const reset = () => {
    pressed = null;
  };
  const records = (changes) => {
    for (const change of changes) {
      const dialog = change.target;
      if (isDialog(dialog)) {
        versions.set(dialog, (versions.get(dialog) ?? 0) + 1);
        pending.get(dialog)?.controller.abort();
      }
    }
  };
  const observer = new MutationObserver(records);
  observer.observe(root, {
    subtree: true,
    attributes: true,
    attributeFilter: ["open"],
  });
  const dispose = () => {
    observer.disconnect();
    root.removeEventListener("pointerdown", down, true);
    root.removeEventListener("click", click, true);
    root.removeEventListener("click", closeButton);
    root.removeEventListener("pointercancel", reset, true);
    root.removeEventListener("cancel", cancel, true);
    root.removeEventListener("submit", submit);
    roots.delete(root);
  };
  roots.set(root, { dispose, flush: () => records(observer.takeRecords()) });
  root.addEventListener("pointerdown", down, true);
  root.addEventListener("click", click, true);
  root.addEventListener("click", closeButton);
  root.addEventListener("pointercancel", reset, true);
  root.addEventListener("cancel", cancel, true);
  root.addEventListener("submit", submit);
  return dispose;
}
export function confirmDialog({
  title = "Bestätigen",
  message,
  confirmLabel = "Bestätigen",
  cancelLabel = "Abbrechen",
  signal,
} = {}) {
  if (signal?.aborted) return Promise.resolve(false);
  installDialogDismissal();
  return new Promise((resolve) => {
    const dialog = document.createElement("dialog");
    dialog.className = "confirmation-dialog";
    dialog.setAttribute("role", "alertdialog");
    const heading = document.createElement("h2"),
      body = document.createElement("p"),
      actions = document.createElement("div"),
      accept = document.createElement("button"),
      cancel = document.createElement("button");
    heading.textContent = title;
    body.textContent = message ?? "";
    actions.className = "dialog-actions";
    accept.type = cancel.type = "button";
    accept.className = "primary";
    cancel.className = "secondary";
    accept.textContent = confirmLabel;
    cancel.textContent = cancelLabel;
    actions.append(cancel, accept);
    dialog.append(heading, body, actions);
    document.body.append(dialog);
    let settled = false;
    const finish = (value) => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener("abort", abort);
        dialog.close();
        dialog.remove();
        resolve(value);
      },
      abort = () => finish(false);
    accept.onclick = () => finish(true);
    cancel.onclick = () => finish(false);
    dialog.addEventListener("close", () =>
      finish(dialog.returnValue === "confirm"),
    );
    signal?.addEventListener("abort", abort, { once: true });
    dialog.showModal();
    cancel.focus();
  });
}

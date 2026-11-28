// SPDX-License-Identifier: GPL-3.0-only
// STNBD001 metadata and systematic stripe addresses belong to this plugin.
export const apiVersion = 1;
export const css =
  '[data-stqr-plugin="bd"] .bd-identity { font-family: monospace; font-size: 11px; } [data-stqr-plugin="bd"] .bd-form { max-width: 36rem; }';
const utf8 = new TextEncoder(),
  text = new TextDecoder("utf-8", { fatal: true }),
  hex = (b) => Array.from(b, (v) => v.toString(16).padStart(2, "0")).join(""),
  unhex = (s) => {
    if (!/^(?:[0-9a-f]{2})+$/.test(s)) throw Error("Ungültige ID.");
    return Uint8Array.from(s.match(/../g), (b) => parseInt(b, 16));
  };
const fail = () => {
  throw Error("Ungültige Blockdevice-Metadaten.");
};
class Reader {
  constructor(b) {
    this.b = b;
    this.at = 0;
  }
  take(n) {
    if (!Number.isSafeInteger(n) || n < 0 || n > this.b.length - this.at)
      fail();
    const out = this.b.subarray(this.at, this.at + n);
    this.at += n;
    return out;
  }
  u16() {
    const b = this.take(2);
    return new DataView(b.buffer, b.byteOffset, 2).getUint16(0, true);
  }
  u32() {
    const b = this.take(4);
    return new DataView(b.buffer, b.byteOffset, 4).getUint32(0, true);
  }
  u64() {
    const b = this.take(8);
    return new DataView(b.buffer, b.byteOffset, 8).getBigUint64(0, true);
  }
  text() {
    let n = 0;
    for (let i = 0; i < 2; i++) {
      const b = this.take(1)[0];
      n |= (b & 127) << (7 * i);
      if (!(b & 128)) {
        if ((i && !b) || n > 255) fail();
        return text.decode(this.take(n));
      }
    }
    fail();
  }
  done() {
    if (this.at !== this.b.length) fail();
  }
}
class Writer {
  constructor() {
    this.a = [];
  }
  raw(b) {
    this.a.push(...b);
    return this;
  }
  u16(n) {
    return this.raw(new Uint8Array([n & 255, n >> 8]));
  }
  u32(n) {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, n, true);
    return this.raw(b);
  }
  u64(n) {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(n), true);
    return this.raw(b);
  }
  text(s) {
    const b = utf8.encode(s);
    let n = b.length;
    do {
      let v = n & 127;
      n >>>= 7;
      this.a.push(v | (n ? 128 : 0));
    } while (n);
    return this.raw(b);
  }
  bytes() {
    return new Uint8Array(this.a);
  }
}
export function validate(m) {
  if (
    !/^[0-9a-f]{32}$/.test(m.id) ||
    /^0+$/.test(m.id) ||
    !m.name ||
    m.size <= 0n ||
    m.size % 512n ||
    m.size > 0x7fffffffffffffffn
  )
    fail();
  for (const s of [m.name, m.manufacturer, m.model, m.serial])
    if (
      typeof s !== "string" ||
      utf8.encode(s).length > 255 ||
      /[\0\r\n]/.test(s) ||
      text.decode(utf8.encode(s)) !== s
    )
      fail();
  return m;
}
export function decodeMetadata(raw) {
  const r = new Reader(raw);
  if (
    text.decode(r.take(8)) !== "STNBD001" ||
    r.u16() !== 1 ||
    r.u32() !== 1048576
  )
    fail();
  const m = {
    size: r.u64(),
    id: hex(r.take(16)),
    name: r.text(),
    manufacturer: r.text(),
    model: r.text(),
    serial: r.text(),
    raw,
  };
  r.done();
  return validate(m);
}
export function encodeMetadata(m) {
  validate(m);
  return new Writer()
    .raw(utf8.encode("STNBD001"))
    .u16(1)
    .u32(1048576)
    .u64(m.size)
    .raw(unhex(m.id))
    .text(m.name)
    .text(m.manufacturer)
    .text(m.model)
    .text(m.serial)
    .bytes();
}
async function hash(data) {
  const { sha256 } = await import("/assets/sdk/codec.mjs");
  return await sha256(data);
}
export async function metadataID(name) {
  const hashBytes = await hash(utf8.encode("STQR NBD metadata v1\0" + name));
  return "0".repeat(16) + hex(hashBytes.subarray(0, 24));
}
export async function stripeID(device, index) {
  const prefix = utf8.encode("STQR NBD stripe v1\0"),
    bytes = new Uint8Array(prefix.length + 24);
  bytes.set(prefix);
  bytes.set(unhex(device), prefix.length);
  new DataView(bytes.buffer).setBigUint64(
    prefix.length + 16,
    BigInt(index),
    false,
  );
  const digest = await hash(bytes),
    id = new Uint8Array(32);
  id.set(digest.subarray(0, 16));
  if (id.subarray(0, 8).every((b) => b === 0)) id[0] = 1;
  id.set(unhex(device).subarray(0, 8), 16);
  new DataView(id.buffer).setBigUint64(24, BigInt(index), false);
  return hex(id);
}
export async function listPage(client, after, limit, query, { signal } = {}) {
  const page = await client.scanObjects(after, limit, { signal }),
    items = [];
  let next = page.next;
  for (const entry of page.objects) {
    if (!entry.id.startsWith("0".repeat(16))) {
      next = "";
      break;
    }
    const { data } = await client.get(entry.id, { signal }),
      m = decodeMetadata(data);
    if ((await metadataID(m.name)) !== entry.id) fail();
    if (m.name.toLocaleLowerCase().includes(query.toLocaleLowerCase()))
      items.push(m);
  }
  return { items, next };
}
export async function mutate(client, action, { signal } = {}) {
  await client.beginObjectLease(false, { signal });
  try {
    const result = await action();
    await client.sync({ signal });
    return result;
  } finally {
    await client.endMaintenance();
  }
}
export async function create(client, { name, size, id }, { signal } = {}) {
  const m = validate({
    name,
    size: BigInt(size),
    id,
    manufacturer: "STQR",
    model: "Striped block device",
    serial: id,
  });
  await client.cas(
    await metadataID(name),
    0n,
    new Uint8Array(),
    encodeMetadata(m),
    { scope: "object", absent: true, signal },
  );
  return m;
}
export async function resize(client, device, size, { signal } = {}) {
  size = BigInt(size);
  if (size < device.size)
    throw Error("Verkleinern wird vom Blockdevice-Format nicht unterstützt.");
  const m = validate({ ...device, size });
  await client.cas(
    await metadataID(device.name),
    0n,
    device.raw,
    encodeMetadata(m),
    { scope: "object", signal },
  );
  return m;
}
export async function rename(client, device, name, { signal } = {}) {
  const next = validate({ ...device, name }),
    oldID = await metadataID(device.name),
    newID = await metadataID(name);
  if (oldID === newID) return device;
  const raw = encodeMetadata(next);
  try {
    await client.cas(newID, 0n, new Uint8Array(), raw, {
      scope: "object",
      absent: true,
      signal,
    });
  } catch (e) {
    if (e.status !== 409 && e.status !== 412) throw e;
    const { data } = await client.get(newID, { signal });
    if (hex(data) !== hex(raw)) throw Error("Der Zielname ist bereits belegt.");
  }
  // Durable destination first: interruption can leave two names, never lose a disk.
  // Repeating the same rename recognizes its destination and finishes removal.
  await client.sync({ signal });
  await client.delete(oldID, {
    expectedLength: BigInt(device.raw.length),
    compare: device.raw,
    signal,
  });
  return next;
}
export async function remove(client, device, { signal } = {}) {
  // Refuse to destroy shared stripes when an interrupted rename left an alias.
  let after = "";
  do {
    const page = await listPage(client, after, 256, "", { signal });
    for (const m of page.items)
      if (m.id === device.id && m.name !== device.name)
        throw Error(
          "Das Gerät hat mehrere Namen. Bitte zuerst die unterbrochene Umbenennung abschließen.",
        );
    after = page.next;
  } while (after);
  await client.delete(await metadataID(device.name), {
    expectedLength: BigInt(device.raw.length),
    compare: device.raw,
    signal,
  });
  await client.sync({ signal });
  // The descriptor is gone before cleanup. A crash leaves harmless orphan stripes
  // for the native collector; no other device can inherit the random disk ID.
  after = "";
  do {
    const page = await client.scanObjects(after, 256, { signal });
    for (const entry of page.objects) {
      if (entry.id.slice(32, 48) !== device.id.slice(0, 16)) continue;
      const index = BigInt("0x" + entry.id.slice(48));
      if ((await stripeID(device.id, index)) === entry.id)
        await client.delete(entry.id, { signal });
    }
    after = page.next;
  } while (after);
}
function parseSize(value) {
  const m = /^\s*(\d+)\s*([KMGT]?)\s*$/i.exec(value);
  if (!m) throw Error("Größe als Bytes oder mit K/M/G/T eingeben.");
  const n =
    BigInt(m[1]) *
    1024n ** BigInt(m[2] ? "KMGT".indexOf(m[2].toUpperCase()) + 1 : 0);
  if (n <= 0n || n % 512n || n > 0x7fffffffffffffffn)
    throw Error("Größe muss ein positives Vielfaches von 512 Bytes sein.");
  return n;
}
export async function open({ bucket, connect, windows, ui, signal }) {
  const window = windows.open({
      title: `Blockdevices · ${bucket.name}`,
      signal,
    }),
    client = await connect();
  if (window.signal.aborted) return;
  const toolbar = document.createElement("div"),
    note = document.createElement("p");
  toolbar.className = "plugin-toolbar";
  note.className = "plugin-note";
  note.textContent =
    "Änderungen benötigen exklusiven Bucket-Zugriff. Angeschlossene NBD-/Blockdevice-Clients vorher trennen. Größen können nur vergrößert werden.";
  window.root.append(toolbar, note);
  const form = (kind, device) => {
    let busy = false;
    const dialog = windows.open({
        title: {
          create: "Volume erstellen",
          resize: "Größe ändern",
          rename: "Volume umbenennen",
          delete: "Volume löschen",
        }[kind],
        signal: window.signal,
        canClose: async () => !busy,
      }),
      root = dialog.root;
    root.innerHTML =
      '<form class="plugin-form bd-form"><label>Name<input name="name" required maxlength="255"></label><label>Größe (Bytes, K/M/G/T)<input name="size" required></label><p class="plugin-note"></p><p class="error" role="alert"></p><div class="plugin-toolbar"><button class="primary" type="submit">Speichern</button></div></form>';
    const f = root.querySelector("form"),
      name = f.elements.name,
      size = f.elements.size,
      error = f.querySelector(".error"),
      submit = f.querySelector("button");
    name.value = device?.name ?? "";
    size.value = String(device?.size ?? 1048576n);
    name.closest("label").hidden = kind === "resize" || kind === "delete";
    name.required = kind === "create" || kind === "rename";
    size.closest("label").hidden = kind === "rename" || kind === "delete";
    size.required = kind === "create" || kind === "resize";
    if (kind === "delete") {
      submit.textContent = "Volume endgültig löschen";
      submit.classList.add("danger-button");
      f.querySelector(".plugin-note").textContent =
        `„${device.name}“ und seine Payload werden gelöscht.`;
    }
    const id = hex(crypto.getRandomValues(new Uint8Array(16)));
    f.onsubmit = async (event) => {
      event.preventDefault();
      if (busy) return;
      busy = true;
      submit.disabled = true;
      error.textContent = "";
      try {
        await mutate(
          client,
          async () => {
            if (kind === "create")
              await create(
                client,
                { name: name.value, size: parseSize(size.value), id },
                { signal: dialog.signal },
              );
            if (kind === "resize")
              await resize(client, device, parseSize(size.value), {
                signal: dialog.signal,
              });
            if (kind === "rename")
              await rename(client, device, name.value, {
                signal: dialog.signal,
              });
            if (kind === "delete")
              await remove(client, device, { signal: dialog.signal });
          },
          { signal: dialog.signal },
        );
        dialog.close();
        await list.load();
      } catch (e) {
        if (!dialog.signal.aborted) error.textContent = e.message;
      } finally {
        busy = false;
        if (!dialog.signal.aborted) submit.disabled = false;
      }
    };
  };
  toolbar.append(
    ui.button("Volume erstellen", () => form("create"), {
      signal: window.signal,
    }),
  );
  const list = ui.createList({
    container: window.root,
    root: window.scrollRoot,
    signal: window.signal,
    columns: ["Name", "Größe", "Geräte-ID", "Aktionen"],
    key: (m) => m.name,
    placeholder: "Volumes suchen …",
    fetchPage: (a, n, q, opts) => listPage(client, a, n, q, opts),
    renderRow: (m) => {
      const row = document.createElement("tr");
      row.innerHTML = `<td>${ui.escape(m.name)}</td><td>${m.size} Bytes</td><td class="bd-identity">${m.id}</td><td><div class="plugin-toolbar"></div></td>`;
      for (const [kind, label] of [
        ["resize", "Größe ändern"],
        ["rename", "Umbenennen"],
        ["delete", "Löschen"],
      ])
        row
          .querySelector(".plugin-toolbar")
          .append(
            ui.button(label, () => form(kind, m), { signal: window.signal }),
          );
      return row;
    },
  });
  await list.load();
}

// SPDX-License-Identifier: GPL-3.0-only
// STFS browser with idempotent empty-root bootstrap. All format rules stay here.
export const apiVersion = 1;
export const css =
  '[data-stqr-plugin="fs"] .fs-tree { list-style: none; padding-left: 20px; } [data-stqr-plugin="fs"] .fs-tree li { padding: 6px 0; } [data-stqr-plugin="fs"] summary { cursor: pointer; } [data-stqr-plugin="fs"] .fs-size { color: #667085; margin-left: 12px; }';
const text = new TextDecoder("utf-8", { fatal: true }),
  hex = (b) => Array.from(b, (v) => v.toString(16).padStart(2, "0")).join(""),
  zero = "0".repeat(64),
  limit = 1048576n;
const compareText = (a, b) => {
  const x = new TextEncoder().encode(a),
    y = new TextEncoder().encode(b);
  for (let i = 0; i < Math.min(x.length, y.length); i++)
    if (x[i] !== y[i]) return x[i] - y[i];
  return x.length - y.length;
};
const fail = (message) => {
  throw Error(message ?? "Ungültige Dateisystem-Metadaten.");
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
  u8() {
    return this.take(1)[0];
  }
  bool() {
    const n = this.u8();
    if (n > 1) fail();
    return !!n;
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
  blob() {
    let n = 0n;
    for (let i = 0; i < 10; i++) {
      const b = this.u8();
      if (i === 9 && b > 1) fail();
      n |= BigInt(b & 127) << BigInt(i * 7);
      if (!(b & 128)) {
        if ((i && b === 0) || n > 128n * limit) fail();
        return this.take(Number(n));
      }
    }
    fail();
  }
  text() {
    return text.decode(this.blob());
  }
  id() {
    return hex(this.take(32));
  }
  done() {
    if (this.at !== this.b.length) fail();
  }
}
export function decodeNode(raw, cowSix = false) {
  const r = new Reader(raw);
  if (raw.length > Number(limit) || text.decode(r.take(4)) !== "STFS") fail();
  const version = r.u16();
  if (version < 1 || version > 7) fail();
  const n = {
    kind: r.u8(),
    mode: r.u32(),
    uid: r.u32(),
    gid: r.u32(),
    mtime: r.u64(),
    length: r.u64(),
    layout: r.u8(),
    inline: null,
    mutable: false,
    root: zero,
    lower: false,
  };
  if (
    n.kind < 1 ||
    n.kind > 5 ||
    n.mode & ~0o7777 ||
    n.length > 0x7fffffffffffffffn
  )
    fail();
  if (n.layout === 0) {
    n.inline = r.blob();
    if (BigInt(n.inline.length) !== n.length || n.inline.length > 4096) fail();
  } else if (n.layout === 1 || n.layout === 2) {
    if (n.layout === 2 && version < 7 && !cowSix)
      fail(
        "Ältere Lower-Dateireferenzen benötigen einen aktuellen Dateisystem-Client.",
      );
    n.lower = n.layout === 2;
    n.depth = r.u8();
    n.root = r.id();
    if (n.depth < 1 || n.depth > 16 || n.root === zero || !n.length) fail();
  } else fail();
  if (version === 2) {
    if (!r.bool() || n.kind !== 2) fail();
    n.removed = true;
  }
  if (version >= 3) {
    n.removed = r.bool();
    n.writePath = r.text();
    if (n.removed && n.kind !== 2) fail();
  }
  if (version >= 4) {
    r.u64();
    r.u64();
  }
  if (version >= 5) {
    n.links = r.u32();
    const count = r.u32();
    if (count > 4096) fail();
    let previous = "";
    for (let i = 0; i < count; i++) {
      const name = r.text(),
        value = r.blob();
      if (!name || compareText(name, previous) <= 0 || value.length > 65536)
        fail();
      previous = name;
    }
  }
  if (version === 6 && !cowSix) {
    n.mutable = r.u64() !== 0n;
    if (!n.mutable) fail();
  }
  if (version >= 7) {
    n.mutable = r.bool();
    n.digest = r.id();
    if (r.bool()) {
      n.lowerOwner = r.id();
      r.id();
      if (n.lowerOwner === zero) fail();
    }
  }
  r.done();
  if (
    (n.kind > 2 && version < 5) ||
    (n.kind === 3 &&
      (!n.inline || !n.length || n.length > 4095n || n.inline.includes(0))) ||
    ((n.kind === 4 || n.kind === 5) && n.length !== 0n) ||
    (n.kind === 2 && version >= 5 && !n.removed && !n.links)
  )
    fail();
  if (n.lower)
    fail(
      "Diese Datei benötigt den schreibgeschützten Basis-Bucket; bitte über den Overlay-Client öffnen.",
    );
  return n;
}
export function decodeDirectory(raw) {
  const r = new Reader(raw);
  if (text.decode(r.take(4)) !== "STDR" || r.u16() !== 1) fail();
  const count = r.u64();
  if (count > BigInt(Math.floor((raw.length - r.at) / 34))) fail();
  const entries = [];
  let previous = "";
  for (let i = 0n; i < count; i++) {
    const name = r.text(),
      id = r.id();
    if (
      !name ||
      new TextEncoder().encode(name).length > 255 ||
      name === "." ||
      name === ".." ||
      /[\/\0]/.test(name) ||
      compareText(name, previous) <= 0 ||
      id === zero
    )
      fail();
    previous = name;
    entries.push({ name, id });
  }
  r.done();
  return entries;
}
export function decodeIndex(raw, length, depth) {
  const r = new Reader(raw);
  if (raw.length > Number(limit) || text.decode(r.take(4)) !== "STIX") fail();
  const version = r.u16(),
    count = r.u32();
  if (version < 1 || version > 3 || !count || count > 1024) fail();
  const segments = [];
  let total = 0n;
  for (let i = 0; i < count; i++) {
    const s = {
      length: r.u64(),
      id: r.id(),
      offset: version === 1 ? BigInt(r.u32()) : r.u64(),
    };
    if (version === 2 && r.u64())
      fail("Historische Lower-Segmente benötigen einen Overlay-Client.");
    if (version === 3 && r.bool())
      fail("Lower-Segmente benötigen einen Overlay-Client.");
    if (
      !s.length ||
      s.length > length - total ||
      s.offset > 0x7fffffffffffffffn ||
      s.length + s.offset > 0x7fffffffffffffffn ||
      (depth > 1 && s.offset !== 0n) ||
      (s.id === zero && s.offset !== 0n) ||
      (depth === 1 && s.id !== zero && s.offset + s.length > limit)
    )
      fail();
    total += s.length;
    segments.push(s);
  }
  r.done();
  if (total !== length) fail();
  return segments;
}
async function hash(data) {
  const { sha256 } = await import("/assets/sdk/codec.mjs");
  return hex(await sha256(data));
}
export async function rootID() {
  return hash(new TextEncoder().encode("STQR filesystem root v1"));
}
async function loadIntent(client, id, signal) {
  const { data: guard } = await client.get(id, { signal });
  let raw = guard,
    state;
  if (text.decode(raw.subarray(0, 4)) === "STFJ") {
    const r = new Reader(raw);
    r.take(4);
    if (r.u16() !== 1) fail();
    state = r.u8();
    const length = r.u32(),
      count = r.u32();
    if (
      length <= 65536 ||
      length > 128 * Number(limit) ||
      count !== Math.ceil(length / Number(limit))
    )
      fail();
    const parts = [];
    for (let i = 0; i < count; i++) {
      const part = r.id();
      if (part === zero || part === id) fail();
      const { data } = await client.get(part, { signal });
      if (data.length !== Math.min(Number(limit), length - i * Number(limit)))
        fail();
      parts.push(data);
    }
    r.done();
    raw = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
      raw.set(part, offset);
      offset += part.length;
    }
  }
  const r = new Reader(raw);
  if (text.decode(r.take(4)) !== "STFT")
    fail(
      "Dateisystem-Transaktion wurde gerade abgeschlossen; bitte erneut laden.",
    );
  const version = r.u16(),
    storedState = r.u8(),
    count = r.u32();
  state ??= storedState;
  if (version < 1 || version > 3 || state > 2 || !count || count > 1024) fail();
  const members = new Map();
  let previous = "";
  for (let i = 0; i < count; i++) {
    const oid = r.id();
    if (oid <= previous || oid === zero || oid === id) fail();
    previous = oid;
    if (version < 3) r.u64();
    const before = r.blob(),
      after = r.blob(),
      writes = new Map();
    if (version >= 2) {
      const n = r.u32();
      if (n > 1048576) fail();
      for (let j = 0; j < n; j++) {
        const wid = r.id();
        if (version < 3) r.u64();
        const w = {
          absent: r.bool(),
          full: r.bool(),
          remove: r.bool(),
          offset: r.u64(),
          data: r.blob(),
        };
        if (version >= 3) r.blob();
        if (
          wid === zero ||
          writes.has(wid) ||
          w.offset > limit ||
          w.offset + BigInt(w.data.length) > limit
        )
          fail();
        writes.set(wid, w);
      }
    }
    members.set(oid, { before, after, writes });
  }
  r.done();
  return { guard, state, members };
}
async function snapshot(client, id, signal) {
  const { data: head } = await client.get(id, { signal });
  let image = head,
    guard,
    view = new Map();
  const magic = text.decode(head.subarray(0, 4));
  if (magic === "STOR")
    fail(
      "Dieser Bucket ist ein Overlay und benötigt seinen Basis-Bucket. Bitte über den Overlay-Client öffnen.",
    );
  if (magic === "STFL") {
    const r = new Reader(head);
    r.take(4);
    if (r.u16() !== 1) fail();
    const txid = r.id();
    r.done();
    const tx = await loadIntent(client, txid, signal),
      member = tx.members.get(id);
    if (!member) fail();
    image = tx.state === 1 ? member.after : member.before;
    if (tx.state === 1) view = member.writes;
    guard = { id: txid, raw: tx.guard };
  }
  let node;
  try {
    node = decodeNode(image);
  } catch (e) {
    if (image[4] === 6 && image[5] === 0) node = decodeNode(image, true);
    else throw e;
  }
  return { ...node, id, head, guard, view };
}
async function objectBytes(client, id, node, signal) {
  const patch = node.view.get(id);
  if (patch?.remove) fail("Dateisystem-Objekt wurde entfernt.");
  if (patch?.full) return patch.data;
  let data;
  try {
    ({ data } = await client.get(id, { signal }));
  } catch (e) {
    if (e.status !== 404 || !patch?.absent) throw e;
    data = new Uint8Array();
  }
  if (patch) {
    const result = new Uint8Array(
      Math.max(data.length, Number(patch.offset) + patch.data.length),
    );
    result.set(data);
    result.set(patch.data, Number(patch.offset));
    return result;
  }
  return data;
}
async function* walk(
  client,
  id,
  length,
  depth,
  node,
  signal,
  ancestors = new Set(),
) {
  signal?.throwIfAborted();
  if (ancestors.has(id)) fail("Zyklus im Dateisystem-Index.");
  const path = new Set(ancestors);
  path.add(id);
  for (const part of decodeIndex(
    await objectBytes(client, id, node, signal),
    length,
    depth,
  )) {
    if (part.id === zero) {
      let left = part.length;
      while (left) {
        const n = left > limit ? limit : left;
        yield new Uint8Array(Number(n));
        left -= n;
      }
    } else if (depth > 1)
      yield* walk(client, part.id, part.length, depth - 1, node, signal, path);
    else {
      const data = await objectBytes(client, part.id, node, signal),
        start = Number(part.offset),
        end = Number(part.offset + part.length);
      if (end > data.length && !node.mutable) fail();
      const out = new Uint8Array(Number(part.length));
      out.set(data.subarray(start, end));
      yield out;
    }
  }
}
async function validate(client, node, signal) {
  const { data } = await client.get(node.id, { signal });
  if (hex(data) !== hex(node.head))
    throw Error("Datei wurde während des Lesens geändert. Bitte erneut laden.");
  if (node.guard) {
    const { data: guard } = await client.get(node.guard.id, { signal });
    if (hex(guard) !== hex(node.guard.raw))
      throw Error(
        "Dateisystem-Transaktion hat sich geändert. Bitte erneut laden.",
      );
  }
}
export async function readFile(client, id, { signal, directory = false } = {}) {
  const node = await snapshot(client, id, signal);
  if (node.removed) fail("Verzeichnis wurde gelöscht.");
  if (directory ? node.kind !== 2 : node.kind !== 1) fail();
  if (directory && node.length > 64n * limit)
    fail("Verzeichnis überschreitet das Leselimit von 64 MiB.");
  const parts = [];
  if (node.inline) parts.push(node.inline);
  else
    for await (const part of walk(
      client,
      node.root,
      node.length,
      node.depth,
      node,
      signal,
    ))
      parts.push(part);
  await validate(client, node, signal);
  return { node, blob: new Blob(parts) };
}
// Match Go encodeFSNode(v7) and encodeFSDirectory(v1). The initial root is the
// only bootstrap write; file-management mutations remain planned future work.
export function encodeEmptyRoot(timestamp, Writer) {
  const encode = new TextEncoder(),
    directory = new Writer().raw(encode.encode("STDR")).u16(1).u64(0).bytes();
  return new Writer()
    .raw(encode.encode("STFS"))
    .u16(7)
    .u8(2)
    .u32(0o755)
    .u32(0)
    .u32(0)
    .u64(timestamp)
    .u64(directory.length)
    .u8(0)
    .blob(directory)
    .bool(false)
    .text("")
    .u64(timestamp)
    .u64(timestamp)
    .u32(2)
    .u32(0)
    .bool(false)
    .raw(new Uint8Array(32))
    .bool(false)
    .bytes();
}
export async function initialize({
  connect,
  signal,
  initializationID,
  state = {},
}) {
  const client = await connect(),
    id = await rootID();
  const existing = async () => {
    const node = await snapshot(client, id, signal);
    if (node.kind !== 2 || node.removed)
      fail("Das Root-Objekt ist kein gültiges Verzeichnis.");
    return node;
  };
  try {
    await existing();
    return;
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  const { Writer } = await import("/assets/sdk/codec.mjs");
  state.startedAt ??= BigInt(Date.now()) * 1000000n;
  state.rootImage ??= encodeEmptyRoot(state.startedAt, Writer);
  try {
    await client.cas(id, 0n, new Uint8Array(), state.rootImage, {
      scope: "object",
      absent: true,
      requestId: initializationID,
      signal,
    });
  } catch (error) {
    if (error.status !== 409 && error.status !== 412) throw error;
    await existing();
  }
}
export async function open({ bucket, connect, windows, ui, signal }) {
  const window = windows.open({ title: `Dateien · ${bucket.name}`, signal }),
    client = await connect();
  if (window.signal.aborted) return;
  const toolbar = document.createElement("div");
  toolbar.className = "plugin-toolbar";
  const note = document.createElement("p");
  note.className = "plugin-note";
  note.textContent =
    "Dateien anklicken zum Herunterladen. Ordner erstellen, Umbenennen, Hochladen und Löschen sind für eine spätere Version geplant.";
  const tree = document.createElement("ul");
  tree.className = "fs-tree";
  tree.setAttribute("role", "tree");
  window.root.append(toolbar, note, tree);
  const directory = async (id, target, path, ancestors) => {
    const { blob } = await readFile(client, id, {
        signal: window.signal,
        directory: true,
      }),
      entries = decodeDirectory(new Uint8Array(await blob.arrayBuffer()));
    target.replaceChildren();
    // Bounded metadata concurrency; directory bodies and child subtrees stay lazy.
    for (let start = 0; start < entries.length; start += 8) {
      window.signal.throwIfAborted();
      const nodes = await Promise.all(
        entries.slice(start, start + 8).map(async (entry) => ({
          ...entry,
          node: await snapshot(client, entry.id, window.signal),
        })),
      );
      for (const entry of nodes) {
        if (entry.node.removed) continue;
        const li = document.createElement("li");
        li.setAttribute("role", "treeitem");
        const icon = document.createElement("span");
        icon.setAttribute("aria-hidden", "true");
        icon.textContent =
          (entry.node.kind === 2 ? "📁" : entry.node.kind === 3 ? "🔗" : "📄") +
          " ";
        const childPath = path + "/" + entry.name;
        if (entry.node.kind === 2) {
          const details = document.createElement("details"),
            summary = document.createElement("summary"),
            children = document.createElement("ul");
          summary.append(icon, document.createTextNode(entry.name));
          children.className = "fs-tree";
          children.setAttribute("role", "group");
          details.append(summary, children);
          let loaded = false,
            busy = false;
          details.addEventListener("toggle", async () => {
            if (!details.open || loaded || busy) return;
            busy = true;
            try {
              if (ancestors.has(entry.id)) fail("Zyklus im Verzeichnisbaum.");
              await directory(
                entry.id,
                children,
                childPath,
                new Set([...ancestors, entry.id]),
              );
              loaded = true;
            } catch (e) {
              if (!window.signal.aborted) {
                children.textContent = e.message;
                details.open = false;
              }
            } finally {
              busy = false;
            }
          });
          li.append(details);
        } else if (entry.node.kind === 1) {
          li.append(
            icon,
            ui.button(
              entry.name,
              async () => {
                const { blob } = await readFile(client, entry.id, {
                  signal: window.signal,
                });
                ui.download(blob, entry.name, { signal: window.signal });
              },
              { kind: "text-button", signal: window.signal },
            ),
          );
          const size = document.createElement("span");
          size.className = "fs-size";
          size.textContent = entry.node.length + " Bytes";
          li.append(size);
        } else {
          li.append(
            icon,
            document.createTextNode(
              entry.name +
                " · " +
                ({ 3: "Symbolischer Link", 4: "FIFO", 5: "Socket" }[
                  entry.node.kind
                ] ?? "Spezialdatei"),
            ),
          );
        }
        target.append(li);
      }
    }
  };
  const root = await rootID();
  let busy = false;
  const refresh = async () => {
    if (busy) return;
    busy = true;
    try {
      await directory(root, tree, "", new Set([root]));
    } finally {
      busy = false;
    }
  };
  toolbar.append(
    ui.button("Aktualisieren", refresh, { signal: window.signal }),
  );
  await refresh();
}

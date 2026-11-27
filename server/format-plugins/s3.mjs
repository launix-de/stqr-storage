// SPDX-License-Identifier: GPL-3.0-only
// STS3 codec and S3 semantics deliberately live entirely in this plugin.
export const apiVersion = 1;
export const css =
  '[data-stqr-plugin="s3"] .s3-key { max-width: 42ch; overflow-wrap: anywhere; } [data-stqr-plugin="s3"] .s3-preview { margin-top: 18px; }';
const utf8 = new TextEncoder(),
  decode = new TextDecoder("utf-8", { fatal: true }),
  hex = (b) => Array.from(b, (v) => v.toString(16).padStart(2, "0")).join("");
const magic = (b, s) =>
  b.length >= s.length &&
  Array.from(s).every((c, i) => b[i] === c.charCodeAt(0));
const compareText = (a, b) => {
  const x = new TextEncoder().encode(a),
    y = new TextEncoder().encode(b);
  for (let i = 0; i < Math.min(x.length, y.length); i++)
    if (x[i] !== y[i]) return x[i] - y[i];
  return x.length - y.length;
};
const fail = () => {
  throw Error("Ungültige S3-Metadaten.");
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
  v() {
    let n = 0n;
    for (let i = 0; i < 10; i++) {
      const b = this.take(1)[0];
      if (i === 9 && b > 1) fail();
      n |= BigInt(b & 127) << BigInt(i * 7);
      if (!(b & 128)) {
        if (i && b === 0) fail();
        if (n > 65536n) fail();
        return Number(n);
      }
    }
    fail();
  }
  text() {
    return decode.decode(this.take(this.v()));
  }
  done() {
    if (this.at !== this.b.length) fail();
  }
}
export async function metadataID(key) {
  const { sha256 } = await import("/assets/sdk/codec.mjs");
  return hex(await sha256(utf8.encode("stqr:s3:meta:v1:\0" + key)));
}
export function decodeObject(data, id, length) {
  const r = new Reader(data);
  if (!magic(r.take(4), "STS3")) return null;
  const version = r.u16(),
    parts = r.u16(),
    flags = r.u32();
  if (
    !(
      (version === 2 && parts === 0) ||
      (version === 3 && parts > 0 && parts <= 10000)
    ) ||
    flags & ~3
  )
    fail();
  const payload = hex(r.take(32)),
    size = r.u64(),
    modified = r.u64(),
    etag = hex(r.take(16)) + (version === 3 ? "-" + parts : ""),
    metadataLength = r.u32(),
    offset = 80 + metadataLength;
  if (
    offset > 65536 ||
    offset > data.length ||
    payload !== id ||
    BigInt(offset) + size !== BigInt(length)
  )
    fail();
  const m = new Reader(data.subarray(80, offset)),
    key = m.text();
  if (!key || utf8.encode(key).length > 1024 || key.includes("\0")) fail();
  const maps = [];
  for (let i = 0; i < 2; i++) {
    const map = Object.create(null),
      count = m.v();
    if (count > 4096) fail();
    let previous = "";
    for (let j = 0; j < count; j++) {
      const k = m.text(),
        v = m.text();
      if (!k || (j && compareText(k, previous) <= 0)) fail();
      map[k] = v;
      previous = k;
    }
    maps.push(map);
  }
  m.done();
  return {
    id,
    key,
    size,
    modified,
    etag,
    flags,
    publicRead: !!(flags & 2),
    deleted: !!(flags & 1),
    headers: maps[0],
    metadata: maps[1],
    offset,
    header: data.slice(0, offset),
  };
}
export async function listPage(client, after, limit, query, { signal } = {}) {
  const page = await client.scanObjects(after, limit, { signal }),
    items = [];
  for (const entry of page.objects) {
    signal?.throwIfAborted();
    let result;
    try {
      result = await client.getRange(entry.id, 0n, 65536n, { signal });
    } catch (e) {
      if (e.status === 404) continue;
      throw e;
    }
    if (!magic(result.data, "STS3")) continue;
    const o = decodeObject(result.data, entry.id, result.info.length);
    if ((await metadataID(o.key)) !== entry.id) fail();
    if (
      !o.deleted &&
      o.key.toLocaleLowerCase().includes(query.toLocaleLowerCase())
    )
      items.push(o);
  }
  return { items, next: page.next };
}
async function chunks(client, object, { signal, max = object.size } = {}) {
  const parts = [];
  let offset = 0n;
  const size = object.size < max ? object.size : max;
  while (offset < size) {
    signal?.throwIfAborted();
    const length = size - offset > 1048576n ? 1048576n : size - offset,
      result = await client.getRange(
        object.id,
        BigInt(object.offset) + offset,
        length,
        {
          signal,
          condition: {
            offset: 0n,
            bytes: object.header,
            length: BigInt(object.offset) + object.size,
          },
        },
      );
    if (BigInt(result.data.length) !== length) fail();
    parts.push(result.data);
    offset += length;
  }
  // Even an empty object must still match the selected metadata image.
  const current = await client.getRange(object.id, 0n, BigInt(object.offset), {
    signal,
  });
  if (
    hex(current.data) !== hex(object.header) ||
    current.info.length !== BigInt(object.offset) + object.size
  )
    throw Error("Objekt wurde inzwischen geändert. Bitte erneut öffnen.");
  return parts;
}
export async function setPublicRead(client, object, value, { signal } = {}) {
  const expected = new Uint8Array(4),
    replacement = new Uint8Array(4);
  new DataView(expected.buffer).setUint32(0, object.flags, true);
  new DataView(replacement.buffer).setUint32(
    0,
    value ? object.flags | 2 : object.flags & ~2,
    true,
  );
  await client.cas(object.id, 8n, expected, replacement, {
    scope: "range",
    signal,
  });
  await client.sync({ signal });
}
export async function open({ bucket, connect, windows, ui, signal }) {
  const window = windows.open({ title: `S3 · ${bucket.name}`, signal });
  const client = await connect();
  if (window.signal.aborted) return;
  const note = document.createElement("p");
  note.className = "plugin-note";
  note.textContent =
    "Suche nach Objektschlüsseln; die Metadaten werden seitenweise im Client gelesen.";
  window.root.append(note);
  const details = async (object) => {
    const detail = windows.open({ title: object.key, signal: window.signal }),
      root = detail.root;
    root.innerHTML =
      '<dl class="plugin-details"></dl><div class="plugin-toolbar"></div><div class="s3-preview"></div>';
    const dl = root.querySelector("dl"),
      toolbar = root.querySelector(".plugin-toolbar"),
      preview = root.querySelector(".s3-preview");
    for (const [key, value] of [
      ["Schlüssel", object.key],
      ["Größe", object.size + " Bytes"],
      ["ETag", object.etag],
      ["Geändert", new Date(Number(object.modified / 1000000n)).toISOString()],
      ...Object.entries(object.headers),
      ...Object.entries(object.metadata).map(([k, v]) => [
        "x-amz-meta-" + k,
        v,
      ]),
    ]) {
      const dt = document.createElement("dt"),
        dd = document.createElement("dd");
      dt.textContent = key;
      dd.textContent = value;
      dl.append(dt, dd);
    }
    const acl = document.createElement("label"),
      select = document.createElement("select");
    acl.textContent = "ACL ";
    select.setAttribute("aria-label", "Objekt-ACL");
    select.innerHTML =
      '<option value="private">Privat</option><option value="public">Öffentlich lesbar</option>';
    select.value = object.publicRead ? "public" : "private";
    acl.append(select);
    toolbar.append(acl);
    toolbar.append(
      ui.button(
        "ACL speichern",
        async () => {
          await setPublicRead(client, object, select.value === "public", {
            signal: detail.signal,
          });
          detail.close();
          await list.load();
        },
        { signal: detail.signal },
      ),
    );
    toolbar.append(
      ui.button(
        "Payload herunterladen",
        async () => {
          const parts = await chunks(client, object, { signal: detail.signal });
          ui.download(
            new Blob(parts),
            object.key.split("/").pop() || "download",
            {
              type:
                object.headers["content-type"] ?? "application/octet-stream",
              signal: detail.signal,
            },
          );
        },
        { signal: detail.signal },
      ),
    );
    const mime = (object.headers["content-type"] ?? "")
        .split(";")[0]
        .trim()
        .toLowerCase(),
      text =
        mime.startsWith("text/") ||
        [
          "application/json",
          "application/xml",
          "application/javascript",
        ].includes(mime),
      image = [
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/webp",
        "image/avif",
        "image/bmp",
      ].includes(mime);
    if (text || image) {
      const loadPreview = async () => {
        const max = text ? 262144n : 8388608n;
        if (image && object.size > max) {
          preview.textContent =
            "Bildvorschau bis 8 MiB. Die vollständige Payload steht als Download bereit.";
          return;
        }
        const parts = await chunks(client, object, {
          signal: detail.signal,
          max,
        });
        if (detail.signal.aborted) return;
        if (text) {
          const pre = document.createElement("pre");
          pre.textContent = await new Blob(parts).text();
          preview.replaceChildren(pre);
          if (object.size > max)
            preview.append("Vorschau auf 256 KiB begrenzt.");
        } else {
          const url = URL.createObjectURL(new Blob(parts, { type: mime })),
            img = document.createElement("img");
          img.alt = object.key;
          img.src = url;
          preview.replaceChildren(img);
          detail.signal.addEventListener(
            "abort",
            () => URL.revokeObjectURL(url),
            { once: true },
          );
        }
      };
      loadPreview().catch((e) => {
        if (!detail.signal.aborted) preview.textContent = e.message;
      });
    }
  };
  const list = ui.createList({
    container: window.root,
    root: window.scrollRoot,
    columns: ["Objektschlüssel", "Größe", "ACL", "Aktionen"],
    key: (o) => o.id,
    signal: window.signal,
    placeholder: "Objektschlüssel suchen …",
    fetchPage: (a, n, q, opts) => listPage(client, a, n, q, opts),
    renderRow: (o) => {
      const row = document.createElement("tr");
      row.innerHTML = `<td class="s3-key">${ui.escape(o.key)}</td><td>${o.size} Bytes</td><td>${o.publicRead ? "Öffentlich lesbar" : "Privat"}</td><td></td>`;
      row.lastElementChild.append(
        ui.button("Details", () => details(o), { signal: window.signal }),
      );
      return row;
    },
  });
  await list.load();
}

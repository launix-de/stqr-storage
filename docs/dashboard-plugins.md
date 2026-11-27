# Dashboard format plugins

The server stores an opaque bucket format string and serves generic native bucket
operations. The dashboard host looks up that string in a file index. All format
schemas, search semantics, previews and mutations live in the selected plugin.
There are no format switches or per-format RPCs in the host.

## Installation

Bundled modules live in `server/format-plugins/` and are embedded into the binary.
Administrator-installed plugins live in `/usr/share/stqr/formats` by default.
Set `format_plugins_dir` in the node YAML configuration to choose a different
folder. A package can install one `FORMAT.mjs` or `FORMAT.js` there without
rebuilding the daemon. These are JavaScript ES modules, including `.js` files.
The filename stem is the exact opaque format string. Names which cannot be local
filenames have no plugin. A module may contain multiline template and CSS strings
and exported helper functions, but needs no companion assets or manifest.

External files override bundled modules with the same stem. Within a directory,
`.mjs` takes precedence over `.js`. The generic authenticated
`GET /api/format-plugins` returns `{plugins:[{format,url}]}`; it never returns
filesystem paths. Modules are served with JavaScript MIME type under
`/format-plugins/`. The bucket refresh button discovers newly installed filenames. Reload the page
after updating the contents of a module:
ES modules are cached for the lifetime of the page. Missing modules keep the
format cell as text. No `admin` module is shipped, so its cell remains text.

Plugins execute with the privileges of the logged-in dashboard user. Install
trusted packages in an administrator-owned directory. This is an extension API,
not an isolation boundary for untrusted JavaScript.

## Module contract, version 1

```js
export const apiVersion = 1;
export const css = `
[data-stqr-plugin="example"] p { line-height: 1.6; }
`;
export async function open({bucket, connect, windows, ui, signal, escape, toast}) {
  const view = windows.open({title: bucket.name, signal});
  const client = await connect();
  // Native bucket operations, interpreted only by this plugin.
  // Append DOM/template content under view.root.
  // Optionally return a synchronous cleanup function.
}
```

A complete minimal example is [examples/format-plugin.mjs](examples/format-plugin.mjs).
`bucket` is dashboard metadata with the opaque `id`, `name` and `format`.
`connect()` lazily opens a bucket-scoped native JS client, using the existing
HTTP-only session and CSRF token in gateway/tunnel mode. The browser retains no
login password. The host closes the client when the plugin's windows close,
on logout or session replacement. The SDK is independently available under
`/assets/sdk/index.mjs`, with codecs under `/assets/sdk/codec.mjs`; see its own
API and transport documentation for standalone direct mode.

`windows.open({title,signal,canClose,dismissible})` returns
`{root,scrollRoot,signal,close,requestClose,closed}`. Titles are
plain text. The host adds `data-stqr-plugin="FORMAT"` to the content root.
Nested windows should use their parent window's signal. Closing a parent closes
its children; closing the last window ends the plugin lifetime. Observe signals
on every asynchronous operation. Never use global IDs or global DOM selectors
for plugin content. Use `view.root.querySelector(...)` instead. Templates must
escape interpolated text; prefer `textContent` for data from buckets.

`ui` supplies `escape`, `button`, `download`, `createList` and `confirm`. Buttons show action
errors and disable themselves during a request. Downloads use Blob URLs with
bounded lifetimes. Blob downloads consume memory proportional to the payload;
large files should use browser streaming-to-disk APIs when available.

## Bucket creation and optional initialization

The creation dialog offers an optional format dropdown populated from the same
plugin-directory index used by the bucket format buttons. Empty selection creates
an unbound bare bucket and never opens a native object connection. A selected
format is bound through the native JS client after administrative creation; no
format fields or format-specific operations are added to the server protocol.
The format is immutable in the existing bucket settings dialog.

A module may additionally export an asynchronous initializer:

```js
export async function initialize({bucket, connect, signal, initializationID, state, ui}) {
  const client = await connect(); // same cached native client for this attempt
  // Format-owned, idempotent bootstrap using native object operations.
  // initializationID and state survive retries of this creation dialog.
}
```

Initializers receive `bucket`, `connect`, `signal`, `initializationID`, `state`,
`ui`, `escape` and `toast`. The host binds the opaque format even when the hook is
absent, awaits the hook and `client.sync()`, then closes the client. User grants
are installed only after initialization succeeds. Validate a plugin module before
creating the bare bucket. A rejected hook or synchronization leaves that bucket
intact; retry resumes initialization on the same bucket and operation identity,
then installs the pending grant without creating a duplicate. Completed phases
are not repeated. Abandoning a partially initialized bucket asks for confirmation.

The initializer must tolerate retries and concurrent first use: prefer absent CAS
and validate existing records rather than overwriting them. Use `state` for stable
request payloads and request IDs. Acknowledged accepted writes whose durability
was uncertain must be replayed/fenced with their original identities on retry.
Do not treat an uncertain sync as successful bootstrap merely because a read sees
its bytes. A reconnect or reload cannot rely on the in-memory `state` object;
format records remain the authority.

`bd` and `s3` have no initializer: a bound bucket with no objects is a valid empty
inventory. `fs` needs an empty directory root for interoperability with the Go
filesystem client. Its plugin alone writes the native STFS v7/STDR v1 root with
absent whole-object CAS, retaining its timestamp and request identity across
retries. An already valid root is preserved, and a corrupt or incompatible root
is never overwritten. Initializing a filesystem does not enable the planned
file-management write actions in its browser.

## Promise-based popup lifecycle

`await view.requestClose(reason)` returns `true` after a permitted user dismissal
or `false` when closing is denied, cancelled, superseded or interrupted. Background
click, Escape and the window close button all use this same asynchronous path.
`canClose({reason,dialog,signal})` returns `boolean | Promise<boolean>`. Repeated
close attempts share the pending Promise, so a confirmation is never asked twice.
Existing native `cancel` listeners can still call `preventDefault()` after the
async condition passes. Failed conditions display an error and keep the popup open.
An earlier decision cannot close a reopened dialog or a window whose policy changed.

```js
const view = windows.open({
  title: bucket.name,
  signal,
  canClose: async ({signal}) => {
    if (saving) return false;
    if (!dirty) return true;
    return ui.confirm({
      title: 'Discard changes?', message: 'Unsaved edits will be lost.',
      confirmLabel: 'Discard', cancelLabel: 'Keep editing', signal,
    });
  },
});
const result = await view.closed; // resolves once; owner-defined string value
```

`ui.confirm(options)` returns `Promise<boolean>`; dismissing or aborting a question
means `false`. `dismissible:false` prevents user dismissal entirely; the native
focus-modal behavior is independent of this policy. Legacy dialogs can opt out
with `data-dialog-dismissible="false"` or use `configureDialog` from
`/assets/dialogs.js`. `await view.close('saved')` completes the window explicitly,
bypassing user guards. Logout and parent-lifetime abort also force cleanup, so a
pending confirmation cannot retain a client after its session ends.

Pending BD mutations and S3 ACL writes veto dismissal. Existing dashboard save,
join, grow, recovery and other operation guards remain active for all dismissal
paths. Clicking content/padding or dragging from content onto the backdrop does
not dismiss a popup. Only the top modal window receives a backdrop dismissal.

## CSS contract

Every selector must begin with `[data-stqr-plugin="FORMAT"]`, followed by a
space or `>` for descendants, or end at the root itself. Every selector in a
comma-separated list needs the prefix. Descendant selectors and `@media` and
`@supports` groups are supported. Global rules, sibling combinators, CSS nesting,
font declarations, imports and keyframes are outside version 1. Use the host's
existing UI classes for common controls. Add no inline `style` attributes.

The host parses and checks the prefix before adding the stylesheet. It uses a
fresh per-response CSP nonce for plugin CSS; the dashboard does not enable
`unsafe-inline`. The nonce is carried in `meta[name=stqr-style-nonce]`. Prefixing
prevents accidental style collisions; it does not restrict executable code.

## Shared search and cursor lists

```js
const list = ui.createList({
  container: view.root,
  root: view.scrollRoot,
  signal: view.signal,
  columns: ['Name', 'Actions'],
  key: item => item.key,
  placeholder: 'Search …',
  fetchPage: async (cursor, limit, query, {signal}) => ({
    items: await formatOwnedRead(cursor, limit, query, signal),
    next: nextOpaqueCursor, // empty string means the end
    // total is optional: never invent a result count before scanning.
  }),
  renderRow: item => createDOMRow(item),
});
await list.load();
```

The provider owns the ordering and query semantics. The host debounces inputs,
invalidates late responses, passes an AbortSignal, deduplicates by the supplied
key, detects cursor cycles, observes the chosen scroll container and renders
loading/error/retry/empty states. It never reorders records unless a comparator
is explicitly supplied to the lower-level `EndlessList`. `destroy()` disconnects
observers and cancels work; the window signal calls it automatically.

Existing user, bucket and placement-group lists use the same cursor engine and
search binding. Volumes filter their already-delivered statistics snapshot,
using the shared input binding. Placement-group queries filter incoming native
pages locally. Unloaded pages remain unloaded until scrolling requires them.
The ordinary `listGroups()` helper still drains pages for complete selection
menus, whose semantics require the full set.

## Bundled plugins

* `fs`: read-only lazy directory tree and downloads, including inline and indexed
  STFS v1–v7 data, sparse mutable segments and logical namespace transaction
  snapshots. It rechecks inode and transaction heads before publishing a download.
  Directory materialization has the Go client's 64 MiB limit. STOR overlays and
  historical cross-bucket lower references require the existing overlay client;
  the plugin reports that condition explicitly. Symlinks/FIFOs/sockets are shown
  without following or reading them. Create directory, rename, upload and delete
  are future work and do not appear as enabled actions.
* `s3`: scans pages in native object-address order and filters keys in the plugin; displays metadata,
  payload downloads, private/public-read ACL editing with byte-comparing ranged CAS,
  and text/image previews. Text is shown literally, including HTML; SVG and other
  active document types are never rendered as documents. Text previews are capped
  at 256 KiB; raster previews at 8 MiB. Payload reads compare the selected metadata
  image. Search may scan many native pages because the server has no key knowledge.
* `bd`: lists native STNBD001 descriptors with exact sizes and persistent disk IDs.
  Create, grow, rename and delete run under the native exclusive bucket lease and
  use CAS/preconditions and explicit durability fences. Growth preserves stripe
  identities; shrinking is rejected by the format. Active writers must disconnect
  before editing. Rename writes and syncs the new descriptor before removing the
  old descriptor: interrupted rename can leave two names for the same device.
  Repeat that rename to finish it. Deletion refuses aliased devices, removes the
  descriptor before reclaiming matching systematic stripes, and never exposes old
  stripes to a recreated name. Interrupted cleanup leaves orphan stripes for the
  existing native collector. A lost connection can leave an uncertain mutation
  outcome; refresh inventory before performing a different operation.

## Verification

Unit tests: `node --test server/dashboard_*_test.mjs`.
Server discovery/authentication/CSP: `go test ./server -run TestFormatPlugins`.
Real-server browser interoperability: set `STQR_TEST_BROWSER_MODULE` to the
installed Playwright module, optionally `STQR_TEST_BROWSER_EXECUTABLE`, then run
`go test ./server -run TestDashboardPluginsBrowser -count=1 -v`.
The browser fixture seeds data using the Go FS/S3/NBD clients and verifies JS
changes again through the Go clients after the browser exits.

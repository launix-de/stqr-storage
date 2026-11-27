# Bounded volume membership routing

A managed data volume may build a fixed 4 MiB, key-only RAM membership filter after
storage readiness. It runs in bounded batches and is optional: cancellation,
missing/incomplete filters and failed builds retain the original exact lookup.
No statistics/rebalancing timer rebuilds it, and no object metadata directory is
retained. An import invalidates the filter. The worker registry contains only
in-flight managed instances and is cancelled on detach/Close.

A negative is usable only while the relevant keys cannot change. Native external
GET/GET_RANGE/STAT and the audited external reference-root read acquire object-key
coordination followed by shared node admission. They release those guards as soon
as the selected Volume read handle pins its bytes, before payload/network output.
Hinted reads use shared admission only when immediately available; a contended
node-wide lock, a single eligible volume, or no skippable candidate keeps the
original full-probe read path without node admission. An unlocked preflight is
only an optimization choice; all used absence proofs are rebuilt under guards.
Generic/internal, COW fallback, subscription and S3 reads keep the full-probe path;
recursively taking admission under an administrative lock or range lease is unsafe.

Mutations additionally coordinate nonzero request IDs in a separate transient
node-local registry. Lock order is object key, request ID, node admission. This
preserves cross-object receipt checks within a node without persistent request
state or a new global cross-node collision guarantee. A probe can skip a volume
only if object, pending-object, request receipt and replica namespaces are all
proven absent. Capacity ranking still includes every eligible volume. Positive
hints, including real Bloom false positives, always require exact lookup.

An unpinned multi-volume cohort may use shared mutation admission only when exactly
one eligible volume remains possible, its authority/accounting/quota hooks are
ready, and an exact probe finds the existing object there. An absent object or
false positive retries with exclusive admission before consuming payload bytes.
New unpinned objects retain capacity-based exclusive placement. Finite logical
and physical quota reservations remain those of scoped parallel admission; all
COW-linked buckets, replicas, changing authority and other unsupported states
retain their existing exclusive fallback. The filter never grants authority.

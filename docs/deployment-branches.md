# Deployment compatibility branches

`fix/capacity-headroom` is a maintenance branch for the currently deployed native
wire-v1 / volume-format-4 nodes. It is not a second feature PR. Its applicable
capacity, accounting, dashboard and idle-cleanup changes are ported into
`fix/publish-handoff` (PR #10), which targets the current master.

Do not merge the maintenance branch wholesale into master: master already uses
the unversioned native object API and volume format 5. Likewise, do not replace
an existing format-4 deployment with a master build without its migration plan.
The maintenance branch preserves a reproducible source base for those deployed
binaries. Subsequent PXE diagnostics may be based on that branch separately;
they are not part of the object-boundary refactor.

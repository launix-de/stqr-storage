# Deleted bucket reclamation

A committed `DELETING` catalog description remains the durable cleanup intent.
Normal client access and new clone edges are rejected in this state. Physical
reclamation can therefore work under the volume lock without holding node
admission (`opMu`). Completion still requires every required cluster member to
confirm its local cleanup before the catalog is marked complete.

The reclaimer alternates bounded object pages (32 records) and receipt pages
(128 records). A receipt traversal belongs to a volume and serves a fixed set
of deleting buckets together. New deletions join the next traversal, starting
at the beginning; they cannot skip records before a previous cursor. Advisory
cursors survive unrelated catalog updates and are reconstructed after restart.
Maintained physical accounting lets wholly absent buckets skip the traversal.

Normal turns try all runnable candidates without queueing behind busy volumes.
Foreground work defers cleanup until it has had no successful turn for 250 ms.
An overdue turn may then queue one bounded volume writer, preventing continuous
readers from winning every admission opportunity. This is an admission interval,
not a promise that an I/O operation finishes in 250 ms. Active mandatory rebuild
I/O retains priority. The range-cleanup handoff gives object reclamation first
choice on its bounded inter-page turn; the independent worker alternates object
and receipt work.

Metadata cleanup uses the existing bounded accepted-write overlay where there
is room for its journal. The physical checkpoint runs after closing the write
handle, outside node admission and the volume metadata mutex. Cursor advancement
and final completion require successful durability confirmation; a failed fence
retains the pending generation. An empty volatile view is not proof of durable
completion.

Very small/full volumes, or a batch exceeding the accepted-write workspace, use
the existing durable scratch-WAL path so cleanup can still release space. This
fallback also runs outside node admission, but retains that existing path's
volume-lock behavior during its physical commit. Removing that remaining
volume-level serialization belongs to the general write-fence work. No WAL,
volume, object, or protocol format changes are introduced here.

Regression coverage checks shared receipt work against a separate-traversal
control, unrelated payload/receipt retention, late participants, busy-volume
skipping, foreground aging, queued reader/writer behavior, blocked and failed
fences, discarded RAM cursors, actual volume reopen after simulated power loss,
and the tiny-volume fallback. The optimization does not claim to fix kernel-FS
Open/Close reference persistence or page-cache invalidation, nor does it assign
all observed background CPU usage to this one path.

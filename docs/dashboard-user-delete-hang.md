# Dashboard deletion latency: diagnosis and correction

The confirmation handler awaited `USER_DELETE` before closing its modal. That
request performs a durable ALL metadata transaction and first waits for the
controller mutex, which is also held during background COW/cleanup and policy
work. The browser had no per-request deadline or pending progress state. Global
administration was additionally dispatched synchronously in the WebSocket's
request worker, so a delayed mutation blocked later independent list/PING
requests on the same dashboard connection.

An isolated daemon test gates RESERVE during a real user deletion. Before the
correction, a second socket answered PING while the dashboard socket could not
answer its following PING. With bounded asynchronous mutation dispatch, the same
socket responds before the gate is released. Existing global metadata locking
and publication remain in force.

Read-only live observations on 25 November 2026 found matching builds on both
nodes and healthy fresh local requests (roughly 43–45 ms). Remote upgrades were
intermittently delayed by 1.9–3.2 seconds; another timed out after four seconds.
Validated copies of both control journals agreed at membership epoch 2, global
sequence 860, generation 5, with no outstanding global requester or participant
slot. This is a snapshot, not evidence identifying the holder of the mutex for
each observed deletion. No real account was deleted and no process restarted.

The correction lets the administrator confirm further deletions immediately.
Rows remain pending until their result arrives; a shared browser deletion queue
bounds in-flight mutations and does not block ordinary requests. Failures remain
on their rows, with stable operation IDs and options for retry. Actual user and
bucket data changes still require the cluster's durable commit.

The browser regression uses a production daemon with an explicit RESERVE gate,
not transport-only fixtures or sleeps. It confirms two users and two buckets,
refreshes and navigates while the first deletion is gated, checks defaults and
opt-out, and verifies both success and a rejected-then-retried transaction. It
also checks that bucket deletion removes only eligible orphan accounts, retaining
administrators and accounts with another valid bucket.

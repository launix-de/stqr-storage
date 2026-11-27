# Bounded reference-root reclamation

Lifetime references remain ordinary bucket objects with stable IDs. When a new
PIN reaches the existing 8,192-entry limit, the server may remove plain PINs
owned by account IDs absent from its authoritative immutable RAM account index.
It preserves every claim, dependency, present account, and disabled account.
The per-account 1,024-entry limit remains unchanged.

Filtering and the new PIN use one ordinary CAS. A private context guard carries
the exact account-index image and removed IDs into actual object-key/node
admission. Account changes invalidate that guard; the caller rereads and
refilters. The original connection-work context and accepted-write fence remain
attached. This operation adds no timer, background scan, persistence operation,
object version, or implicit Sync.

Reclamation requires an active, stable single-copy policy and map: no desired
policy, everywhere placement, pending volume authority, or remote replica path.
Actual admission repeats that proof and performs an exact pending-replica-key
lookup on every allowed local volume. Existing pending-replica/topology fences
protect transitions into the single-copy map. A pending prepare must complete
or abort before reclamation; missing or uncertain evidence fails closed.
Ordinary replicated lifetime references retain their existing behavior. Already
prepared distributed operations may complete after account deletion; this
change does not claim distributed account-deletion quiescence or reclaim their
roots. Distributed reclamation requires a separate end-to-end protocol proof.

Native Basic authentication binds an original, process-local account continuity
token before authentication can wait. Cookies retain the token from session
creation. Deletion/recreation, even with identical account fields, creates a new
token. Reference-root mutation validates this original identity under actual
node admission. Single-copy publication keeps that admission through its CAS;
account deletion uses exclusive admission. Tokens are bounded by current users
and in-flight principals, rather than an accumulating deletion history.

Scoped frontend credentials attest an HTTP/S3 operation rather than the
original account incarnation across nodes. They therefore neither negotiate
lifetime-reference capabilities nor execute ObjectReference or mutate the
reserved reference root through ordinary object operations. Ordinary frontend
object access and S3 operations continue to work. Supporting scoped frontend
lifetime references requires original authentication continuity at the issuer,
issuer grant verification, and a matching receiver-side continuity proof.

This change is stacked on the accepted-reference combiner PR. Its legacy-startup
review base has no asynchronous storage-warming state; integration retains the
additional warming check before trusting account authority. The existing
spare-slot decoder integration test uses a present disabled owner to exercise
the unchanged global cap; missing-owner roots are covered by reclamation tests.

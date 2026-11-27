# Bucket COW inheritance

A new clone persists a directed relationship in its two catalog descriptions:
`child` on the source, `parent` on the fresh destination. Descriptions containing
these directions require schema 3. Every required cluster member must advertise
the matching control and COW semantics before these descriptions are committed;
older catalog readers reject schema 3 rather than interpreting it symmetrically.
Later catalog edits retain the required schema and directions.

A namespace resolves missing physical records through parents, never children.
Before changing an object it preserves the original preimage, including prior
absence, into children that do not yet have a physical record. It first resolves
and materializes its own inherited preimage when necessary. An existing local
record or tombstone remains a materialization boundary. Object IDs stay stable.
For A → B → C, changing A preserves B; C subsequently reads that preserved B.
Changing C never writes absence markers or preimages into B or A.

The complete reciprocal graph remains available for lifetime protection,
publication proofs, detach and deletion. Its digest includes edge directions.
Legacy links with no direction remain symmetric; new directed links can coexist
with them. Unknown or nonreciprocal directions fail closed. Detach and prospective
quota evaluation resolve each namespace using the same directional read rules.

Ordinary accepted leaf writes do not request physical Sync. Their own inherited
materialization shares the writing bucket's accepted completion contract; the
explicit client fence covers its touched volume generations. A source mutation
which must preserve a different namespace retains the existing independent-owner
durability barrier. Existing physical presence is also confirmed through a
finite volume-generation cut: a failed accepted child CAS may have left its
inherited preimage visible without ever receiving a client fence. This change does not remove that dependency or claim a fence
on one owner certifies another owner's data.

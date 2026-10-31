---
title: Stream failover
description: Design for automatic failover of replicated Plexus streams by sealing surviving replicas and promoting the furthest, which also lets durable streams confirm on a majority.
---

This is a proposed design. Today a replicated durable stream that loses its
owner waits for an operator: the controller records a pending recovery, the
recovery driver refuses streams, and the stream shows up under controller
attention. Durable streams also confirm only once every copy holds a record,
which is what makes promoting any follower safe today.

## Goals

- Fail over a replicated durable stream automatically when its owner is lost,
  without losing any confirmed record.
- Let durable streams confirm on a majority of copies, like queues, so one
  slow or lost follower does not stall confirms.
- Reuse the existing seal operation and pending recovery bookkeeping.

Out of scope: learner rejoin and the follower loss hold for streams, recovery
of the express tiers, and verifying stream contents byte for byte. Streams
have no queue state to rebuild, so the record log tail is the evidence that
matters.

## Safety argument

A stream assignment has `E` copies and confirms a record once `W` of them hold
it durably, the owner included. Recovery seals `R = E - W + 1` copies. Any set
of `W` copies and any set of `R` copies share at least one copy, since
`W + R = E + 1`. So among any `R` sealed copies, at least one holds every
confirmed record, and the copy with the furthest durable record tail holds
them all, because each copy's log is a prefix of the old owner's log.

Sealing fences each copy at a new epoch first, so its tail cannot grow from
the old owner while the controller decides. The promoted copy becomes owner at
its own tail, exactly as `PromoteFollowerToOwner` does today.

With `W = E`, which is today's every-copy policy, `R = 1` and any surviving
copy can be promoted. With 3 copies and `W = 2`, `R = 2` and both surviving
followers must answer before failover proceeds. If they cannot, the stream
waits, as a queue does, instead of guessing.

## Flow

1. The owner is lost, through liveness expiry or eager failover. The controller
   proposes a new owner as today and, because the stream is replicated,
   records a pending recovery instead of moving at once.
2. The recovery driver, which already runs on the controller, sends a stream
   seal at the next epoch to every surviving copy of the old assignment and
   collects replies until it has `R` of them.
3. It picks the copy with the highest durable record tail. Ties go to the
   highest cursor event tail, then to the lowest node id, so the choice is
   deterministic.
4. It commits the new assignment with that copy as owner at the next epoch,
   the other sealed copies as followers, and clears the pending recovery.
   Placement adds a replacement copy later, as for any lost follower.
5. The new owner serves from its tail. Followers catch up from it as usual.

## Copies with a longer tail

A copy that was not sealed, typically the old owner coming back, can hold
records past the new owner's tail. None of them was confirmed, since a
confirmed record is on the promoted copy. When such a copy rejoins under the
new epoch, it truncates its record log to the new owner's tail at that epoch
before catching up, instead of failing on the conflicting offsets. The cursor
event log is truncated the same way. Cursor positions above the new tail are
clamped to it, so a returning reader resumes from a valid offset.

## Cursors

The promoted copy may hold fewer cursor commits than the old owner, since
cursor events reach followers only once durable on the owner and are written
without a per-commit fsync. A reader resuming from such a cursor receives some
records again, which at-least-once allows.

## Majority confirms

Once failover seals as above, durable streams use the cluster's assignment
durability policy, `majority_durable` by default, instead of requiring every
copy. The stream delivery gate and the producer confirm already wait on the
assignment's requirement, so they follow automatically.

## Tests

- Unit: the seal threshold for `E` and `W` combinations, the choice of the
  furthest copy and its tie-break, and waiting when fewer than `R` copies
  answer.
- Broker: a returning copy with a longer unconfirmed suffix truncates to the
  new owner's tail and catches up, with cursors clamped.
- Cluster: kill the owner of a replicated durable stream under confirmed load
  with 3 copies and `majority_durable`. Every confirmed record is delivered
  after failover, no record is delivered twice at different offsets, and the
  stream serves again without an operator.

## Order of work

1. Stream seals and the furthest-copy choice in the recovery driver, with the
   pending recovery cleared on commit.
2. Truncation on rejoin for copies with a longer tail.
3. The majority policy for durable streams.
4. A stream owner kill case in the cluster bench.

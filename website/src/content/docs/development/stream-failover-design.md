---
title: Stream failover
description: How a replicated Plexus stream fails over by sealing surviving copies and promoting the furthest, and why that lets durable streams confirm on a majority.
---

A replicated durable stream that loses its owner fails over without an
operator. The recovery worker on the coordination leader seals the surviving
copies, promotes the one with the furthest record tail and commits the new
assignment. Durable streams confirm by the cluster assignment policy,
`majority_durable` by default, instead of waiting for every copy.

Out of scope: learner rejoin and the follower loss hold for streams, recovery
of the express tiers, and verifying stream contents byte for byte. Streams have
no queue state to rebuild, so the record log tail is the evidence that matters.

## Safety argument

A stream assignment has `E` copies and confirms a record once `W` of them hold
it durably, the owner included. Recovery seals at least `R = E - W + 1` copies.
Any set of `W` copies and any set of `R` copies share at least one copy, since
`W + R = E + 1`. So among the sealed copies at least one holds every confirmed
record, and the copy with the furthest durable record tail holds them all,
because each copy's log is a prefix of the old owner's log.

Sealing fences each copy at the next epoch first, so its tail cannot grow from
the old owner while the controller decides. The promoted copy becomes owner at
its own tail.

With `W = E` (every copy) `R = 1` and any surviving copy can be promoted. With
3 copies and `W = 2`, `R = 2` and both surviving followers must answer. If they
cannot, the stream waits, as a queue does, instead of guessing.

## One rule for W

`stream_confirm_policy` in the broker's coordination module turns the policy
stamped on a stream assignment into the stream's confirm rule. The broker
confirms by it, and the controller's write requirement, from which the pending
recovery and the seal threshold take `R`, uses the same function. The two
cannot disagree.

The controller stamps the cluster assignment policy on every replicated stream
assignment. `local_durable`, which is also what an assignment carries if it was
placed before streams had a policy, means every copy, since a replicated stream
has no owner-only mode. A count above the replica set is capped at the whole
set, and an accepted count is read as a durable one.

## Flow

1. The owner is lost, through liveness expiry or eager failover. The
   controller proposes a new owner as before and, because the stream is
   replicated, records a pending recovery instead of moving at once.
2. The recovery worker on the coordination leader sends a stream seal at the
   next epoch to every copy of the old assignment at once. Sealing more than
   `R` copies only adds followers, and the furthest of a larger set is still
   correct. Copies that fail to answer in time are left out.
3. With at least `R` answers it picks the copy with the highest record tail.
   Ties go to the highest cursor event tail, then to the lowest node id, so the
   choice is deterministic. If that copy's record tail is below the latest
   recorded history start (see below), it waits instead, as it does with fewer
   than `R` answers.
4. One guarded consensus write commits the new assignment, clears the pending
   recovery and appends where the new owner's history starts (its record and
   cursor event tails at the new epoch) to the stream's history starts. The
   owner is the chosen copy, the other sealed copies follow it, and members the
   proposal added that held no copy join as followers too.
5. Each broker applies the new assignment. A sealed copy that the assignment
   includes releases its seal and continues, the new owner promoting at its own
   tails. Copies left out stay sealed until a later assignment includes them.

A stream change that keeps the owner and every follower and needs no more
copies to confirm commits without recovery. A record confirmed on `W` old copies
meets any `E' - W' + 1` new copies when `W' <= W`, so new copies may start empty
and catch up. That covers placement adding a replacement follower after a
failover, and moving a stream from every copy to a majority.

## The confirmed prefix

The intersection argument covers records confirmed under the previous
assignment. Records confirmed before it lie below the latest recorded history
start, the promoted copy's record tail when it took over, and right after a
failover that copy may be the only one holding them until its followers catch
up. So recovery never promotes a copy whose record tail is below that start.
It waits for a copy at or past it, reads the starts from the same snapshot its
guarded commit checks, and the controller lists the wait under attention with
the start's record count and the node that took over there. Attention judges
from the tails copies publish in their heartbeats, so it stays quiet while a
live copy has published nothing.

With this check every promoted tail is at least the previous start, so the
recorded starts only grow.

## Copies with a longer tail

A copy that was not sealed, typically the old owner coming back, can hold
records past the new owner's start. None of them was confirmed, since a
confirmed record is on the promoted copy. Before any role change at a new
epoch, a broker asks storage to bring the copy onto the history of that epoch:

- The copy's own log epochs say which owner history it followed. A sealed copy
  uses the epochs its seal replaced, since the seal advanced them without new
  data.
- The cut is the lowest history start recorded for an epoch after the copy's
  own and up to the target epoch. Both the record log and the cursor event log
  are cut there. A copy already on the target history is left alone, so a
  follower that caught up past the start never loses confirmed records.
- A stream snapshot covering cursor events past the cut keeps its state but has
  its boundary lowered, so the new owner's events at those offsets replay.
- On the next open the stream tail comes down to the record log end and any
  cursor above it is clamped to it, so a returning reader resumes from a valid
  offset.

The cut never goes below the retained head and never touches entries at or
below the start. Repeating it is harmless, since the copy's epochs only move
once the role change after it advances them. A fresh follower worker resumes at
the copy's own tails, which the cut has made a prefix of the owner's log.

History starts of a stream that no longer has an assignment are dropped, so a
recreated stream does not inherit them. An unreadable list withholds the
assignment rather than letting a copy follow without it.

## Seals

A stream seal only fences and reports tails. Nothing is installed from it, so,
unlike a queue seal, it is released in place once the new assignment includes
the copy at or after the fence epoch. A newer transition at the same fence
epoch takes over an existing stream seal, which happens when an operator
withdraws a stream recovery and the controller records a new one.

## Cursors

The promoted copy may hold fewer cursor commits than the old owner, since
cursor events reach followers only once durable on the owner and are written
without a per-commit fsync. A reader resuming from such a cursor receives some
records again, which at-least-once allows.

## Known limits

- Right after a failover the new owner may be the only copy holding records
  confirmed under the old assignment until its followers catch up. Losing it in
  that window is a second failure, beyond what the policy promises. The next
  recovery then waits for a copy that holds the confirmed prefix, usually that
  owner coming back, and shows under controller attention until one answers or
  an operator accepts the loss.
- A copy whose broker did not apply its last role change before being sealed
  reports a raw tail that can include an older owner's unconfirmed suffix.
- Sealing reads the whole retained log to compute its digest, and a broker
  takes one recovery seal at a time, so many large streams failing over from
  one node recover one after another.
- Accepting owner loss moves a stream without sealing anything, so the start
  recorded for that epoch is the new owner's tails as it last published them
  in its heartbeat. They can only lag its log, so a returning copy may cut
  lower than needed and fetch shared records again, and the owner of an epoch
  is never cut at its own start. If the new owner published nothing, no start
  is recorded and a returning copy is not cut back. Such a promotion can also
  land below the confirmed prefix, which the operator accepted, and the broker
  logs a warning.

## Tests

- Unit: the seal threshold for `E` and `W` combinations, the choice of the
  furthest copy and its tie-break, waiting when fewer than `R` copies answer,
  a second owner loss waiting for the confirmed prefix and promoting once a
  copy at the start answers, the matching attention entry, the start recorded
  on an accepted owner loss, and queue changes still recovering where stream
  follower additions do not.
- Storage and broker: a returning copy with a longer suffix cuts to the new
  start with cursors clamped and takes the new owner's entries, and a sealed
  copy is released and promoted.
- Cluster: kill the owner of a replicated durable stream with 3 copies on
  `majority_durable`. Every confirmed record is delivered at one offset each,
  the stream serves again without an operator, and the old owner rejoins as a
  follower without another recovery. The cluster bench has a stream owner kill
  case (`--stream durable --kill-owner-at-secs`).

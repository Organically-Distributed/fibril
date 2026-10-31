//! Automatic failover of a replicated stream from sealed surviving copies.
//!
//! A stream confirms a record once `W` of its `E` copies hold it durably.
//! Recovery seals `R = E - W + 1` copies of the previous assignment at the
//! next epoch. Any `W` copies and any `R` copies share one, so the sealed copy
//! with the furthest record tail holds every confirmed record, and it becomes
//! the owner. The other sealed copies follow it.
//!
//! Each promotion records where the new owner's history starts. A copy that
//! followed an older owner cuts its logs back to that start before it follows
//! again, since anything past it was never confirmed.
use crate::{promotion::PendingRecovery, GanglionCoordination};
use fibril_broker::coordination::StreamEpochStart;
use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use std::collections::BTreeMap;

pub const STREAM_EPOCH_STARTS_PREFIX: &str = "fibril/stream-epoch-starts/";

pub fn epoch_starts_key(resource: &ResourceIdentity) -> String {
    format!(
        "{STREAM_EPOCH_STARTS_PREFIX}{}",
        serde_json::to_string(resource).expect("resource identity serializes")
    )
}

/// Every recorded start of an owner history for this stream, oldest first.
pub fn epoch_starts(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> Result<Vec<StreamEpochStart>, String> {
    snapshot
        .attributes
        .get(&epoch_starts_key(resource))
        .map(|raw| {
            serde_json::from_str(raw).map_err(|e| format!("invalid stream epoch starts: {e}"))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

/// Drop history starts of streams that no longer have an assignment. A
/// recreated stream numbers its epochs again and must not inherit them.
pub(crate) fn prune_epoch_starts(snapshot: &mut CoordinationSnapshot) {
    let stale: Vec<String> = snapshot
        .attributes
        .keys()
        .filter(|key| key.starts_with(STREAM_EPOCH_STARTS_PREFIX))
        .filter(|key| {
            serde_json::from_str::<ResourceIdentity>(&key[STREAM_EPOCH_STARTS_PREFIX.len()..])
                .map_or(true, |resource| {
                    !snapshot.assignments.contains_key(&resource)
                })
        })
        .cloned()
        .collect();
    for key in stale {
        snapshot.attributes.remove(&key);
    }
}

/// Tails one sealed copy reported, already fenced at the recovery epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedStreamCopy {
    pub node: String,
    pub records: u64,
    pub cursor_events: u64,
}

impl SealedStreamCopy {
    pub fn from_seal(
        pending: &PendingRecovery,
        seal: &fibril_broker::recovery::BrokerSealedReplica,
    ) -> Result<Self, String> {
        let request = &seal.seal.request;
        if request.transition != pending.transition_digest()?
            || request.fence_epoch != pending.proposed.epoch
        {
            return Err("stream seal answers another recovery".into());
        }
        match seal.seal.history.logs {
            fibril_broker::recovery::RetainedLogs::Stream {
                records,
                cursor_events,
            } => Ok(Self {
                node: seal.node_id.clone(),
                records: records.next,
                cursor_events: cursor_events.next,
            }),
            fibril_broker::recovery::RetainedLogs::Queue(_) => {
                Err("stream seal returned a queue history".into())
            }
        }
    }
}

/// Copies to seal before a replicated stream may fail over: `E - W + 1` for
/// the previous assignment, with `W` from the same rule the broker confirms by.
pub fn seal_threshold(previous: &PartitionAssignment) -> Result<usize, String> {
    previous
        .replica_set_size()
        .checked_sub(crate::promotion::write_requirement(previous)?)
        .map(|n| n + 1)
        .ok_or_else(|| "write requirement exceeds the replica set".into())
}

/// The furthest record tail wins, then the furthest cursor event tail, then
/// the lowest node id, so every controller picks the same copy.
pub fn choose_owner(copies: &[SealedStreamCopy]) -> Option<&SealedStreamCopy> {
    copies.iter().max_by(|a, b| {
        a.records
            .cmp(&b.records)
            .then(a.cursor_events.cmp(&b.cursor_events))
            .then(b.node.cmp(&a.node))
    })
}

/// The assignment that ends `pending` from `copies`, and the start of its
/// owner history. Fails while fewer than the threshold of copies answered.
pub fn recovery_assignment(
    pending: &PendingRecovery,
    copies: &[SealedStreamCopy],
) -> Result<(PartitionAssignment, StreamEpochStart), String> {
    let previous = &pending.previous;
    if previous.resource.namespace != crate::STREAM_NAMESPACE {
        return Err("stream recovery applies to streams only".into());
    }
    let required = seal_threshold(previous)?;
    if required != pending.required_old_witnesses {
        return Err("stream recovery threshold differs from the pending request".into());
    }
    let old: Vec<&String> = std::iter::once(&previous.owner)
        .chain(previous.followers.iter())
        .collect();
    let mut seen = std::collections::BTreeSet::new();
    for copy in copies {
        if !old.contains(&&copy.node) || !seen.insert(copy.node.as_str()) {
            return Err(format!(
                "{} is not a distinct copy of the previous assignment",
                copy.node
            ));
        }
    }
    if copies.len() < required {
        return Err(format!(
            "stream recovery has {} of {required} required sealed copies",
            copies.len()
        ));
    }
    let owner = choose_owner(copies).expect("at least one copy");
    // Sealed copies keep their previous order. Members the proposal added,
    // which held no copy before, join as followers. Old copies that did not
    // answer are left out and placement can add them back later.
    let mut followers: Vec<String> = old
        .iter()
        .filter(|node| ***node != owner.node && seen.contains(node.as_str()))
        .map(|node| (*node).clone())
        .collect();
    for node in std::iter::once(&pending.proposed.owner).chain(pending.proposed.followers.iter()) {
        if !old.contains(&node) && !followers.contains(node) {
            followers.push(node.clone());
        }
    }
    let mut assignment = PartitionAssignment::new(
        previous.resource.clone(),
        owner.node.clone(),
        followers,
        pending.proposed.epoch,
    );
    assignment.durability = pending.proposed.durability;
    crate::promotion::write_requirement(&assignment)?;
    Ok((
        assignment,
        StreamEpochStart {
            epoch: pending.proposed.epoch,
            records: owner.records,
            cursor_events: owner.cursor_events,
        },
    ))
}

fn error(message: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(message.to_string())
}

impl GanglionCoordination {
    /// Publish the recovered assignment, its owner history start and the
    /// removal of the pending request in one guarded consensus write.
    pub async fn commit_stream_recovery(
        &self,
        pending: &PendingRecovery,
        copies: &[SealedStreamCopy],
    ) -> Result<PartitionAssignment, OpenraftAdapterError> {
        let key = crate::promotion::pending_recovery_key(&pending.previous.resource);
        let raw = serde_json::to_string(pending).map_err(error)?;
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            if snapshot.attributes.get(&key) != Some(&raw)
                || snapshot.assignments.get(&pending.previous.resource) != Some(&pending.previous)
            {
                return Err(error("stream recovery request changed before commit"));
            }
            let (assignment, start) = recovery_assignment(pending, copies).map_err(error)?;
            let mut starts = epoch_starts(&snapshot, &assignment.resource).map_err(error)?;
            starts.retain(|known| known.epoch < start.epoch);
            starts.push(start);
            let attributes = BTreeMap::from([
                (key.clone(), None),
                (
                    crate::recovery_candidate::key(pending).map_err(error)?,
                    None,
                ),
                (
                    epoch_starts_key(&assignment.resource),
                    Some(serde_json::to_string(&starts).map_err(error)?),
                ),
            ]);
            match self
                .forward_command(MetadataRaftCommand::UpdatePartitionGuarded {
                    expected_generation: snapshot.generation,
                    assignment: assignment.clone(),
                    attributes,
                })
                .await
            {
                Ok(_) => return Ok(assignment),
                Err(OpenraftAdapterError::GenerationMismatch { .. }) => {
                    tokio::task::yield_now().await
                }
                Err(e) => return Err(e),
            }
        }
        Err(error(
            "stream recovery raced metadata changes; retry with backoff",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_core::ReplicationDurabilityPolicy;

    fn stream(
        owner: &str,
        followers: &[&str],
        durability: ReplicationDurabilityPolicy,
    ) -> PartitionAssignment {
        let mut a = PartitionAssignment::new(
            ResourceIdentity::new(crate::STREAM_NAMESPACE, "s", 0, None::<String>),
            owner,
            followers.iter().map(|f| f.to_string()).collect(),
            4,
        );
        a.durability = durability;
        a
    }

    fn pending(previous: PartitionAssignment, proposed: PartitionAssignment) -> PendingRecovery {
        let mut proposed = proposed;
        proposed.epoch = previous.epoch + 1;
        PendingRecovery {
            version: 1,
            resource_incarnation: None,
            previous_activation: None,
            requested_generation: 1,
            previous_write_nodes: crate::promotion::write_requirement(&previous).unwrap(),
            proposed_write_nodes: crate::promotion::write_requirement(&proposed).unwrap(),
            required_old_witnesses: seal_threshold(&previous).unwrap(),
            previous,
            proposed,
        }
    }

    fn copy(node: &str, records: u64, cursor_events: u64) -> SealedStreamCopy {
        SealedStreamCopy {
            node: node.into(),
            records,
            cursor_events,
        }
    }

    #[test]
    fn every_copy_streams_need_one_sealed_copy() {
        for followers in [
            &[][..],
            &["b"][..],
            &["b", "c"][..],
            &["b", "c", "d", "e"][..],
        ] {
            let a = stream("a", followers, ReplicationDurabilityPolicy::LocalDurable);
            assert_eq!(seal_threshold(&a).unwrap(), 1, "{followers:?}");
        }
    }

    #[test]
    fn furthest_record_tail_wins_then_cursor_events_then_lowest_node() {
        let copies = [copy("c", 10, 1), copy("b", 12, 0), copy("d", 11, 9)];
        assert_eq!(choose_owner(&copies).unwrap().node, "b");
        let tied = [copy("c", 12, 3), copy("b", 12, 2)];
        assert_eq!(choose_owner(&tied).unwrap().node, "c");
        let all_tied = [copy("d", 12, 3), copy("b", 12, 3), copy("c", 12, 3)];
        assert_eq!(choose_owner(&all_tied).unwrap().node, "b");
        // Reply order never matters.
        let mut reversed = all_tied.clone();
        reversed.reverse();
        assert_eq!(choose_owner(&reversed).unwrap().node, "b");
        assert!(choose_owner(&[]).is_none());
    }

    #[test]
    fn waits_for_a_sealed_copy_then_promotes_the_furthest() {
        let every = ReplicationDurabilityPolicy::LocalDurable;
        let previous = stream("a", &["b", "c"], every);
        let p = pending(previous.clone(), stream("b", &["c"], every));
        assert_eq!(p.required_old_witnesses, 1);
        let error = recovery_assignment(&p, &[]).unwrap_err();
        assert!(error.contains("0 of 1"), "{error}");

        let (assignment, start) =
            recovery_assignment(&p, &[copy("b", 9, 1), copy("c", 11, 0)]).unwrap();
        assert_eq!(assignment.owner, "c");
        assert_eq!(assignment.followers, vec!["b"]);
        assert_eq!(assignment.epoch, previous.epoch + 1);
        assert_eq!(
            start,
            StreamEpochStart {
                epoch: previous.epoch + 1,
                records: 11,
                cursor_events: 0
            }
        );
        let (alone, _) = recovery_assignment(&p, &[copy("c", 5, 5)]).unwrap();
        assert_eq!(alone.owner, "c");
        assert!(alone.followers.is_empty(), "b did not answer");
    }

    #[test]
    fn new_members_of_the_proposal_follow_and_outsiders_are_refused() {
        let every = ReplicationDurabilityPolicy::LocalDurable;
        let p = pending(
            stream("a", &["b", "c"], every),
            stream("a", &["b", "d"], every),
        );
        let (assignment, _) = recovery_assignment(&p, &[copy("a", 7, 2), copy("b", 7, 2)]).unwrap();
        assert_eq!(assignment.owner, "a");
        assert_eq!(assignment.followers, vec!["b", "d"]);

        for bad in [
            vec![copy("a", 1, 0), copy("x", 9, 0)],
            vec![copy("a", 1, 0), copy("a", 1, 0)],
        ] {
            assert!(recovery_assignment(&p, &bad).is_err());
        }
        let mut queue = p.clone();
        queue.previous.resource.namespace = crate::QUEUE_NAMESPACE.into();
        assert!(recovery_assignment(&queue, &[copy("a", 1, 0), copy("b", 1, 0)]).is_err());
    }

    #[test]
    fn threshold_is_copies_minus_write_quorum_plus_one() {
        use ReplicationDurabilityPolicy::*;
        let followers = ["b", "c", "d", "e"];
        for copies in 2..=5usize {
            let f = &followers[..copies - 1];
            for w in 1..=copies {
                let a = stream("a", f, ReplicaDurable { nodes: w });
                assert_eq!(
                    seal_threshold(&a).unwrap(),
                    copies - w + 1,
                    "E={copies} W={w}"
                );
            }
        }
        // W = E: any single survivor is enough.
        assert_eq!(
            seal_threshold(&stream("a", &["b", "c"], ReplicaDurable { nodes: 3 })).unwrap(),
            1
        );
        // Three copies on a majority: both survivors must answer.
        assert_eq!(
            seal_threshold(&stream("a", &["b", "c"], MajorityDurable)).unwrap(),
            2
        );
        assert_eq!(
            seal_threshold(&stream("a", &["b", "c", "d", "e"], MajorityDurable)).unwrap(),
            3
        );
        // A policy count above the replica set means every copy.
        assert_eq!(
            seal_threshold(&stream("a", &["b"], ReplicaDurable { nodes: 3 })).unwrap(),
            1
        );
    }

    #[test]
    fn majority_waits_for_both_survivors_then_promotes_the_furthest() {
        let majority = ReplicationDurabilityPolicy::MajorityDurable;
        let previous = stream("a", &["b", "c"], majority);
        let p = pending(previous.clone(), stream("b", &["c"], majority));
        assert_eq!(p.required_old_witnesses, 2);
        let error = recovery_assignment(&p, &[copy("b", 9, 1)]).unwrap_err();
        assert!(error.contains("1 of 2"), "{error}");

        let (assignment, start) =
            recovery_assignment(&p, &[copy("b", 9, 1), copy("c", 11, 0)]).unwrap();
        assert_eq!(assignment.owner, "c");
        assert_eq!(assignment.followers, vec!["b"]);
        assert_eq!(assignment.durability, majority);
        assert_eq!(start.records, 11);
        assert_eq!(crate::promotion::write_requirement(&assignment).unwrap(), 2);
    }

    #[test]
    fn history_starts_of_removed_streams_are_dropped() {
        let kept = stream("a", &[], ReplicationDurabilityPolicy::LocalDurable);
        let mut gone = kept.clone();
        gone.resource.name = "gone".into();
        let mut snapshot = CoordinationSnapshot::default();
        snapshot
            .assignments
            .insert(kept.resource.clone(), kept.clone());
        for resource in [&kept.resource, &gone.resource] {
            snapshot
                .attributes
                .insert(epoch_starts_key(resource), "[]".into());
        }
        prune_epoch_starts(&mut snapshot);
        assert!(snapshot
            .attributes
            .contains_key(&epoch_starts_key(&kept.resource)));
        assert!(!snapshot
            .attributes
            .contains_key(&epoch_starts_key(&gone.resource)));
    }

    #[test]
    fn a_tampered_threshold_is_refused() {
        let every = ReplicationDurabilityPolicy::LocalDurable;
        let mut p = pending(stream("a", &["b", "c"], every), stream("b", &["c"], every));
        p.required_old_witnesses = 2;
        assert!(recovery_assignment(&p, &[copy("b", 1, 0), copy("c", 1, 0)]).is_err());
    }
}

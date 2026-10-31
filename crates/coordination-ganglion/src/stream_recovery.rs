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

/// One recorded owner history start and the node that took over there. The
/// start's record tail is the prefix holding every record confirmed under
/// the assignment before it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecordedEpochStart {
    #[serde(flatten)]
    pub start: StreamEpochStart,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

/// Every recorded start of an owner history for this stream, oldest first.
pub fn recorded_epoch_starts(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> Result<Vec<RecordedEpochStart>, String> {
    snapshot
        .attributes
        .get(&epoch_starts_key(resource))
        .map(|raw| {
            serde_json::from_str(raw).map_err(|e| format!("invalid stream epoch starts: {e}"))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

/// The recorded starts without their owners, as brokers use them.
pub fn epoch_starts(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> Result<Vec<StreamEpochStart>, String> {
    Ok(recorded_epoch_starts(snapshot, resource)?
        .into_iter()
        .map(|recorded| recorded.start)
        .collect())
}

/// The latest start before `epoch`. Every record confirmed before that start
/// lies below its record tail, and only the copy promoted there was sure to
/// hold them until its followers caught up.
pub fn confirmed_prefix(starts: &[RecordedEpochStart], epoch: u64) -> Option<&RecordedEpochStart> {
    starts
        .iter()
        .filter(|recorded| recorded.start.epoch < epoch)
        .max_by_key(|recorded| recorded.start.epoch)
}

/// Why a recovery must wait for a copy holding the confirmed prefix, if it
/// must.
pub fn prefix_wait(prefix: &RecordedEpochStart, furthest: u64) -> Option<String> {
    (furthest < prefix.start.records).then(|| {
        format!(
            "waiting for a copy that holds the confirmed prefix of {} records from epoch {}{}, the furthest copy holds {furthest}",
            prefix.start.records,
            prefix.start.epoch,
            prefix
                .owner
                .as_ref()
                .map(|owner| format!(" (owner {owner})"))
                .unwrap_or_default(),
        )
    })
}

/// Record and cursor event tails a stream copy last published in its
/// heartbeat. Advisory: a label can lag the copy.
pub(crate) fn published_tails(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
    node: &str,
) -> Option<(u64, u64)> {
    let label = crate::applied_tail_label(&fibril_broker::coordination::QueueIdentity::new(
        resource.name.clone(),
        fibril_broker::Partition::new(u32::try_from(resource.partition).ok()?),
        None,
    ));
    snapshot
        .nodes
        .get(node)?
        .labels
        .get(&label)
        .and_then(|raw| crate::parse_applied_tail(raw, true))
}

/// Why a stream recovery is stuck on the confirmed prefix, judged from the
/// tails live copies published. `None` when some live copy may hold it or a
/// live copy published nothing, since the driver is then expected to finish.
pub(crate) fn live_copies_lack_prefix(
    snapshot: &CoordinationSnapshot,
    pending: &PendingRecovery,
    serving: &dyn Fn(&str) -> bool,
) -> Option<String> {
    let starts = recorded_epoch_starts(snapshot, &pending.previous.resource).ok()?;
    let prefix = confirmed_prefix(&starts, pending.proposed.epoch)?;
    let mut furthest = 0;
    for node in std::iter::once(&pending.previous.owner).chain(pending.previous.followers.iter()) {
        if serving(node) {
            furthest = furthest.max(published_tails(snapshot, &pending.previous.resource, node)?.0);
        }
    }
    prefix_wait(prefix, furthest)
}

/// The start to record when an operator moves a stream off a lost owner:
/// the new owner's tails as it last published them, at the new epoch. They
/// can only lag what it holds when it takes over, and cutting a returning
/// copy lower than needed only makes it fetch shared records again. Returns
/// the attribute to write, or `None` when the new owner published nothing.
pub(crate) fn accepted_loss_start(
    committed: &CoordinationSnapshot,
    proposed: &PartitionAssignment,
) -> Result<Option<(String, String)>, String> {
    let Some((records, cursor_events)) =
        published_tails(committed, &proposed.resource, &proposed.owner)
    else {
        tracing::warn!(
            topic = proposed.resource.name,
            partition = proposed.resource.partition,
            owner = proposed.owner,
            "new stream owner published no tails, a returning copy is not cut back"
        );
        return Ok(None);
    };
    let mut updated = CoordinationSnapshot::default();
    if let Some(raw) = committed
        .attributes
        .get(&epoch_starts_key(&proposed.resource))
    {
        updated
            .attributes
            .insert(epoch_starts_key(&proposed.resource), raw.clone());
    }
    let start = RecordedEpochStart {
        start: StreamEpochStart {
            epoch: proposed.epoch,
            records,
            cursor_events,
        },
        owner: Some(proposed.owner.clone()),
    };
    record_epoch_start(&mut updated, &proposed.resource, start)?;
    let key = epoch_starts_key(&proposed.resource);
    let value = updated.attributes.remove(&key).expect("start recorded");
    Ok(Some((key, value)))
}

/// Append `start` to the stream's recorded starts in `snapshot`, replacing
/// any at the same or a later epoch.
pub(crate) fn record_epoch_start(
    snapshot: &mut CoordinationSnapshot,
    resource: &ResourceIdentity,
    start: RecordedEpochStart,
) -> Result<(), String> {
    let mut starts = recorded_epoch_starts(snapshot, resource)?;
    starts.retain(|known| known.start.epoch < start.start.epoch);
    starts.push(start);
    snapshot.attributes.insert(
        epoch_starts_key(resource),
        serde_json::to_string(&starts).map_err(|e| e.to_string())?,
    );
    Ok(())
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
/// owner history. Fails while fewer than the threshold of copies answered,
/// and while even the furthest copy lacks the confirmed prefix recorded at
/// the last promotion in `starts`.
pub fn recovery_assignment(
    pending: &PendingRecovery,
    copies: &[SealedStreamCopy],
    starts: &[RecordedEpochStart],
) -> Result<(PartitionAssignment, RecordedEpochStart), String> {
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
    // The intersection argument covers records confirmed under the previous
    // assignment only. Older ones are known to be below the last start.
    if let Some(wait) = confirmed_prefix(starts, pending.proposed.epoch)
        .and_then(|prefix| prefix_wait(prefix, owner.records))
    {
        return Err(wait);
    }
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
        RecordedEpochStart {
            start: StreamEpochStart {
                epoch: pending.proposed.epoch,
                records: owner.records,
                cursor_events: owner.cursor_events,
            },
            owner: Some(owner.node.clone()),
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
            // Starts come from the same snapshot the guarded write checks, so
            // a stale view cannot skip the confirmed prefix.
            let starts =
                recorded_epoch_starts(&snapshot, &pending.previous.resource).map_err(error)?;
            let (assignment, start) =
                recovery_assignment(pending, copies, &starts).map_err(error)?;
            let mut updated = snapshot.clone();
            record_epoch_start(&mut updated, &assignment.resource, start).map_err(error)?;
            let starts_key = epoch_starts_key(&assignment.resource);
            let attributes = BTreeMap::from([
                (key.clone(), None),
                (
                    crate::recovery_candidate::key(pending).map_err(error)?,
                    None,
                ),
                (starts_key.clone(), updated.attributes.remove(&starts_key)),
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
        let error = recovery_assignment(&p, &[], &[]).unwrap_err();
        assert!(error.contains("0 of 1"), "{error}");

        let (assignment, start) =
            recovery_assignment(&p, &[copy("b", 9, 1), copy("c", 11, 0)], &[]).unwrap();
        assert_eq!(assignment.owner, "c");
        assert_eq!(assignment.followers, vec!["b"]);
        assert_eq!(assignment.epoch, previous.epoch + 1);
        assert_eq!(
            start.start,
            StreamEpochStart {
                epoch: previous.epoch + 1,
                records: 11,
                cursor_events: 0
            }
        );
        let (alone, _) = recovery_assignment(&p, &[copy("c", 5, 5)], &[]).unwrap();
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
        let (assignment, _) =
            recovery_assignment(&p, &[copy("a", 7, 2), copy("b", 7, 2)], &[]).unwrap();
        assert_eq!(assignment.owner, "a");
        assert_eq!(assignment.followers, vec!["b", "d"]);

        for bad in [
            vec![copy("a", 1, 0), copy("x", 9, 0)],
            vec![copy("a", 1, 0), copy("a", 1, 0)],
        ] {
            assert!(recovery_assignment(&p, &bad, &[]).is_err());
        }
        let mut queue = p.clone();
        queue.previous.resource.namespace = crate::QUEUE_NAMESPACE.into();
        assert!(recovery_assignment(&queue, &[copy("a", 1, 0), copy("b", 1, 0)], &[]).is_err());
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
        let error = recovery_assignment(&p, &[copy("b", 9, 1)], &[]).unwrap_err();
        assert!(error.contains("1 of 2"), "{error}");

        let (assignment, start) =
            recovery_assignment(&p, &[copy("b", 9, 1), copy("c", 11, 0)], &[]).unwrap();
        assert_eq!(assignment.owner, "c");
        assert_eq!(assignment.followers, vec!["b"]);
        assert_eq!(assignment.durability, majority);
        assert_eq!(start.start.records, 11);
        assert_eq!(start.owner.as_deref(), Some("c"));
        assert_eq!(crate::promotion::write_requirement(&assignment).unwrap(), 2);
    }

    fn recorded(epoch: u64, records: u64, owner: &str) -> RecordedEpochStart {
        RecordedEpochStart {
            start: StreamEpochStart {
                epoch,
                records,
                cursor_events: 0,
            },
            owner: Some(owner.into()),
        }
    }

    /// The owner promoted at the last failover is lost before its followers
    /// held everything confirmed before it took over. The next recovery waits
    /// for a copy at or past that start instead of promoting a shorter one.
    #[test]
    fn a_second_owner_loss_waits_for_the_confirmed_prefix() {
        let majority = ReplicationDurabilityPolicy::MajorityDurable;
        // b took over at epoch 4 holding 100 records. c and d lag behind it.
        let previous = stream("b", &["c", "d"], majority);
        let p = pending(previous, stream("c", &["d"], majority));
        let starts = [recorded(2, 40, "a"), recorded(4, 100, "b")];
        let error =
            recovery_assignment(&p, &[copy("c", 90, 3), copy("d", 99, 1)], &starts).unwrap_err();
        assert!(
            error.contains("confirmed prefix of 100 records from epoch 4"),
            "{error}"
        );
        assert!(error.contains("owner b"), "{error}");

        // Once a copy at the start answers, it is promoted.
        let (assignment, start) =
            recovery_assignment(&p, &[copy("c", 90, 3), copy("d", 100, 1)], &starts).unwrap();
        assert_eq!(assignment.owner, "d");
        assert_eq!(start.start.records, 100);
        let (assignment, _) =
            recovery_assignment(&p, &[copy("c", 130, 3), copy("d", 99, 1)], &starts).unwrap();
        assert_eq!(assignment.owner, "c");
        // A start at or after the recovery epoch is not a prefix of it.
        let later = [recorded(p.proposed.epoch, 500, "x")];
        assert!(recovery_assignment(&p, &[copy("c", 1, 0), copy("d", 2, 0)], &later).is_ok());
    }

    #[test]
    fn recorded_starts_read_without_an_owner_and_replace_later_epochs() {
        let resource = stream("a", &[], ReplicationDurabilityPolicy::LocalDurable).resource;
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.attributes.insert(
            epoch_starts_key(&resource),
            r#"[{"epoch":2,"records":7,"cursor_events":1}]"#.into(),
        );
        assert_eq!(
            recorded_epoch_starts(&snapshot, &resource).unwrap()[0].owner,
            None
        );
        record_epoch_start(&mut snapshot, &resource, recorded(5, 9, "b")).unwrap();
        record_epoch_start(&mut snapshot, &resource, recorded(4, 8, "c")).unwrap();
        let starts = epoch_starts(&snapshot, &resource).unwrap();
        assert_eq!(
            starts.iter().map(|s| s.epoch).collect::<Vec<_>>(),
            vec![2, 4]
        );
    }

    fn with_tail(snapshot: &mut CoordinationSnapshot, node: &str, records: u64) {
        let mut info = ganglion_core::NodeInfo::new(node, "127.0.0.1:1", None::<String>);
        info.labels.insert(
            crate::applied_tail_label(&fibril_broker::coordination::QueueIdentity::new(
                "s",
                fibril_broker::Partition::new(0),
                None,
            )),
            crate::encode_stream_applied_tail(records, 0),
        );
        snapshot.nodes.insert(node.into(), info);
    }

    #[test]
    fn attention_names_the_owner_holding_the_confirmed_prefix() {
        let majority = ReplicationDurabilityPolicy::MajorityDurable;
        let previous = stream("b", &["c", "d"], majority);
        let p = pending(previous.clone(), stream("c", &["d"], majority));
        let mut snapshot = CoordinationSnapshot::default();
        snapshot
            .assignments
            .insert(previous.resource.clone(), previous.clone());
        snapshot.attributes.insert(
            crate::promotion::pending_recovery_key(&previous.resource),
            serde_json::to_string(&p).unwrap(),
        );
        record_epoch_start(&mut snapshot, &previous.resource, recorded(4, 100, "b")).unwrap();
        let live = |node: &str| node != "b";
        // A live copy that published nothing may still hold the prefix.
        with_tail(&mut snapshot, "c", 90);
        assert!(crate::operator_override::recovery_attention(&snapshot, live).is_empty());
        with_tail(&mut snapshot, "d", 95);
        let attention = crate::operator_override::recovery_attention(&snapshot, live);
        assert_eq!(attention.len(), 1, "{attention:?}");
        assert!(attention[0].reason.contains("confirmed prefix"));
        let detail = attention[0].detail.as_deref().unwrap();
        assert!(
            detail.contains("owner b") && detail.contains("holds 95"),
            "{detail}"
        );
        with_tail(&mut snapshot, "d", 100);
        assert!(crate::operator_override::recovery_attention(&snapshot, live).is_empty());
    }

    #[test]
    fn accepted_owner_loss_records_the_new_owners_published_tails() {
        let mut moved = stream("c", &[], ReplicationDurabilityPolicy::LocalDurable);
        moved.epoch = 7;
        let mut committed = CoordinationSnapshot::default();
        record_epoch_start(&mut committed, &moved.resource, recorded(4, 100, "b")).unwrap();
        assert_eq!(accepted_loss_start(&committed, &moved).unwrap(), None);
        with_tail(&mut committed, "c", 120);
        let (key, value) = accepted_loss_start(&committed, &moved).unwrap().unwrap();
        committed.attributes.insert(key, value);
        let starts = recorded_epoch_starts(&committed, &moved.resource).unwrap();
        assert_eq!(starts.len(), 2);
        assert_eq!(starts[1], recorded(7, 120, "c"));
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
        assert!(recovery_assignment(&p, &[copy("b", 1, 0), copy("c", 1, 0)], &[]).is_err());
    }
}

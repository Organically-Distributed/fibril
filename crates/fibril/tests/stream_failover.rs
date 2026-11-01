//! Three real broker processes with `majority_durable` assignments and a
//! durable stream on all three. Killing the stream's owner fails it over to
//! the furthest surviving copy without an operator: every confirmed record is
//! still delivered, no offset carries two different records, and the stream
//! takes new records again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fibril_client::{Client, ClientOptions, NewMessage, StreamConfig, SubEvent};
use tokio::process::{Child, Command};

#[allow(dead_code)]
mod common;

const SECRET: &str = "stream-failover-test-secret";
const TOPIC: &str = "ticks";

struct Node {
    name: String,
    broker: String,
    admin: String,
    child: Option<Child>,
    log: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

fn spawn_node(root: &Path, id: u64, raft: &[String; 3]) -> Node {
    let broker = common::free_loopback_addr().to_string();
    let admin = common::free_loopback_addr().to_string();
    spawn_node_on(root, id, raft, broker, admin)
}

/// Start node `id` on fixed addresses, also to restart it in place.
fn spawn_node_on(root: &Path, id: u64, raft: &[String; 3], broker: String, admin: String) -> Node {
    let toml = format!(
        r#"[server]
data_dir = {data}

[broker.listener]
bind = "{broker}"

[admin.listener]
bind = "{admin}"

[coordination]
mode = "ganglion"
node_id = "node-{id}"

[coordination.ganglion]
raft_node_id = {id}
listen = "{listen}"
bootstrap = {bootstrap}
heartbeat_interval_ms = 300
liveness_ttl_ms = 2000
controller_tick_ms = 300
controller_startup_grace_ms = 0
target_followers = 2
stream_replication_factor = 2

[coordination.ganglion.peers]
"1" = "{r1}"
"2" = "{r2}"
"3" = "{r3}"

[coordination.ganglion.assignment_durability]
mode = "majority_durable"
"#,
        data = serde_json::to_string(&root.join(format!("data-{id}"))).unwrap(),
        listen = raft[id as usize - 1],
        bootstrap = id == 1,
        r1 = raft[0],
        r2 = raft[1],
        r3 = raft[2],
    );
    let config = root.join(format!("node-{id}.toml"));
    std::fs::write(&config, toml).unwrap();
    let log = root.join(format!("node-{id}.log"));
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_fibril-server"))
        .env("FIBRIL_CONFIG", &config)
        .env("FIBRIL_CLUSTER_SECRET", SECRET)
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(out.try_clone().unwrap())
        .stderr(out)
        .kill_on_drop(true)
        .spawn()
        .expect("spawn fibril-server");
    Node {
        name: format!("node-{id}"),
        broker,
        admin,
        child: Some(child),
        log,
    }
}

async fn admin_get(admin: &str, path: &str) -> Option<serde_json::Value> {
    reqwest::Client::new()
        .get(format!("http://{admin}{path}"))
        .basic_auth("fibril", Some("fibril"))
        .send()
        .await
        .ok()?
        .json::<serde_json::Value>()
        .await
        .ok()
}

async fn stream_assignment(admin: &str) -> Option<serde_json::Value> {
    let topology = admin_get(admin, "/admin/api/topology").await?;
    topology["coordination"]["stream_assignments"]
        .as_array()?
        .iter()
        .find(|a| a["topic"] == TOPIC && a["partition"].as_u64() == Some(0))
        .cloned()
}

async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn connect(broker: &str) -> Client {
    ClientOptions::new()
        .auth("fibril", "fibril")
        .publish_timeout_ms(30_000)
        .connect(broker)
        .await
        .unwrap()
}

async fn publish_confirmed(client: &Client, payload: &str) {
    let publisher = client.publisher(TOPIC).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match publisher.publish_confirmed(NewMessage::text(payload)).await {
            Ok(_) => return,
            Err(err) => {
                assert!(
                    Instant::now() < deadline,
                    "publish of {payload} never confirmed: {err}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_a_durable_stream_owner_fails_over_without_losing_confirmed_records() {
    let _permit = common::BOOT_PERMITS.acquire_many(3).await.unwrap();
    let root = common::temp_root("fibril-stream-failover", "cluster");
    let raft: [String; 3] = std::array::from_fn(|_| common::free_loopback_addr().to_string());
    let mut nodes: Vec<Node> = (1..=3).map(|id| spawn_node(&root, id, &raft)).collect();

    let result = tokio::time::timeout(
        Duration::from_secs(240),
        run_scenario(&mut nodes, &root, &raft),
    )
    .await;
    if !matches!(result, Ok(())) {
        for node in &nodes {
            let log = std::fs::read_to_string(&node.log).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(80).collect();
            eprintln!("---- {} log tail ----", node.name);
            for line in tail.into_iter().rev() {
                eprintln!("{line}");
            }
        }
        panic!("scenario failed or timed out: {result:?}");
    }
    let _ = std::fs::remove_dir_all(&root);
}

async fn run_scenario(nodes: &mut [Node], root: &Path, raft: &[String; 3]) {
    let admin = nodes[0].admin.clone();
    wait_until(
        "three registered brokers",
        Duration::from_secs(60),
        || async {
            admin_get(&admin, "/admin/api/topology")
                .await
                .is_some_and(|t| {
                    t["coordination"]["nodes"]
                        .as_array()
                        .is_some_and(|nodes| nodes.len() == 3)
                })
        },
    )
    .await;

    let declarer = connect(&nodes[0].broker).await;
    let mut declared = false;
    for _ in 0..50 {
        let config = StreamConfig::new(TOPIC)
            .unwrap()
            .durable()
            .replication_factor(2);
        match declarer.declare_plexus(config).await {
            Ok(()) => {
                declared = true;
                break;
            }
            Err(err) => {
                eprintln!("declare: {err}");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    }
    assert!(declared, "stream never declared");
    wait_until(
        "three copies confirming on a majority",
        Duration::from_secs(60),
        || async {
            stream_assignment(&admin).await.is_some_and(|a| {
                a["followers"].as_array().is_some_and(|f| f.len() == 2)
                    && a["durability"]["mode"] == "majority_durable"
            })
        },
    )
    .await;
    let before = stream_assignment(&admin).await.unwrap();
    let owner = before["owner"].as_str().unwrap().to_string();
    let epoch = before["epoch"].as_u64().unwrap();

    // Publish through a broker that survives, so the client keeps its
    // bootstrap connection across the kill.
    let survivor = nodes.iter().position(|n| n.name != owner).unwrap();
    let client = connect(&nodes[survivor].broker).await;
    let mut confirmed = BTreeSet::new();
    for i in 0..200 {
        let payload = format!("before-{i}");
        publish_confirmed(&client, &payload).await;
        confirmed.insert(payload);
    }

    let victim_index = nodes.iter().position(|n| n.name == owner).unwrap();
    let victim = &mut nodes[victim_index];
    victim.child.as_mut().unwrap().kill().await.unwrap();
    victim.child = None;

    let survivor_admin = nodes[survivor].admin.clone();
    wait_until(
        "a new stream owner at the next epoch",
        Duration::from_secs(90),
        || async {
            stream_assignment(&survivor_admin).await.is_some_and(|a| {
                a["owner"] != owner.as_str() && a["epoch"].as_u64().is_some_and(|e| e > epoch)
            })
        },
    )
    .await;
    let after = stream_assignment(&survivor_admin).await.unwrap();
    assert_eq!(after["followers"].as_array().unwrap().len(), 1, "{after}");
    let topology = admin_get(&survivor_admin, "/admin/api/topology")
        .await
        .unwrap();
    eprintln!(
        "after failover: {after}, consensus {}",
        topology["consensus"]
    );

    // The stream serves again without an operator.
    for i in 0..50 {
        let payload = format!("after-{i}");
        publish_confirmed(&client, &payload).await;
        confirmed.insert(payload);
    }

    let reader = connect(&nodes[survivor].broker).await;
    let mut sub = reader
        .stream(TOPIC)
        .unwrap()
        .partitions(1)
        .from_earliest()
        .sub_auto_ack()
        .await
        .unwrap();
    let mut by_offset: BTreeMap<u64, String> = BTreeMap::new();
    let mut received = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !confirmed.is_subset(&received) {
        assert!(
            Instant::now() < deadline,
            "lost confirmed records: {:?}",
            confirmed.difference(&received).collect::<Vec<_>>()
        );
        match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
            Ok(SubEvent::Delivery(msg)) => {
                let payload = String::from_utf8(msg.payload.clone()).unwrap();
                let offset = msg.delivery_tag.epoch;
                if let Some(seen) = by_offset.insert(offset, payload.clone()) {
                    assert_eq!(seen, payload, "offset {offset} delivered two records");
                }
                received.insert(payload);
            }
            Ok(SubEvent::Closed(reason)) => panic!("subscription closed: {reason:?}"),
            Err(_) => eprintln!("no delivery for 10s, still waiting"),
        }
    }
    // Offsets stay dense: the new owner continued from its own tail.
    let offsets: Vec<u64> = by_offset.keys().copied().collect();
    assert_eq!(offsets, (0..offsets.len() as u64).collect::<Vec<_>>());

    // The old owner comes back and rejoins as a follower. Adding it keeps
    // the owner and every follower, so no recovery runs.
    let (broker, admin) = (
        nodes[victim_index].broker.clone(),
        nodes[victim_index].admin.clone(),
    );
    let id = victim_index as u64 + 1;
    nodes[victim_index] = spawn_node_on(root, id, raft, broker, admin);
    let new_owner = after["owner"].as_str().unwrap().to_string();
    wait_until(
        "the old owner following again",
        Duration::from_secs(90),
        || async {
            stream_assignment(&survivor_admin).await.is_some_and(|a| {
                a["owner"] == new_owner.as_str()
                    && a["followers"]
                        .as_array()
                        .is_some_and(|f| f.iter().any(|n| n == owner.as_str()))
            })
        },
    )
    .await;
    let rejoined = stream_assignment(&survivor_admin).await.unwrap();
    assert_eq!(rejoined["epoch"], after["epoch"], "{rejoined}");
    for i in 0..20 {
        publish_confirmed(&client, &format!("rejoined-{i}")).await;
    }
    let topology = admin_get(&survivor_admin, "/admin/api/topology")
        .await
        .unwrap();
    assert_eq!(
        topology["consensus"]["controller"]["pending_recoveries"],
        serde_json::json!([]),
        "{topology}"
    );
}

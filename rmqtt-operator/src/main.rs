use anyhow::{anyhow, bail, Context, Result};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::time::Duration;
use tokio::fs;
use tokio::time::sleep;

const GROUP: &str = "rmqtt.io";
const VERSION: &str = "v1alpha1";
const PLURAL: &str = "rmqttclusters";
const FIELD_MANAGER: &str = "rmqtt-operator";
const DEFAULT_NAMESPACE: &str = "rmqtt-test";
const DEFAULT_IMAGE: &str = "rmqtt-dynamic-test:official-amd64-20260713-155254";

const RENDER_SCRIPT: &str = r###"#!/bin/sh
set -eu

WORK_DIR="${RMQTT_WORK_DIR:-/work}"
SOURCE_DIR="${RMQTT_SOURCE_DIR:-/app/rmqtt}"
POD_NAME="${HOSTNAME}"
ORDINAL="${POD_NAME##*-}"
NODE_ID="$((ORDINAL + 1))"
REPLICAS="${RMQTT_REPLICAS:-3}"
HEADLESS="${RMQTT_HEADLESS_SERVICE}"
NAMESPACE="${POD_NAMESPACE}"
GRPC_PORT="${RMQTT_GRPC_PORT:-5363}"
RAFT_PORT="${RMQTT_RAFT_PORT:-6003}"
HTTP_PORT="${RMQTT_HTTP_API_PORT:-6060}"
RAFT_BOOTSTRAP="${RMQTT_RAFT_BOOTSTRAP:-false}"

case "${RAFT_BOOTSTRAP}" in
  true|false) ;;
  *) echo "invalid RMQTT_RAFT_BOOTSTRAP=${RAFT_BOOTSTRAP}; expected true or false" >&2; exit 1 ;;
esac

mkdir -p "${WORK_DIR}/rmqtt-plugins" "${WORK_DIR}/rmqtt-bin"
cp "${SOURCE_DIR}/rmqtt.toml" "${WORK_DIR}/rmqtt.toml"
cp "${SOURCE_DIR}"/rmqtt-plugins/*.toml "${WORK_DIR}/rmqtt-plugins/"
cp "${SOURCE_DIR}/rmqtt-bin/rmqtt.pem" "${WORK_DIR}/rmqtt-bin/" 2>/dev/null || true
cp "${SOURCE_DIR}/rmqtt-bin/rmqtt.key" "${WORK_DIR}/rmqtt-bin/" 2>/dev/null || true

grpc_peers=""
raft_peers=""
i=1
while [ "$i" -le "$REPLICAS" ]; do
  ordinal="$((i - 1))"
  # Both services are in the pod namespace. The short stable hostname lets
  # the resolver append the correct namespace search domain on its first try.
  host="${RMQTT_NAME}-${ordinal}.${HEADLESS}"
  grpc_peer="${i}@${host}:${GRPC_PORT}"
  raft_peer="${i}@${host}:${RAFT_PORT}"
  if [ -z "$grpc_peers" ]; then
    grpc_peers="\"${grpc_peer}\""
    raft_peers="\"${raft_peer}\""
  else
    grpc_peers="${grpc_peers}, \"${grpc_peer}\""
    raft_peers="${raft_peers}, \"${raft_peer}\""
  fi
  i="$((i + 1))"
done

sed -i "s/^node.id = .*/node.id = ${NODE_ID}/" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^rpc.server_addr = .*#rpc.server_addr = \"0.0.0.0:${GRPC_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i 's/^log.to = .*/log.to = "console"/' "${WORK_DIR}/rmqtt.toml"
sed -i 's/#"rmqtt-cluster-raft"/"rmqtt-cluster-raft"/' "${WORK_DIR}/rmqtt.toml"

sed -i "s#^http_laddr = .*#http_laddr = \"0.0.0.0:${HTTP_PORT}\"#" "${WORK_DIR}/rmqtt-plugins/rmqtt-http-api.toml"
sed -i "s#^node_grpc_addrs = .*#node_grpc_addrs = [${grpc_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
sed -i "s#^raft_peer_addrs = .*#raft_peer_addrs = [${raft_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
if grep -q '^leader_id = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i 's/^leader_id = .*/leader_id = 1/' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\nleader_id = 1\n' >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
if grep -q '^bootstrap = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i "s/^bootstrap = .*/bootstrap = ${RAFT_BOOTSTRAP}/" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\nbootstrap = %s\n' "${RAFT_BOOTSTRAP}" >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
if grep -q '^join_as_learner = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i 's/^join_as_learner = .*/join_as_learner = true/' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  printf '\njoin_as_learner = true\n' >> "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi
if grep -q '^#laddr = ' "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"; then
  sed -i "s|^#laddr = .*|laddr = \"0.0.0.0:${RAFT_PORT}\"|" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
else
  sed -i "s|^laddr = .*|laddr = \"0.0.0.0:${RAFT_PORT}\"|" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
fi

echo "rendered rmqtt config: node_id=${NODE_ID}, replicas=${REPLICAS}, pod=${POD_NAME}, raft_bootstrap=${RAFT_BOOTSTRAP}"
echo "raft peers: [${raft_peers}]"
echo "grpc peers: [${grpc_peers}]"
"###;

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct ObjectList<T> {
    #[serde(default)]
    items: Vec<T>,
}

#[derive(Debug, Deserialize, Clone)]
struct Metadata {
    name: String,
    #[serde(default)]
    generation: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
struct RmqttCluster {
    metadata: Metadata,
    #[serde(default)]
    spec: ClusterSpec,
    #[serde(default)]
    status: Option<ClusterStatus>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ClusterSpec {
    #[serde(default = "default_replicas")]
    replicas: i32,
    #[serde(default = "default_image")]
    image: String,
    #[serde(default)]
    service: ServiceSpec,
    #[serde(default)]
    raft: RaftSpec,
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            image: default_image(),
            service: ServiceSpec::default(),
            raft: RaftSpec::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ServiceSpec {
    #[serde(default = "default_mqtt_port")]
    mqtt_port: i32,
    #[serde(default = "default_http_api_port")]
    http_api_port: i32,
    #[serde(default = "default_grpc_port")]
    grpc_port: i32,
    #[serde(default = "default_raft_port")]
    raft_port: i32,
}

impl Default for ServiceSpec {
    fn default() -> Self {
        Self {
            mqtt_port: default_mqtt_port(),
            http_api_port: default_http_api_port(),
            grpc_port: default_grpc_port(),
            raft_port: default_raft_port(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct RaftSpec {
    #[serde(default = "default_min_replicas")]
    min_replicas: i32,
    #[serde(default = "default_drain_timeout_seconds")]
    drain_timeout_seconds: u64,
    #[serde(default)]
    allow_leader_scale_in: bool,
}

impl Default for RaftSpec {
    fn default() -> Self {
        Self {
            min_replicas: default_min_replicas(),
            drain_timeout_seconds: default_drain_timeout_seconds(),
            allow_leader_scale_in: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PodList {
    #[serde(default)]
    items: Vec<Pod>,
}

#[derive(Debug, Deserialize)]
struct Pod {
    #[serde(default)]
    metadata: PodMetadata,
    #[serde(default)]
    status: PodStatus,
}

#[derive(Debug, Deserialize, Default)]
struct PodMetadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Default)]
struct PodStatus {
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    conditions: Vec<PodCondition>,
}

#[derive(Debug, Deserialize)]
struct PodCondition {
    #[serde(rename = "type")]
    kind: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct StatefulSet {
    #[serde(default)]
    metadata: StatefulSetMetadata,
    #[serde(default)]
    spec: StatefulSetSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetMetadata {
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

impl StatefulSet {
    fn replicas(&self) -> i32 {
        self.spec.replicas.unwrap_or_default()
    }

    fn bootstrap_allowed(&self) -> bool {
        self.metadata.annotations.get("rmqtt.io/raft-bootstrap-allowed").is_some_and(|value| value == "true")
    }
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetSpec {
    #[serde(default)]
    replicas: Option<i32>,
}

#[derive(Debug, Deserialize, Default)]
struct DrainStatus {
    #[serde(default)]
    safe_to_leave: bool,
    #[serde(default)]
    strict_safe_to_leave: Option<bool>,
    #[serde(default)]
    session_handoff_complete: Option<bool>,
    #[serde(default)]
    blocking_reasons: Vec<String>,
}

impl DrainStatus {
    fn scale_in_safe_to_leave(&self) -> bool {
        self.strict_safe_to_leave.unwrap_or(self.safe_to_leave)
            && self.session_handoff_complete.unwrap_or(true)
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RoutingStatus {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    target_id: i32,
    #[serde(default)]
    client_states: usize,
    #[serde(default)]
    routes: usize,
    #[serde(default)]
    active_forwards: usize,
    #[serde(default)]
    converged: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct MembershipStatus {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    leader_id: Option<i32>,
    #[serde(default)]
    is_leader: bool,
    #[serde(default)]
    available: bool,
    #[serde(default)]
    started: bool,
    #[serde(default)]
    caught_up: bool,
    #[serde(default)]
    snapshot_restored: bool,
    #[serde(default)]
    draining: bool,
    #[serde(default)]
    applied_index: u64,
    #[serde(default)]
    committed_index: u64,
    #[serde(default)]
    last_index: u64,
    #[serde(default)]
    voters: Vec<i32>,
    #[serde(default)]
    learners: Vec<i32>,
    #[serde(default)]
    raft_peers: Vec<RaftPeerStatus>,
    #[serde(default)]
    business_peers: Vec<Value>,
    #[serde(default)]
    supports: Value,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RaftPeerStatus {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    available: bool,
    #[serde(default)]
    matched_index: u64,
    #[serde(default)]
    recent_active: bool,
    #[serde(default)]
    pending_snapshot: u64,
    #[serde(default)]
    progress_state: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    #[serde(default)]
    conf_change: Option<ConfChangeStatus>,
    #[serde(default)]
    operation: Option<OperationStatus>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ConfChangeStatus {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: String,
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    from_replicas: i32,
    #[serde(default)]
    to_replicas: i32,
    #[serde(default)]
    requested_replicas: i32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct OperationStatus {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: String,
    #[serde(default)]
    node_id: i32,
    #[serde(default)]
    from_replicas: i32,
    #[serde(default)]
    to_replicas: i32,
    #[serde(default)]
    requested_replicas: i32,
}

impl OperationStatus {
    fn to_conf_change(&self) -> ConfChangeStatus {
        ConfChangeStatus {
            kind: self.kind.clone(),
            node_id: self.node_id,
            from_replicas: self.from_replicas,
            to_replicas: self.to_replicas,
            requested_replicas: self.requested_replicas,
        }
    }
}

fn default_replicas() -> i32 {
    3
}

fn default_min_replicas() -> i32 {
    3
}

fn default_image() -> String {
    DEFAULT_IMAGE.to_string()
}

fn default_mqtt_port() -> i32 {
    1883
}

fn default_http_api_port() -> i32 {
    6060
}

fn default_grpc_port() -> i32 {
    5363
}

fn default_raft_port() -> i32 {
    6003
}

fn default_drain_timeout_seconds() -> u64 {
    300
}

#[tokio::main]
async fn main() -> Result<()> {
    let namespace = env::var("NAMESPACE").unwrap_or_else(|_| {
        std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| DEFAULT_NAMESPACE.to_string())
    });
    let interval =
        env::var("RECONCILE_INTERVAL_SECONDS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(5);

    let kube = KubeClient::in_cluster().await?;
    println!("rmqtt-operator started, namespace={namespace}, interval={interval}s");

    loop {
        if let Err(e) = reconcile_all(&kube, &namespace).await {
            eprintln!("reconcile error: {e:?}");
        }
        sleep(Duration::from_secs(interval)).await;
    }
}

async fn reconcile_all(kube: &KubeClient, namespace: &str) -> Result<()> {
    let path = format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}");
    let clusters: ObjectList<RmqttCluster> = kube.get_json(&path).await?;
    for cluster in clusters.items {
        if let Err(e) = reconcile_cluster(kube, namespace, cluster.clone()).await {
            eprintln!("cluster {} reconcile error: {e:?}", cluster.metadata.name);
            let _ = patch_status(
                kube,
                namespace,
                &cluster.metadata.name,
                json!({
                    "phase": "Degraded",
                    "message": e.to_string(),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await;
        }
    }
    Ok(())
}

async fn reconcile_cluster(kube: &KubeClient, namespace: &str, cluster: RmqttCluster) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    validate_spec(&cluster.spec)?;

    apply_config_map(kube, namespace, &cluster).await?;
    apply_services(kube, namespace, &cluster).await?;
    apply_pod_disruption_budget(kube, namespace, &cluster).await?;

    let stateful_set = get_stateful_set(kube, namespace, name).await?;
    let Some(stateful_set) = stateful_set else {
        apply_stateful_set(kube, namespace, &cluster, 1, true).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Bootstrapping",
                "message": "starting the single authorized raft bootstrap node",
                "replicas": 1,
                "readyReplicas": 0,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    };

    let current_replicas = stateful_set.replicas();
    ensure_unready_pods_not_serving(kube, namespace, name, current_replicas).await?;
    if stateful_set.bootstrap_allowed() {
        finish_controlled_bootstrap(kube, namespace, &cluster, current_replicas).await?;
        return Ok(());
    }

    if let Some(conf_change) = cluster.status.as_ref().and_then(|s| {
        s.conf_change.clone().or_else(|| s.operation.as_ref().map(OperationStatus::to_conf_change))
    }) {
        continue_conf_change(kube, namespace, &cluster, current_replicas, conf_change).await?;
        return Ok(());
    }

    if current_replicas < cluster.spec.replicas {
        let next_replicas = current_replicas + 1;
        if current_replicas > 0 {
            let membership = match leader_membership(kube, namespace, &cluster, current_replicas).await {
                Ok(membership) => membership,
                Err(e) => {
                    patch_status(
                        kube,
                        namespace,
                        name,
                        json!({
                            "phase": "WaitingRaftLeader",
                            "message": format!("no authoritative raft leader membership is available; refusing to start the next scale-out step: {e}"),
                            "replicas": current_replicas,
                            "desiredReplicas": cluster.spec.replicas,
                            "operation": null,
                            "confChange": null,
                            "observedGeneration": cluster.metadata.generation,
                        }),
                    )
                    .await?;
                    return Ok(());
                }
            };
            if !membership_ready_for_replicas(&membership, current_replicas) {
                patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "WaitingRaftMembers",
                        "message": format!("raft membership is not ready for current replicas {current_replicas}; refusing to start next confchange"),
                        "replicas": current_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "leaderId": membership.leader_id,
                        "raftStatus": membership_status_json(&membership, current_replicas),
                        "operation": null,
                        "confChange": null,
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                return Ok(());
            }
        }
        apply_stateful_set(kube, namespace, &cluster, next_replicas, false).await?;
        let conf_change = ConfChangeStatus {
            kind: "AddLearnerAndPromote".to_string(),
            node_id: next_replicas,
            from_replicas: current_replicas,
            to_replicas: next_replicas,
            requested_replicas: cluster.spec.replicas,
        };
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScalingOut",
                "message": format!(
                    "started node {next_replicas} as a non-voting learner; waiting for snapshot restore and log catch-up before promotion"
                ),
                "replicas": next_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "podStatus": {
                    "replicas": next_replicas,
                    "readyReplicas": ready_pods(kube, namespace, name).await?.len()
                },
                "operation": operation_status_json(&conf_change, "Preparing", "LearnerCatchUp"),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if current_replicas > cluster.spec.replicas {
        scale_in_one(kube, namespace, &cluster, current_replicas, current_replicas - 1).await?;
        return Ok(());
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let membership = match leader_membership(kube, namespace, &cluster, current_replicas).await {
        Ok(membership) => membership,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftLeader",
                    "message": format!("all pods are locally ready but no authoritative raft leader membership is available: {e}"),
                    "replicas": current_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": null,
                    "confChange": null,
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };
    if let Some(node_id) = node_requiring_membership_repair(&membership, current_replicas) {
        let conf_change = ConfChangeStatus {
            kind: "AddLearnerAndPromote".to_string(),
            node_id,
            from_replicas: current_replicas,
            to_replicas: current_replicas,
            requested_replicas: cluster.spec.replicas,
        };
        continue_add_learner_and_promote(kube, namespace, &cluster, current_replicas, &conf_change).await?;
        return Ok(());
    }

    if ready_replicas < current_replicas {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingPodsReady",
                "replicas": current_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let leader_id = membership.leader_id;
    let members = membership.business_peers.clone();
    let raft_ready = membership_ready_for_replicas(&membership, current_replicas);
    if raft_ready {
        ensure_ready_pods_serving(kube, namespace, name, current_replicas).await?;
    }
    let phase =
        if ready_replicas == cluster.spec.replicas && raft_ready { "Running" } else { "WaitingRaftMembers" };

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": phase,
            "replicas": current_replicas,
            "readyReplicas": ready_replicas,
            "leaderId": leader_id,
            "members": members,
            "podStatus": {
                "replicas": current_replicas,
                "readyReplicas": ready_replicas
            },
            "raftStatus": {
                "leaderId": leader_id,
                "membersReady": raft_ready
            },
            "drainingNodeId": null,
            "message": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

async fn finish_controlled_bootstrap(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    if current_replicas != 1 {
        bail!("controlled raft bootstrap must run with exactly one replica, found {current_replicas}");
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    if ready_replicas < 1 {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Bootstrapping",
                "message": "waiting for the authorized bootstrap node to become raft-ready",
                "replicas": 1,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let membership = match leader_membership(kube, namespace, cluster, 1).await {
        Ok(membership) => membership,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "Bootstrapping",
                    "message": format!("waiting for bootstrap membership: {e}"),
                    "replicas": 1,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": null,
                    "confChange": null,
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };

    if !membership_ready_for_replicas(&membership, 1) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "Bootstrapping",
                "message": "bootstrap node is running but its raft membership is not ready",
                "replicas": 1,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, 1),
                "operation": null,
                "confChange": null,
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_pod_serving(kube, namespace, &pod_name(name, 1)).await?;

    // Disable bootstrap in the StatefulSet template before creating any
    // additional pods. OnDelete keeps the healthy bootstrap pod running, while
    // every future pod creation or restart receives join-existing-only mode.
    apply_stateful_set(kube, namespace, cluster, 1, false).await?;
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "BootstrapReady",
            "message": "controlled raft bootstrap completed; future starts must join the existing cluster",
            "replicas": 1,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, 1),
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

fn validate_spec(spec: &ClusterSpec) -> Result<()> {
    if spec.replicas < spec.raft.min_replicas {
        bail!("spec.replicas {} must be >= raft.minReplicas {}", spec.replicas, spec.raft.min_replicas);
    }
    if spec.replicas < 1 {
        bail!("spec.replicas must be >= 1");
    }
    Ok(())
}

async fn continue_conf_change(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: ConfChangeStatus,
) -> Result<()> {
    match conf_change.kind.as_str() {
        "AddLearnerAndPromote" => {
            continue_add_learner_and_promote(kube, namespace, cluster, current_replicas, &conf_change).await
        }
        "AddVoterDirect" => {
            continue_add_voter_direct(kube, namespace, cluster, current_replicas, &conf_change).await
        }
        "RemoveVoter" => {
            if current_replicas <= conf_change.to_replicas {
                let ready_replicas = ready_pods(kube, namespace, &cluster.metadata.name).await?.len() as i32;
                let membership = leader_membership(kube, namespace, cluster, current_replicas).await?;
                let leader_id = membership.leader_id;
                let members = membership.business_peers.clone();
                let members_ready = membership_ready_for_replicas(&membership, current_replicas);
                if members_ready && ready_replicas == current_replicas {
                    ensure_ready_pods_serving(kube, namespace, &cluster.metadata.name, current_replicas)
                        .await?;
                }
                let phase = if current_replicas != cluster.spec.replicas {
                    "ScalingIn"
                } else if members_ready && ready_replicas == current_replicas {
                    "Running"
                } else {
                    "WaitingRaftMembers"
                };
                patch_status(
                    kube,
                    namespace,
                    &cluster.metadata.name,
                    json!({
                        "phase": phase,
                        "message": null,
                        "replicas": current_replicas,
                        "readyReplicas": ready_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "leaderId": leader_id,
                        "members": members,
                        "podStatus": {
                            "replicas": current_replicas,
                            "readyReplicas": ready_replicas
                        },
                        "raftStatus": {
                            "leaderId": leader_id,
                            "membersReady": members_ready,
                            "voters": membership.voters,
                            "learners": membership.learners,
                            "available": membership.available,
                            "started": membership.started
                        },
                        "drainingNodeId": null,
                        "operation": null,
                        "confChange": null,
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                Ok(())
            } else {
                scale_in_one(kube, namespace, cluster, current_replicas, conf_change.to_replicas).await
            }
        }
        other => bail!("unsupported confChange type {other}"),
    }
}

async fn continue_add_voter_direct(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let target_replicas = conf_change.to_replicas;

    if current_replicas < target_replicas {
        apply_stateful_set(kube, namespace, cluster, target_replicas, false).await?;
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let membership = match leader_membership(kube, namespace, cluster, target_replicas).await {
        Ok(membership) => membership,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftLeader",
                    "message": format!(
                        "node {} was created but no authoritative raft leader membership is available: {e}",
                        conf_change.node_id
                    ),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "podStatus": {
                        "replicas": target_replicas,
                        "readyReplicas": ready_replicas
                    },
                    "operation": operation_status_json(conf_change, "WaitingLeader", "DirectAddVoter"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };
    let leader_id = membership.leader_id;
    let members = membership.business_peers.clone();
    let raft_member_exists = voter_exists(&membership, conf_change.node_id);
    let raft_peer_is_available = raft_peer_available(&membership, conf_change.node_id);
    let business_member_exists = member_exists(&members, conf_change.node_id);
    let raft_ready = membership_ready_for_replicas(&membership, target_replicas);

    if !raft_ready {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingRaftMembers",
                "message": format!(
                    "leader membership has not confirmed node {}; voter={}, raftPeerAvailable={}, businessPeer={}",
                    conf_change.node_id, raft_member_exists, raft_peer_is_available, business_member_exists
                ),
                "replicas": target_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": leader_id,
                "members": members,
                "podStatus": {
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, target_replicas),
                "operation": operation_status_json(conf_change, "WaitingLeaderMembership", "DirectAddVoter"),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if ready_replicas < target_replicas {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingPodsReady",
                "message": format!(
                    "leader confirmed node {} membership; waiting for its local readiness before enabling client traffic",
                    conf_change.node_id
                ),
                "replicas": target_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": leader_id,
                "members": members,
                "podStatus": {
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas
                },
                "raftStatus": membership_status_json(&membership, target_replicas),
                "operation": operation_status_json(conf_change, "WaitingPodLocalReady", "DirectAddVoter"),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    ensure_ready_pods_serving(kube, namespace, name, target_replicas).await?;
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": if target_replicas == cluster.spec.replicas && raft_ready { "Running" } else { "ScalingOut" },
            "message": if target_replicas == cluster.spec.replicas {
                Value::Null
            } else {
                json!(format!("node {} joined; waiting to add next node toward desired replicas {}", conf_change.node_id, cluster.spec.replicas))
            },
            "replicas": target_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": leader_id,
            "members": members,
            "podStatus": {
                "replicas": target_replicas,
                "readyReplicas": ready_replicas
            },
            "raftStatus": {
                "leaderId": leader_id,
                "membersReady": raft_ready,
                "voters": membership.voters,
                "learners": membership.learners,
                "available": membership.available,
                "started": membership.started,
                "mode": "DirectAddVoter",
                "note": "legacy direct-voter operation recovered from an in-flight status created by an older operator"
            },
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

async fn continue_add_learner_and_promote(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let target_replicas = conf_change.to_replicas;
    let node_id = conf_change.node_id;

    if current_replicas < target_replicas {
        apply_stateful_set(kube, namespace, cluster, target_replicas, false).await?;
    }

    let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
    let membership = match leader_membership(kube, namespace, cluster, target_replicas).await {
        Ok(membership) => membership,
        Err(e) => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftLeader",
                    "message": format!("learner {node_id} is starting but no authoritative raft leader is available: {e}"),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "operation": operation_status_json(conf_change, "WaitingLeader", "LearnerCatchUp"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
    };

    let is_voter = voter_exists(&membership, node_id);
    let is_learner = membership.learners.contains(&node_id);
    let peer_available = raft_peer_available(&membership, node_id);
    let peer_caught_up = raft_peer_caught_up(&membership, node_id);
    let business_member = member_exists(&membership.business_peers, node_id);

    if !is_voter {
        if !is_learner || !peer_available || !peer_caught_up {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "WaitingRaftCatchUp",
                    "message": format!(
                        "waiting for learner {node_id} to catch up; learner={is_learner}, raftPeerAvailable={peer_available}, caughtUp={peer_caught_up}, leaderCommit={}",
                        membership.committed_index
                    ),
                    "replicas": target_replicas,
                    "readyReplicas": ready_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": membership.leader_id,
                    "raftStatus": membership_status_json(&membership, target_replicas),
                    "operation": operation_status_json(conf_change, "CatchingUp", "LearnerCatchUp"),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }

        let leader_id = membership.id;
        rmqtt_action_value_with_payload(
            namespace,
            cluster,
            leader_id,
            json!({"action": "promote_learner", "id": node_id}),
        )
        .await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "PromotingRaftLearner",
                "message": format!("learner {node_id} caught up through index {}; promotion to voter requested", membership.committed_index),
                "replicas": target_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, target_replicas),
                "operation": operation_status_json(conf_change, "Promoting", "LearnerCatchUp"),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    let raft_ready = membership_ready_for_replicas(&membership, target_replicas);
    if !peer_caught_up || !business_member || ready_replicas < target_replicas || !raft_ready {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "WaitingPodsReady",
                "message": format!(
                    "node {node_id} is a voter; waiting for local state-machine readiness and business NodeUp; caughtUp={peer_caught_up}, businessPeer={business_member}, readyReplicas={ready_replicas}/{target_replicas}"
                ),
                "replicas": target_replicas,
                "readyReplicas": ready_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "members": membership.business_peers.clone(),
                "raftStatus": membership_status_json(&membership, target_replicas),
                "operation": operation_status_json(conf_change, "WaitingBusinessReady", "LearnerCatchUp"),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    ensure_ready_pods_serving(kube, namespace, name, target_replicas).await?;
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": if target_replicas == cluster.spec.replicas { "Running" } else { "ScalingOut" },
            "message": if target_replicas == cluster.spec.replicas {
                Value::Null
            } else {
                json!(format!("node {node_id} safely promoted and admitted; continuing toward {} replicas", cluster.spec.replicas))
            },
            "replicas": target_replicas,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "members": membership.business_peers.clone(),
            "raftStatus": membership_status_json(&membership, target_replicas),
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

async fn scale_in_one(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    target_replicas: i32,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let desired = target_replicas;
    let victim_id = current_replicas;
    let victim_pod = pod_name(name, victim_id);
    let conf_change = ConfChangeStatus {
        kind: "RemoveVoter".to_string(),
        node_id: victim_id,
        from_replicas: current_replicas,
        to_replicas: desired,
        requested_replicas: cluster.spec.replicas,
    };
    let membership = leader_membership(kube, namespace, cluster, current_replicas).await?;
    if !membership_ready_for_replicas(&membership, current_replicas) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScaleInBlocked",
                "message": format!("raft membership is not ready for current replicas {current_replicas}; refusing to remove node {victim_id}"),
                "replicas": current_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "leaderId": membership.leader_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": operation_status_json(&conf_change, "Blocked", "RemoveVoter"),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScalingIn",
            "message": format!("draining node {victim_id}"),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, current_replicas),
            "operation": operation_status_json(&conf_change, "Draining", "RemoveVoter"),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;

    if !cluster.spec.raft.allow_leader_scale_in && membership.leader_id == Some(victim_id) {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScaleInBlocked",
                "message": format!("node {victim_id} is raft leader; leader scale-in is disabled"),
                "leaderId": victim_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": operation_status_json(&conf_change, "Blocked", "RemoveVoter"),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if membership.leader_id == Some(victim_id) {
        let transfer_target = membership
            .voters
            .iter()
            .copied()
            .filter(|id| *id != victim_id)
            .filter(|id| raft_peer_caught_up(&membership, *id))
            .min()
            .ok_or_else(|| anyhow!("no caught-up voter is available for leadership transfer"))?;
        rmqtt_action_value_with_payload(
            namespace,
            cluster,
            victim_id,
            json!({"action": "transfer_leader", "id": transfer_target}),
        )
        .await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "TransferringRaftLeader",
                "message": format!("requested leadership transfer from node {victim_id} to caught-up voter {transfer_target} before scale-in"),
                "replicas": current_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "drainingNodeId": victim_id,
                "leaderId": victim_id,
                "raftStatus": membership_status_json(&membership, current_replicas),
                "operation": operation_status_json(&conf_change, "TransferringLeader", "RemoveVoter"),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_pod_draining(kube, namespace, &victim_pod).await?;
    let _ = rmqtt_action_value(namespace, cluster, victim_id, "drain").await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(cluster.spec.raft.drain_timeout_seconds);
    let mut stable_rounds = 0u8;
    loop {
        let status = rmqtt_action::<DrainStatus>(namespace, cluster, victim_id, "drain_status").await?;
        let routing = routing_barrier_status(namespace, cluster, current_replicas, victim_id).await?;
        let local_ready = status.scale_in_safe_to_leave();
        let routing_ready = routing_barrier_ready(&routing);
        if local_ready && routing_ready {
            stable_rounds += 1;
        } else {
            stable_rounds = 0;
        }
        let blockers = drain_blockers(&status, &routing);
        if stable_rounds >= 2 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "ScaleInBlocked",
                    "message": format!(
                        "node {victim_id} drain timed out after {} seconds; {}",
                        cluster.spec.raft.drain_timeout_seconds,
                        blockers
                    ),
                    "drainingNodeId": victim_id,
                    "operation": operation_status_json(&conf_change, "Blocked", "RemoveVoter"),
                    "confChange": conf_change_status_json(&conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        sleep(Duration::from_secs(5)).await;
    }

    let leave = rmqtt_action_value(namespace, cluster, victim_id, "leave").await?;
    if leave.get("ok").and_then(Value::as_bool) != Some(true) {
        bail!("node {victim_id} leave returned unexpected response: {leave}");
    }

    wait_node_removed(kube, namespace, cluster, victim_id, desired).await?;
    apply_stateful_set(kube, namespace, cluster, desired, false).await?;

    let ready_replicas = wait_ready_pod_count_at_most(kube, namespace, name, desired).await?;
    let membership = leader_membership(kube, namespace, cluster, desired).await?;
    let leader_id = membership.leader_id;
    let members = membership.business_peers.clone();
    let raft_ready = membership_ready_for_replicas(&membership, desired);
    if raft_ready && ready_replicas == desired {
        ensure_ready_pods_serving(kube, namespace, name, desired).await?;
    }

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "Running",
            "message": if desired == cluster.spec.replicas {
                Value::Null
            } else {
                json!(format!("node {victim_id} drained and removed; waiting to remove next node toward desired replicas {}", cluster.spec.replicas))
            },
            "replicas": desired,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": leader_id,
            "members": members,
            "podStatus": {
                "replicas": desired,
                "readyReplicas": ready_replicas
            },
            "raftStatus": {
                "leaderId": leader_id,
                "membersReady": raft_ready,
                "voters": membership.voters,
                "learners": membership.learners,
                "available": membership.available,
                "started": membership.started
            },
            "drainingNodeId": null,
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    Ok(())
}

async fn wait_ready_pod_count_at_most(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    replicas: i32,
) -> Result<i32> {
    for _ in 0..30 {
        let ready_replicas = ready_pods(kube, namespace, name).await?.len() as i32;
        if ready_replicas <= replicas {
            return Ok(ready_replicas);
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(ready_pods(kube, namespace, name).await?.len() as i32)
}

async fn wait_node_removed(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    victim_id: i32,
    desired: i32,
) -> Result<()> {
    for _ in 0..60 {
        let membership = leader_membership(kube, namespace, cluster, desired).await?;
        if !voter_exists(&membership, victim_id)
            && !membership
                .business_peers
                .iter()
                .any(|n| n.get("id").and_then(Value::as_i64) == Some(i64::from(victim_id)))
        {
            return Ok(());
        }
        sleep(Duration::from_secs(2)).await;
    }
    bail!("node {victim_id} was not removed from raft voters and business peers before timeout");
}

async fn get_stateful_set(kube: &KubeClient, namespace: &str, name: &str) -> Result<Option<StatefulSet>> {
    let path = format!("/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}");
    match kube.get_json::<StatefulSet>(&path).await {
        Ok(sts) => Ok(Some(sts)),
        Err(e) if e.to_string().contains("404") => Ok(None),
        Err(e) => Err(e),
    }
}

async fn ready_pods(kube: &KubeClient, namespace: &str, name: &str) -> Result<Vec<Pod>> {
    Ok(cluster_pods(kube, namespace, name).await?.into_iter().filter(pod_is_ready).collect())
}

async fn cluster_pods(kube: &KubeClient, namespace: &str, name: &str) -> Result<Vec<Pod>> {
    let selector = format!("app.kubernetes.io/instance={name}");
    let path = format!("/api/v1/namespaces/{namespace}/pods?labelSelector={selector}");
    let pods: PodList = kube.get_json(&path).await?;
    Ok(pods.items)
}

fn pod_is_ready(pod: &Pod) -> bool {
    pod.status.phase.as_deref() == Some("Running")
        && pod.status.conditions.iter().any(|c| c.kind == "Ready" && c.status == "True")
}

fn is_authoritative_leader(membership: &MembershipStatus) -> bool {
    membership.id > 0 && membership.is_leader && membership.leader_id == Some(membership.id)
}

async fn leader_membership(
    _kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
) -> Result<MembershipStatus> {
    let mut observations = Vec::new();
    for id in 1..=replicas {
        match rmqtt_action::<MembershipStatus>(namespace, cluster, id, "membership").await {
            Ok(status) if is_authoritative_leader(&status) => return Ok(status),
            Ok(status) => observations.push(format!(
                "node {} reported id={}, leaderId={:?}, isLeader={}",
                id, status.id, status.leader_id, status.is_leader
            )),
            Err(e) => observations.push(format!("node {id} query failed: {e:#}")),
        }
    }
    bail!("no authoritative raft leader returned membership; {}", observations.join("; "));
}

async fn rmqtt_action<T>(namespace: &str, cluster: &RmqttCluster, node_id: i32, action: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let value = rmqtt_action_value(namespace, cluster, node_id, action).await?;
    serde_json::from_value(value).with_context(|| format!("decode rmqtt action {action} response"))
}

async fn rmqtt_action_value(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    action: &str,
) -> Result<Value> {
    rmqtt_action_value_with_payload(namespace, cluster, node_id, json!({ "action": action })).await
}

async fn rmqtt_action_value_with_payload(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    payload: Value,
) -> Result<Value> {
    let name = cluster.metadata.name.as_str();
    let http_port = cluster.spec.service.http_api_port;
    let url = format!(
        "http://{}-{}.{}-headless.{}.svc:{}/api/v1/plugins/{}/rmqtt-cluster-raft/send",
        name,
        node_id - 1,
        name,
        namespace,
        http_port,
        node_id
    );
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let res = client
        .post(url)
        .json(&payload)
        .send()
        .await
        .with_context(|| format!("send rmqtt action payload {payload} to node {node_id}"))?;
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("rmqtt action payload {payload} on node {node_id} failed: {status} {text}");
    }
    serde_json::from_str(&text)
        .with_context(|| format!("parse rmqtt action payload {payload} response: {text}"))
}

async fn routing_barrier_status(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    target_id: i32,
) -> Result<Vec<RoutingStatus>> {
    let mut statuses = Vec::with_capacity(replicas as usize);
    for node_id in 1..=replicas {
        let value = rmqtt_action_value_with_payload(
            namespace,
            cluster,
            node_id,
            json!({"action": "routing_status", "id": target_id}),
        )
        .await?;
        let status = serde_json::from_value(value)
            .with_context(|| format!("decode routing status from node {node_id} for target {target_id}"))?;
        statuses.push(status);
    }
    Ok(statuses)
}

fn routing_barrier_ready(statuses: &[RoutingStatus]) -> bool {
    !statuses.is_empty()
        && statuses.iter().all(|status| {
            status.converged && status.client_states == 0 && status.routes == 0 && status.active_forwards == 0
        })
}

fn drain_blockers(status: &DrainStatus, routing: &[RoutingStatus]) -> String {
    let mut blockers = status.blocking_reasons.clone();
    for item in routing.iter().filter(|item| {
        !item.converged || item.client_states > 0 || item.routes > 0 || item.active_forwards > 0
    }) {
        blockers.push(format!(
            "node {} still sees target {} clientStates={} routes={} activeForwards={}",
            item.id, item.target_id, item.client_states, item.routes, item.active_forwards
        ));
    }
    if blockers.is_empty() {
        "waiting for a second stable drain observation".to_string()
    } else {
        format!("blocking: {}", blockers.join(", "))
    }
}

async fn apply_config_map(kube: &KubeClient, namespace: &str, cluster: &RmqttCluster) -> Result<()> {
    let name = format!("{}-operator-scripts", cluster.metadata.name);
    let obj = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels(&cluster.metadata.name),
        },
        "data": {
            "render-rmqtt-config.sh": RENDER_SCRIPT,
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/configmaps/{name}"), &obj).await
}

async fn apply_services(kube: &KubeClient, namespace: &str, cluster: &RmqttCluster) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let headless = format!("{name}-headless");
    let labels = labels(name);
    let pod_selector = selector(name);

    let headless_obj = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": headless,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": true,
            "selector": pod_selector,
            "ports": service_ports(&cluster.spec.service),
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/services/{name}-headless"), &headless_obj).await?;

    let mut client_selector = selector(name);
    client_selector.insert("rmqtt.io/serving".to_string(), "true".to_string());
    let client_obj = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "selector": client_selector,
            "ports": [
                {"name": "mqtt", "port": cluster.spec.service.mqtt_port, "targetPort": "mqtt"},
                {"name": "http-api", "port": cluster.spec.service.http_api_port, "targetPort": "http-api"}
            ]
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/services/{name}"), &client_obj).await
}

async fn apply_pod_disruption_budget(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let obj = json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels(name),
        },
        "spec": {
            "maxUnavailable": 1,
            "selector": {"matchLabels": selector(name)},
        }
    });
    kube.apply(&format!("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets/{name}"), &obj).await
}

async fn apply_stateful_set(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    bootstrap_allowed: bool,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let labels = labels(name);
    let selector = selector(name);
    let scripts_name = format!("{name}-operator-scripts");
    let headless = format!("{name}-headless");
    let obj = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
            "annotations": {
                "rmqtt.io/raft-bootstrap-allowed": bootstrap_allowed.to_string()
            },
        },
        "spec": {
            "serviceName": headless,
            "replicas": replicas,
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "OnDelete"},
            "selector": {"matchLabels": selector},
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "rmqtt",
                        "app.kubernetes.io/instance": name,
                        // A new or recreated pod is excluded from the client
                        // Service until both local readiness and the leader's
                        // authoritative membership have been confirmed.
                        "rmqtt.io/serving": "false",
                        "rmqtt.io/draining": "false"
                    }
                },
                "spec": {
                    "containers": [{
                        "name": "rmqtt",
                        "image": cluster.spec.image,
                        // TODO: @wy 临时修改
                        "imagePullPolicy": "Always",
                        "command": ["/bin/sh", "-ec"],
                        "args": ["/operator-scripts/render-rmqtt-config.sh && cd /work && exec /app/rmqtt/rmqtt-bin/rmqttd"],
                        "env": [
                            {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                            {"name": "RMQTT_NAME", "value": name},
                            {"name": "RMQTT_HEADLESS_SERVICE", "value": format!("{name}-headless")},
                            {"name": "RMQTT_REPLICAS", "value": replicas.to_string()},
                            {"name": "RMQTT_MQTT_PORT", "value": cluster.spec.service.mqtt_port.to_string()},
                            {"name": "RMQTT_HTTP_API_PORT", "value": cluster.spec.service.http_api_port.to_string()},
                            {"name": "RMQTT_GRPC_PORT", "value": cluster.spec.service.grpc_port.to_string()},
                            {"name": "RMQTT_RAFT_PORT", "value": cluster.spec.service.raft_port.to_string()},
                            {"name": "RMQTT_RAFT_BOOTSTRAP", "value": bootstrap_allowed.to_string()}
                        ],
                        "ports": [
                            {"name": "mqtt", "containerPort": cluster.spec.service.mqtt_port},
                            {"name": "http-api", "containerPort": cluster.spec.service.http_api_port},
                            {"name": "grpc", "containerPort": cluster.spec.service.grpc_port},
                            {"name": "raft", "containerPort": cluster.spec.service.raft_port}
                        ],
                        "readinessProbe": {
                            "httpGet": {"path": "/api/v1/health/ready", "port": "http-api"},
                            "initialDelaySeconds": 5,
                            "periodSeconds": 5,
                            "timeoutSeconds": 3,
                            "failureThreshold": 12
                        },
                        "volumeMounts": [
                            {"name": "work", "mountPath": "/work"},
                            {"name": "operator-scripts", "mountPath": "/operator-scripts"}
                        ]
                    }],
                    "volumes": [
                        {"name": "work", "emptyDir": {}},
                        {"name": "operator-scripts", "configMap": {"name": scripts_name, "defaultMode": 493}}
                    ]
                }
            }
        }
    });
    kube.apply(&format!("/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}"), &obj).await
}

async fn patch_pod_draining(kube: &KubeClient, namespace: &str, pod: &str) -> Result<()> {
    let patch = json!({
        "metadata": {
            "labels": {
                "rmqtt.io/serving": "false",
                "rmqtt.io/draining": "true"
            }
        }
    });
    kube.merge_patch(&format!("/api/v1/namespaces/{namespace}/pods/{pod}"), &patch).await
}

async fn patch_pod_serving(kube: &KubeClient, namespace: &str, pod: &str) -> Result<()> {
    let patch = json!({
        "metadata": {
            "labels": {
                "rmqtt.io/serving": "true",
                "rmqtt.io/draining": "false"
            }
        }
    });
    kube.merge_patch(&format!("/api/v1/namespaces/{namespace}/pods/{pod}"), &patch).await
}

async fn ensure_ready_pods_serving(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    replicas: i32,
) -> Result<()> {
    let pods = cluster_pods(kube, namespace, name).await?;
    for node_id in 1..=replicas {
        let expected_name = pod_name(name, node_id);
        let Some(pod) = pods.iter().find(|pod| pod.metadata.name == expected_name) else {
            continue;
        };
        if pod_is_ready(pod)
            && pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str) != Some("true")
        {
            patch_pod_serving(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

async fn ensure_unready_pods_not_serving(
    kube: &KubeClient,
    namespace: &str,
    name: &str,
    replicas: i32,
) -> Result<()> {
    let pods = cluster_pods(kube, namespace, name).await?;
    for node_id in 1..=replicas {
        let expected_name = pod_name(name, node_id);
        let Some(pod) = pods.iter().find(|pod| pod.metadata.name == expected_name) else {
            continue;
        };
        if !pod_is_ready(pod)
            && pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str) == Some("true")
        {
            patch_pod_draining(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

async fn patch_status(kube: &KubeClient, namespace: &str, name: &str, status: Value) -> Result<()> {
    let patch = json!({ "status": status });
    kube.merge_patch(
        &format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{name}/status"),
        &patch,
    )
    .await
}

fn labels(name: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), "rmqtt".to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    labels.insert("app.kubernetes.io/managed-by".to_string(), "rmqtt-operator".to_string());
    labels
}

fn selector(name: &str) -> BTreeMap<String, String> {
    let mut selector = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".to_string(), "rmqtt".to_string());
    selector.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    selector
}

fn service_ports(spec: &ServiceSpec) -> Value {
    json!([
        {"name": "mqtt", "port": spec.mqtt_port, "targetPort": "mqtt"},
        {"name": "http-api", "port": spec.http_api_port, "targetPort": "http-api"},
        {"name": "grpc", "port": spec.grpc_port, "targetPort": "grpc"},
        {"name": "raft", "port": spec.raft_port, "targetPort": "raft"}
    ])
}

fn pod_name(cluster_name: &str, node_id: i32) -> String {
    format!("{}-{}", cluster_name, node_id - 1)
}

fn conf_change_status_json(conf_change: &ConfChangeStatus) -> Value {
    json!({
        "type": conf_change.kind,
        "nodeId": conf_change.node_id,
        "fromReplicas": conf_change.from_replicas,
        "toReplicas": conf_change.to_replicas,
        "requestedReplicas": conf_change.requested_replicas
    })
}

fn operation_status_json(conf_change: &ConfChangeStatus, phase: &str, mode: &str) -> Value {
    json!({
        "id": format!(
            "{}-{}-{}-{}",
            conf_change.kind, conf_change.node_id, conf_change.from_replicas, conf_change.to_replicas
        ),
        "type": conf_change.kind,
        "phase": phase,
        "nodeId": conf_change.node_id,
        "fromReplicas": conf_change.from_replicas,
        "toReplicas": conf_change.to_replicas,
        "requestedReplicas": conf_change.requested_replicas,
        "mode": mode
    })
}

fn membership_status_json(membership: &MembershipStatus, expected_replicas: i32) -> Value {
    json!({
        "id": membership.id,
        "leaderId": membership.leader_id,
        "isLeader": membership.is_leader,
        "membersReady": membership_ready_for_replicas(membership, expected_replicas),
        "available": membership.available,
        "started": membership.started,
        "caughtUp": membership.caught_up,
        "snapshotRestored": membership.snapshot_restored,
        "draining": membership.draining,
        "appliedIndex": membership.applied_index,
        "committedIndex": membership.committed_index,
        "lastIndex": membership.last_index,
        "voters": membership.voters,
        "learners": membership.learners,
        "raftPeers": membership.raft_peers.iter().map(|peer| json!({
            "id": peer.id,
            "available": peer.available,
            "matchedIndex": peer.matched_index,
            "recentActive": peer.recent_active,
            "pendingSnapshot": peer.pending_snapshot,
            "progressState": peer.progress_state,
        })).collect::<Vec<_>>(),
        "businessPeers": membership.business_peers,
        "supports": membership.supports,
    })
}

fn member_exists(members: &[Value], node_id: i32) -> bool {
    members.iter().any(|m| m.get("id").and_then(Value::as_i64) == Some(i64::from(node_id)))
}

fn contains_all_expected_members(members: &[Value], replicas: i32) -> bool {
    (1..=replicas).all(|node_id| member_exists(members, node_id))
}

fn voter_exists(membership: &MembershipStatus, node_id: i32) -> bool {
    membership.voters.contains(&node_id)
}

fn contains_all_expected_voters(membership: &MembershipStatus, replicas: i32) -> bool {
    (1..=replicas).all(|node_id| voter_exists(membership, node_id))
}

fn node_requiring_membership_repair(membership: &MembershipStatus, replicas: i32) -> Option<i32> {
    (1..=replicas)
        .find(|node_id| membership.learners.contains(node_id) || !voter_exists(membership, *node_id))
}

fn raft_peer_available(membership: &MembershipStatus, node_id: i32) -> bool {
    membership.raft_peers.iter().any(|peer| peer.id == node_id && peer.available)
}

fn raft_peer_caught_up(membership: &MembershipStatus, node_id: i32) -> bool {
    membership.raft_peers.iter().any(|peer| {
        peer.id == node_id
            && peer.available
            && peer.recent_active
            && peer.pending_snapshot == 0
            && peer.matched_index >= membership.committed_index
    })
}

fn contains_all_expected_available_raft_peers(membership: &MembershipStatus, replicas: i32) -> bool {
    (1..=replicas).all(|node_id| raft_peer_available(membership, node_id))
}

fn membership_ready_for_replicas(membership: &MembershipStatus, replicas: i32) -> bool {
    is_authoritative_leader(membership)
        && membership.started
        && membership.available
        && membership.caught_up
        && membership.snapshot_restored
        && !membership.draining
        && contains_all_expected_voters(membership, replicas)
        && contains_all_expected_available_raft_peers(membership, replicas)
        && contains_all_expected_members(&membership.business_peers, replicas)
}

struct KubeClient {
    client: Client,
    base_url: String,
    token: String,
}

impl KubeClient {
    async fn in_cluster() -> Result<Self> {
        let host = env::var("KUBERNETES_SERVICE_HOST").context("KUBERNETES_SERVICE_HOST is not set")?;
        let port = env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_string());
        let token = fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")
            .await
            .context("read serviceaccount token")?;
        let ca = fs::read("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
            .await
            .context("read serviceaccount ca.crt")?;
        let cert = reqwest::Certificate::from_pem(&ca).context("parse serviceaccount ca.crt")?;
        let client = Client::builder()
            .add_root_certificate(cert)
            .timeout(Duration::from_secs(20))
            .build()
            .context("build kubernetes http client")?;
        Ok(Self { client, base_url: format!("https://{host}:{port}"), token })
    }

    async fn get_json<T>(&self, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let res = self.request(Method::GET, path).send().await?;
        self.decode(res, path).await
    }

    async fn apply(&self, path: &str, obj: &Value) -> Result<()> {
        let path = format!("{path}?fieldManager={FIELD_MANAGER}&force=true");
        let res = self
            .request(Method::PATCH, &path)
            .header(CONTENT_TYPE, "application/apply-patch+yaml")
            .body(obj.to_string())
            .send()
            .await?;
        self.expect_success(res, &path).await
    }

    async fn merge_patch(&self, path: &str, patch: &Value) -> Result<()> {
        let res = self
            .request(Method::PATCH, path)
            .header(CONTENT_TYPE, "application/merge-patch+json")
            .body(patch.to_string())
            .send()
            .await?;
        self.expect_success(res, path).await
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        self.client
            .request(method, url)
            .header(AUTHORIZATION, format!("Bearer {}", self.token.trim()))
            .header(ACCEPT, "application/json")
    }

    async fn decode<T>(&self, res: reqwest::Response, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if status == StatusCode::NOT_FOUND {
            bail!("404 not found: {path}");
        }
        if !status.is_success() {
            bail!("kubernetes request failed: {path}: {status} {text}");
        }
        serde_json::from_str(&text).with_context(|| format!("decode kubernetes response from {path}: {text}"))
    }

    async fn expect_success(&self, res: reqwest::Response, path: &str) -> Result<()> {
        let status = res.status();
        if status.is_success() {
            return Ok(());
        }
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("kubernetes request failed: {path}: {status} {text}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateful_set_bootstrap_annotation_is_explicit() {
        let enabled: StatefulSet = serde_json::from_value(json!({
            "metadata": {"annotations": {"rmqtt.io/raft-bootstrap-allowed": "true"}},
            "spec": {"replicas": 1},
        }))
        .unwrap();
        assert_eq!(enabled.replicas(), 1);
        assert!(enabled.bootstrap_allowed());

        let disabled: StatefulSet = serde_json::from_value(json!({
            "metadata": {"annotations": {"rmqtt.io/raft-bootstrap-allowed": "false"}},
            "spec": {"replicas": 5},
        }))
        .unwrap();
        assert_eq!(disabled.replicas(), 5);
        assert!(!disabled.bootstrap_allowed());

        let legacy: StatefulSet = serde_json::from_value(json!({"spec": {"replicas": 3}})).unwrap();
        assert!(!legacy.bootstrap_allowed());
    }

    #[test]
    fn membership_readiness_requires_all_expected_nodes() {
        let membership = MembershipStatus {
            id: 1,
            leader_id: Some(1),
            is_leader: true,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            voters: vec![1, 2, 3],
            learners: vec![],
            raft_peers: vec![
                RaftPeerStatus { id: 1, available: true, ..Default::default() },
                RaftPeerStatus { id: 2, available: true, ..Default::default() },
                RaftPeerStatus { id: 3, available: true, ..Default::default() },
            ],
            business_peers: vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
            supports: Value::Null,
            ..Default::default()
        };
        assert!(membership_ready_for_replicas(&membership, 3));
        assert!(!membership_ready_for_replicas(&membership, 4));
    }

    #[test]
    fn membership_readiness_requires_authoritative_leader_and_available_peers() {
        let mut membership = MembershipStatus {
            id: 2,
            leader_id: Some(1),
            is_leader: false,
            available: true,
            started: true,
            caught_up: true,
            snapshot_restored: true,
            voters: vec![1, 2, 3],
            learners: vec![],
            raft_peers: vec![
                RaftPeerStatus { id: 1, available: true, ..Default::default() },
                RaftPeerStatus { id: 2, available: true, ..Default::default() },
                RaftPeerStatus { id: 3, available: true, ..Default::default() },
            ],
            business_peers: vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
            supports: Value::Null,
            ..Default::default()
        };
        assert!(!membership_ready_for_replicas(&membership, 3));

        membership.id = 1;
        membership.is_leader = true;
        membership.raft_peers[2].available = false;
        assert!(!membership_ready_for_replicas(&membership, 3));
    }

    #[test]
    fn membership_repair_detects_restarted_learner_without_replica_change() {
        let membership = MembershipStatus { voters: vec![1, 2], learners: vec![3], ..Default::default() };
        assert_eq!(node_requiring_membership_repair(&membership, 3), Some(3));

        let repaired = MembershipStatus { voters: vec![1, 2, 3], ..Default::default() };
        assert_eq!(node_requiring_membership_repair(&repaired, 3), None);
    }

    #[test]
    fn raft_peer_progress_uses_membership_camel_case_fields() {
        let peer: RaftPeerStatus = serde_json::from_value(json!({
            "id": 3,
            "available": true,
            "matchedIndex": 42,
            "recentActive": true,
            "pendingSnapshot": 0,
            "progressState": "Replicate"
        }))
        .unwrap();
        assert_eq!(peer.matched_index, 42);
        assert!(peer.recent_active);
        assert_eq!(peer.progress_state, "Replicate");
    }

    #[test]
    fn learner_catch_up_rejects_pending_snapshot_or_stale_match() {
        let mut membership = MembershipStatus {
            committed_index: 42,
            raft_peers: vec![RaftPeerStatus {
                id: 3,
                available: true,
                matched_index: 42,
                recent_active: true,
                pending_snapshot: 0,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(raft_peer_caught_up(&membership, 3));

        membership.raft_peers[0].pending_snapshot = 40;
        assert!(!raft_peer_caught_up(&membership, 3));
        membership.raft_peers[0].pending_snapshot = 0;
        membership.raft_peers[0].matched_index = 41;
        assert!(!raft_peer_caught_up(&membership, 3));
    }

    #[test]
    fn drain_requires_session_handoff_when_plugin_reports_it() {
        let migrated = DrainStatus {
            safe_to_leave: true,
            strict_safe_to_leave: Some(true),
            session_handoff_complete: Some(true),
            ..Default::default()
        };
        assert!(migrated.scale_in_safe_to_leave());

        let pending = DrainStatus {
            safe_to_leave: true,
            strict_safe_to_leave: Some(true),
            session_handoff_complete: Some(false),
            ..Default::default()
        };
        assert!(!pending.scale_in_safe_to_leave());
    }

    #[test]
    fn routing_barrier_requires_every_raft_node_to_drop_target_routes() {
        let ready = RoutingStatus { id: 1, target_id: 5, converged: true, ..Default::default() };
        let stale = RoutingStatus {
            id: 2,
            target_id: 5,
            client_states: 1,
            routes: 2,
            converged: false,
            ..Default::default()
        };
        let forwarding =
            RoutingStatus { id: 3, target_id: 5, active_forwards: 1, converged: false, ..Default::default() };

        assert!(routing_barrier_ready(&[
            RoutingStatus { id: 1, target_id: 5, converged: true, ..Default::default() },
            RoutingStatus { id: 2, target_id: 5, converged: true, ..Default::default() },
        ]));
        assert!(!routing_barrier_ready(&[ready, stale]));
        assert!(!routing_barrier_ready(&[forwarding]));
        assert!(!routing_barrier_ready(&[]));
    }
}

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::fs;
use tokio::time::sleep;

const GROUP: &str = "rmqtt.io";
const VERSION: &str = "v1alpha1";
const PLURAL: &str = "rmqttclusters";
const FIELD_MANAGER: &str = "rmqtt-operator";
const DEFAULT_NAMESPACE: &str = "rmqtt-test";
const DEFAULT_LEADER_ELECTION_LEASE_NAME: &str = "rmqtt-operator-leader";
const DEFAULT_LEASE_DURATION_SECONDS: i64 = 30;
const DEFAULT_IMAGE: &str =
    "registry.cn-sh-02.sensecore.cn/ccr-ams-public/mirror/docker/rmqtt-test/rmqtt:latest";
const OPERATOR_BUILD_ID: &str = match option_env!("RMQTT_OPERATOR_BUILD_ID") {
    Some(build_id) => build_id,
    None => "dev",
};

static LAST_LOGGED_STATUS: OnceLock<Mutex<HashMap<String, Value>>> = OnceLock::new();

const RENDER_SCRIPT: &str = r###"#!/bin/sh
set -eu

WORK_DIR="${RMQTT_WORK_DIR:-/work}"
SOURCE_DIR="${RMQTT_SOURCE_DIR:-/app/rmqtt}"
BASE_CONFIG_DIR="${RMQTT_BASE_CONFIG_DIR:-}"
POD_NAME="${HOSTNAME}"
ORDINAL="${POD_NAME##*-}"
NODE_ID="$((ORDINAL + 1))"
REPLICAS="${RMQTT_REPLICAS:-3}"
HEADLESS="${RMQTT_HEADLESS_SERVICE}"
NAMESPACE="${POD_NAMESPACE}"
MQTT_PORT="${RMQTT_MQTT_PORT:-1883}"
MQTTS_PORT="${RMQTT_MQTTS_PORT:-8883}"
WS_PORT="${RMQTT_WS_PORT:-8080}"
WSS_PORT="${RMQTT_WSS_PORT:-8083}"
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
if [ -n "${BASE_CONFIG_DIR}" ]; then
  if [ -f "${BASE_CONFIG_DIR}/rmqtt.toml" ]; then
    cp "${BASE_CONFIG_DIR}/rmqtt.toml" "${WORK_DIR}/rmqtt.toml"
  fi
  for config_file in "${BASE_CONFIG_DIR}"/rmqtt-*.toml; do
    [ -f "${config_file}" ] || continue
    cp "${config_file}" "${WORK_DIR}/rmqtt-plugins/"
  done
fi

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
sed -i 's#^plugins.dir = .*#plugins.dir = "rmqtt-plugins/"#' "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.tcp.external.addr = .*#listener.tcp.external.addr = \"0.0.0.0:${MQTT_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.tls.external.addr = .*#listener.tls.external.addr = \"0.0.0.0:${MQTTS_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.ws.external.addr = .*#listener.ws.external.addr = \"0.0.0.0:${WS_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i "s#^listener.wss.external.addr = .*#listener.wss.external.addr = \"0.0.0.0:${WSS_PORT}\"#" "${WORK_DIR}/rmqtt.toml"
sed -i 's/#"rmqtt-cluster-raft"/"rmqtt-cluster-raft"/' "${WORK_DIR}/rmqtt.toml"

sed -i "s#^http_laddr = .*#http_laddr = \"0.0.0.0:${HTTP_PORT}\"#" "${WORK_DIR}/rmqtt-plugins/rmqtt-http-api.toml"
sed -i "s#^node_grpc_addrs = .*#node_grpc_addrs = [${grpc_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
sed -i "s#^raft_peer_addrs = .*#raft_peer_addrs = [${raft_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-raft.toml"
if [ -f "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-broadcast.toml" ]; then
  sed -i "s#^node_grpc_addrs = .*#node_grpc_addrs = [${grpc_peers}]#" "${WORK_DIR}/rmqtt-plugins/rmqtt-cluster-broadcast.toml"
fi
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
    #[serde(default = "default_image_pull_policy")]
    image_pull_policy: String,
    #[serde(default)]
    service: ServiceSpec,
    #[serde(default)]
    raft: RaftSpec,
    #[serde(default)]
    config: ConfigSpec,
    #[serde(default)]
    resources: Option<ResourceRequirementsSpec>,
    #[serde(default)]
    pod: BrokerPodSpec,
    #[serde(default)]
    monitoring: MonitoringSpec,
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            image: default_image(),
            image_pull_policy: default_image_pull_policy(),
            service: ServiceSpec::default(),
            raft: RaftSpec::default(),
            config: ConfigSpec::default(),
            resources: None,
            pod: BrokerPodSpec::default(),
            monitoring: MonitoringSpec::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ServiceSpec {
    #[serde(default = "default_mqtt_port")]
    mqtt_port: i32,
    #[serde(default = "default_mqtts_port")]
    mqtts_port: i32,
    #[serde(default = "default_ws_port")]
    ws_port: i32,
    #[serde(default = "default_wss_port")]
    wss_port: i32,
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
            mqtts_port: default_mqtts_port(),
            ws_port: default_ws_port(),
            wss_port: default_wss_port(),
            http_api_port: default_http_api_port(),
            grpc_port: default_grpc_port(),
            raft_port: default_raft_port(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ConfigSpec {
    #[serde(default)]
    base_config_map_name: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct ResourceRequirementsSpec {
    #[serde(default)]
    requests: ResourceListSpec,
    #[serde(default)]
    limits: ResourceListSpec,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct ResourceListSpec {
    #[serde(default)]
    cpu: Option<String>,
    #[serde(default)]
    memory: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct BrokerPodSpec {
    #[serde(default)]
    annotations: BTreeMap<String, String>,
    #[serde(default)]
    service_account_name: Option<String>,
    #[serde(default)]
    image_pull_secrets: Vec<NamedReference>,
    #[serde(default)]
    scheduler_name: Option<String>,
    #[serde(default)]
    affinity: Option<Value>,
    #[serde(default)]
    tolerations: Vec<Value>,
    #[serde(default)]
    enable_service_links: Option<bool>,
    #[serde(default)]
    termination_grace_period_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct NamedReference {
    name: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct MonitoringSpec {
    #[serde(default)]
    service_monitor: ServiceMonitorSpec,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ServiceMonitorSpec {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default = "default_metrics_path")]
    path: String,
    #[serde(default = "default_metrics_interval")]
    interval: String,
    #[serde(default = "default_metrics_scrape_timeout")]
    scrape_timeout: String,
}

impl Default for ServiceMonitorSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            labels: BTreeMap::new(),
            path: default_metrics_path(),
            interval: default_metrics_interval(),
            scrape_timeout: default_metrics_scrape_timeout(),
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
    #[serde(rename = "deletionTimestamp", default)]
    deletion_timestamp: Option<String>,
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

    fn rmqtt_container(&self) -> Option<&StatefulSetContainerSpec> {
        self.spec.template.spec.containers.iter().find(|container| container.name == "rmqtt")
    }

    fn broker_template_matches(&self, spec: &ClusterSpec) -> bool {
        self.rmqtt_container().is_some_and(|container| {
            container.image == spec.image && container.image_pull_policy == spec.image_pull_policy
        })
    }

    fn bootstrap_allowed(&self) -> bool {
        self.metadata.annotations.get("rmqtt.io/raft-bootstrap-allowed").is_some_and(|value| value == "true")
    }
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetSpec {
    #[serde(default)]
    replicas: Option<i32>,
    #[serde(default)]
    template: StatefulSetPodTemplateSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetPodTemplateSpec {
    #[serde(default)]
    spec: StatefulSetPodSpec,
}

#[derive(Debug, Deserialize, Default)]
struct StatefulSetPodSpec {
    #[serde(default)]
    containers: Vec<StatefulSetContainerSpec>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StatefulSetContainerSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    image_pull_policy: String,
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
    #[serde(default)]
    data_loss_detected: bool,
}

impl DrainStatus {
    fn scale_in_safe_to_leave(&self) -> bool {
        !self.data_loss_detected
            && self.strict_safe_to_leave.unwrap_or(self.safe_to_leave)
            && self.session_handoff_complete.unwrap_or(true)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RmqttActionErrorKind {
    Timeout,
    Connection,
    HttpStatus,
    InvalidResponse,
}

#[derive(Debug)]
struct RmqttActionError {
    kind: RmqttActionErrorKind,
    node_id: i32,
    payload: Value,
    detail: String,
}

impl RmqttActionError {
    fn new(kind: RmqttActionErrorKind, node_id: i32, payload: &Value, detail: impl Into<String>) -> Self {
        Self { kind, node_id, payload: payload.clone(), detail: detail.into() }
    }

    fn is_transient(&self) -> bool {
        matches!(self.kind, RmqttActionErrorKind::Timeout | RmqttActionErrorKind::Connection)
    }
}

impl fmt::Display for RmqttActionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rmqtt action {:?} payload {} on node {} failed: {}",
            self.kind, self.payload, self.node_id, self.detail
        )
    }
}

impl std::error::Error for RmqttActionError {}

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

#[derive(Debug, Deserialize, Clone, Default)]
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

#[derive(Debug, Deserialize, Clone, Default)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VictimPodState {
    Present,
    Terminating,
    Absent,
}

impl fmt::Display for VictimPodState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Present => f.write_str("Present"),
            Self::Terminating => f.write_str("Terminating"),
            Self::Absent => f.write_str("Absent"),
        }
    }
}

#[derive(Debug)]
struct ScaleInObservation {
    pod_state: VictimPodState,
    stateful_set_replicas: i32,
    raft_member: bool,
    business_member: bool,
    membership: MembershipStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScaleInRecoveryDecision {
    RetryTargetRequest,
    RemoveStaleRaftMember,
    RemoveStaleBusinessMember,
    FinishScaleIn,
}

fn scale_in_recovery_decision(
    pod_state: VictimPodState,
    raft_member: bool,
    business_member: bool,
) -> ScaleInRecoveryDecision {
    if !raft_member && !business_member {
        return ScaleInRecoveryDecision::FinishScaleIn;
    }
    if pod_state == VictimPodState::Present {
        return ScaleInRecoveryDecision::RetryTargetRequest;
    }
    if raft_member {
        ScaleInRecoveryDecision::RemoveStaleRaftMember
    } else {
        ScaleInRecoveryDecision::RemoveStaleBusinessMember
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    #[serde(default)]
    phase: String,
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
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    retry_count: u32,
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

fn default_image_pull_policy() -> String {
    "IfNotPresent".to_string()
}

fn default_mqtt_port() -> i32 {
    1883
}

fn default_mqtts_port() -> i32 {
    8883
}

fn default_ws_port() -> i32 {
    8080
}

fn default_wss_port() -> i32 {
    8083
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

fn default_metrics_path() -> String {
    "/api/v1/metrics/prometheus/sum".to_string()
}

fn default_metrics_interval() -> String {
    "60s".to_string()
}

fn default_metrics_scrape_timeout() -> String {
    "10s".to_string()
}

fn lease_can_be_acquired(
    lease: &Value,
    candidate: &str,
    now: DateTime<Utc>,
    default_duration_seconds: i64,
) -> bool {
    let holder = lease.pointer("/spec/holderIdentity").and_then(Value::as_str);
    if holder.is_none() || holder == Some(candidate) {
        return true;
    }
    let duration_seconds = lease
        .pointer("/spec/leaseDurationSeconds")
        .and_then(Value::as_i64)
        .unwrap_or(default_duration_seconds);
    let Some(renew_time) = lease.pointer("/spec/renewTime").and_then(Value::as_str) else {
        return false;
    };
    let Ok(renew_time) = DateTime::parse_from_rfc3339(renew_time) else {
        return false;
    };
    now.signed_duration_since(renew_time.with_timezone(&Utc)).num_seconds() > duration_seconds
}

async fn try_acquire_operator_lease(
    kube: &KubeClient,
    namespace: &str,
    lease_name: &str,
    holder_identity: &str,
    lease_duration_seconds: i64,
) -> Result<bool> {
    let collection_path = format!("/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases");
    let resource_path = format!("{collection_path}/{lease_name}");
    let now = Utc::now();
    let now_text = kubernetes_microtime(now);
    let Some(existing) = kube.get_json_optional::<Value>(&resource_path).await? else {
        let lease = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": lease_name,
                "namespace": namespace
            },
            "spec": {
                "holderIdentity": holder_identity,
                "leaseDurationSeconds": lease_duration_seconds,
                "acquireTime": now_text,
                "renewTime": now_text,
                "leaseTransitions": 0
            }
        });
        return kube.create_json(&collection_path, &lease).await;
    };

    if !lease_can_be_acquired(&existing, holder_identity, now, lease_duration_seconds) {
        return Ok(false);
    }
    let resource_version = existing
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("leader Lease {namespace}/{lease_name} has no metadata.resourceVersion"))?;
    let previous_holder = existing.pointer("/spec/holderIdentity").and_then(Value::as_str);
    let changed_holder = previous_holder != Some(holder_identity);
    let previous_transitions =
        existing.pointer("/spec/leaseTransitions").and_then(Value::as_i64).unwrap_or(0);
    let acquire_time = if changed_holder {
        json!(now_text)
    } else {
        existing.pointer("/spec/acquireTime").cloned().unwrap_or_else(|| json!(now_text))
    };
    let lease = json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": lease_name,
            "namespace": namespace,
            "resourceVersion": resource_version
        },
        "spec": {
            "holderIdentity": holder_identity,
            "leaseDurationSeconds": lease_duration_seconds,
            "acquireTime": acquire_time,
            "renewTime": now_text,
            "leaseTransitions": previous_transitions + i64::from(changed_holder)
        }
    });
    kube.replace_json(&resource_path, &lease).await
}

async fn reconcile_while_renewing_lease(
    kube: &KubeClient,
    namespace: &str,
    lease_name: &str,
    holder_identity: &str,
    lease_duration_seconds: i64,
) -> Result<bool> {
    let renew_interval_seconds = (lease_duration_seconds / 3).max(1) as u64;
    let reconcile = reconcile_all(kube, namespace);
    tokio::pin!(reconcile);
    loop {
        tokio::select! {
            result = &mut reconcile => {
                result?;
                return Ok(true);
            }
            _ = sleep(Duration::from_secs(renew_interval_seconds)) => {
                if !try_acquire_operator_lease(
                    kube,
                    namespace,
                    lease_name,
                    holder_identity,
                    lease_duration_seconds,
                )
                .await?
                {
                    return Ok(false);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    if env::args_os().skip(1).any(|arg| arg == "--version" || arg == "-V") {
        println!("rmqtt-operator/{}-{OPERATOR_BUILD_ID}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let namespace = env::var("NAMESPACE").unwrap_or_else(|_| {
        std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| DEFAULT_NAMESPACE.to_string())
    });
    let interval =
        env::var("RECONCILE_INTERVAL_SECONDS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(5);
    let lease_name = env::var("LEADER_ELECTION_LEASE_NAME")
        .unwrap_or_else(|_| DEFAULT_LEADER_ELECTION_LEASE_NAME.to_string());
    let lease_duration_seconds = env::var("LEADER_ELECTION_LEASE_DURATION_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 10)
        .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);
    let holder_identity = env::var("POD_NAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("rmqtt-operator-{}", std::process::id()));

    let kube = KubeClient::in_cluster().await?;
    log::info!(
        "operator started namespace={} reconcile_interval_seconds={} leader_election_lease={} lease_duration_seconds={} holder_identity={}",
        namespace,
        interval,
        lease_name,
        lease_duration_seconds,
        holder_identity
    );

    let mut held_lease = false;
    loop {
        match try_acquire_operator_lease(
            &kube,
            &namespace,
            &lease_name,
            &holder_identity,
            lease_duration_seconds,
        )
        .await
        {
            Ok(true) => {
                if !held_lease {
                    log::info!(
                        "operator acquired leader lease namespace={} lease={} holder_identity={}",
                        namespace,
                        lease_name,
                        holder_identity
                    );
                }
                held_lease = true;
                match reconcile_while_renewing_lease(
                    &kube,
                    &namespace,
                    &lease_name,
                    &holder_identity,
                    lease_duration_seconds,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        log::warn!(
                            "operator lost leader lease during reconciliation namespace={} lease={} holder_identity={}; in-flight reconciliation cancelled",
                            namespace,
                            lease_name,
                            holder_identity
                        );
                        held_lease = false;
                    }
                    Err(error) => {
                        log::error!(
                            "reconcile or leader lease renewal failed namespace={} lease={} error={:#}",
                            namespace,
                            lease_name,
                            error
                        );
                        held_lease = false;
                    }
                }
            }
            Ok(false) => {
                if held_lease {
                    log::warn!(
                        "operator lost leader lease namespace={} lease={} holder_identity={}; reconciliation paused",
                        namespace,
                        lease_name,
                        holder_identity
                    );
                }
                held_lease = false;
            }
            Err(error) => {
                log::warn!(
                    "operator leader lease request failed namespace={} lease={} holder_identity={} error={:#}; reconciliation paused",
                    namespace,
                    lease_name,
                    holder_identity,
                    error
                );
                held_lease = false;
            }
        }
        sleep(Duration::from_secs(interval)).await;
    }
}

async fn reconcile_all(kube: &KubeClient, namespace: &str) -> Result<()> {
    let path = format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}");
    let clusters: ObjectList<RmqttCluster> = kube.get_json(&path).await?;
    for cluster in clusters.items {
        if let Err(e) = reconcile_cluster(kube, namespace, cluster.clone()).await {
            log::error!(
                "cluster reconcile failed namespace={} cluster={} generation={:?} error={e:?}",
                namespace,
                cluster.metadata.name,
                cluster.metadata.generation
            );
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
    log::debug!(
        "reconciling cluster namespace={} cluster={} generation={:?} desired_replicas={}",
        namespace,
        name,
        cluster.metadata.generation,
        cluster.spec.replicas
    );
    validate_spec(&cluster.spec)?;
    ensure_base_config_map_exists(kube, namespace, &cluster).await?;

    apply_broker_service_account(kube, namespace, &cluster).await?;
    apply_config_map(kube, namespace, &cluster).await?;
    apply_services(kube, namespace, &cluster).await?;
    apply_pod_disruption_budget(kube, namespace, &cluster).await?;
    apply_service_monitor(kube, namespace, &cluster).await?;

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
    let planned_draining_node_id = cluster
        .status
        .as_ref()
        .and_then(|status| {
            status
                .conf_change
                .clone()
                .or_else(|| status.operation.as_ref().map(OperationStatus::to_conf_change))
        })
        .filter(|change| change.kind == "RemoveVoter")
        .map(|change| change.node_id);
    ensure_unready_pods_not_serving(kube, namespace, name, current_replicas, planned_draining_node_id)
        .await?;
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

    if !stateful_set.broker_template_matches(&cluster.spec) {
        apply_stateful_set(kube, namespace, &cluster, current_replicas, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "UpdatingStatefulSetTemplate",
                "message": "StatefulSet pod template was updated from RmqttCluster spec; updateStrategy=OnDelete requires existing pods to be recreated before they use the new template",
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
    if !matches!(spec.image_pull_policy.as_str(), "Always" | "IfNotPresent" | "Never") {
        bail!(
            "spec.imagePullPolicy must be one of Always, IfNotPresent or Never, got {}",
            spec.image_pull_policy
        );
    }
    for (name, port) in [
        ("mqttPort", spec.service.mqtt_port),
        ("mqttsPort", spec.service.mqtts_port),
        ("wsPort", spec.service.ws_port),
        ("wssPort", spec.service.wss_port),
        ("httpApiPort", spec.service.http_api_port),
        ("grpcPort", spec.service.grpc_port),
        ("raftPort", spec.service.raft_port),
    ] {
        if !(1..=65535).contains(&port) {
            bail!("spec.service.{name} must be between 1 and 65535, got {port}");
        }
    }
    Ok(())
}

async fn ensure_base_config_map_exists(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<()> {
    let Some(config_map_name) = cluster.spec.config.base_config_map_name.as_deref() else {
        return Ok(());
    };
    kube.get_json::<Value>(&format!("/api/v1/namespaces/{namespace}/configmaps/{config_map_name}"))
        .await
        .with_context(|| {
            format!(
                "spec.config.baseConfigMapName references missing or unreadable ConfigMap {namespace}/{config_map_name}"
            )
        })?;
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
            // The desired replica count may be restored while a drain is
            // blocked. Cancel the in-flight removal instead of continuing to
            // drain a node that is needed again.
            if current_replicas > conf_change.to_replicas && cluster.spec.replicas >= current_replicas {
                let victim_id = conf_change.node_id;
                let victim_pod = pod_name(&cluster.metadata.name, victim_id);
                rmqtt_action_value(namespace, cluster, victim_id, "undrain").await?;
                patch_pod_serving(kube, namespace, &victim_pod).await?;
                patch_status(
                    kube,
                    namespace,
                    &cluster.metadata.name,
                    json!({
                        "phase": "ScaleInCancelled",
                        "message": format!(
                            "cancelled removal of node {victim_id} because desired replicas changed to {}",
                            cluster.spec.replicas
                        ),
                        "replicas": current_replicas,
                        "readyReplicas": ready_pods(kube, namespace, &cluster.metadata.name).await?.len(),
                        "desiredReplicas": cluster.spec.replicas,
                        "drainingNodeId": null,
                        "operation": null,
                        "confChange": null,
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
                return Ok(());
            }
            if current_replicas <= conf_change.to_replicas {
                let ready_replicas = ready_pods(kube, namespace, &cluster.metadata.name).await?.len() as i32;
                let membership = leader_membership(kube, namespace, cluster, current_replicas).await?;
                let leader_id = membership.leader_id;
                let victim_id = conf_change.node_id;
                if voter_exists(&membership, victim_id) || membership.learners.contains(&victim_id) {
                    let leader_id =
                        leader_id.ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
                    rmqtt_action_value_with_payload(
                        namespace,
                        cluster,
                        leader_id,
                        json!({"action": "remove_voter", "id": victim_id}),
                    )
                    .await?;
                    patch_status(
                        kube,
                        namespace,
                        &cluster.metadata.name,
                        json!({
                            "phase": "RecoveringScaleIn",
                            "message": format!(
                                "pod for node {victim_id} is already absent; requested authoritative leader {leader_id} to remove its stale raft voter/learner membership"
                            ),
                            "replicas": current_replicas,
                            "readyReplicas": ready_replicas,
                            "desiredReplicas": cluster.spec.replicas,
                            "leaderId": leader_id,
                            "raftStatus": membership_status_json(&membership, current_replicas),
                            "operation": operation_status_json(&conf_change, "CleaningStaleRaftMember", "RemoveVoter"),
                            "confChange": conf_change_status_json(&conf_change),
                            "observedGeneration": cluster.metadata.generation,
                        }),
                    )
                    .await?;
                    return Ok(());
                }
                if member_exists(&membership.business_peers, victim_id) {
                    let leader_id =
                        leader_id.ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
                    rmqtt_action_value_with_payload(
                        namespace,
                        cluster,
                        leader_id,
                        json!({"action": "force_node_remove", "id": victim_id}),
                    )
                    .await?;
                    patch_status(
                        kube,
                        namespace,
                        &cluster.metadata.name,
                        json!({
                            "phase": "RecoveringScaleIn",
                            "message": format!(
                                "pod and raft membership for node {victim_id} are absent; requested cleanup of its stale business routing metadata"
                            ),
                            "replicas": current_replicas,
                            "readyReplicas": ready_replicas,
                            "desiredReplicas": cluster.spec.replicas,
                            "leaderId": leader_id,
                            "raftStatus": membership_status_json(&membership, current_replicas),
                            "operation": operation_status_json(&conf_change, "CleaningStaleBusinessMember", "RemoveVoter"),
                            "confChange": conf_change_status_json(&conf_change),
                            "observedGeneration": cluster.metadata.generation,
                        }),
                    )
                    .await?;
                    return Ok(());
                }
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

fn has_matching_scale_in_operation(cluster: &RmqttCluster, conf_change: &ConfChangeStatus) -> bool {
    cluster.status.as_ref().is_some_and(|status| {
        status.conf_change.as_ref().is_some_and(|existing| {
            existing.kind == conf_change.kind
                && existing.node_id == conf_change.node_id
                && existing.to_replicas == conf_change.to_replicas
        }) || status.operation.as_ref().is_some_and(|existing| {
            existing.kind == conf_change.kind
                && existing.node_id == conf_change.node_id
                && existing.to_replicas == conf_change.to_replicas
        })
    })
}

async fn finish_scale_in_step(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    conf_change: &ConfChangeStatus,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let desired = conf_change.to_replicas;
    let victim_id = conf_change.node_id;
    let stateful_set_replicas = get_stateful_set(kube, namespace, name)
        .await?
        .map(|stateful_set| stateful_set.replicas())
        .unwrap_or(0);
    if stateful_set_replicas > desired {
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScalingIn",
                "message": format!(
                    "node {victim_id} is absent from raft and business membership; scaling StatefulSet from {stateful_set_replicas} to {desired}"
                ),
                "replicas": stateful_set_replicas,
                "desiredReplicas": cluster.spec.replicas,
                "drainingNodeId": victim_id,
                "operation": scale_in_operation_status_json(
                    cluster,
                    conf_change,
                    "ScalingStatefulSet",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        apply_stateful_set(kube, namespace, cluster, desired, false).await?;
        patch_status(
            kube,
            namespace,
            name,
            json!({
                "phase": "ScalingIn",
                "message": format!(
                    "StatefulSet now targets {desired} replicas; waiting for node {victim_id} pod termination"
                ),
                "replicas": desired,
                "desiredReplicas": cluster.spec.replicas,
                "drainingNodeId": victim_id,
                "operation": scale_in_operation_status_json(
                    cluster,
                    conf_change,
                    "WaitingPodTermination",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
    }

    let ready_replicas = wait_ready_pod_count_at_most(kube, namespace, name, desired).await?;
    let membership = leader_membership(kube, namespace, cluster, desired).await?;
    if voter_exists(&membership, victim_id)
        || membership.learners.contains(&victim_id)
        || member_exists(&membership.business_peers, victim_id)
    {
        bail!("node {victim_id} reappeared in authoritative membership while finalizing scale-in");
    }

    let raft_ready = membership_ready_for_replicas(&membership, desired);
    if raft_ready && ready_replicas == desired {
        ensure_ready_pods_serving(kube, namespace, name, desired).await?;
    }
    let phase = if !raft_ready || ready_replicas != desired {
        "WaitingRaftMembers"
    } else if desired == cluster.spec.replicas {
        "Running"
    } else {
        "ScalingIn"
    };
    let message = if !raft_ready || ready_replicas != desired {
        json!(format!(
            "node {victim_id} was removed; waiting for the remaining {desired} replicas to become ready"
        ))
    } else if desired == cluster.spec.replicas {
        Value::Null
    } else {
        json!(format!(
            "node {victim_id} drained and removed; continuing toward desired replicas {}",
            cluster.spec.replicas
        ))
    };

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": phase,
            "message": message,
            "replicas": desired,
            "readyReplicas": ready_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "leaderId": membership.leader_id,
            "members": membership.business_peers.clone(),
            "podStatus": {
                "replicas": desired,
                "readyReplicas": ready_replicas
            },
            "raftStatus": membership_status_json(&membership, desired),
            "drainingNodeId": null,
            "operation": null,
            "confChange": null,
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
}

async fn recover_scale_in_after_target_error(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
    failed_phase: &str,
    error: &str,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let victim_id = conf_change.node_id;
    log::warn!(
        "scale-in target request failed; verifying cluster facts namespace={} cluster={} node_id={} phase={} error={}",
        namespace,
        name,
        victim_id,
        failed_phase,
        error
    );

    let observation = match observe_scale_in_state(kube, namespace, cluster, current_replicas, victim_id)
        .await
    {
        Ok(observation) => observation,
        Err(observation_error) => {
            let combined_error =
                format!("{error}; unable to verify pod and authoritative membership: {observation_error}");
            patch_status(
                    kube,
                    namespace,
                    name,
                    json!({
                        "phase": "RecoveringScaleIn",
                        "message": format!(
                            "node {victim_id} request failed and current removal state cannot yet be verified; no destructive action will be taken"
                        ),
                        "replicas": current_replicas,
                        "desiredReplicas": cluster.spec.replicas,
                        "drainingNodeId": victim_id,
                        "operation": scale_in_operation_status_json(
                            cluster,
                            conf_change,
                            "RetryingObservation",
                            Some(&combined_error),
                            true
                        ),
                        "confChange": conf_change_status_json(conf_change),
                        "observedGeneration": cluster.metadata.generation,
                    }),
                )
                .await?;
            return Ok(());
        }
    };

    let decision = scale_in_recovery_decision(
        observation.pod_state,
        observation.raft_member,
        observation.business_member,
    );
    log::warn!(
        "scale-in facts after target request failure namespace={} cluster={} node_id={} pod_state={} stateful_set_replicas={} raft_member={} business_member={} decision={:?}",
        namespace,
        name,
        victim_id,
        observation.pod_state,
        observation.stateful_set_replicas,
        observation.raft_member,
        observation.business_member,
        decision
    );

    match decision {
        ScaleInRecoveryDecision::RetryTargetRequest => {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "ScalingIn",
                    "message": format!(
                        "node {victim_id} is still present in Kubernetes and membership; retrying the scale-in observation without deleting the pod"
                    ),
                    "replicas": observation.stateful_set_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": observation.membership.leader_id,
                    "raftStatus": membership_status_json(
                        &observation.membership,
                        observation.stateful_set_replicas
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        "Retrying",
                        Some(error),
                        true
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await
        }
        ScaleInRecoveryDecision::RemoveStaleRaftMember => {
            let leader_id = observation
                .membership
                .leader_id
                .ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
            let cleanup = try_rmqtt_action_value_with_payload(
                namespace,
                cluster,
                leader_id,
                json!({"action": "remove_voter", "id": victim_id}),
            )
            .await;
            let (phase, last_error) = match cleanup {
                Ok(_) => ("WaitingMembershipRemoval", None),
                Err(cleanup_error) => ("RetryingMembershipRemoval", Some(cleanup_error.to_string())),
            };
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RecoveringScaleIn",
                    "message": format!(
                        "node {victim_id} pod is {}; cleaning stale raft membership through leader {leader_id}",
                        observation.pod_state
                    ),
                    "replicas": observation.stateful_set_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": leader_id,
                    "raftStatus": membership_status_json(
                        &observation.membership,
                        conf_change.to_replicas
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        phase,
                        last_error.as_deref(),
                        last_error.is_some()
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await
        }
        ScaleInRecoveryDecision::RemoveStaleBusinessMember => {
            let leader_id = observation
                .membership
                .leader_id
                .ok_or_else(|| anyhow!("authoritative membership has no leader id"))?;
            let cleanup = try_rmqtt_action_value_with_payload(
                namespace,
                cluster,
                leader_id,
                json!({"action": "force_node_remove", "id": victim_id}),
            )
            .await;
            let (phase, last_error) = match cleanup {
                Ok(_) => ("WaitingMembershipRemoval", None),
                Err(cleanup_error) => ("RetryingMembershipRemoval", Some(cleanup_error.to_string())),
            };
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "RecoveringScaleIn",
                    "message": format!(
                        "node {victim_id} pod and raft membership are absent; cleaning stale business membership through leader {leader_id}"
                    ),
                    "replicas": observation.stateful_set_replicas,
                    "desiredReplicas": cluster.spec.replicas,
                    "leaderId": leader_id,
                    "raftStatus": membership_status_json(
                        &observation.membership,
                        conf_change.to_replicas
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        conf_change,
                        phase,
                        last_error.as_deref(),
                        last_error.is_some()
                    ),
                    "confChange": conf_change_status_json(conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await
        }
        ScaleInRecoveryDecision::FinishScaleIn => {
            finish_scale_in_step(kube, namespace, cluster, conf_change).await
        }
    }
}

async fn block_scale_in_after_non_transient_target_error(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    conf_change: &ConfChangeStatus,
    failed_phase: &str,
    error: &RmqttActionError,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let victim_id = conf_change.node_id;
    log::warn!(
        "scale-in target request returned a non-transient error namespace={} cluster={} node_id={} phase={} error={}",
        namespace,
        name,
        victim_id,
        failed_phase,
        error
    );
    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScaleInBlocked",
            "message": format!(
                "node {victim_id} returned a non-transient error during {failed_phase}; no pod or membership removal was attempted"
            ),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "operation": scale_in_operation_status_json(
                cluster,
                conf_change,
                "Blocked",
                Some(&error.to_string()),
                true
            ),
            "confChange": conf_change_status_json(conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await
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
    let resume_blocked_drain =
        cluster.status.as_ref().map(|status| status.phase == "ScaleInBlocked").unwrap_or(false);
    if has_matching_scale_in_operation(cluster, &conf_change) {
        match observe_scale_in_state(kube, namespace, cluster, current_replicas, victim_id).await {
            Ok(observation)
                if scale_in_recovery_decision(
                    observation.pod_state,
                    observation.raft_member,
                    observation.business_member,
                ) != ScaleInRecoveryDecision::RetryTargetRequest =>
            {
                recover_scale_in_after_target_error(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    "Resuming",
                    "resuming an in-flight scale-in from authoritative cluster facts",
                )
                .await?;
                return Ok(());
            }
            Err(error) => {
                recover_scale_in_after_target_error(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    "Resuming",
                    &format!("failed to observe an in-flight scale-in: {error}"),
                )
                .await?;
                return Ok(());
            }
            Ok(_) => {}
        }
    }
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
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "Blocked",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    if !resume_blocked_drain {
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
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "Draining",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
    }

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
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "Blocked",
                    None,
                    false
                ),
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
                "operation": scale_in_operation_status_json(
                    cluster,
                    &conf_change,
                    "TransferringLeader",
                    None,
                    false
                ),
                "confChange": conf_change_status_json(&conf_change),
                "observedGeneration": cluster.metadata.generation,
            }),
        )
        .await?;
        return Ok(());
    }

    patch_pod_draining(kube, namespace, &victim_pod).await?;
    if let Err(error) =
        try_rmqtt_action_value_with_payload(namespace, cluster, victim_id, json!({"action": "drain"})).await
    {
        if !error.is_transient() {
            block_scale_in_after_non_transient_target_error(
                kube,
                namespace,
                cluster,
                current_replicas,
                &conf_change,
                "Draining",
                &error,
            )
            .await?;
            return Ok(());
        }
        recover_scale_in_after_target_error(
            kube,
            namespace,
            cluster,
            current_replicas,
            &conf_change,
            "Draining",
            &error.to_string(),
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
            "message": format!(
                "node {victim_id} is draining; waiting for message, session handoff, and routing barriers to remain stable"
            ),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, current_replicas),
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "WaitingDrainStable",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(cluster.spec.raft.drain_timeout_seconds);
    let mut stable_rounds = 0u8;
    let mut last_progress_log = tokio::time::Instant::now() - Duration::from_secs(30);
    loop {
        let status =
            match try_rmqtt_action::<DrainStatus>(namespace, cluster, victim_id, "drain_status").await {
                Ok(status) => status,
                Err(error) => {
                    if !error.is_transient() {
                        block_scale_in_after_non_transient_target_error(
                            kube,
                            namespace,
                            cluster,
                            current_replicas,
                            &conf_change,
                            "WaitingDrainStable",
                            &error,
                        )
                        .await?;
                        return Ok(());
                    }
                    recover_scale_in_after_target_error(
                        kube,
                        namespace,
                        cluster,
                        current_replicas,
                        &conf_change,
                        "WaitingDrainStable",
                        &error.to_string(),
                    )
                    .await?;
                    return Ok(());
                }
            };
        let routing = match routing_barrier_status(namespace, cluster, current_replicas, victim_id).await {
            Ok(routing) => routing,
            Err(error) => {
                recover_scale_in_after_target_error(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    "WaitingRoutingBarrier",
                    &error.to_string(),
                )
                .await?;
                return Ok(());
            }
        };
        let local_ready = status.scale_in_safe_to_leave();
        let routing_ready = routing_barrier_ready(&routing);
        if local_ready && routing_ready {
            stable_rounds += 1;
        } else {
            stable_rounds = 0;
        }
        let blockers = drain_blockers(&status, &routing);
        if last_progress_log.elapsed() >= Duration::from_secs(30) {
            log::info!(
                "scale-in drain progress namespace={} cluster={} node_id={} local_ready={} routing_ready={} stable_rounds={} blockers={}",
                namespace,
                name,
                victim_id,
                local_ready,
                routing_ready,
                stable_rounds,
                blockers
            );
            last_progress_log = tokio::time::Instant::now();
        }
        if status.data_loss_detected {
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "ScaleInBlocked",
                    "message": format!(
                        "node {victim_id} discarded queue or inflight messages during drain; refusing removal; {blockers}"
                    ),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        &conf_change,
                        "Blocked",
                        Some("drain reported data_loss_detected=true"),
                        false
                    ),
                    "confChange": conf_change_status_json(&conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        if stable_rounds >= 2 {
            break;
        }
        if resume_blocked_drain && (!local_ready || !routing_ready) {
            // Once an operation has timed out, keep it sticky. Continue
            // observing on each reconcile so it can recover automatically,
            // but do not start a fresh drain timeout every five seconds.
            patch_status(
                kube,
                namespace,
                name,
                json!({
                    "phase": "ScaleInBlocked",
                    "message": format!("node {victim_id} drain remains blocked; {blockers}"),
                    "drainingNodeId": victim_id,
                    "operation": scale_in_operation_status_json(
                        cluster,
                        &conf_change,
                        "Blocked",
                        None,
                        false
                    ),
                    "confChange": conf_change_status_json(&conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
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
                    "operation": scale_in_operation_status_json(
                        cluster,
                        &conf_change,
                        "Blocked",
                        Some("drain timeout expired before all safety barriers converged"),
                        false
                    ),
                    "confChange": conf_change_status_json(&conf_change),
                    "observedGeneration": cluster.metadata.generation,
                }),
            )
            .await?;
            return Ok(());
        }
        sleep(Duration::from_secs(5)).await;
    }

    patch_status(
        kube,
        namespace,
        name,
        json!({
            "phase": "ScalingIn",
            "message": format!(
                "node {victim_id} drain barriers are stable; requesting raft and business membership removal"
            ),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "leaderId": membership.leader_id,
            "raftStatus": membership_status_json(&membership, current_replicas),
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "RequestingLeave",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    let leave =
        match try_rmqtt_action_value_with_payload(namespace, cluster, victim_id, json!({"action": "leave"}))
            .await
        {
            Ok(leave) => leave,
            Err(error) => {
                if !error.is_transient() {
                    block_scale_in_after_non_transient_target_error(
                        kube,
                        namespace,
                        cluster,
                        current_replicas,
                        &conf_change,
                        "RequestingLeave",
                        &error,
                    )
                    .await?;
                    return Ok(());
                }
                recover_scale_in_after_target_error(
                    kube,
                    namespace,
                    cluster,
                    current_replicas,
                    &conf_change,
                    "RequestingLeave",
                    &error.to_string(),
                )
                .await?;
                return Ok(());
            }
        };
    if leave.get("ok").and_then(Value::as_bool) != Some(true) {
        recover_scale_in_after_target_error(
            kube,
            namespace,
            cluster,
            current_replicas,
            &conf_change,
            "RequestingLeave",
            &format!("node {victim_id} leave returned unexpected response: {leave}"),
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
            "message": format!("waiting for node {victim_id} to disappear from raft and business membership"),
            "replicas": current_replicas,
            "desiredReplicas": cluster.spec.replicas,
            "drainingNodeId": victim_id,
            "operation": scale_in_operation_status_json(
                cluster,
                &conf_change,
                "WaitingMembershipRemoval",
                None,
                false
            ),
            "confChange": conf_change_status_json(&conf_change),
            "observedGeneration": cluster.metadata.generation,
        }),
    )
    .await?;
    if let Err(error) = wait_node_removed(kube, namespace, cluster, victim_id, desired).await {
        recover_scale_in_after_target_error(
            kube,
            namespace,
            cluster,
            current_replicas,
            &conf_change,
            "WaitingMembershipRemoval",
            &error.to_string(),
        )
        .await?;
        return Ok(());
    }
    finish_scale_in_step(kube, namespace, cluster, &conf_change).await
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
            && !membership.learners.contains(&victim_id)
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
    let node_ids = (1..=replicas).collect::<Vec<_>>();
    leader_membership_from_nodes(namespace, cluster, &node_ids).await
}

async fn leader_membership_from_nodes(
    namespace: &str,
    cluster: &RmqttCluster,
    node_ids: &[i32],
) -> Result<MembershipStatus> {
    let mut observations = Vec::new();
    for id in node_ids {
        match rmqtt_action::<MembershipStatus>(namespace, cluster, *id, "membership").await {
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

async fn observe_scale_in_state(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    current_replicas: i32,
    victim_id: i32,
) -> Result<ScaleInObservation> {
    let name = cluster.metadata.name.as_str();
    let stateful_set_replicas = get_stateful_set(kube, namespace, name)
        .await?
        .map(|stateful_set| stateful_set.replicas())
        .unwrap_or(0);
    let victim_pod_name = pod_name(name, victim_id);
    let pod_state = match cluster_pods(kube, namespace, name)
        .await?
        .into_iter()
        .find(|pod| pod.metadata.name == victim_pod_name)
    {
        Some(pod) if pod.metadata.deletion_timestamp.is_some() => VictimPodState::Terminating,
        Some(_) => VictimPodState::Present,
        None => VictimPodState::Absent,
    };
    let remaining_node_ids =
        (1..=current_replicas).filter(|node_id| *node_id != victim_id).collect::<Vec<_>>();
    let membership = leader_membership_from_nodes(namespace, cluster, &remaining_node_ids).await?;
    let raft_member = voter_exists(&membership, victim_id) || membership.learners.contains(&victim_id);
    let business_member = member_exists(&membership.business_peers, victim_id);
    Ok(ScaleInObservation { pod_state, stateful_set_replicas, raft_member, business_member, membership })
}

async fn rmqtt_action<T>(namespace: &str, cluster: &RmqttCluster, node_id: i32, action: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    try_rmqtt_action(namespace, cluster, node_id, action).await.map_err(anyhow::Error::new)
}

async fn try_rmqtt_action<T>(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    action: &str,
) -> std::result::Result<T, RmqttActionError>
where
    T: for<'de> Deserialize<'de>,
{
    let payload = json!({ "action": action });
    let value = try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload.clone()).await?;
    serde_json::from_value(value).map_err(|error| {
        RmqttActionError::new(
            RmqttActionErrorKind::InvalidResponse,
            node_id,
            &payload,
            format!("decode response: {error}"),
        )
    })
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
    try_rmqtt_action_value_with_payload(namespace, cluster, node_id, payload)
        .await
        .map_err(anyhow::Error::new)
}

async fn try_rmqtt_action_value_with_payload(
    namespace: &str,
    cluster: &RmqttCluster,
    node_id: i32,
    payload: Value,
) -> std::result::Result<Value, RmqttActionError> {
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
    let client = Client::builder().timeout(Duration::from_secs(10)).build().map_err(|error| {
        RmqttActionError::new(
            RmqttActionErrorKind::Connection,
            node_id,
            &payload,
            format!("build HTTP client: {error}"),
        )
    })?;
    let res = client.post(url).json(&payload).send().await.map_err(|error| {
        let kind =
            if error.is_timeout() { RmqttActionErrorKind::Timeout } else { RmqttActionErrorKind::Connection };
        RmqttActionError::new(kind, node_id, &payload, error.to_string())
    })?;
    let status = res.status();
    let text = res.text().await.map_err(|error| {
        let kind =
            if error.is_timeout() { RmqttActionErrorKind::Timeout } else { RmqttActionErrorKind::Connection };
        RmqttActionError::new(kind, node_id, &payload, format!("read response body: {error}"))
    })?;
    if !status.is_success() {
        return Err(RmqttActionError::new(
            RmqttActionErrorKind::HttpStatus,
            node_id,
            &payload,
            format!("{status} {text}"),
        ));
    }
    serde_json::from_str(&text).map_err(|error| {
        RmqttActionError::new(
            RmqttActionErrorKind::InvalidResponse,
            node_id,
            &payload,
            format!("parse response: {error}; body={text}"),
        )
    })
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

async fn apply_broker_service_account(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
) -> Result<()> {
    let Some(service_account_name) = cluster.spec.pod.service_account_name.as_deref() else {
        return Ok(());
    };
    let obj = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": service_account_name,
            "namespace": namespace,
            "labels": labels(&cluster.metadata.name),
        }
    });
    kube.apply(&format!("/api/v1/namespaces/{namespace}/serviceaccounts/{service_account_name}"), &obj).await
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
            "ports": headless_service_ports(&cluster.spec.service),
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
            "ports": client_service_ports(&cluster.spec.service),
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

async fn apply_service_monitor(kube: &KubeClient, namespace: &str, cluster: &RmqttCluster) -> Result<()> {
    let spec = &cluster.spec.monitoring.service_monitor;
    if !spec.enabled {
        return Ok(());
    }
    let name = cluster.metadata.name.as_str();
    let mut monitor_labels = labels(name);
    monitor_labels.extend(spec.labels.clone());
    let obj = json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": monitor_labels,
        },
        "spec": {
            "selector": {"matchLabels": selector(name)},
            "endpoints": [{
                "port": "http-api",
                "path": spec.path,
                "interval": spec.interval,
                "scrapeTimeout": spec.scrape_timeout,
            }]
        }
    });
    kube.apply(&format!("/apis/monitoring.coreos.com/v1/namespaces/{namespace}/servicemonitors/{name}"), &obj)
        .await
}

async fn apply_stateful_set(
    kube: &KubeClient,
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    bootstrap_allowed: bool,
) -> Result<()> {
    let name = cluster.metadata.name.as_str();
    let obj = stateful_set_object(namespace, cluster, replicas, bootstrap_allowed);
    kube.apply(&format!("/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}"), &obj).await
}

fn stateful_set_object(
    namespace: &str,
    cluster: &RmqttCluster,
    replicas: i32,
    bootstrap_allowed: bool,
) -> Value {
    let name = cluster.metadata.name.as_str();
    let labels = labels(name);
    let selector = selector(name);
    let scripts_name = format!("{name}-operator-scripts");
    let headless = format!("{name}-headless");
    let mut env = vec![
        json!({"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}}),
        json!({"name": "RUST_BACKTRACE", "value": "1"}),
        json!({"name": "RMQTT_NAME", "value": name}),
        json!({"name": "RMQTT_HEADLESS_SERVICE", "value": format!("{name}-headless")}),
        json!({"name": "RMQTT_REPLICAS", "value": replicas.to_string()}),
        json!({"name": "RMQTT_MQTT_PORT", "value": cluster.spec.service.mqtt_port.to_string()}),
        json!({"name": "RMQTT_MQTTS_PORT", "value": cluster.spec.service.mqtts_port.to_string()}),
        json!({"name": "RMQTT_WS_PORT", "value": cluster.spec.service.ws_port.to_string()}),
        json!({"name": "RMQTT_WSS_PORT", "value": cluster.spec.service.wss_port.to_string()}),
        json!({"name": "RMQTT_HTTP_API_PORT", "value": cluster.spec.service.http_api_port.to_string()}),
        json!({"name": "RMQTT_GRPC_PORT", "value": cluster.spec.service.grpc_port.to_string()}),
        json!({"name": "RMQTT_RAFT_PORT", "value": cluster.spec.service.raft_port.to_string()}),
        json!({"name": "RMQTT_RAFT_BOOTSTRAP", "value": bootstrap_allowed.to_string()}),
    ];
    let mut volume_mounts = vec![
        json!({"name": "work", "mountPath": "/work"}),
        json!({"name": "rmqtt-data", "mountPath": "/var/lib/rmqtt"}),
        json!({"name": "operator-scripts", "mountPath": "/operator-scripts", "readOnly": true}),
    ];
    let mut volumes = vec![
        json!({"name": "work", "emptyDir": {}}),
        json!({"name": "rmqtt-data", "emptyDir": {}}),
        json!({"name": "operator-scripts", "configMap": {"name": scripts_name, "defaultMode": 493}}),
    ];
    if let Some(base_config_map_name) = cluster.spec.config.base_config_map_name.as_deref() {
        env.push(json!({"name": "RMQTT_BASE_CONFIG_DIR", "value": "/base-config"}));
        volume_mounts.push(json!({
            "name": "base-config",
            "mountPath": "/base-config",
            "readOnly": true
        }));
        volumes.push(json!({
            "name": "base-config",
            "configMap": {"name": base_config_map_name}
        }));
    }

    let mut container = json!({
        "name": "rmqtt",
        "image": cluster.spec.image,
        "imagePullPolicy": cluster.spec.image_pull_policy,
        "command": ["/bin/sh", "-ec"],
        "args": [
            "/operator-scripts/render-rmqtt-config.sh && cd /work && exec /app/rmqtt/rmqtt-bin/rmqttd --config /work/rmqtt.toml --plugins-default-startups rmqtt-cluster-raft rmqtt-sys-topic rmqtt-http-api"
        ],
        "env": env,
        "ports": [
            {"name": "mqtt", "containerPort": cluster.spec.service.mqtt_port, "protocol": "TCP"},
            {"name": "mqtts", "containerPort": cluster.spec.service.mqtts_port, "protocol": "TCP"},
            {"name": "ws", "containerPort": cluster.spec.service.ws_port, "protocol": "TCP"},
            {"name": "wss", "containerPort": cluster.spec.service.wss_port, "protocol": "TCP"},
            {"name": "http-api", "containerPort": cluster.spec.service.http_api_port, "protocol": "TCP"},
            {"name": "cluster-grpc", "containerPort": cluster.spec.service.grpc_port, "protocol": "TCP"},
            {"name": "cluster-raft", "containerPort": cluster.spec.service.raft_port, "protocol": "TCP"}
        ],
        "startupProbe": {
            "tcpSocket": {"port": "http-api"},
            "periodSeconds": 5,
            "timeoutSeconds": 10,
            "failureThreshold": 120
        },
        "livenessProbe": {
            "tcpSocket": {"port": "mqtt"},
            "initialDelaySeconds": 20,
            "periodSeconds": 10,
            "timeoutSeconds": 10,
            "failureThreshold": 3
        },
        "readinessProbe": {
            "httpGet": {"path": "/api/v1/health/ready", "port": "http-api"},
            "initialDelaySeconds": 5,
            "periodSeconds": 5,
            "timeoutSeconds": 3,
            "failureThreshold": 12
        },
        "volumeMounts": volume_mounts
    });
    if let Some(resources) = cluster.spec.resources.as_ref() {
        container["resources"] = resource_requirements_json(resources);
    }

    let mut pod_spec = json!({
        "containers": [container],
        "volumes": volumes,
        "enableServiceLinks": cluster.spec.pod.enable_service_links.unwrap_or(false),
        "terminationGracePeriodSeconds": cluster.spec.pod.termination_grace_period_seconds.unwrap_or(30),
    });
    if let Some(service_account_name) = cluster.spec.pod.service_account_name.as_deref() {
        pod_spec["serviceAccountName"] = json!(service_account_name);
    }
    if !cluster.spec.pod.image_pull_secrets.is_empty() {
        pod_spec["imagePullSecrets"] = json!(cluster.spec.pod.image_pull_secrets);
    }
    if let Some(scheduler_name) = cluster.spec.pod.scheduler_name.as_deref() {
        pod_spec["schedulerName"] = json!(scheduler_name);
    }
    if let Some(affinity) = cluster.spec.pod.affinity.as_ref() {
        pod_spec["affinity"] = affinity.clone();
    }
    if !cluster.spec.pod.tolerations.is_empty() {
        pod_spec["tolerations"] = json!(cluster.spec.pod.tolerations);
    }

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
                    "annotations": cluster.spec.pod.annotations,
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
                "spec": pod_spec
            }
        }
    });
    obj
}

fn resource_requirements_json(spec: &ResourceRequirementsSpec) -> Value {
    fn resource_list_json(spec: &ResourceListSpec) -> Value {
        let mut value = json!({});
        if let Some(cpu) = spec.cpu.as_deref() {
            value["cpu"] = json!(cpu);
        }
        if let Some(memory) = spec.memory.as_deref() {
            value["memory"] = json!(memory);
        }
        value
    }

    json!({
        "requests": resource_list_json(&spec.requests),
        "limits": resource_list_json(&spec.limits),
    })
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

async fn patch_pod_unavailable(kube: &KubeClient, namespace: &str, pod: &str) -> Result<()> {
    let patch = json!({
        "metadata": {
            "labels": {
                "rmqtt.io/serving": "false",
                // `draining` is reserved for an explicit, coordinated
                // RemoveVoter operation. A transient readiness failure must
                // not be mistaken for a planned scale-in.
                "rmqtt.io/draining": "false"
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
    planned_draining_node_id: Option<i32>,
) -> Result<()> {
    let pods = cluster_pods(kube, namespace, name).await?;
    for node_id in 1..=replicas {
        let expected_name = pod_name(name, node_id);
        let Some(pod) = pods.iter().find(|pod| pod.metadata.name == expected_name) else {
            continue;
        };
        if pod_is_ready(pod) {
            continue;
        }

        let expected_draining = planned_draining_node_id == Some(node_id);
        let serving = pod.metadata.labels.get("rmqtt.io/serving").map(String::as_str);
        let draining = pod.metadata.labels.get("rmqtt.io/draining").map(String::as_str);
        if expected_draining {
            if serving != Some("false") || draining != Some("true") {
                patch_pod_draining(kube, namespace, &expected_name).await?;
            }
        } else if serving != Some("false") || draining != Some("false") {
            patch_pod_unavailable(kube, namespace, &expected_name).await?;
        }
    }
    Ok(())
}

async fn patch_status(kube: &KubeClient, namespace: &str, name: &str, status: Value) -> Result<()> {
    let patch = json!({ "status": &status });
    kube.merge_patch(
        &format!("/apis/{GROUP}/{VERSION}/namespaces/{namespace}/{PLURAL}/{name}/status"),
        &patch,
    )
    .await?;
    log_status_transition(namespace, name, &status);
    Ok(())
}

fn status_log_signature(status: &Value) -> Value {
    json!({
        "phase": status.get("phase"),
        "observedGeneration": status.get("observedGeneration"),
        "replicas": status.get("replicas"),
        "readyReplicas": status.get("readyReplicas"),
        "desiredReplicas": status.get("desiredReplicas"),
        "leaderId": status.get("leaderId"),
        "membersReady": status.pointer("/raftStatus/membersReady"),
        "drainingNodeId": status.get("drainingNodeId"),
        "operationType": status.pointer("/operation/type"),
        "operationPhase": status.pointer("/operation/phase"),
        "operationMode": status.pointer("/operation/mode"),
        "operationNodeId": status.pointer("/operation/nodeId"),
        "operationRetryCount": status.pointer("/operation/retryCount"),
        "confChangeType": status.pointer("/confChange/type"),
        "confChangeNodeId": status.pointer("/confChange/nodeId"),
    })
}

fn status_field(status: &Value, pointer: &str) -> String {
    match status.pointer(pointer) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Null) | None => "-".to_string(),
        Some(value) => value.to_string(),
    }
}

fn log_status_transition(namespace: &str, name: &str, status: &Value) {
    let signature = status_log_signature(status);
    let cache = LAST_LOGGED_STATUS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{namespace}/{name}");
    let changed = {
        let mut statuses = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if statuses.get(&key) == Some(&signature) {
            false
        } else {
            statuses.insert(key, signature);
            true
        }
    };
    if !changed {
        return;
    }

    let phase = status_field(status, "/phase");
    let generation = status_field(status, "/observedGeneration");
    let replicas = status_field(status, "/replicas");
    let ready_replicas = status_field(status, "/readyReplicas");
    let desired_replicas = status_field(status, "/desiredReplicas");
    let leader_id = status_field(status, "/leaderId");
    let members_ready = status_field(status, "/raftStatus/membersReady");
    let draining_node_id = status_field(status, "/drainingNodeId");
    let operation_type = status_field(status, "/operation/type");
    let operation_phase = status_field(status, "/operation/phase");
    let operation_mode = status_field(status, "/operation/mode");
    let operation_node_id = status_field(status, "/operation/nodeId");
    let operation_retry_count = status_field(status, "/operation/retryCount");
    let message = status_field(status, "/message");

    let details = format!(
        "cluster status changed namespace={namespace} cluster={name} generation={generation} phase={phase} \
         replicas={replicas} ready_replicas={ready_replicas} desired_replicas={desired_replicas} \
         leader_id={leader_id} members_ready={members_ready} draining_node_id={draining_node_id} \
         operation_type={operation_type} operation_phase={operation_phase} operation_mode={operation_mode} \
         operation_node_id={operation_node_id} operation_retry_count={operation_retry_count} message={message:?}"
    );
    if matches!(phase.as_str(), "Degraded" | "ScaleInBlocked") {
        log::warn!("{details}");
    } else {
        log::info!("{details}");
    }
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

fn client_service_ports(spec: &ServiceSpec) -> Value {
    json!([
        {"name": "mqtt", "port": spec.mqtt_port, "targetPort": "mqtt"},
        {"name": "mqtts", "port": spec.mqtts_port, "targetPort": "mqtts"},
        {"name": "ws", "port": spec.ws_port, "targetPort": "ws"},
        {"name": "wss", "port": spec.wss_port, "targetPort": "wss"},
        {"name": "http-api", "port": spec.http_api_port, "targetPort": "http-api"},
    ])
}

fn headless_service_ports(spec: &ServiceSpec) -> Value {
    json!([
        {"name": "cluster-grpc", "port": spec.grpc_port, "targetPort": "cluster-grpc"},
        {"name": "cluster-raft", "port": spec.raft_port, "targetPort": "cluster-raft"}
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
    let now = utc_now_rfc3339();
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
        "mode": mode,
        "startedAt": now,
        "lastAttemptAt": now,
        "retryCount": 0,
        "lastError": null
    })
}

fn scale_in_operation_status_json(
    cluster: &RmqttCluster,
    conf_change: &ConfChangeStatus,
    phase: &str,
    last_error: Option<&str>,
    increment_retry: bool,
) -> Value {
    let previous = cluster.status.as_ref().and_then(|status| status.operation.as_ref()).filter(|operation| {
        operation.kind == conf_change.kind
            && operation.node_id == conf_change.node_id
            && operation.from_replicas == conf_change.from_replicas
            && operation.to_replicas == conf_change.to_replicas
    });
    let now = utc_now_rfc3339();
    let started_at =
        previous.and_then(|operation| operation.started_at.clone()).unwrap_or_else(|| now.clone());
    let retry_count =
        previous.map(|operation| operation.retry_count).unwrap_or(0) + u32::from(increment_retry);

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
        "mode": "RemoveVoter",
        "startedAt": started_at,
        "lastAttemptAt": now,
        "retryCount": retry_count,
        "lastError": last_error
    })
}

fn utc_now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn kubernetes_microtime(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Micros, true)
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

    async fn get_json_optional<T>(&self, path: &str) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let res = self.request(Method::GET, path).send().await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        self.decode(res, path).await.map(Some)
    }

    async fn create_json(&self, path: &str, obj: &Value) -> Result<bool> {
        let res = self
            .request(Method::POST, path)
            .header(CONTENT_TYPE, "application/json")
            .body(obj.to_string())
            .send()
            .await?;
        self.expect_success_or_conflict(res, path).await
    }

    async fn replace_json(&self, path: &str, obj: &Value) -> Result<bool> {
        let res = self
            .request(Method::PUT, path)
            .header(CONTENT_TYPE, "application/json")
            .body(obj.to_string())
            .send()
            .await?;
        self.expect_success_or_conflict(res, path).await
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

    async fn expect_success_or_conflict(&self, res: reqwest::Response, path: &str) -> Result<bool> {
        let status = res.status();
        if status.is_success() {
            return Ok(true);
        }
        if status == StatusCode::CONFLICT {
            return Ok(false);
        }
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("kubernetes request failed: {path}: {status} {text}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_log_signature_deduplicates_message_only_updates() {
        let first = json!({
            "phase": "WaitingRaftCatchUp",
            "message": "learner 4 matched index 10",
            "observedGeneration": 2,
            "replicas": 4,
            "readyReplicas": 3,
            "desiredReplicas": 5,
            "leaderId": 1,
            "operation": {
                "type": "AddLearnerAndPromote",
                "phase": "CatchingUp",
                "mode": "LearnerCatchUp",
                "nodeId": 4
            }
        });
        let message_progress = json!({
            "phase": "WaitingRaftCatchUp",
            "message": "learner 4 matched index 20",
            "observedGeneration": 2,
            "replicas": 4,
            "readyReplicas": 3,
            "desiredReplicas": 5,
            "leaderId": 1,
            "operation": {
                "type": "AddLearnerAndPromote",
                "phase": "CatchingUp",
                "mode": "LearnerCatchUp",
                "nodeId": 4
            }
        });
        let promoted = json!({
            "phase": "PromotingRaftLearner",
            "message": "learner 4 caught up and promotion was requested",
            "observedGeneration": 2,
            "replicas": 4,
            "readyReplicas": 3,
            "desiredReplicas": 5,
            "leaderId": 1,
            "operation": {
                "type": "AddLearnerAndPromote",
                "phase": "Promoting",
                "mode": "LearnerCatchUp",
                "nodeId": 4
            }
        });

        assert_eq!(status_log_signature(&first), status_log_signature(&message_progress));
        assert_ne!(status_log_signature(&first), status_log_signature(&promoted));
    }

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
    fn stateful_set_detects_broker_template_image_drift() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "image": "example/rmqtt:new",
            "imagePullPolicy": "IfNotPresent",
        }))
        .unwrap();
        let matching: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:new",
                            "imagePullPolicy": "IfNotPresent"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(matching.broker_template_matches(&spec));

        let stale_image: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:old",
                            "imagePullPolicy": "IfNotPresent"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(!stale_image.broker_template_matches(&spec));

        let stale_pull_policy: StatefulSet = serde_json::from_value(json!({
            "spec": {
                "replicas": 3,
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "rmqtt",
                            "image": "example/rmqtt:new",
                            "imagePullPolicy": "Always"
                        }]
                    }
                }
            }
        }))
        .unwrap();
        assert!(!stale_pull_policy.broker_template_matches(&spec));
    }

    #[test]
    fn production_compatibility_fields_deserialize_and_render() {
        let spec: ClusterSpec = serde_json::from_value(json!({
            "replicas": 3,
            "image": "example/rmqtt:test",
            "imagePullPolicy": "IfNotPresent",
            "service": {
                "mqttPort": 1883,
                "mqttsPort": 8883,
                "wsPort": 8080,
                "wssPort": 8083,
                "httpApiPort": 6060,
                "grpcPort": 5363,
                "raftPort": 6003
            },
            "config": {"baseConfigMapName": "ams-rmqtt-base-config"},
            "resources": {
                "requests": {"cpu": "8", "memory": "8Gi"},
                "limits": {"cpu": "16", "memory": "16Gi"}
            },
            "pod": {
                "annotations": {"ams.studio.sensecore.cn/scheduler": "volcano"},
                "serviceAccountName": "ams-rmqtt",
                "imagePullSecrets": [{"name": "image-pull-secret-public"}],
                "schedulerName": "default-scheduler",
                "enableServiceLinks": false,
                "terminationGracePeriodSeconds": 30,
                "affinity": {"nodeAffinity": {}},
                "tolerations": [{"key": "dedicated", "operator": "Exists"}]
            },
            "monitoring": {
                "serviceMonitor": {
                    "enabled": true,
                    "labels": {"release": "kube-prometheus-stack"}
                }
            },
            "raft": {"minReplicas": 3}
        }))
        .unwrap();

        validate_spec(&spec).unwrap();
        assert_eq!(spec.service.mqtts_port, 8883);
        assert_eq!(spec.service.ws_port, 8080);
        assert_eq!(spec.service.wss_port, 8083);
        assert_eq!(spec.config.base_config_map_name.as_deref(), Some("ams-rmqtt-base-config"));
        assert_eq!(spec.pod.service_account_name.as_deref(), Some("ams-rmqtt"));
        assert!(spec.monitoring.service_monitor.enabled);

        let resources = resource_requirements_json(spec.resources.as_ref().unwrap());
        assert_eq!(resources["requests"]["cpu"], "8");
        assert_eq!(resources["limits"]["memory"], "16Gi");

        let client_ports = client_service_ports(&spec.service);
        let names = client_ports
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|port| port.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["mqtt", "mqtts", "ws", "wss", "http-api"]);
    }

    #[test]
    fn render_script_overlays_base_config_and_dynamic_listener_ports() {
        assert!(RENDER_SCRIPT.contains("RMQTT_BASE_CONFIG_DIR"));
        assert!(RENDER_SCRIPT.contains("listener.tls.external.addr"));
        assert!(RENDER_SCRIPT.contains("listener.ws.external.addr"));
        assert!(RENDER_SCRIPT.contains("listener.wss.external.addr"));
        assert!(RENDER_SCRIPT.contains("rmqtt-cluster-broadcast.toml"));
    }

    #[test]
    fn stateful_set_preserves_runtime_and_scheduling_compatibility() {
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "ams-rmqtt"},
            "spec": {
                "replicas": 3,
                "image": "example/rmqtt:test",
                "imagePullPolicy": "IfNotPresent",
                "config": {"baseConfigMapName": "ams-rmqtt-base-config"},
                "resources": {
                    "requests": {"cpu": "8", "memory": "8Gi"},
                    "limits": {"cpu": "16", "memory": "16Gi"}
                },
                "pod": {
                    "annotations": {"ams.studio.sensecore.cn/scheduler": "volcano"},
                    "serviceAccountName": "ams-rmqtt",
                    "imagePullSecrets": [
                        {"name": "image-pull-secret-stack"},
                        {"name": "image-pull-secret-public"}
                    ],
                    "schedulerName": "default-scheduler",
                    "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {}}},
                    "tolerations": [{"key": "dedicated", "operator": "Exists"}]
                }
            }
        }))
        .unwrap();

        let object = stateful_set_object("studio-ams-system", &cluster, 3, false);
        assert_eq!(object["spec"]["podManagementPolicy"], "OrderedReady");
        assert_eq!(object["spec"]["updateStrategy"]["type"], "OnDelete");

        let pod = &object["spec"]["template"]["spec"];
        assert_eq!(pod["serviceAccountName"], "ams-rmqtt");
        assert_eq!(pod["schedulerName"], "default-scheduler");
        assert_eq!(pod["imagePullSecrets"].as_array().unwrap().len(), 2);

        let container = &pod["containers"][0];
        assert_eq!(container["resources"]["requests"]["cpu"], "8");
        assert_eq!(container["resources"]["limits"]["memory"], "16Gi");
        assert_eq!(container["startupProbe"]["tcpSocket"]["port"], "http-api");
        assert_eq!(container["livenessProbe"]["tcpSocket"]["port"], "mqtt");
        assert_eq!(container["readinessProbe"]["httpGet"]["path"], "/api/v1/health/ready");

        let port_names = container["ports"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|port| port.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            port_names,
            vec!["mqtt", "mqtts", "ws", "wss", "http-api", "cluster-grpc", "cluster-raft"]
        );
        assert!(pod["volumes"].as_array().unwrap().iter().any(|volume| volume["name"] == "base-config"));
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

        let discarded = DrainStatus {
            safe_to_leave: true,
            strict_safe_to_leave: Some(true),
            session_handoff_complete: Some(true),
            data_loss_detected: true,
            ..Default::default()
        };
        assert!(!discarded.scale_in_safe_to_leave());
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

    #[test]
    fn scale_in_recovery_never_removes_a_present_member_after_timeout() {
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Present, true, true),
            ScaleInRecoveryDecision::RetryTargetRequest
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Present, false, true),
            ScaleInRecoveryDecision::RetryTargetRequest
        );
    }

    #[test]
    fn scale_in_recovery_cleans_membership_only_after_pod_termination_started() {
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Terminating, true, true),
            ScaleInRecoveryDecision::RemoveStaleRaftMember
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Absent, false, true),
            ScaleInRecoveryDecision::RemoveStaleBusinessMember
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Absent, false, false),
            ScaleInRecoveryDecision::FinishScaleIn
        );
        assert_eq!(
            scale_in_recovery_decision(VictimPodState::Present, false, false),
            ScaleInRecoveryDecision::FinishScaleIn
        );
    }

    #[test]
    fn rmqtt_action_error_distinguishes_transient_failures() {
        let payload = json!({"action": "drain_status"});
        let timeout = RmqttActionError::new(RmqttActionErrorKind::Timeout, 5, &payload, "timed out");
        let connection =
            RmqttActionError::new(RmqttActionErrorKind::Connection, 5, &payload, "connection reset");
        let http = RmqttActionError::new(RmqttActionErrorKind::HttpStatus, 5, &payload, "400 bad request");
        let invalid =
            RmqttActionError::new(RmqttActionErrorKind::InvalidResponse, 5, &payload, "invalid json");

        assert!(timeout.is_transient());
        assert!(connection.is_transient());
        assert!(!http.is_transient());
        assert!(!invalid.is_transient());
    }

    #[test]
    fn leader_lease_rejects_a_second_live_holder_and_allows_expired_takeover() {
        let now = DateTime::parse_from_rfc3339("2026-07-30T08:00:30Z").unwrap().with_timezone(&Utc);
        let live = json!({
            "spec": {
                "holderIdentity": "operator-a",
                "leaseDurationSeconds": 30,
                "renewTime": "2026-07-30T08:00:10Z"
            }
        });
        assert!(!lease_can_be_acquired(&live, "operator-b", now, 30));
        assert!(lease_can_be_acquired(&live, "operator-a", now, 30));

        let expired = json!({
            "spec": {
                "holderIdentity": "operator-a",
                "leaseDurationSeconds": 30,
                "renewTime": "2026-07-30T07:59:59Z"
            }
        });
        assert!(lease_can_be_acquired(&expired, "operator-b", now, 30));
    }

    #[test]
    fn leader_lease_does_not_take_over_an_unparseable_live_record() {
        let now = DateTime::parse_from_rfc3339("2026-07-30T08:00:30Z").unwrap().with_timezone(&Utc);
        let malformed = json!({
            "spec": {
                "holderIdentity": "operator-a",
                "leaseDurationSeconds": 30,
                "renewTime": "not-a-time"
            }
        });
        assert!(!lease_can_be_acquired(&malformed, "operator-b", now, 30));
        assert!(lease_can_be_acquired(&json!({"spec": {}}), "operator-b", now, 30));
    }

    #[test]
    fn kubernetes_lease_time_uses_microsecond_precision() {
        let time = DateTime::parse_from_rfc3339("2026-07-30T08:02:03.865123Z").unwrap().with_timezone(&Utc);
        assert_eq!(kubernetes_microtime(time), "2026-07-30T08:02:03.865123Z");
    }

    #[test]
    fn scale_in_operation_status_preserves_start_time_and_increments_retries() {
        let cluster: RmqttCluster = serde_json::from_value(json!({
            "metadata": {"name": "rmqtt"},
            "spec": {"replicas": 3},
            "status": {
                "phase": "ScalingIn",
                "operation": {
                    "type": "RemoveVoter",
                    "nodeId": 5,
                    "fromReplicas": 5,
                    "toReplicas": 4,
                    "requestedReplicas": 3,
                    "startedAt": "2026-07-30T08:00:00.000Z",
                    "retryCount": 2
                }
            }
        }))
        .unwrap();
        let conf_change = ConfChangeStatus {
            kind: "RemoveVoter".to_string(),
            node_id: 5,
            from_replicas: 5,
            to_replicas: 4,
            requested_replicas: 3,
        };

        let operation =
            scale_in_operation_status_json(&cluster, &conf_change, "Retrying", Some("timeout"), true);
        assert_eq!(operation["startedAt"], "2026-07-30T08:00:00.000Z");
        assert_eq!(operation["retryCount"], 3);
        assert_eq!(operation["lastError"], "timeout");
        assert_eq!(operation["phase"], "Retrying");
    }
}

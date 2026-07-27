# RMQTT Raft Operator

First Rust MVP operator for RMQTT raft clusters.

## Current Scope

The operator owns the Kubernetes reconciliation flow for an RMQTT raft cluster:

- renders RMQTT raft config for stable StatefulSet pod identities
- creates ConfigMap, Services, and StatefulSet
- bootstraps a new cluster with one explicitly authorized pod, disables bootstrap in the StatefulSet template, then adds the remaining pods one at a time
- makes every later pod creation and restart join-existing-only, so DNS or network discovery failures cannot create a second raft cluster
- creates a PodDisruptionBudget with `maxUnavailable: 1`
- separates local Kubernetes pod readiness, leader-authoritative raft membership, and client traffic admission
- scales out one node at a time
- scales in one node at a time
- keeps a `status.operation`/`status.confChange` lock so a single `RmqttCluster` only advances one raft membership change at a time
- adds new and rebuilt nodes as non-voting learners, verifies snapshot restoration and leader-side replication progress, and only then promotes them to voters
- checks both raft voters and business-layer gRPC peers before completing a scale step
- drains and removes one voter before reducing StatefulSet replicas

The broker readiness probe uses `GET /api/v1/health/ready` and only succeeds when the local node has restored a snapshot, applied its local committed index, is a voter, reports raft `started` and `available`, has a current leader, is present in business peers, and is not draining. Deploy the operator together with a broker image containing this endpoint.

New and recreated pods start with `rmqtt.io/serving=false`, so they are excluded from the client Service. The operator queries every reachable raft node, accepts membership only from the node that reports itself as the current leader, and changes the pod to `rmqtt.io/serving=true` only after local readiness and leader-authoritative voter, raft-peer, and business-peer checks all pass. The headless Service continues to publish not-ready addresses so raft discovery does not depend on client traffic admission.

Controlled bootstrap is intentionally fail-closed. The operator sets `bootstrap=true` only on the first single-node StatefulSet. After that node is healthy, it updates the template to `bootstrap=false` before creating another pod. A restarted or scaled-out node retries leader discovery and exits without calling `raft.lead()` if no existing leader can be found.

`status.confChange` is kept for compatibility with the first MVP. Newer logic also writes `status.operation`, which records the operation type, phase, node id, replica transition, and reconfiguration mode.

Scale-out and restart recovery use the learner flow, represented in status as:

```yaml
status:
  operation:
    type: AddLearnerAndPromote
    phase: CatchingUp
    mode: LearnerCatchUp
  confChange:
    type: AddLearnerAndPromote
```

`rmqtt-cluster-raft` exposes the operations and status needed by this flow:

- learner join and promotion
- local applied/committed/snapshot-restored status
- leader-side matched index, recent activity, and pending snapshot status
- voter/learner membership inspection
- leadership transfer before an allowed leader scale-in
- leader-side voter removal and target-node graceful leave

The add-node flow is:

```text
create pod -> join as learner -> restore snapshot/apply committed log
           -> leader confirms matched >= committed and no pending snapshot
           -> promote learner -> publish business NodeUp -> pod ready -> admit client traffic
```

## Build

```bash
docker buildx build --platform linux/amd64 \
  -f rmqtt-operator/Dockerfile \
  -t rmqtt-operator:local \
  --load .
```

The RMQTT broker image used by the sample is:

```text
registry.cn-sh-02.sensecore.cn/ccr-ams-public/mirror/docker/rmqtt-test/rmqtt:latest
```

## Deploy

```bash
kubectl apply -f rmqtt-operator/config/crd.yaml
kubectl apply -f rmqtt-operator/config/operator.yaml
kubectl apply -f rmqtt-operator/config/sample.yaml
```

The default namespace is:

```text
rmqtt-test
```

## Existing Cluster Compatibility

`spec.service` supports the client-facing MQTT, MQTTS, WebSocket, secure
WebSocket and HTTP API ports independently from the headless gRPC/Raft ports.
`spec.resources`, `spec.pod` and `spec.monitoring.serviceMonitor` preserve the
resource guarantees, scheduling constraints, image pull secrets and monitoring
settings of an existing Helm deployment.

An existing RMQTT ConfigMap can be reused through
`spec.config.baseConfigMapName`. The operator copies the base files into a
writable work directory, then overrides only values owned by the dynamic
cluster lifecycle:

- local node ID
- MQTT/MQTTS/WS/WSS, HTTP API, gRPC and Raft listen ports
- Raft and business-layer peer lists
- controlled-bootstrap and learner-join flags

All other broker settings, including packet limits, MQTT inflight and message
queue limits, session expiry, logging and Raft tuning, remain inherited from
the base ConfigMap.

For the `studio-ams-system/ams-rmqtt` dev04 replacement, preserve the current
Helm-generated configuration before uninstalling the old release:

```bash
kubectl -n studio-ams-system get configmap ams-rmqtt-config -o json \
  | jq 'del(.metadata.annotations,.metadata.creationTimestamp,.metadata.labels,.metadata.managedFields,.metadata.ownerReferences,.metadata.resourceVersion,.metadata.uid) |
        .metadata.name = "ams-rmqtt-base-config"' \
  | kubectl apply -f -
```

Then deploy the dev04 operator and cluster manifests:

```bash
kubectl apply -f rmqtt-operator/config/crd.yaml
kubectl apply -f rmqtt-operator/config/dev04-operator.yaml
kubectl apply -f rmqtt-operator/config/dev04.yaml
```

The dev04 manifest intentionally differs from the legacy StatefulSet only
where the safe dynamic lifecycle requires it: `OrderedReady`, `OnDelete`, the
Raft-aware readiness endpoint, client traffic admission, learner catch-up,
drain and one-at-a-time membership changes.

## Scale Out

```bash
kubectl -n rmqtt-test patch rmqttcluster rmqtt-optest --type merge -p '{"spec":{"replicas":4}}'
```

The operator advances scale-out one node at a time. If `replicas` changes from `3` to `5`, it first reconciles `3 -> 4`. Node 4 joins as a learner and remains outside the client Service and business peer table. The operator queries the current leader and waits for the learner to be recently active, have no pending snapshot, and reach the leader's committed index. It then promotes node 4. Only after node 4 locally confirms snapshot restoration, voter membership, applied state, and business `NodeUp` does the operator enable `serving=true` and continue with `4 -> 5`.

The new pod receives peers up to the next single-step replica count; existing pods are not restarted because the StatefulSet uses `OnDelete`.

If a Pod object or container is recreated, its in-memory Raft state is empty, so it removes its stale member identity and rejoins as a learner. The operator detects this even when `spec.replicas` did not change, runs the same catch-up and promotion flow, and keeps `serving=false` until recovery completes. Other healthy pods remain Ready and continue serving as long as a Raft quorum is available.

## Scale In

```bash
kubectl -n rmqtt-test patch rmqttcluster rmqtt-optest --type merge -p '{"spec":{"replicas":3}}'
```

The operator advances scale-in one node at a time. If `replicas` changes from `5` to `3`, the operator first reconciles `5 -> 4`, waits for the highest ordinal node to drain and leave raft membership, clears `status.operation`/`status.confChange`, and then reconciles `4 -> 3`.

For each single-step scale-in, the operator verifies the current raft membership is available. It refuses to remove the current leader unless `allowLeaderScaleIn=true`; when enabled, it first transfers leadership to a caught-up voter and confirms the leader changed on a later reconciliation. It then removes the victim from the client Service, asks its clients to reconnect while keeping business routing active, waits for `safe_to_leave=true`, calls `leave`, waits until raft voters and business-layer peers no longer contain the victim, and finally reduces StatefulSet replicas.

`safe_to_leave` requires connections, sessions, handshakes, message queues, inbound/outbound inflights, forwards, message storages, and delayed publishes all to reach zero. A drain timeout blocks the operation; it never forces pod deletion.

## Failure Boundary

This conservative flow preserves service during one-at-a-time restarts and updates as long as a raft quorum remains running. `rmqtt-raft 0.6.0` still stores raft logs and hard state in memory, so a simultaneous loss or restart of every pod cannot be recovered automatically without a future persistent WAL implementation.

## Inspect

```bash
kubectl -n rmqtt-test get rmqttclusters
kubectl -n rmqtt-test get pods
kubectl -n rmqtt-test logs deploy/rmqtt-operator
```

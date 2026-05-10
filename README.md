# kubernetes-event-logger

<img src="images/icon.png" alt="kubernetes-event-logger" width="150" />

[![CI](https://github.com/patbos/kubernetes-event-logger/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/patbos/kubernetes-event-logger/actions/workflows/ci.yml)

A lightweight Kubernetes event logger that watches cluster events and writes new events as JSON to stdout.

## Overview

`kubernetes-event-logger` watches `core/v1` `Event` resources across the cluster and emits them in a log-friendly JSON envelope. It ignores historical events that already existed before the active leader began processing, supports repeated exclusion rules, exposes HTTP health endpoints on port `8080`, and exposes Prometheus metrics on port `9090`.

The binary is intended to run either:

- in-cluster as a Deployment, typically with `replicaCount > 1` and leader election enabled
- locally with a kubeconfig for development or troubleshooting

## Features

- Cluster-wide Kubernetes event watching
- JSON output to stdout for log shipping pipelines
- Historical event suppression on startup and leader transitions
- Repeatable event exclusion filters
- In-cluster and kubeconfig-based authentication
- Leader election for active/standby deployments
- Prometheus metrics at `/metrics`
- HTTP health endpoints at `/healthz` and `/readyz`
- Distroless container image
- Helm chart with RBAC, probes, PDB, Service, optional ServiceMonitor, and optional NetworkPolicy

## Requirements

- Kubernetes `1.25+` (for stable PodDisruptionBudget API)
- Go `1.26.2+` to build from source
- Access to a Kubernetes cluster
- RBAC permission to read `events`
- RBAC permission to manage a `Lease` in the deployment namespace for leader election
- If you use Pod Security Admission, label the target namespace separately; this chart does not enforce PSA by applying pod labels

## Installation

### Helm

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger --version <version>
```

Override values inline:

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger \
  --version <version> \
  --set replicaCount=2 \
  --set image.tag=v0.2.2 \
  --set image.digest=sha256:<digest> \
  --set 'excludeFilters[0].kind=Node' \
  --set 'excludeFilters[0].type=Normal' \
  --set 'excludeFilters[1].namespace=kube-system' \
  --set 'excludeFilters[1].reason=Scheduled'
```

Or use a values file:

```yaml
replicaCount: 2

image:
  tag: v0.2.2
  digest: sha256:<digest>

excludeFilters:
  - kind: Node
    type: Normal
  - namespace: kube-system
    reason: Scheduled

serviceMonitor:
  enabled: true

resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 512Mi
```

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger \
  --version <version> \
  -f my-values.yaml
```

### Local Binary

```bash
go build -o kubernetes-event-logger .
./kubernetes-event-logger
```

Use a specific kubeconfig:

```bash
./kubernetes-event-logger -kubeconfig=/path/to/kubeconfig
```

### Docker

```bash
docker build -t kubernetes-event-logger .
docker run -v ~/.kube/config:/config:ro kubernetes-event-logger -kubeconfig=/config
```

## Verifying Images

Release images are signed with `cosign` in the GitHub Actions release workflow and should be verified by digest.

Resolve a digest for a published tag:

```bash
docker buildx imagetools inspect ghcr.io/patbos/kubernetes-event-logger:v0.2.2
```

Verify the signature for a specific digest:

```bash
cosign verify \
  --certificate-identity-regexp 'https://github.com/patbos/kubernetes-event-logger/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/patbos/kubernetes-event-logger@sha256:<digest>
```

Use the verified digest in Helm:

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger \
  --version 0.2.2 \
  --set image.tag=v0.2.2 \
  --set image.digest=sha256:<digest>
```

## Configuration

### Authentication

The binary uses a kubeconfig when `-kubeconfig` is non-empty. If `-kubeconfig` is empty, it uses in-cluster configuration.

- Default `-kubeconfig`: `~/.kube/config` when that file exists; otherwise empty
- An invalid kubeconfig path or unreadable kubeconfig is fatal; the process does not retry with in-cluster configuration after kubeconfig loading fails
- In Kubernetes, set `POD_NAMESPACE` from `metadata.namespace` so leader election uses the correct namespace

### Command-line Flags

| Flag | Description | Default |
|---|---|---|
| `-kubeconfig` | Path to kubeconfig file; uses in-cluster config only when empty | `~/.kube/config` if it exists, otherwise empty |
| `-lease-name` | Name of the leader election Lease resource | `kubernetes-event-logger` |
| `-lease-duration` | Duration a leader lease remains valid | `15s` |
| `-renew-deadline` | Time the leader has to renew the lease | `10s` |
| `-retry-period` | Retry interval for acquiring or renewing the lease | `2s` |
| `-health-addr` | Address for HTTP health endpoints | `:8080` |
| `-metrics-addr` | Address for Prometheus metrics endpoint | `:9090` |
| `-log-format` | Event JSON log format: `flat`, `legacy`, or `message` | `flat` |
| `-enable-detailed-metrics` | Enable high-cardinality metrics (namespace, reason, kind) | `false` |
| `-exclude-filter` | Exclude events matching all clauses in one rule; repeatable | none |

`-exclude-filter` syntax:

```text
field=value[,field=value]
```

Supported filter fields:

- `namespace`
- `kind`
- `name`
- `reason`
- `type`
- `reporting-component`
- `reporting-controller`
- `source-component`

Example:

```bash
./kubernetes-event-logger \
  -exclude-filter=kind=Node,type=Normal \
  -exclude-filter=namespace=kube-system,reason=Scheduled
```

Each filter rule is AND-matched internally, and the full rule set is OR-matched across rules.

#### Wildcard matching

Filter values support shell-style wildcards using [Go's `path.Match`](https://pkg.go.dev/path#Match) syntax:

- `*` matches any run of non-separator characters
- `?` matches a single non-separator character
- `[abc]` and `[a-z]` character classes are supported

Values without wildcards keep exact-match behavior. Patterns are validated at startup and a malformed pattern (for example an unclosed `[`) makes the binary exit with a clear error.

```bash
./kubernetes-event-logger \
  -exclude-filter='namespace=kube-*' \
  -exclude-filter='reason=BackOff*'
```

Quote wildcard patterns when invoking from a shell so the shell does not expand `*` itself. In Helm values, quote patterns that start with `*` (YAML interprets a leading `*` as an alias).

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `POD_NAMESPACE` | Namespace used for the leader election `Lease` object | `default` |

### Helm Values

Common chart values:

| Value | Description | Default |
|---|---|---|
| `replicaCount` | Number of pods to run; leader election ensures only one actively processes events | `2` |
| `image.repository` | Container image repository | `ghcr.io/patbos/kubernetes-event-logger` |
| `image.tag` | Image tag; falls back to chart `appVersion` | `""` |
| `image.digest` | Immutable image digest; when set, the chart renders `repository[:tag]@digest` | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `serviceAccount.create` | Create a dedicated ServiceAccount for this release | `true` |
| `serviceAccount.name` | ServiceAccount name override; required when `serviceAccount.create=false` | `""` |
| `excludeFilters` | List of event exclusion rules | `[]` |
| `logFormat` | Event JSON log format: `flat`, `legacy`, or `message` | `flat` |
| `enableDetailedMetrics` | Enable high-cardinality Prometheus metrics | `false` |
| `healthPort.containerPort` | Container port for `/healthz` and `/readyz` | `8080` |
| `metricsPort.containerPort` | Container and Service port for `/metrics` | `9090` |
| `leaderElection.leaseDuration` | Leader lease duration | `15s` |
| `leaderElection.renewDeadline` | Lease renew deadline | `10s` |
| `leaderElection.retryPeriod` | Lease retry interval | `2s` |
| `serviceMonitor.enabled` | Create a Prometheus Operator `ServiceMonitor` | `false` |
| `networkPolicy.enabled` | Create a `NetworkPolicy` for metrics ingress and Kubernetes API egress | `false` |
| `networkPolicy.egress.dns.enabled` | Allow DNS egress to the configured DNS namespace and pod selectors; disabled by default because in-cluster API access does not require DNS | `false` |
| `networkPolicy.egress.dns.namespaceSelector` | Namespace selector for optional DNS egress | `kubernetes.io/metadata.name: kube-system` |
| `networkPolicy.egress.dns.podSelector` | Pod selector for optional DNS egress | `k8s-app: kube-dns` |
| `podDisruptionBudget.enabled` | Create a PodDisruptionBudget | `true` |
| `podDisruptionBudget.minAvailable` | Minimum available pods during voluntary disruptions | `1` |
| `resources` | Pod resource requests and limits | see [`chart/values.yaml`](chart/values.yaml) |
| `affinity` | Custom affinity rules; `null` activates built-in pod anti-affinity | `null` |
| `topologySpreadConstraints` | Topology spread constraints for pod distribution | `[]` |
| `tolerations` | Pod tolerations | `[]` |
| `nodeSelector` | Node selector for pod scheduling | `{}` |
| `priorityClassName` | Priority class for pods | `""` |
| `terminationGracePeriodSeconds` | Grace period for graceful shutdown | `30` |
| `strategy` | Deployment update strategy | RollingUpdate (maxSurge=0, maxUnavailable=1) |

See [`chart/values.yaml`](chart/values.yaml) for the full chart surface, including probes, port configuration, security context, and ServiceMonitor labels.

## High Availability

Leader election is always used. Only the current leader processes and logs events; standby replicas wait for failover.

The leader election `Lease`:

- is named by `-lease-name`; the chart sets this to the release fullname
- lives in the namespace from `POD_NAMESPACE`
- uses `<hostname>_<uuid>` as the holder identity

During failover or rollout, some events can be logged twice. Downstream consumers should treat the stream as at-least-once.

## RBAC

The application needs:

- a cluster-scoped permission set to `get`, `list`, and `watch` `events`
- a namespaced permission set to `get` and `update` the leader-election `Lease` in `coordination.k8s.io`

The bundled Helm chart creates the required ServiceAccount, ClusterRole, ClusterRoleBinding, Role, RoleBinding, and pre-created `Lease` resources.
If you set `serviceAccount.create=false`, you must also set `serviceAccount.name` so the chart binds permissions to an explicit existing ServiceAccount.

## Pod Security

The chart is configured to be compatible with the Kubernetes restricted profile by running as non-root, dropping Linux capabilities, using `RuntimeDefault` seccomp, and setting a read-only root filesystem.

Pod Security Admission enforcement is namespace-scoped. If you want Kubernetes to enforce the restricted policy, label the namespace that will run this workload, for example:

```bash
kubectl label namespace <namespace> \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/audit=restricted \
  pod-security.kubernetes.io/warn=restricted
```

## HTTP Endpoints

The process listens on separate ports for health and metrics. Health endpoints are not exposed through the chart Service by default; the Service targets only the metrics port for Prometheus scraping.

| Port | Path | Purpose |
|---|---|---|
| `8080` | `/healthz` | JSON health response used by the liveness probe |
| `8080` | `/readyz` | JSON health response used by the readiness probe |
| `9090` | `/metrics` | Prometheus metrics |

Example `/healthz` response:

```json
{
  "status": "healthy",
  "leader": true,
  "cache_synced": true,
  "uptime_seconds": 42.7,
  "version": "dev"
}
```

Both endpoints currently return the same JSON payload and status code. They return HTTP `503` until informer cache sync completes. Non-leader replicas still report healthy once synced because they are ready to take over.

## Metrics

Prometheus metrics are exposed at `/metrics` on the metrics port, which defaults to `9090`:

- `kubernetes_event_logger_events_total`
- `kubernetes_event_logger_events_filtered_total`
- `kubernetes_event_logger_events_failed_total`
- `kubernetes_event_logger_event_processing_duration_seconds`
- `kubernetes_event_logger_last_event_processed_timestamp_seconds`
- `kubernetes_event_logger_leader`
- `kubernetes_event_logger_leader_elections_total`
- `kubernetes_event_logger_informer_cache_sync_duration_seconds`
- `kubernetes_event_logger_events_by_namespace_total` (optional, via `-enable-detailed-metrics`)
- `kubernetes_event_logger_events_by_reason_total` (optional, via `-enable-detailed-metrics`)
- `kubernetes_event_logger_events_by_object_kind_total` (optional, via `-enable-detailed-metrics`)

## Output Format

Each log line is a JSON object. The default `-log-format=flat` emits selected event fields as top-level JSON fields.

`-log-format=legacy` emits the previous envelope with:

- `time`: event timestamp chosen from `eventTime`, `series.lastObservedTime`, `lastTimestamp`, or `firstTimestamp` (in that order)
- `level`: derived from event type (`Warning` -> `warn`, `Normal` -> `info`, other values -> `info`)
- `event`: the original Kubernetes event object

`-log-format=message` emits only `time`, `level`, and `message`.

Example:

```json
{
  "time": "2025-11-23T15:00:00Z",
  "level": "info",
  "event": {
    "metadata": {
      "name": "pod-example.17a1b2c3d4e5f6",
      "namespace": "default",
      "creationTimestamp": "2025-11-23T15:00:00Z"
    },
    "involvedObject": {
      "kind": "Pod",
      "namespace": "default",
      "name": "example-pod"
    },
    "reason": "Started",
    "message": "Started container app",
    "type": "Normal",
    "firstTimestamp": "2025-11-23T15:00:00Z",
    "lastTimestamp": "2025-11-23T15:00:00Z"
  }
}
```

## Development

### Setup

Install the following tools before building, testing, or linting:

**Go** (1.26.2+)

Follow the [official installation guide](https://go.dev/doc/install).

**golangci-lint** (v2)

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
```

**Helm**

Follow the [official installation guide](https://helm.sh/docs/intro/install/).

**helm-unittest plugin**

```bash
helm plugin install https://github.com/helm-unittest/helm-unittest
```

**hadolint** (Dockerfile linter)

```bash
# macOS
brew install hadolint

# Linux
wget -O /usr/local/bin/hadolint https://github.com/hadolint/hadolint/releases/latest/download/hadolint-Linux-x86_64
chmod +x /usr/local/bin/hadolint
```

**Docker** — required for container image builds only. Follow the [official installation guide](https://docs.docker.com/get-docker/).

### Running all validations

```bash
make all
```

This runs formatting check, lint, Go tests, Helm lint, and Helm unit tests. CI also runs secret scanning and Dockerfile linting.

Individual targets:

```bash
make fmt-check     # check Go formatting
make lint          # run golangci-lint
make test          # run Go unit tests
make helm-lint     # lint the Helm chart
make helm-test     # run Helm chart unit tests
make dockerfile-lint  # lint the Dockerfile with hadolint
make validate      # all of the above including dockerfile-lint
make build         # build the binary
make docker-build  # build the container image
make fmt           # format Go files in place
make clean         # remove the built binary
```

### Build

```bash
make build
# or: go build -o kubernetes-event-logger .
```

### Test

```bash
make test
# or: go test ./...
```

Current automated tests cover event filter parsing and matching, health endpoint behavior, timestamp selection, and related helper logic in `filters_test.go` and `main_test.go`.

### Lint

```bash
make lint
# or: golangci-lint run ./...
```

Static analysis and security linting checks for:
- Unchecked error returns
- Unused variables and imports
- Security anti-patterns (gosec)
- Code simplifications and inefficiencies
- Context and error handling issues

### Helm Validation

```bash
make helm-lint helm-test
# or: helm lint chart && helm unittest chart
```

Helm unit tests live in `chart/tests/*_test.yaml`.

### Container Build

```bash
make docker-build
# or: docker build -t kubernetes-event-logger .
```

The Docker image uses a multi-stage build and a distroless runtime image.

## Publishing

GitHub Actions publishes multi-architecture container images and OCI Helm charts to GitHub Container Registry (`ghcr.io`) when version tags matching `v*` are pushed. CI runs on pull requests, pushes to `main`, and manual workflow dispatches; the separate PR image workflow builds and scans a local image for relevant pull requests but does not publish it.

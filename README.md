# kubernetes-event-logger

A lightweight Kubernetes event logger that watches cluster events and outputs them as JSON to stdout in real-time.

## Overview

This tool connects to a Kubernetes cluster and continuously monitors events, logging them as JSON for easy integration with log aggregation systems like CloudWatch, Elasticsearch, or other logging platforms. It filters out historical events and only logs events that occur after the application starts.

## Features

- Real-time Kubernetes event monitoring
- JSON-formatted output for easy parsing
- Filters out historical events (only logs new events after startup)
- Works both in-cluster and locally with kubeconfig
- Graceful shutdown handling (SIGINT/SIGTERM)
- Lightweight distroless container image
- Multi-architecture support (`linux/amd64` and `linux/arm64`)
- Leader election for high-availability deployments (active/standby)

## Requirements

- Go 1.24+ (for building from source)
- Access to a Kubernetes cluster
- Appropriate RBAC permissions to read events

## Usage

### Helm (Kubernetes)

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger --version <version>
```

To customise, override values:

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger \
  --version <version> \
  --set replicaCount=1 \
  --set image.tag=v1.2.3 \
  --set 'excludeFilters[0].kind=Node' \
  --set 'excludeFilters[0].type=Normal' \
  --set 'excludeFilters[1].namespace=kube-system' \
  --set 'excludeFilters[1].reason=Scheduled'
```

`excludeFilters` is a Helm list of rule objects. Each rule excludes events only when all configured fields match.

Or with a values file:

```yaml
# my-values.yaml
replicaCount: 2

image:
  tag: v1.2.3

excludeFilters:
  - kind: Node
    type: Normal
  - namespace: kube-system
    reason: Scheduled

resources:
  requests:
    cpu: 10m
    memory: 32Mi
  limits:
    memory: 64Mi
```

```bash
helm install kubernetes-event-logger oci://ghcr.io/patbos/kubernetes-event-logger \
  --version <version> \
  -f my-values.yaml
```

### Running Locally

```bash
# Build the binary
go build -o kubernetes-event-logger main.go

# Run with default kubeconfig (~/.kube/config)
./kubernetes-event-logger

# Run with custom kubeconfig path
./kubernetes-event-logger -kubeconfig=/path/to/kubeconfig
```

### Using Docker

```bash
# Build the image
docker build -t kubernetes-event-logger .

# Run with local kubeconfig
docker run -v ~/.kube/config:/config kubernetes-event-logger -kubeconfig=/config
```

## Configuration

### Helm Values

| Value | Description | Default |
|---|---|---|
| `excludeFilters` | List of event exclusion rules; all fields in a rule must match | `[]` |

### Command-line Flags

| Flag | Description | Default |
|---|---|---|
| `-kubeconfig` | Path to kubeconfig file | `~/.kube/config` |
| `-lease-duration` | Duration a leader lease is valid before another candidate can take over | `15s` |
| `-renew-deadline` | Duration the leader has to renew the lease before losing it | `10s` |
| `-retry-period` | How often candidates retry acquiring or renewing the lease | `2s` |
| `-exclude-filter` | Exclude events matching all clauses in a rule; repeatable `field=value[,field=value]` | none |

### Environment Variables

The application automatically detects whether it's running in-cluster or locally:
- When running in a Kubernetes pod, it uses the in-cluster configuration
- When running locally, it uses the kubeconfig file

| Variable | Description | Default |
|---|---|---|
| `POD_NAMESPACE` | Namespace used for the leader election `Lease` object | `default` |

## High Availability (Leader Election)

The application supports running with multiple replicas using Kubernetes leader election. Only the elected leader processes and logs events; the standby replica waits and takes over automatically if the leader dies.

To run with 2 replicas, set `replicas: 2` in your Deployment. The pods use their hostname (pod name) as a unique identity and coordinate via a `Lease` object named `kubernetes-event-logger` in the namespace specified by `POD_NAMESPACE`.

> **Note:** During a leadership transition, a small number of events may be logged twice — once by the old leader before it lost the lease, and again by the new leader after it takes over. This is inherent to the at-least-once delivery model and should be accounted for in downstream log processing.

### Required RBAC

The application needs two sets of permissions:
- **ClusterRole** to read `events` across all namespaces
- **Role** (namespaced) to manage the leader election `Lease` object

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kubernetes-event-logger
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubernetes-event-logger
rules:
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kubernetes-event-logger
subjects:
  - kind: ServiceAccount
    name: kubernetes-event-logger
    namespace: default
roleRef:
  kind: ClusterRole
  name: kubernetes-event-logger
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kubernetes-event-logger-leaderelection
  namespace: default
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kubernetes-event-logger-leaderelection
  namespace: default
subjects:
  - kind: ServiceAccount
    name: kubernetes-event-logger
    namespace: default
roleRef:
  kind: Role
  name: kubernetes-event-logger-leaderelection
  apiGroup: rbac.authorization.k8s.io
```

### Example Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kubernetes-event-logger
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kubernetes-event-logger
  template:
    metadata:
      labels:
        app: kubernetes-event-logger
    spec:
      serviceAccountName: kubernetes-event-logger
      containers:
        - name: kubernetes-event-logger
          image: kubernetes-event-logger:latest
          env:
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
```

## Output Format

Events are logged as JSON objects to stdout. Example output:

```json
{
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
```

## Building

### Local Build

```bash
go build -o kubernetes-event-logger main.go
```

### Docker Build

```bash
docker build -t kubernetes-event-logger .
```

The Docker image uses a multi-stage build with a distroless base image for minimal size and security.

## Publishing

GitHub Actions publishes multi-architecture container images to GitHub Container Registry (`ghcr.io`) on pushes to `main`, version tags matching `v*`, or manual workflow runs.

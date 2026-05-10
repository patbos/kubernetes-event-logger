# Repository Guidelines

## Project Structure & Module Organization
This repository is intentionally small. `main.go` contains the application entrypoint, Kubernetes event watcher, leader election, HTTP health and metrics endpoints, and Prometheus metrics setup. Event filter parsing and matching live in `filters.go`, with unit coverage in `filters_test.go`. Go module metadata lives in `go.mod` and `go.sum`. The Helm chart is under `chart/` with templates in `chart/templates/`. Release automation is defined in `.github/workflows/`.

## Build, Test, and Development Commands
- `go test ./...`: run the Go test suite, including event filtering, timestamp, health, metrics, and helper tests.
- `golangci-lint run ./...`: run static analysis and security linting (checks for unchecked errors, unused code, security patterns, code quality).
- `go build -o kubernetes-event-logger .`: build the local binary used in the README examples.
- `./kubernetes-event-logger -kubeconfig=/path/to/config`: run the logger against a cluster from your workstation.
- `./kubernetes-event-logger -exclude-filter=kind=Node,type=Normal`: run locally with an exclusion rule to validate filter behavior.
- `docker build -t kubernetes-event-logger .`: build the container image from the multi-stage `Dockerfile`.
- `helm lint chart`: validate Helm chart structure before opening a PR.
- `helm unittest chart`: run the Helm chart unit test suite (requires the `helm-unittest` plugin: `helm plugin install https://github.com/helm-unittest/helm-unittest`). Tests live in `chart/tests/*_test.yaml` and cover template rendering, security contexts, resource limits, RBAC, probes, and optional features. Runs automatically in GitHub Actions CI.

## Testing & CI/CD
Pull requests, pushes to `main`, and manual runs trigger automated validation via GitHub Actions (`.github/workflows/ci.yml`):
- Secret scanning
- Go formatting, linting, and unit tests
- Dockerfile linting
- Helm lint validation
- Helm chart unit tests (`helm unittest chart`)

Relevant pull requests also trigger `.github/workflows/build-pr.yml`, which builds a local image and scans it with Trivy without publishing it.

Tests must pass before merging to `main`. Run locally before opening PRs to catch issues early.

## Coding Style & Naming Conventions
Use standard Go formatting and imports; run `gofmt -w` on changed Go files before committing. Run `golangci-lint run ./...` to catch security issues, unchecked errors, and code quality problems before opening a PR. Follow existing Go naming: exported identifiers use `CamelCase`, internal helpers use `camelCase`, and flags use kebab-case such as `-exclude-filter`. Keep new logic in small helper functions rather than growing `main()` further. For Helm templates, preserve the current lowercase dashed resource naming and values structure.

## Testing Guidelines
Add table-driven Go tests in `_test.go` files beside the code they cover. Prioritize unit tests for pure helpers such as event filtering, timestamp selection, health handling, and config loading behavior where it can be isolated cleanly. Validate chart changes with `helm lint chart`. Run `golangci-lint run ./...` and `go test ./...` before opening a PR to catch errors and security issues early. When adding behavior that affects output, flags, metrics, or health endpoints, include at least one regression test or a documented manual verification step in the PR.

## Commit & Pull Request Guidelines
Recent history uses short, imperative commit subjects such as `Add Prometheus metrics and Helm chart for event logger` and `Chart improvements`. Keep commits focused and descriptive. PRs should explain the behavior change, call out any chart or flag changes, link related issues when applicable, and include example output or Helm values when user-facing behavior changes.

## Security & Configuration Tips
Do not commit kubeconfig files, cluster secrets, or generated binaries. Prefer testing locally with `-kubeconfig` and keep RBAC or chart permission changes minimal and explicit in both code and PR notes. Pin GitHub Actions by commit SHA instead of version tags, keeping the version as an inline comment for readability.

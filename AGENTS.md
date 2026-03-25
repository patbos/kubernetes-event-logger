# Repository Guidelines

## Project Structure & Module Organization
This repository is intentionally small. `main.go` contains the application entrypoint, Kubernetes event watcher, leader election, and Prometheus metrics setup. Go module metadata lives in `go.mod` and `go.sum`. The Helm chart is under `chart/` with templates in `chart/templates/`. Release automation is defined in `.github/workflows/`.

## Build, Test, and Development Commands
- `go test ./...`: run the Go test suite. At present this verifies the module builds because there are no `_test.go` files yet.
- `go build -o kubernetes-event-logger .`: build the local binary used in the README examples.
- `./kubernetes-event-logger -kubeconfig=/path/to/config`: run the logger against a cluster from your workstation.
- `docker build -t kubernetes-event-logger .`: build the container image from the multi-stage `Dockerfile`.
- `helm lint chart`: validate Helm chart structure before opening a PR.

## Coding Style & Naming Conventions
Use standard Go formatting and imports; run `gofmt -w` on changed Go files before committing. Follow existing Go naming: exported identifiers use `CamelCase`, internal helpers use `camelCase`, and flags use kebab-case such as `-exclude-kinds`. Keep new logic in small helper functions rather than growing `main()` further. For Helm templates, preserve the current lowercase dashed resource naming and values structure.

## Testing Guidelines
Add table-driven Go tests in `_test.go` files beside the code they cover. Prioritize unit tests for pure helpers such as event filtering and config handling, then validate chart changes with `helm lint chart`. When adding behavior that affects output or flags, include at least one regression test or a documented manual verification step in the PR.

## Commit & Pull Request Guidelines
Recent history uses short, imperative commit subjects such as `Add Prometheus metrics and Helm chart for event logger` and `Chart improvements`. Keep commits focused and descriptive. PRs should explain the behavior change, call out any chart or flag changes, link related issues when applicable, and include example output or Helm values when user-facing behavior changes.

## Security & Configuration Tips
Do not commit kubeconfig files, cluster secrets, or generated binaries. Prefer testing locally with `-kubeconfig` and keep RBAC or chart permission changes minimal and explicit in both code and PR notes.

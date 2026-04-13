BINARY := kubernetes-event-logger
CHART  := chart

.PHONY: all build test lint fmt fmt-check helm-lint helm-test docker-build validate clean help

## Run all validations (CI equivalent)
all: fmt-check lint test helm-lint helm-test

## Build the binary
build:
	go build -o $(BINARY) .

## Run Go unit tests
test:
	go test ./...

## Run golangci-lint
lint:
	golangci-lint run ./...

## Format Go source files
fmt:
	gofmt -w .

## Check Go formatting without modifying files
fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted files (run 'make fmt' to fix):"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

## Lint the Helm chart
helm-lint:
	helm lint $(CHART)

## Run Helm chart unit tests
helm-test:
	helm unittest $(CHART)

## Build the Docker image
docker-build:
	docker build -t $(BINARY) .

## Lint the Dockerfile
dockerfile-lint:
	hadolint Dockerfile

## Run all validations including Docker checks
validate: all dockerfile-lint

## Remove the built binary
clean:
	rm -f $(BINARY)

## Show this help
help:
	@grep -E '^##' Makefile | sed 's/^## //'

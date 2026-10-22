FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-X main.version=${VERSION}" -o kubernetes-event-logger .

# Follow the distroless static:nonroot tag, while pinning the resolved digest for reproducible builds.
FROM gcr.io/distroless/static:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88

WORKDIR /

COPY --from=builder /app/kubernetes-event-logger /kubernetes-event-logger

USER 65532:65532

ENTRYPOINT ["/kubernetes-event-logger"]

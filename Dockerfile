FROM --platform=$BUILDPLATFORM golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-X main.version=${VERSION}" -o kubernetes-event-logger .

# Follow the distroless static:nonroot tag, while pinning the resolved digest for reproducible builds.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7

WORKDIR /

COPY --from=builder /app/kubernetes-event-logger /kubernetes-event-logger

USER 65532:65532

ENTRYPOINT ["/kubernetes-event-logger"]

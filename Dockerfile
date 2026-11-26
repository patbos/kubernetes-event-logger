FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder

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

FROM --platform=$BUILDPLATFORM golang:1.27rc2-alpine@sha256:7870fdc211100210e7380f487953c4188fcbeac99646a56926a973161a3eedcd AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-X main.version=${VERSION}" -o kubernetes-event-logger .

FROM gcr.io/distroless/static:nonroot@sha256:963fa6c544fe5ce420f1f54fb88b6fb01479f054c8056d0f74cc2c6000df5240

WORKDIR /

COPY --from=builder /app/kubernetes-event-logger /kubernetes-event-logger

USER 65532:65532

ENTRYPOINT ["/kubernetes-event-logger"]

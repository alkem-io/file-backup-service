# syntax=docker/dockerfile:1.24
ARG GO_VERSION=1.26
ARG ALPINE_VERSION=3.24

# Build stage — static binary, no CGO (no libvips; this service does no image work)
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
WORKDIR /app
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /bin/file-backup-service ./cmd/file-backup-service/

# Runtime stage — minimal Alpine, non-root (matches K8s fsGroup 65532)
FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache ca-certificates
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S -G nonroot nonroot
COPY --from=builder /bin/file-backup-service /bin/file-backup-service
USER nonroot:nonroot
EXPOSE 4004
ENTRYPOINT ["/bin/file-backup-service"]

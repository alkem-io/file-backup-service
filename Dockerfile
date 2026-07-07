# syntax=docker/dockerfile:1.24
# Base images are pinned by minor-version tag, NOT by @sha256 digest, on purpose: absent a
# renovate/dependabot policy in this repo to bump digests, a frozen digest silently misses
# Alpine/Go base-image CVE patches, whereas the tag picks them up on the next CI rebuild. If a
# fleet-wide digest-pinning + auto-update policy lands (shared github-workflows), pin here too.
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
# Provision the primary-store mount so `restore --to /storage` works as nonroot.
RUN mkdir -p /storage && chown -R 65532:65532 /storage
COPY --from=builder /bin/file-backup-service /bin/file-backup-service
USER nonroot:nonroot
VOLUME ["/storage"]
EXPOSE 4004
ENTRYPOINT ["/bin/file-backup-service"]

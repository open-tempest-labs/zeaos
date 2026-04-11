# ── Builder ──────────────────────────────────────────────────────────────────
FROM golang:1.26-bookworm AS builder

# go-duckdb requires CGO and a C compiler
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libc6-dev ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy workspace and module manifests first for layer caching.
# go work sync requires source trees; use go mod download per module instead.
COPY go.work go.work.sum ./
COPY go.mod go.sum ./
COPY zeaberg/go.mod zeaberg/go.sum ./zeaberg/
RUN go mod download && \
    cd zeaberg && go mod download && cd ..

COPY . .

ARG VERSION=dev
RUN mkdir -p /out && go build \
    -tags duckdb_arrow \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/zeaos \
    ./cmd/zeaos

# ── Test runner ──────────────────────────────────────────────────────────────
FROM builder AS tester

# Run unit + integration tests (excluding FUSE/ZeaDrive which need macFUSE)
CMD ["go", "test", "-tags", "duckdb_arrow", "-v", "-count=1", "./...", "./zeaberg/..."]

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/zeaos /usr/local/bin/zeaos

# Runtime entrypoint: writes ZeaDrive S3 config from env vars, then launches
# the REPL. ZEA_TEST_S3_ENDPOINT / ZEA_TEST_S3_BUCKET are set by docker-compose.
# ZeaDrive FUSE cloud mounts are not available in containers; SDK mode is used.
COPY scripts/docker-entrypoint-runtime.sh /usr/local/bin/zeaos-entrypoint.sh
RUN chmod +x /usr/local/bin/zeaos-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/zeaos-entrypoint.sh"]

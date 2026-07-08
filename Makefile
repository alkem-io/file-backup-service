.PHONY: build docker test cover-check lint generate sqlc-generate openapi migrate setup-hooks run clean

BINARY := file-backup-service
GO := go
GOFLAGS := -race
# Minimum total statement coverage (constitution §VII). cover-check fails below it.
COVER_MIN := 95.0
# Pin the sqlc CLI version: sqlc stamps its own version into every generated file's header,
# so an unpinned `sqlc` on PATH at a different version produces spurious drift-check failures
# on the comment lines alone. `go run <pkg>@<ver>` pins it deterministically without adding
# sqlc's (large) dependency tree to go.mod/go.sum.
SQLC := $(GO) run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0

build:
	mkdir -p bin/
	$(GO) build -o bin/$(BINARY) ./cmd/file-backup-service/

docker:
	docker build -t alkemio/file-backup-service:latest .

test:
	$(GO) test $(GOFLAGS) -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

# cover-check gates total statement coverage at COVER_MIN (constitution §VII). It fails the
# build when coverage drops below the bar, so CI and pre-commit catch untested code — the 95%
# is met with real invariant/integration tests (pgmock/pgxmock/pgxpoolmock for the pgx
# adapters, httptest for the sinks), never coverage padding.
cover-check:
	$(GO) test $(GOFLAGS) -coverpkg=./... -coverprofile=coverage.out ./...
	@total=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$$NF); print $$NF}'); \
	awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN { \
	  printf "total coverage: %s%% (minimum %s%%)\n", t, m; \
	  if (t+0 < m+0) { printf "FAIL: coverage %s%% is below the required %s%%\n", t, m; exit 1 } \
	  printf "OK: coverage meets the %s%% bar\n", m }'

lint:
	golangci-lint run

generate:
	$(GO) generate ./...

sqlc-generate:
	$(SQLC) -f db/sqlc.yaml generate

openapi:
	apispec --dir . --output openapi.yaml --config apispec.yaml --skip-cgo

migrate:
	$(GO) run ./cmd/file-backup-service/ migrate

setup-hooks:
	git config core.hooksPath .githooks
	@echo "Git hooks configured"

run:
	$(GO) run ./cmd/file-backup-service/ serve

clean:
	rm -rf bin/ coverage.out

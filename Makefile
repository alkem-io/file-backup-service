.PHONY: build docker test test-integration cover-check lint generate sqlc-generate openapi migrate setup-hooks run clean

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

# test-integration runs the build-tagged (`integration`) suite: the pgx adapters, migrate, and
# the cmd subcommands against a throwaway Postgres container (testcontainers) — requires Docker.
test-integration:
	$(GO) test $(GOFLAGS) -tags integration ./...

# cover-check gates total statement coverage at COVER_MIN (constitution §VII). It fails the
# build when coverage drops below the bar, so CI and pre-commit catch untested code — the 95%
# is met with real invariant/integration tests (pgmock/pgxmock/pgxpoolmock for the pgx
# adapters, httptest for the sinks, a testcontainers Postgres for the live-DB paths), never
# coverage padding. Runs WITH `-tags integration` (needs Docker) so the DB/cmd paths count.
#
# Coverage is collected via GOCOVERDIR (`-test.gocoverdir`) and merged with `go tool covdata`,
# NOT a single `-coverprofile` text file: with `-coverpkg=./...` across MANY test binaries, the
# legacy text-profile merge feeds `go tool cover -func` DUPLICATE blocks (one per binary that
# compiled the package), which it double-counts — badly under-reporting (a 1-statement function
# reads as 50%). covdata's binary format merges the per-binary streams into the correct UNION.
# COVER_EXCLUDE drops NON-hand-written code from the coverage metric: the sqlc-GENERATED query
# layer (`db/queries`, "DO NOT EDIT") and the integration test HARNESS (`testsupport`). §VII
# measures OUR tests of OUR code — counting the generator's output or test scaffolding dilutes the
# bar and would only reward padding tests of generated code (which §VII forbids). The generated
# SQL is exercised for real by the testcontainers integration suite regardless; it just doesn't
# count toward the denominator.
COVER_EXCLUDE := internal/adapter/outbound/db/queries/|internal/testsupport/
COVERDIR := coverage.covdir
cover-check:
	rm -rf $(COVERDIR)
	mkdir -p $(COVERDIR)
	$(GO) test $(GOFLAGS) -tags integration -coverpkg=./... ./... -args -test.gocoverdir=$(CURDIR)/$(COVERDIR)
	$(GO) tool covdata textfmt -i=$(COVERDIR) -o=coverage.out
	@grep -vE '$(COVER_EXCLUDE)' coverage.out > coverage.product.out
	@total=$$($(GO) tool cover -func=coverage.product.out | awk '/^total:/ {gsub(/%/,"",$$NF); print $$NF}'); \
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
	rm -rf bin/ coverage.out coverage.product.out $(COVERDIR)

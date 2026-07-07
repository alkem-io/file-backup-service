.PHONY: build docker test lint generate sqlc-generate openapi migrate setup-hooks run clean

BINARY := file-backup-service
GO := go
GOFLAGS := -race
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

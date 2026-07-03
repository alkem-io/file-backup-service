# Alkemio File Backup Service (Go) Constitution

> Inherited and adapted from the file-service constitution. Only principles that
> fit a **backup worker** are kept; auth/public-endpoint rules are narrowed
> because this service has no public API.

## Core Principles

### I. Hexagonal Architecture
All code MUST follow the hexagonal (ports and adapters) pattern. Business logic
lives in the domain core and MUST NOT depend on infrastructure.
- Domain types and interfaces MUST reside in dedicated domain packages.
- Each external dependency (a sink, the file-service, a database) MUST have its
  own adapter implementing a domain port.

### II. Content-Addressed Storage Abstraction
Backup targets MUST be abstracted behind one `Sink` port and MUST be **dumb,
content-addressed byte stores** (store / exists / fetch by hash).
- File content MUST be addressed by its **SHA3-256** hash (the file-service
  convention); the hash is identity, key, and integrity check.
- Targets MUST NOT require an index, packing, or a catalog — a stored object
  MUST be independently restorable with nothing but its bytes and a hash check.
- Target selection and options (type, required, compression, immutability) MUST
  be configuration-driven.

### III. Internal Worker, Alkemio Integration First
This service is a **cluster-internal worker**; it exposes **no public,
authorization-guarded endpoints** (only health/metrics).
- It integrates via the file-service internal content API and the shared
  Postgres (outbox in the Alkemio DB via a **scoped role**; ledger in its own DB).
- It MUST reuse existing Alkemio conventions (SHA3-256 addressing, `actorId`
  identity for breadcrumbs) rather than inventing new ones.

### IV. Type-Safe Database Access
Database interactions MUST use **sqlc** for query generation and **pgx**.
- SQL queries MUST be defined in `.sql` files and compiled via sqlc.
- The ledger DB schema MUST use versioned migrations (**golang-migrate**,
  embedded); migrations are the single source of truth (sqlc reads them).
- The service MUST hold **least-privilege** DB roles: SELECT/UPDATE only on the
  outbox; owner rights only on its own ledger DB.

### V. Security by Design
- Target credentials MUST be least-privilege — **write-only / no-delete** on any
  immutable target.
- Secrets, tokens, and credentials MUST NOT be logged or included in errors.
- Inter-service and target traffic MUST use TLS in production; objects MUST be
  encrypted at rest on targets (provider SSE). Client-side encryption is
  deferred to the OpenBao/Vault rollout.
- The service MUST NOT have a delete path to the primary store or to an
  immutable target.

### VI. Durability & Integrity (backup-specific, NON-NEGOTIABLE)
- Every stored object MUST be verified against its content hash; a mismatch MUST
  be detected and reported, never stored under a wrong key.
- Capture MUST be at-least-once and idempotent by hash; nothing is marked done
  until it is durably stored and verified on all **required** targets.
- Retention is durability-first: no GC by default; live deletions never prune
  backups.

### VII. Test-First for Invariants & Root-Cause Analysis
- Tests MUST defend real invariants (idempotency, hash-verify, no-loss under
  fault) — no coverage-padding tests. Fault-injection and restore-drill tests
  are first-class.
- All debugging MUST be driven by root-cause analysis; the cause MUST be
  documented with evidence before a fix is applied.

## Governance
This constitution governs component-internal work in this repo. Cross-repo
concerns and the feature's authoritative spec live in the workspace
(`agents-hq/specs/008-continuous-file-backup/`). Amendments MUST be recorded
here with rationale.

**Version**: 1.0.0 (adapted from file-service) · **Ratified**: 2026-07-03

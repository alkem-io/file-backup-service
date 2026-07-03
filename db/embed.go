// Package dbmigrations embeds the ledger database migrations for golang-migrate.
package dbmigrations

import "embed"

// FS holds the golang-migrate SQL migrations for the ledger database.
//
//go:embed migrations/*.sql
var FS embed.FS

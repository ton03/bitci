# BitCI agent guide

BitCI is public. Add only public-safe code and docs. Never add credentials,
local logs, private repository details, personal paths, or planning notes.

## Build

- Keep one Go binary in `cmd/bitci` and controller code in `internal/bitci`.
- Keep dependencies small. Prefer Go standard library.
- Use strict `bitci.json` config and configured task IDs only.
- Never add arbitrary shell command input to CLI, UI, watcher, or agent tools.

## Verify

Run `gofmt`, `go test ./...`, `go test -race ./...`, and `go vet ./...`.
Add a contract test for controller behavior changes.

## Agent use

Use `.agents/skills/bitci-ci` when operating a repository with `bitci.json`.
Agents use local MCP tools first, then the CLI only if MCP is unavailable.
Agents never use the UI; it is for humans. Plan before submit. Inspect capped
logs before retrying. Do not install or stop the managed service without the
user's request.

## Product boundary

BitCI owns local task queueing, resource leases, logs, and safe reporting.
Keep UI server-rendered. Keep MCP local and read-first. Do not build hosted
execution, workflow language, remote workers, or a plugin system without need.

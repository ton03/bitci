# BitCI

Small, local CI for a trusted development machine.

BitCI runs only named tasks from `bitci.json`. It queues work, limits workers,
leases shared resources, keeps capped logs, and records the tested Git SHA. It
never accepts a shell command from a CLI, UI, or agent.

**Alpha:** `v0.0.1-alpha.1`. Use a dedicated, trusted checkout. BitCI is not a
hosted service, sandbox, or multi-user security boundary.

## Install

Download a binary from [Releases](https://github.com/ton03/bitci/releases), or:

```sh
go install github.com/ton03/bitci/cmd/bitci@v0.0.1-alpha.1
bitci version
```

## Set up a project

Add `bitci.json` at the repository root:

```json
{
  "version": 1,
  "resources": { "browser": 1 },
  "tasks": {
    "unit": { "run": ["go", "test", "./..."], "paths": ["**"] },
    "browser": {
      "run": ["npm", "run", "test:browser"],
      "needs": ["unit"],
      "resources": ["browser"]
    }
  }
}
```

Validate it:

```sh
bitci validate
```

For a quick local run, start the controller in one terminal:

```sh
bitci serve --max-workers 2
```

Then submit configured task IDs:

```sh
bitci plan --paths internal/app.go
bitci submit unit
bitci status
bitci logs --tail 80 1
```

`serve` owns the queue. It starts only submitted tasks. Use the macOS service
below when this project needs an always-on controller.

## How it works

```text
bitci.json --plan/submit--> SQLite queue --claim--> serve
                                               |       |
                                               |       +--> resource leases
                                               |       +--> recorded-SHA worktree
                                               |       +--> configured argv
                                               |       +--> capped local log
                                               v
                                           status / logs

agent MCP --> owner-only Unix socket --> serve
human CLI --> local queue and logs
```

- `plan` selects task IDs from changed paths.
- `submit` records those task IDs, their config, and the source SHA.
- `serve` claims FIFO jobs when worker, disk, and resource limits allow them.
- SHA-backed jobs run in a detached worktree. Each result records `tested_sha`.
- BitCI keeps each recorded SHA reachable with a private Git ref while it keeps its job record.
- `status`, `logs`, `cancel`, and `retry` inspect or control the queue.

`cancel` affects queued work only. Retry only after reading logs. Never print
secrets from tasks.

Use literal `redact` values to hide known secrets from BitCI log reads. It does
not change the retained files. Use `log_retention` to keep the newest N
finished job logs; `0` removes every older log before the next job starts.

### Task environment

Use `env` for fixed, task-specific values. BitCI inherits the controller
environment, then applies these values. Agents cannot supply environment values.
Do not put secrets in `bitci.json` or task output.

```json
"unit": {
  "run": ["go", "test", "./..."],
  "env": {"CI": "true"}
}
```

## Keep it running on macOS

Install BitCI once in a permanent location. Use `go install` above, or keep a
Release binary at a permanent path.

From the project root, this one command installs and starts the local service:

```sh
bitci service --max-workers 2 install
```

Run this only once per project. Running it again safely replaces the same
service; it does not create a second controller.

```sh
bitci service status
bitci service uninstall
```

`launchd` starts BitCI at sign-in and restarts it after exit. Use `uninstall`
to stop and remove it. BitCI refuses service changes while jobs run. Do not use
`go run` for the service because its binary is temporary.

## Agents: skill + MCP

Read [contributor rules](AGENTS.md) first. The included
[BitCI skill](.agents/skills/bitci-ci/SKILL.md) tells agents to use MCP first,
never use the UI, plan before submit, inspect logs before retry, and never send
arbitrary commands.

Start `serve`, then add this local MCP server to the agent client:

```json
{
  "mcpServers": {
    "bitci": {
      "command": "/Users/me/.local/bin/bitci",
      "args": ["mcp", "--allow-runs"]
    }
  }
}
```

Without `--allow-runs`, MCP only reads `status`, `plan`, logs, and disk health.
With it, agents may submit, cancel, or retry configured tasks. The agent flow is:

```text
skill -> plan -> submit configured IDs -> status -> read_logs(cursor) -> retry only if needed
```

`read_logs` returns capped complete lines and a cursor. Pass that cursor to the
next call while a job runs. Use `tail_logs` for the final context.

CLI fallback: `bitci logs --cursor 0 <job-id>` returns the same lines, cursor,
and state.

The UI is for humans only. Agents use MCP; CLI is their fallback.

## External CI and pull requests

Use absolute paths outside the checkout:

```sh
bitci submit --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project unit
bitci status --state-dir /var/lib/bitci/project
```

`stage-pr` is for a dedicated CI checkout only. It requires
`BITCI_GITHUB_TOKEN`, rejects fork heads and dirty checkouts, cleans ignored
`.next` output, and verifies the fetched SHA before work starts.

```sh
bitci stage-pr --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project 42
```

## Examples and releases

Copy an example for a [Go backend](examples/go-backend.bitci.json),
[Node backend](examples/node-backend.bitci.json), or
[Nx monorepo](examples/nx-monorepo.bitci.json). BitCI runs configured argv; it
does not require a framework preset. SHA-isolated jobs start with tracked files
only. Use `prepare` for a safe, configured bootstrap that BitCI runs in each
job worktree before its task; the Node and Nx examples use `npm ci`.

Alpha tags use `v0.0.1-alpha.N`. Each tag builds macOS and Linux archives with
checksums. See [SECURITY.md](SECURITY.md) before exposing a controller.

## Development

```sh
gofmt -w cmd internal
go test ./...
go test -race ./...
go vet ./...
```

[MIT](LICENSE)

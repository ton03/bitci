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

## Dogfood

This repository uses [bitci.json](bitci.json) to run `test`, `build`, `race`,
and `vet` tasks from the same local controller BitCI ships.

## Set up a project

Add `bitci.json` at the repository root:

```json
{
  "version": 1,
  "resources": { "browser": 1 },
  "tasks": {
    "unit": { "run": ["go", "test", "./..."], "paths": ["**"], "max_retries": 1 },
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

Start the local controller and dashboard:

```sh
bitci serve --max-workers 2 --http 127.0.0.1:8787
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

### Concurrent jobs

Set `--max-workers` to the number of jobs the host can safely run:

```sh
bitci serve --max-workers 2
```

Recorded-SHA jobs use separate disposable worktrees, so independent unit and
typecheck tasks can overlap. Declare only real shared services as resources:

```json
{
  "resources": { "browser": 1, "supabase": 1 },
  "tasks": {
    "unit": { "run": ["go", "test", "./..."] },
    "browser": {
      "run": ["npm", "run", "test:browser"],
      "resources": ["browser", "supabase"]
    }
  }
}
```

Workers still use one SQLite connection for queue mutations. Jobs run in
parallel; claims, leases, and cleanup stay serialized and transaction-safe.

Open `http://127.0.0.1:8787` for the local, read-only dashboard. It refreshes
every three seconds and shows job state, tested SHA, timing, resource leases,
disk space, capped logs, and the average duration of passing jobs in seven days.
The dashboard only accepts the loopback address. It has no task controls.

## How it works

```text
bitci.json --plan/submit--> SQLite queue --claim--> serve
                                               |       |
                                               |       +--> resource leases
                                               |       +--> recorded-SHA checkout
                                               |       +--> configured argv
                                               |       +--> capped local log
                                               v
                                           status / logs

agent MCP --> owner-only Unix socket --> serve
human CLI --> local queue and logs
```

- `plan` selects task IDs from changed paths.
- `submit` records those task IDs, their config, and the source SHA.
- `submitted_ref` keeps the exact SHA text supplied by the caller. `ref` stores
  its verified full commit SHA, and `tested_sha` records the SHA checked out
  inside the job worktree.
- `serve` claims FIFO jobs when worker, disk, and resource limits allow them.
- SHA-backed jobs run in a detached checkout with independent Git metadata.
  The checkout reads source objects through Git alternates.
  Its Git commands cannot change source refs or config.
  Each result records `tested_sha`.
- BitCI keeps each recorded SHA reachable with a private Git ref until its batch finishes.
- `status`, `logs`, `cancel`, and `retry` inspect or control the queue.

`max_retries` caps manual reruns for that task. BitCI never retries jobs
automatically. Status records each attempt, prior exit code, queue wait,
duration, and terminal result.

`cancel` affects queued work only. Retry only after reading logs. Never print
secrets from tasks.

If a running task process disappears, `serve` marks the job failed after a
short grace period and releases its resources. Use `bitci recover <job-id>` for
the same bounded check on one running job. Recovery never kills a process.

Use literal `redact` values to hide known secrets from BitCI log reads. It does
not change the retained files. Use `log_retention` to keep the newest N
finished job logs; omit it or use `0` to keep all finished logs.

### Task environment

Use `env` for fixed, task-specific values. BitCI inherits the controller
environment, then applies these values. Agents cannot supply environment values.
BitCI sets `PWD` and `OLDPWD` to the job directory. Do not put secrets in
`bitci.json` or task output.

Use direct argv commands. SHA-backed jobs reject shell and language evaluator
flags such as `sh -c` and `node -e` because they bypass path checks.

```json
"unit": {
  "run": ["go", "test", "./..."],
  "env": {"CI": "true"}
}
```

## Keep it running on macOS

Install BitCI once in a permanent location. Use `go install` above, or keep a
Release binary at a permanent path.

From the project root, these commands install and start the local service:

```sh
mkdir -p "$HOME/.local/bin"
go build -o "$HOME/.local/bin/bitci" ./cmd/bitci
"$HOME/.local/bin/bitci" start --max-workers 2 --http 127.0.0.1:8787
"$HOME/.local/bin/bitci" service status
```

`start` creates one `launchd` job per absolute config path. Run it again and it
reports the existing job without starting another controller. `launchd` starts
BitCI at sign-in and restarts it after exit. Run `bitci stop` to remove it.
BitCI refuses service changes while jobs run.

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
With it, agents may submit, cancel, retry, or recover configured tasks. The agent flow is:

```text
skill -> plan -> submit configured IDs -> status -> read_logs(cursor) -> retry only if needed
```

`read_logs` returns capped complete lines and a cursor. A finished job may return
one final line without a newline. Oversized lines are skipped through bounded
reads. Pass the cursor to the next call while a job runs. Use `tail_logs` for
the final context. Recorded-SHA logs include submitted and tested SHA, worker
cap, declared resources, timeout, and free disk at task start.

CLI fallback: `bitci logs --cursor 0 <job-id>` returns the same lines, cursor,
and state.

The UI is for humans only. Agents use MCP; CLI is their fallback.

## External CI and pull requests

Use absolute paths outside the checkout:

```sh
bitci submit --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project unit
bitci status --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project
```

`stage-pr` is for a dedicated CI checkout only. It requires
`BITCI_GITHUB_TOKEN`, rejects fork heads and dirty checkouts, cleans ignored
`.next` output, and verifies the fetched SHA before work starts. It records the
trusted SHA and rejects submits after the staged checkout changes.

```sh
bitci stage-pr --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project 42
bitci submit --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project --ref <stage.SHA> unit
```

## Examples and releases

Copy an example for a [Go backend](examples/go-backend.bitci.json),
[Node backend](examples/node-backend.bitci.json), or
[Nx monorepo](examples/nx-monorepo.bitci.json). BitCI runs configured argv; it
does not require a framework preset. SHA-isolated jobs start with tracked files
only. Use `prepare` for a safe, configured bootstrap that BitCI runs in each
job checkout before its task; the Node and Nx examples use `npm ci`.

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

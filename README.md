# BitCI

Small, local CI for trusted development machines.

BitCI runs only named tasks from `bitci.json`. It queues work, limits workers,
leases shared resources, keeps capped logs, and records the checked-out Git SHA.
It never accepts a shell command from a CLI, UI, or agent.

**Alpha:** `v0.0.1-alpha.1`. Use a dedicated, trusted checkout. BitCI is not a
hosted service, sandbox, or multi-user security boundary.

## Install

Download a binary from [Releases](https://github.com/ton03/bitci/releases), or:

```sh
go install github.com/ton03/bitci/cmd/bitci@v0.0.1-alpha.1
bitci version
```

## Start

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

Validate, then start the controller in one terminal:

```sh
bitci validate
bitci serve --max-workers 2
```

In another terminal, submit configured task IDs:

```sh
bitci plan --paths internal/app.go
bitci submit unit
bitci status
bitci logs --tail 80 1
```

`serve` owns the queue. It starts submitted tasks only. Keep it running in a
terminal, or use the macOS service below.

## How it works

```text
bitci.json -> queue -> serve -> configured argv -> capped local log
                  ^         |
              CLI / MCP <- owner-only Unix socket
```

- `plan` selects task IDs from changed paths.
- `submit` queues only those configured IDs and their dependencies.
- `serve` claims FIFO jobs when worker, disk, and resource limits permit.
- The worker verifies Git `HEAD`, runs configured argv, and stores its result.
- `status`, `logs`, `cancel`, and `retry` inspect or control that queue.

`cancel` affects queued work only. Retry only after reading logs. Logs are not
redacted in this alpha. Never print secrets from tasks.

## Keep it running on macOS

Build or download one fixed binary path. Then let `launchd` own `serve`:

```sh
mkdir -p "$HOME/.local/bin"
go build -o "$HOME/.local/bin/bitci" ./cmd/bitci
"$HOME/.local/bin/bitci" service --max-workers 2 install
"$HOME/.local/bin/bitci" service status
```

`launchd` starts BitCI at sign-in and restarts it after exit. Run `service
uninstall` to remove it. BitCI refuses service changes while jobs run.

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
skill -> plan -> submit configured IDs -> status -> logs -> retry only if needed
```

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

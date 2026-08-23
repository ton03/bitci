# BitCI

BitCI is a small, local-first CI controller for trusted development machines.

It runs only task IDs from a repository `bitci.json`. It uses argv execution,
SQLite queue state, FIFO claims, worker limits, and named resource leases.

## Stack

- **Controller and CLI:** one Go binary.
- **State:** local SQLite through one pure-Go driver.
- **Runner:** child processes use configured argv only.
- **Background service:** macOS `launchd` keeps `bitci serve` running.
- **UI:** next stage uses server HTML, small CSS, and SSE. No frontend framework.
- **Agents:** local MCP connects to the controller's owner-only Unix socket.
- **UI:** reserved for humans. Agents use MCP first and CLI only as a fallback.

## Pilot commands

```sh
bitci validate
bitci plan --paths src/main.go
bitci submit unit
bitci serve --max-workers 3
bitci status
bitci cancel 12
bitci retry 12
bitci logs --tail 80 12
bitci logs --search "error" 12
bitci doctor
bitci mcp --allow-runs
```

`serve` stays active and starts queued configured tasks. Stop it with `Ctrl-C`.

## External CI commands

Use absolute paths when a CI process runs outside the configured checkout.
`submit` reads the fixed config path. `status` reads only the state directory,
so it works without a checkout or `bitci.json` in the current directory.

```sh
bitci submit --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project unit
bitci status --state-dir /var/lib/bitci/project
```

Keep `--config` and `--state-dir` before task IDs. Existing CI can keep its
current commands; these commands add an optional BitCI reporting path.

## Trusted pull-request staging

Use `stage-pr` only for a dedicated CI checkout. It requires a
`BITCI_GITHUB_TOKEN` with read access to pull requests or contents. BitCI
rejects fork heads, dirty checkouts, queued work, and mismatched fetched SHAs.
It removes ignored `.next` output before a trusted ref switch, then records the
verified `HEAD` SHA when you submit the task.

```sh
BITCI_GITHUB_TOKEN=... bitci stage-pr --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project 42
bitci submit --config /srv/project-ci/bitci.json --state-dir /var/lib/bitci/project unit
```

## Agent MCP

Start the controller first. It creates an owner-only socket in its state
directory. By default BitCI uses a per-checkout directory under
`~/.local/state/bitci`; pass `--state-dir` when CI needs a fixed shared path.
`bitci mcp` exposes stdio MCP tools to an agent client. The default tools read
status, plans, capped logs, and the disk guard. Add `--allow-runs` only when
the agent may submit, cancel, or retry work.

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

Agents never use the UI. They call MCP `plan`, `submit`, `status`,
`tail_logs`, and `search_logs`; use the typed CLI only if MCP is unavailable.

## Run in the background on macOS

Build a fixed binary path first. Do not use `go run` for a background service.

```sh
mkdir -p "$HOME/.local/bin"
go build -o "$HOME/.local/bin/bitci" ./cmd/bitci
"$HOME/.local/bin/bitci" validate
"$HOME/.local/bin/bitci" service --max-workers 3 install
"$HOME/.local/bin/bitci" service status
```

`launchd` starts `bitci serve` at sign-in and restarts it if it exits. The
controller waits for queued configured tasks; it does not run CI until you or
an agent submits a task ID.

`service install` saves the resolved directories for configured task commands
in the launchd plist. Set `BITCI_PATH` before install when task scripts need a
custom runtime path. Run install again after changing the runtime or binary;
BitCI refuses the upgrade while jobs are queued or running.

After a controller restart, BitCI marks the active job failed with exit code
125 and cancels the remaining jobs in that batch. Inspect logs, then retry the
task when ready.

```sh
"$HOME/.local/bin/bitci" submit unit
"$HOME/.local/bin/bitci" status
"$HOME/.local/bin/bitci" logs --tail 80 1
"$HOME/.local/bin/bitci" service uninstall
```

Run `service install` again after replacing the binary.

## `bitci.json`

```json
{
  "version": 1,
  "resources": { "browser": 1 },
  "min_free_bytes": 10737418240,
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

The config is strict JSON. Unknown fields fail validation. BitCI does not run
commands supplied by the CLI or by another process.

## Stack examples

BitCI is stack-neutral. It runs configured argv from the repository root; it
does not detect project types or add presets. Copy and adapt the examples for
[Go backends](examples/go-backend.bitci.json),
[Node backends](examples/node-backend.bitci.json), or
[Nx monorepos](examples/nx-monorepo.bitci.json).

`cancel` only cancels queued jobs. `retry` creates a new configured run. Logs
return at most 80 lines and are available while a job runs. On a Git checkout,
BitCI records the verified `HEAD` SHA at submission and refuses a changed
checkout before task start. The pilot does not redact logs. Do not print
secrets from configured tasks or expose logs that contain them.

## Development

```sh
go test ./...
go build ./cmd/bitci
```

## License

[MIT](LICENSE)

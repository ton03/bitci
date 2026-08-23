# BitCI

BitCI is a small, local-first CI controller for trusted development machines.

It runs only task IDs from a repository `bitci.json`. It uses argv execution,
SQLite queue state, FIFO claims, worker limits, and named resource leases.

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
```

`serve` stays active and starts queued configured tasks. Stop it with `Ctrl-C`.

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

`cancel` only cancels queued jobs. `retry` creates a new configured run. Logs
return at most 80 lines.

## Development

```sh
go test ./...
go build ./cmd/bitci
```

## License

[MIT](LICENSE)

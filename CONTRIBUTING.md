# Contributing to BitCI

BitCI is an early alpha for trusted local development machines. Interfaces can
change before `1.0.0`.

## Before a pull request

Use Conventional Commits for every new commit. For example:

```text
feat(queue): add paused state
fix(logs): cap long lines
docs: clarify agent setup
```

Run these checks:

```sh
gofmt -w cmd internal
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/bitci
```

Keep tasks configured in `bitci.json`. Do not add arbitrary shell input,
credentials, local paths, logs, or private repository details.

## Bug reports

Include BitCI version, operating system, sanitized configuration, and the
smallest reproduction. Remove tokens, credentials, private paths, and task
logs before posting.

Report security problems through the private process in [SECURITY.md](SECURITY.md).

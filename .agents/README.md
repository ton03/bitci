# Shared agent assets

`AGENTS.md` at the repository root is the contributor rulebook. Read it before
changing code or docs.

This directory holds portable agent assets, such as skills. They are
platform-neutral source material, not a promise of automatic discovery by every
agent client. Configure a client that needs an adapter to load `AGENTS.md`.

- `skills/bitci-ci` explains safe BitCI use in a checkout with `bitci.json`.

Keep this directory public-safe. Do not add credentials, local paths, logs, or
private planning notes.

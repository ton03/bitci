---
name: bitci-ci
description: Run and inspect configured BitCI tasks safely when working in a repository with bitci.json. Use for local CI planning, task submission, and failure triage; do not use for arbitrary shell execution.
---

# BitCI

Use BitCI as the local CI control plane. It runs task IDs from `bitci.json`; it
does not accept commands from agents. Use local BitCI MCP tools when available.
Never use the UI. The UI is for humans only. Use the CLI only when MCP is not
available.

1. Use `plan` before `submit`.
2. Submit only returned configured task IDs.
3. Use `status`, then `tail_logs` on failure.
4. Use `search_logs` before `retry`.

MCP starts read-only. The user must explicitly enable run-control tools. CLI
fallback: validate, plan, submit, status, logs, cancel, and retry.

Cancel only queued work. Retry only after inspecting the failure. A retry runs
the configured task and dependencies again.

Do not install, remove, or restart the background service unless the user asks.
Do not use raw commands, edit `bitci.json`, expose logs with secrets, or submit
work when disk guard or resource leases block it.

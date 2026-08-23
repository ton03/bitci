---
name: bitci-ci
description: Run and inspect configured BitCI tasks safely when working in a repository with bitci.json. Use for local CI planning, task submission, and failure triage; do not use for arbitrary shell execution.
---

# BitCI

Use BitCI as the local CI control plane. It runs task IDs from `bitci.json`; it
does not accept commands from agents.

1. Validate first: `bitci validate`.
2. Plan before submit: `bitci plan --paths <changed-paths>`.
3. Submit only returned configured task IDs: `bitci submit <task-id>`.
4. Inspect `bitci status`, then `bitci logs --tail 80 <job-id>` on failure.
5. Search logs before retrying: `bitci logs --search <text> <job-id>`.

Cancel only queued work. Retry only after inspecting the failure. A retry runs
the configured task and dependencies again.

Do not install, remove, or restart the background service unless the user asks.
Do not use raw commands, edit `bitci.json`, expose logs with secrets, or submit
work when disk guard or resource leases block it.

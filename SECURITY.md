# Security policy

BitCI `0.0.1-alpha` supports a trusted local machine and a trusted checkout.
It does not provide a multi-user service, hosted execution, sandboxing, or
on-disk log sanitization. Configured literal redaction applies only when BitCI
reads a job log. The retained log file can still contain the original value.

GitHub private vulnerability reporting is enabled.
Do not publish vulnerability details, tokens, logs, or private repository data.
Report security issues using GitHub Security Advisories with the
**Private vulnerability reporting** form.

For non-security issues use the normal issue templates in `.github/ISSUE_TEMPLATE`.

Use a supported alpha release only after you understand these limits.

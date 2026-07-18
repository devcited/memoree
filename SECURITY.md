# Security policy

## Supported versions

Security fixes are applied to the latest released version of Memoree. The project is pre-1.0, so an upgrade may include a documented compatibility change.

## Reporting a vulnerability

Please do not open a public issue for an undisclosed vulnerability. Email `security@devcited.cc` with:

- the affected version and platform;
- the impact and required preconditions;
- a minimal reproduction or proof of concept; and
- any suggested remediation or disclosure deadline.

We will acknowledge a report within three business days, keep the reporter informed while it is assessed, and coordinate disclosure after a fix is available. Never include real credentials, private memory data, or third-party personal data in a report.

## Security boundaries

- The default endpoint is an owner-only Unix socket. Loopback TCP is host-local, not user-private.
- Normal storage and retrieval operations have no network path or telemetry.
- `memoree remember` is an explicit caller-side command that may invoke a locally authenticated Codex CLI. Preview is the default; the daemon has no model provider or credential loader.
- The CLI does not automatically check for or download updates.
- Release archives are accompanied by SHA-256 checksums and GitHub build provenance.

The complete operational boundary is documented at [memoree.dev/security](https://memoree.dev/security/).

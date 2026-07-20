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
- `memoree remember` is an explicit caller-side command that may invoke a selected locally authenticated Codex or Claude CLI. Preview is the default; the daemon has no model provider or credential loader.
- Installer-managed copies may check the release pointer on eligible interactive starts. They prompt before changing anything, verify an Ed25519-signed manifest plus the exact installer and archive digests, serialize reconciliation, and never prompt in CI or non-interactive commands.
- Release archives are accompanied by SHA-256 checksums and GitHub build provenance. The v0.4 trust-root public key is `m64qvSA8wHiltREGcb/XvIqSSBfGb36JRvW9EOKnisA=` (raw Ed25519 public key, base64).

The complete operational boundary is documented at [memoree.dev/security](https://memoree.dev/security/).

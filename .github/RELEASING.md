# Releasing Memoree

Memoree distributes checksummed binaries through `memoree.dev`, backed by a public immutable GitHub Release. It is not published to crates.io.

For a stable release:

1. Update the Cargo version, changelog, website release metadata, and the Kubernetes image tag in their respective commits.
2. Run the normal CI and `dist generate --check`; confirm `dist plan` contains the four supported macOS/Linux archive targets and no alternate binary-only installer. `memoree.dev/install.sh` is the single supported installer because it owns upgrade reconciliation.
3. Push the release commit and wait for main-branch CI to pass.
4. Enable GitHub release immutability before the first release.
5. Create the version as a draft GitHub Release targeting the exact release commit. The draft must exist before the tag is pushed because cargo-dist is configured with `create-release = false`.
6. Confirm the repository secret `MEMOREE_UPDATE_SIGNING_KEY_B64` is present. Create and push the matching annotated tag. The Release workflow first proves that the secret derives the public key embedded in the tagged binary, then uploads every archive, checksum, source bundle, and attestation to the draft, creates an Ed25519-signed `memoree-release.json` covering the installer and all four archive digests, and publishes only after all uploads succeed.
7. Wait for both the Release and Site image workflows. Do not move the production site image/pointers until the GitHub Release is published and all four versioned downloads pass checksum verification.
8. Deploy the versioned site image through GitOps, then smoke-test `memoree.dev`, the discovery pointer, signed release manifest, redirects, a clean install, an isolated confirmation-based automatic update, and an upgrade of the immutable v0.2.0 fixture with both running and stopped daemon states.

The signed manifest pins the exact bytes at the otherwise mutable `https://memoree.dev/install.sh` URL. Do not deploy any installer-byte change ahead of the release whose signed manifest covers it; after a release, any subsequent installer change must ship with a new signed release before its pointer becomes current.

Published releases are immutable. Correct a release with a new version; never replace a published asset or move an existing version tag.

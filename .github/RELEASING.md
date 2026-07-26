# Releasing Memoree

Memoree distributes checksummed binaries through `memoree.dev`, backed by a public immutable GitHub Release. It is not published to crates.io.

For a stable release:

1. Update the Cargo version, changelog, and website release metadata in their respective commits. Update the Kubernetes image only after the versioned site image exists, in a separate GitOps commit pinned to its registry digest.
2. Run the normal CI and `dist generate --check`; confirm `dist plan` contains the four supported macOS/Linux archive targets and no alternate binary-only installer. `memoree.dev/install.sh` is the single supported installer because it owns upgrade reconciliation.
3. Push the release commit and wait for main-branch CI to pass.
4. Enable GitHub release immutability before the first release.
5. Create the version as a draft GitHub Release targeting the exact release commit. The draft must exist before the tag is pushed because cargo-dist is configured with `create-release = false`.
6. Confirm the repository secret `MEMOREE_UPDATE_SIGNING_KEY_B64` is present. Create and push the matching annotated tag. The Release workflow first proves that the secret derives the public key embedded in the tagged binary, then uploads every archive, checksum, source bundle, and attestation to the draft, creates an Ed25519-signed `memoree-release.json` covering the installer and all four archive digests, and publishes only after all uploads succeed.
7. Wait for the Release workflow to publish. Publication then triggers the Site image workflow, which re-checks that the release is out of draft and carries the signed manifest before it pushes `harbor.devcited.cc/memoree/site:<version>`. The site image is never built from a bare tag push, because it bakes in a discovery pointer whose download URLs redirect to release assets.
8. Deploy the versioned site image through GitOps using the exact Harbor `tag@digest`, wait for the two-replica rollout, then smoke-test `memoree.dev`, the discovery pointer, signed release manifest, redirects, a clean install, an isolated confirmation-based automatic update, and an upgrade of the immutable v0.2.0 fixture with both running and stopped daemon states.

The signed manifest pins the exact bytes at the otherwise mutable `https://memoree.dev/install.sh` URL. Do not deploy any installer-byte change ahead of the release whose signed manifest covers it; after a release, any subsequent installer change must ship with a new signed release before its pointer becomes current.

Published releases are immutable. Correct a release with a new version; never replace a published asset or move an existing version tag.

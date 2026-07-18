# Releasing Memoree

Memoree distributes checksummed binaries through `memoree.dev`, backed by a public immutable GitHub Release. It is not published to crates.io.

For a stable release:

1. Update the Cargo version, changelog, website release metadata, and the Kubernetes image tag in their respective commits.
2. Run the normal CI and `dist generate --check`; confirm `dist plan` contains the four supported macOS/Linux archive targets.
3. Push the release commit and wait for main-branch CI to pass.
4. Enable GitHub release immutability before the first release.
5. Create the version as a draft GitHub Release targeting the exact release commit. The draft must exist before the tag is pushed because cargo-dist is configured with `create-release = false`.
6. Create and push the matching annotated tag. The generated Release workflow uploads every archive, checksum, installer, source bundle, and attestation to the draft, then publishes it only after all uploads succeed.
7. Wait for both the Release and Site image workflows. Do not move the production site image/pointers until the GitHub Release is published and all four versioned downloads pass checksum verification.
8. Deploy the versioned site image through GitOps, then smoke-test `memoree.dev`, the release manifest, redirects, and a clean installer run.

Published releases are immutable. Correct a release with a new version; never replace a published asset or move an existing version tag.

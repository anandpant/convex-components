# Publishing

The public packages are `@shpitdev/convex-openrouter-observability` and
`@shpitdev/convex-cliproxy-observability`. Each package needs its own first publication
and trusted publisher registration. Later releases use GitHub releases and npm OIDC.

## First release

The first publication reserves the npm package name. Complete it through an interactive npm login
with two-factor authentication, then replace that manual path with trusted publishing.

1. From the repository root, run `pnpm install --frozen-lockfile`.
2. From the repository root, run `pnpm check`.
3. Authenticate for the public scope with
   `npm login --scope=@shpitdev --registry=https://registry.npmjs.org`.
4. From the package directory (`packages/openrouter-observability` or `packages/cliproxy-observability`), run `npm publish --access public`.
5. In the npm package settings, configure the GitHub Actions trusted publisher:
   - Repository: `anandpant/convex-components`
   - Workflow: `publish.yml`
   - Environment: `npm`
   - Allowed action: `npm publish`
6. Require two-factor authentication and disallow token-based publishing after the trusted
   publisher succeeds.

## Later releases

1. Update the package version and release notes in a pull request.
2. Merge only after CI passes.
3. Create a GitHub release tagged `openrouter-observability-v<version>` or `cliproxy-observability-v<version>` from the merged commit.

The release workflow verifies the tag against `package.json`, reruns the complete package checks,
and publishes through npm's short-lived GitHub OIDC credential. The workflow uses a GitHub-hosted
runner because npm trusted publishing does not support self-hosted runners.

## CLIProxy native artifacts

CLIProxy tags also build and test against official stock 7.3.5, then attach the Linux
amd64 plugin/exporter archive and SHA256SUMS to the GitHub release. The archive includes
an ABI/schema/source manifest and per-file checksums. PR CI publishes the same artifact
shape for review. Native builds use the pinned Go bookworm image; deployment verifies
the release archive and its inner checksums before installation.

When an initial interactive publication already exists, the release workflow compares
its registry integrity against a freshly packed artifact. It skips only an identical
version; a differing package fails. No existing registry version is overwritten.

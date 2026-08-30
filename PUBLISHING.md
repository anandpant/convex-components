# Publishing

`@anandpant/convex-openrouter-observability` is published from GitHub releases after the
repository and package are public.

## First release

The first publication reserves the npm package name. Complete it through an interactive npm login
with two-factor authentication, then replace that manual path with trusted publishing.

1. Make this repository public after reviewing its complete Git history for secrets and private
   application data.
2. Run `pnpm check` from the repository root.
3. From `packages/openrouter-observability`, run `npm publish --access public`.
4. In the npm package settings, configure the GitHub Actions trusted publisher:
   - Repository: `anandpant/convex-components`
   - Workflow: `publish.yml`
   - Environment: `npm`
   - Allowed action: `npm publish`
5. Require two-factor authentication and disallow token-based publishing after the trusted
   publisher succeeds.

## Later releases

1. Update the package version and release notes in a pull request.
2. Merge only after CI passes.
3. Create a GitHub release tagged `openrouter-observability-v<version>` from the merged commit.

The release workflow verifies the tag against `package.json`, reruns the complete package checks,
and publishes through npm's short-lived GitHub OIDC credential. The workflow uses a GitHub-hosted
runner because npm trusted publishing does not support self-hosted runners.

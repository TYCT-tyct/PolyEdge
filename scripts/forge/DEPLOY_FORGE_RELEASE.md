# Forge Release Deploy

This repository now supports exact-commit Forge deploys from Windows via:

`scripts/forge/deploy_forge_release.ps1`

The script enforces the deployment baseline that production must come from GitHub, not from ad-hoc edits on the servers.

## What It Does

1. Verifies the local repository root and `origin` remote.
2. Refuses to deploy a dirty worktree by default.
3. Resolves a full commit SHA.
4. Fetches `origin` locally and refuses to deploy commits that are not already on GitHub.
5. SSHes to Ireland API / Ireland recorder / Tokyo as requested.
6. Fetches `origin` on the remote bootstrap repo.
7. Creates an immutable release worktree at an exact commit.
8. Copies the bootstrap repo `.env` into the release directory when present.
9. Runs the existing Forge setup script from that release directory.
10. Verifies the systemd services and release commit environment on the remote host.

## Preconditions

- Commit your changes locally.
- Push the deploy commit to `origin`.
- Ensure the remote bootstrap repo exists at `/home/ubuntu/PolyEdge`.
- Ensure the remote bootstrap repo `origin` points to `TYCT-tyct/PolyEdge`.
- Ensure the Windows host has `git` and `ssh` available.
- Ensure the SSH keys exist:
  - `C:\Users\Shini\Documents\PolyEdge.pem`
  - `C:\Users\Shini\Documents\dongjing.pem`

## Examples

Deploy both Tokyo then the full Ireland stack using the current `HEAD` commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\forge\deploy_forge_release.ps1 -Target both
```

Deploy only Ireland for a specific commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\forge\deploy_forge_release.ps1 -Target ireland -Commit dc4d6e0
```

Deploy only Ireland API for a specific commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\forge\deploy_forge_release.ps1 -Target ireland-api -Commit dc4d6e0
```

Deploy only Ireland recorder for a specific commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\forge\deploy_forge_release.ps1 -Target ireland-recorder -Commit dc4d6e0
```

Emergency override for a dirty worktree. Avoid this unless you are intentionally deploying an already-pushed non-HEAD commit:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\forge\deploy_forge_release.ps1 -Target ireland -Commit <sha> -AllowDirty
```

## Release Layout

- Ireland release root: `/data/polyedge-releases`
- Tokyo release root: `/home/ubuntu/polyedge-releases`
- Bootstrap repo: `/home/ubuntu/PolyEdge`

Each deploy creates a release directory:

```text
<release-root>/<shortsha>-<timestamp>
```

Systemd services receive:

```text
Environment=POLYEDGE_RELEASE_COMMIT=<full_sha>
```

That keeps disk guard pruning and runtime version introspection aligned with the active release.

## Notes

- The setup scripts now support role-targeted Ireland deploys via `FORGE_IRELAND_ROLE=api|recorder|both`.
- Ireland API-only deploys rebuild dashboard assets; recorder-only deploys skip that work.
- The Ireland setup script now uses the release directory for dashboard assets instead of a hardcoded mutable repo path.
- This script does not create commits and does not push to GitHub for you. Commit and push first, then deploy.

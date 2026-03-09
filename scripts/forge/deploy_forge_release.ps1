[CmdletBinding()]
param(
    [ValidateSet("ireland", "tokyo", "both")]
    [string]$Target = "both",

    [string]$Commit = "HEAD",

    [string]$RepositoryRoot = "",

    [string]$IrelandHost = "54.77.232.166",
    [string]$IrelandUser = "ubuntu",
    [string]$IrelandKeyPath = "C:\Users\Shini\Documents\PolyEdge.pem",

    [string]$TokyoHost = "57.180.89.145",
    [string]$TokyoUser = "ubuntu",
    [string]$TokyoKeyPath = "C:\Users\Shini\Documents\dongjing.pem",

    [string]$BootstrapRepo = "/home/ubuntu/PolyEdge",
    [string]$IrelandReleaseRoot = "/data/polyedge-releases",
    [string]$TokyoReleaseRoot = "/home/ubuntu/polyedge-releases",

    [switch]$AllowDirty
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[forge-deploy] $Message"
}

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & git @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed.`n$($output | Out-String)"
    }

    return ($output | Out-String).Trim()
}

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Assert-FileExists {
    param([string]$PathToCheck)
    if (-not (Test-Path -LiteralPath $PathToCheck)) {
        throw "Required file not found: $PathToCheck"
    }
}

function Assert-CleanWorktree {
    param([string]$RepoRoot)
    $status = & git -C $RepoRoot status --porcelain=v1 --untracked-files=all
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect git worktree status."
    }
    if ($status) {
        throw "Working tree is not clean. Commit or stash changes before deployment, or use -AllowDirty only for emergencies."
    }
}

function Resolve-CommitSha {
    param(
        [string]$RepoRoot,
        [string]$Revision
    )
    $resolved = Invoke-Git -Arguments @("-C", $RepoRoot, "rev-parse", $Revision)
    if ($resolved -notmatch "^[0-9a-f]{40}$") {
        throw "Resolved commit is not a full SHA: $resolved"
    }
    return $resolved
}

function Assert-OriginLooksCorrect {
    param([string]$RepoRoot)
    $originUrl = Invoke-Git -Arguments @("-C", $RepoRoot, "remote", "get-url", "origin")
    if ($originUrl -notmatch "TYCT-tyct/PolyEdge(\.git)?$") {
        throw "Unexpected origin remote: $originUrl"
    }
}

function Assert-CommitIsOnOrigin {
    param(
        [string]$RepoRoot,
        [string]$CommitSha
    )
    Write-Step "Fetching origin refs locally before deploy"
    & git -C $RepoRoot fetch origin --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "git fetch origin failed."
    }

    $remoteBranches = Invoke-Git -Arguments @("-C", $RepoRoot, "branch", "-r", "--contains", $CommitSha)
    if (-not $remoteBranches) {
        throw "Commit $CommitSha is not present on any fetched origin/* branch. Push to GitHub before deploying."
    }
}

function New-RemoteDeployScript {
    param(
        [string]$CommitSha,
        [string]$ShortSha,
        [string]$Timestamp,
        [string]$BootstrapRepoPath,
        [string]$ReleaseRoot,
        [string]$SetupScriptRelativePath,
        [string]$TargetName,
        [string]$PostChecks
    )

    $template = @'
set -euo pipefail

COMMIT='__COMMIT__'
SHORT_SHA='__SHORT_SHA__'
TIMESTAMP='__TIMESTAMP__'
BOOTSTRAP_REPO='__BOOTSTRAP_REPO__'
RELEASE_ROOT='__RELEASE_ROOT__'
SETUP_SCRIPT='__SETUP_SCRIPT__'
TARGET_NAME='__TARGET_NAME__'

cleanup_on_error() {
  local status="$1"
  if [ "$status" -ne 0 ] && [ -n "${RELEASE_DIR:-}" ] && [ -d "$RELEASE_DIR" ]; then
    echo "[deploy] cleaning failed release worktree: $RELEASE_DIR" >&2
    git -C "$BOOTSTRAP_REPO" worktree remove --force "$RELEASE_DIR" >/dev/null 2>&1 || rm -rf "$RELEASE_DIR"
  fi
}

trap 'cleanup_on_error $?' EXIT

echo "[deploy] target=$TARGET_NAME commit=$COMMIT"

if [ ! -d "$BOOTSTRAP_REPO/.git" ]; then
  echo "[deploy] bootstrap repo missing or not a git repo: $BOOTSTRAP_REPO" >&2
  exit 1
fi

origin_url="$(git -C "$BOOTSTRAP_REPO" remote get-url origin)"
case "$origin_url" in
  *TYCT-tyct/PolyEdge* ) ;;
  * )
    echo "[deploy] unexpected origin url: $origin_url" >&2
    exit 1
    ;;
esac

git -C "$BOOTSTRAP_REPO" fetch --tags origin
git -C "$BOOTSTRAP_REPO" cat-file -e "$COMMIT^{commit}"
git -C "$BOOTSTRAP_REPO" worktree prune

mkdir -p "$RELEASE_ROOT"
RELEASE_DIR="$RELEASE_ROOT/$SHORT_SHA-$TIMESTAMP"
if [ -e "$RELEASE_DIR" ]; then
  echo "[deploy] release dir already exists: $RELEASE_DIR" >&2
  exit 1
fi

git -C "$BOOTSTRAP_REPO" worktree add --detach "$RELEASE_DIR" "$COMMIT"

if [ -f "$BOOTSTRAP_REPO/.env" ] && [ ! -f "$RELEASE_DIR/.env" ]; then
  cp "$BOOTSTRAP_REPO/.env" "$RELEASE_DIR/.env"
fi

export REPO_DIR="$RELEASE_DIR"
export POLYEDGE_RELEASE_COMMIT="$COMMIT"
bash "$RELEASE_DIR/$SETUP_SCRIPT"

__POST_CHECKS__

trap - EXIT
echo "[deploy] release_dir=$RELEASE_DIR"
'@

    return $template.
        Replace("__COMMIT__", $CommitSha).
        Replace("__SHORT_SHA__", $ShortSha).
        Replace("__TIMESTAMP__", $Timestamp).
        Replace("__BOOTSTRAP_REPO__", $BootstrapRepoPath).
        Replace("__RELEASE_ROOT__", $ReleaseRoot).
        Replace("__SETUP_SCRIPT__", $SetupScriptRelativePath).
        Replace("__TARGET_NAME__", $TargetName).
        Replace("__POST_CHECKS__", $PostChecks.Trim())
}

function Invoke-RemoteScript {
    param(
        [string]$Host,
        [string]$User,
        [string]$KeyPath,
        [string]$ScriptBody
    )

    $sshArgs = @(
        "-i", $KeyPath,
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "$User@$Host",
        "bash -s"
    )

    $ScriptBody | & ssh @sshArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Remote deploy failed on $Host."
    }
}

Assert-Command -Name "git"
Assert-Command -Name "ssh"

$scriptDir = Split-Path -Parent $PSCommandPath
if (-not $RepositoryRoot) {
    $RepositoryRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
}

$RepositoryRoot = (Resolve-Path $RepositoryRoot).Path
$RepositoryRoot = Invoke-Git -Arguments @("-C", $RepositoryRoot, "rev-parse", "--show-toplevel")

Write-Step "Repository root: $RepositoryRoot"
Assert-OriginLooksCorrect -RepoRoot $RepositoryRoot

if (-not $AllowDirty) {
    Write-Step "Checking local worktree cleanliness"
    Assert-CleanWorktree -RepoRoot $RepositoryRoot
}

$resolvedCommit = Resolve-CommitSha -RepoRoot $RepositoryRoot -Revision $Commit
$shortSha = $resolvedCommit.Substring(0, 7)
$timestamp = Get-Date -Format "yyyyMMddHHmmss"

Write-Step "Resolved commit: $resolvedCommit"
Assert-CommitIsOnOrigin -RepoRoot $RepositoryRoot -CommitSha $resolvedCommit

$irelandPostChecks = @'
systemctl is-active --quiet polyedge-forge-ireland-recorder.service
systemctl is-active --quiet polyedge-forge-ireland-api.service
systemctl show polyedge-forge-ireland-recorder.service -p Environment --value | grep -q "POLYEDGE_RELEASE_COMMIT=$COMMIT"
systemctl show polyedge-forge-ireland-api.service -p Environment --value | grep -q "POLYEDGE_RELEASE_COMMIT=$COMMIT"
curl -fsS http://127.0.0.1:9810/health/live >/dev/null
systemctl --no-pager --full status polyedge-forge-ireland-recorder.service | sed -n '1,5p'
systemctl --no-pager --full status polyedge-forge-ireland-api.service | sed -n '1,5p'
'@

$tokyoPostChecks = @'
systemctl is-active --quiet polyedge-forge-tokyo.service
systemctl show polyedge-forge-tokyo.service -p Environment --value | grep -q "POLYEDGE_RELEASE_COMMIT=$COMMIT"
systemctl --no-pager --full status polyedge-forge-tokyo.service | sed -n '1,5p'
'@

$deployQueue = switch ($Target) {
    "ireland" { @("ireland") }
    "tokyo" { @("tokyo") }
    default { @("tokyo", "ireland") }
}

foreach ($targetName in $deployQueue) {
    switch ($targetName) {
        "ireland" {
            Assert-FileExists -PathToCheck $IrelandKeyPath
            Write-Step "Deploying commit $shortSha to Ireland"
            $script = New-RemoteDeployScript `
                -CommitSha $resolvedCommit `
                -ShortSha $shortSha `
                -Timestamp $timestamp `
                -BootstrapRepoPath $BootstrapRepo `
                -ReleaseRoot $IrelandReleaseRoot `
                -SetupScriptRelativePath "scripts/forge/setup_ireland_forge.sh" `
                -TargetName "ireland" `
                -PostChecks $irelandPostChecks
            Invoke-RemoteScript -Host $IrelandHost -User $IrelandUser -KeyPath $IrelandKeyPath -ScriptBody $script
        }
        "tokyo" {
            Assert-FileExists -PathToCheck $TokyoKeyPath
            Write-Step "Deploying commit $shortSha to Tokyo"
            $script = New-RemoteDeployScript `
                -CommitSha $resolvedCommit `
                -ShortSha $shortSha `
                -Timestamp $timestamp `
                -BootstrapRepoPath $BootstrapRepo `
                -ReleaseRoot $TokyoReleaseRoot `
                -SetupScriptRelativePath "scripts/forge/setup_tokyo_forge.sh" `
                -TargetName "tokyo" `
                -PostChecks $tokyoPostChecks
            Invoke-RemoteScript -Host $TokyoHost -User $TokyoUser -KeyPath $TokyoKeyPath -ScriptBody $script
        }
        default {
            throw "Unexpected target in deploy queue: $targetName"
        }
    }
}

Write-Step "Deploy finished for target=$Target commit=$resolvedCommit"

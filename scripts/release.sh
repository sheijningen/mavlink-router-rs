#!/usr/bin/env bash
#
# Cut a release: bump Cargo.toml + Cargo.lock + CHANGELOG.md, commit, tag, push.
# The bump kind (major|minor|patch) is applied to the version currently in
# Cargo.toml. The tag push fires .github/workflows/release.yml which builds
# and publishes.
#
# Usage: scripts/release.sh [--dry-run] {major|minor|patch}
#
# With --dry-run, runs every preflight check (branch, clean tree, version
# compute, tag availability) and prints the actions that would be taken, but
# does not mutate any file, commit, tag, or push.

set -euo pipefail

# Tracks the furthest stage reached; the trap rolls back from that point
# backwards on any nonzero exit. Values: none, files-bumped, committed, tagged.
STAGE="none"
VERSION=""

rollback() {
    local exit_code=$?
    set +e
    case "$STAGE" in
        tagged)
            echo "rolling back: deleting local tag v$VERSION and resetting release commit" >&2
            git tag -d "v$VERSION" >/dev/null
            git reset --hard HEAD~1 >/dev/null
            ;;
        committed)
            echo "rolling back: resetting release commit" >&2
            git reset --hard HEAD~1 >/dev/null
            ;;
        files-bumped)
            echo "rolling back: restoring Cargo.toml, Cargo.lock, CHANGELOG.md" >&2
            git checkout -- Cargo.toml Cargo.lock CHANGELOG.md
            ;;
        none) ;;
    esac
    exit "$exit_code"
}

parse_args() {
    DRY_RUN=0
    local positional=()
    for arg in "$@"; do
        case "$arg" in
            --dry-run) DRY_RUN=1 ;;
            -*)
                echo "error: unknown flag '$arg'" >&2
                echo "usage: $0 [--dry-run] {major|minor|patch}" >&2
                exit 1
                ;;
            *) positional+=("$arg") ;;
        esac
    done
    if [[ ${#positional[@]} -ne 1 ]]; then
        echo "usage: $0 [--dry-run] {major|minor|patch}" >&2
        exit 1
    fi
    BUMP_KIND="${positional[0]}"
    case "$BUMP_KIND" in
        major|minor|patch) ;;
        *)
            echo "error: invalid bump kind '$BUMP_KIND' (expected major|minor|patch)" >&2
            exit 1
            ;;
    esac
}

read_current_version() {
    # Anchor on the [package] section so a stray `version = ...` in another table isn't picked up.
    CURRENT_VERSION="$(awk '
        /^\[/ { in_pkg = ($0 == "[package]"); next }
        in_pkg && /^version = "[0-9]+\.[0-9]+\.[0-9]+"$/ {
            gsub(/^version = "|"$/, "")
            print
            exit
        }
    ' Cargo.toml)"
    if [[ -z "$CURRENT_VERSION" ]]; then
        echo "error: could not read [package].version from Cargo.toml" >&2
        exit 1
    fi
}

compute_next_version() {
    local major minor patch
    IFS='.' read -r major minor patch <<< "$CURRENT_VERSION"
    case "$BUMP_KIND" in
        major) VERSION="$((major + 1)).0.0" ;;
        minor) VERSION="$major.$((minor + 1)).0" ;;
        patch) VERSION="$major.$minor.$((patch + 1))" ;;
    esac
}

cd_to_repo_root() {
    local root
    root="$(cd "$(dirname "$0")/.." && pwd)"
    cd "$root"
}

ensure_clean_working_tree() {
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "error: working tree is dirty (uncommitted or untracked changes); commit, stash, or remove them first" >&2
        exit 1
    fi
}

ensure_on_main_branch() {
    local branch
    branch="$(git rev-parse --abbrev-ref HEAD)"
    if [[ "$branch" != "main" ]]; then
        echo "error: releases must be cut from main; current branch is '$branch'" >&2
        exit 1
    fi
}

ensure_tag_unused() {
    if git rev-parse --verify "v$VERSION" >/dev/null 2>&1; then
        echo "error: tag v$VERSION already exists locally" >&2
        exit 1
    fi
    if git ls-remote --exit-code --tags origin "refs/tags/v$VERSION" >/dev/null 2>&1; then
        echo "error: tag v$VERSION already exists on origin" >&2
        exit 1
    fi
}

confirm_release() {
    echo "About to release:"
    echo "  current: $CURRENT_VERSION"
    echo "  bump:    $BUMP_KIND"
    echo "  new:     v$VERSION"
    local reply
    read -r -p "Proceed? [y/N] " reply
    case "$reply" in
        [yY]|[yY][eE][sS]) ;;
        *)
            echo "aborted; no changes made"
            exit 1
            ;;
    esac
}

bump_cargo_toml() {
    # Anchor on the [package] section so a stray `version = ...` in another table isn't hit.
    awk -v ver="$VERSION" '
        BEGIN { in_pkg = 0; done = 0 }
        /^\[/ { in_pkg = ($0 == "[package]") }
        in_pkg && !done && /^version = / {
            print "version = \"" ver "\""
            done = 1
            next
        }
        { print }
        END { if (!done) { print "[package].version not found" > "/dev/stderr"; exit 1 } }
    ' Cargo.toml > Cargo.toml.tmp
    mv Cargo.toml.tmp Cargo.toml
}

bump_cargo_lock() {
    # Lockfile fields are alphabetically sorted within a [[package]] block, so for the
    # workspace root entry (no source/checksum) `version` is the line immediately after
    # `name`. The flag-and-clear pattern below acts only on that adjacent pair.
    awk -v ver="$VERSION" '
        BEGIN { saw_name = 0; done = 0 }
        saw_name && !done && /^version = "[0-9]+\.[0-9]+\.[0-9]+"$/ {
            print "version = \"" ver "\""
            saw_name = 0
            done = 1
            next
        }
        { saw_name = ($0 == "name = \"rmr\""); print }
        END { if (!done) { print "rmr package version not found in Cargo.lock" > "/dev/stderr"; exit 1 } }
    ' Cargo.lock > Cargo.lock.tmp
    mv Cargo.lock.tmp Cargo.lock
}

verify_version_bumps() {
    if ! grep -q "^version = \"$VERSION\"$" Cargo.toml; then
        echo "error: Cargo.toml bump failed" >&2
        exit 1
    fi
    if ! grep -A1 '^name = "rmr"$' Cargo.lock | grep -q "^version = \"$VERSION\"$"; then
        echo "error: Cargo.lock bump failed" >&2
        exit 1
    fi
}

bump_changelog() {
    local release_date
    release_date="$(date -u +%Y-%m-%d)"
    awk -v ver="$VERSION" -v date="$release_date" '
        !done && /^## \[Unreleased\]$/ {
            print "## [Unreleased]"
            print ""
            print "## [" ver "] - " date
            done = 1
            next
        }
        { print }
        END { if (!done) { print "CHANGELOG.md is missing the ## [Unreleased] header" > "/dev/stderr"; exit 1 } }
    ' CHANGELOG.md > CHANGELOG.md.tmp
    mv CHANGELOG.md.tmp CHANGELOG.md
}

commit_and_tag() {
    git commit -am "release $VERSION"
    STAGE="committed"
    git tag -a "v$VERSION" -m "Release $VERSION"
    STAGE="tagged"
}

push_release() {
    local branch
    branch="$(git rev-parse --abbrev-ref HEAD)"
    # --atomic so the branch update and the tag land together (or both abort).
    git push --atomic origin "$branch" "refs/tags/v$VERSION"
}

main() {
    parse_args "$@"
    cd_to_repo_root
    ensure_on_main_branch
    ensure_clean_working_tree

    read_current_version
    compute_next_version
    ensure_tag_unused

    if [[ "$DRY_RUN" -eq 1 ]]; then
        local branch
        branch="$(git rev-parse --abbrev-ref HEAD)"
        echo "dry run: would release v$VERSION ($BUMP_KIND bump from $CURRENT_VERSION)"
        echo "  would update: Cargo.toml, Cargo.lock, CHANGELOG.md"
        echo "  would commit: release $VERSION"
        echo "  would tag:    v$VERSION"
        echo "  would push:   origin $branch refs/tags/v$VERSION (atomic)"
        exit 0
    fi

    confirm_release

    # Arm the rollback trap only once VERSION is known and we're about to mutate
    # files. Any earlier failure (parse, dirty tree, tag clash) leaves nothing
    # to undo, so the trap would only get in the way.
    trap rollback EXIT

    # Set STAGE before each mutation, not after both — otherwise a failure in
    # bump_cargo_lock leaves Cargo.toml modified with the trap thinking STAGE=none.
    STAGE="files-bumped"
    bump_cargo_toml
    bump_cargo_lock
    verify_version_bumps
    bump_changelog

    commit_and_tag
    push_release

    # Push succeeded — disarm the trap so a non-zero exit from the echo (or any
    # future post-push step) does not undo a published release.
    trap - EXIT
    STAGE="none"

    echo "released v$VERSION ($BUMP_KIND bump from $CURRENT_VERSION); the tag push has fired the release workflow"
}

main "$@"

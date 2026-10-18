#!/bin/sh
# Installs fzn from a release: one static binary, put in ~/.local/bin, nothing else touched.
#
#   curl -fsSL https://github.com/Sambigeara/fuzzynote/releases/latest/download/install.sh | sh
#   curl -fsSL ... | sh -s -- --version 0.34.0
#   curl -fsSL ... | sh -s -- --system
#
# The archive for this machine is fetched, its SHA-256 is checked against the sum published
# beside it, and the binary is put in place with one rename, so a running fzn or relay keeps
# the file it has open. Running it again with the version already there does nothing. Notes
# and keys are never read or written: they live under ~/.fzn and ~/.config/fzn, and --uninstall
# leaves them where they are.
#
# Where the releases live is the one thing to change if the repository moves: FZN_RELEASES.
# Anything laid out as GitHub lays out releases will do, a directory served over http or
# file:// included (see release.sh), which is how a release built by hand reaches a machine.
#
# Everything happens in main, called on the last line, so that a download cut short and piped
# into sh defines a few functions and runs nothing.
set -eu

usage() {
    cat <<'EOF'
install.sh: installs fzn from a release

  --version VERSION  this version rather than the latest (FZN_VERSION)
  --dir DIR          put the binary here rather than in ~/.local/bin (FZN_INSTALL_DIR)
  --system           put it in /usr/local/bin, with sudo if that needs it
  --target TARGET    this build rather than the one for this machine (FZN_TARGET)
  --force            install even if that version is already there
  --uninstall        remove the binary; notes and keys are kept
  -h, --help

FZN_RELEASES names where the releases are, in GitHub's layout:
  https://github.com/Sambigeara/fuzzynote/releases to begin with.
EOF
}

say() { printf 'install.sh: %s\n' "$*" >&2; }
fail() { say "$*"; exit 1; }

main() {
    releases="${FZN_RELEASES:-https://github.com/Sambigeara/fuzzynote/releases}"
    version="${FZN_VERSION:-latest}"
    dir="${FZN_INSTALL_DIR:-$HOME/.local/bin}"
    target="${FZN_TARGET:-}"
    force=
    uninstall=
    system=

    while [ $# -gt 0 ]; do
        case "$1" in
            --version) [ $# -ge 2 ] || fail "--version needs a version"; version="$2"; shift ;;
            --dir) [ $# -ge 2 ] || fail "--dir needs a directory"; dir="$2"; shift ;;
            --target) [ $# -ge 2 ] || fail "--target needs a target"; target="$2"; shift ;;
            --system) system=1; dir=/usr/local/bin ;;
            --force) force=1 ;;
            --uninstall) uninstall=1 ;;
            -h | --help) usage; exit 0 ;;
            *) usage >&2; fail "unknown argument $1" ;;
        esac
        shift
    done
    version="${version#v}"

    # Writes to a directory only this user cannot write go through sudo, when --system asks.
    as_root=
    if [ -n "$system" ] && [ ! -w "$dir" ] && [ "$(id -u)" != 0 ]; then
        command -v sudo >/dev/null 2>&1 || fail "$dir is not writable and there is no sudo"
        as_root=sudo
    fi

    if [ -n "$uninstall" ]; then
        if [ -e "$dir/fzn" ]; then
            $as_root rm -f "$dir/fzn" || fail "could not remove $dir/fzn"
            say "removed $dir/fzn; the notes under ~/.fzn and the keys under ~/.config/fzn are kept"
        else
            say "nothing at $dir/fzn"
        fi
        exit 0
    fi

    # The build for this machine. Linux is always the static musl build, so it runs on any
    # distribution; a Mac running under Rosetta is an arm64 Mac.
    if [ -z "$target" ]; then
        os="$(uname -s)"
        arch="$(uname -m)"
        case "$os:$arch" in
            Linux:x86_64 | Linux:amd64) target=x86_64-unknown-linux-musl ;;
            Linux:aarch64 | Linux:arm64) target=aarch64-unknown-linux-musl ;;
            Darwin:arm64 | Darwin:aarch64) target=aarch64-apple-darwin ;;
            Darwin:x86_64)
                if [ "$(sysctl -n hw.optional.arm64 2>/dev/null || echo 0)" = 1 ]; then
                    target=aarch64-apple-darwin
                else
                    fail "there is no build for Intel Macs; cargo install --path crates/fzn builds one"
                fi
                ;;
            *) fail "there is no build for $os on $arch; cargo install --path crates/fzn builds one" ;;
        esac
    fi
    asset="fzn-$target.tar.gz"

    # Fetching: curl, else wget. Over https nothing older than TLS 1.2 and no detour through
    # another scheme, as rustup's installer does it.
    if command -v curl >/dev/null 2>&1; then
        fetch() {
            case "$1" in
                https://*) curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "$2" "$1" ;;
                *) curl -fsSL --retry 3 -o "$2" "$1" ;;
            esac
        }
        # Where a URL first leads, for a release named "latest", which GitHub answers with a
        # redirect to the release it is. Only the first: the ones after it go to storage whose
        # addresses say nothing of the version. A mirror with no redirect says nothing.
        resolve() { curl -fsSI -o /dev/null -w '%{redirect_url}' "$1" 2>/dev/null || true; }
    elif command -v wget >/dev/null 2>&1; then
        fetch() { wget -q -O "$2" "$1"; }
        resolve() { :; }
    else
        fail "neither curl nor wget is here to fetch with"
    fi

    # A checksum is SHA-256 whichever tool this machine has for it.
    if command -v sha256sum >/dev/null 2>&1; then
        sum() { sha256sum "$1" | cut -d' ' -f1; }
    elif command -v shasum >/dev/null 2>&1; then
        sum() { shasum -a 256 "$1" | cut -d' ' -f1; }
    elif command -v openssl >/dev/null 2>&1; then
        sum() { openssl dgst -sha256 "$1" | sed 's/.*= *//'; }
    else
        fail "nothing here computes a SHA-256 (sha256sum, shasum or openssl)"
    fi

    if [ "$version" = latest ]; then
        url="$releases/latest/download/$asset"
        # The version the latest release is, from where its URL leads, so that an install that
        # is already that version can stop before fetching anything of size.
        led="$(resolve "$url")"
        case "$led" in
            */download/v*/*) version="$(printf '%s' "$led" | sed 's|.*/download/v\([^/]*\)/.*|\1|')" ;;
        esac
    else
        url="$releases/download/v$version/$asset"
    fi

    installed() { "$dir/fzn" --version 2>/dev/null | sed -n 's/^fzn \([^ ]*\).*/\1/p'; }
    if [ -z "$force" ] && [ "$version" != latest ] && [ -x "$dir/fzn" ] && [ "$(installed)" = "$version" ]; then
        say "fzn $version is already at $dir/fzn"
        exit 0
    fi

    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT INT TERM

    say "fetching $url"
    fetch "$url" "$tmp/$asset" || fail "could not fetch $url"
    fetch "$url.sha256" "$tmp/$asset.sha256" || fail "could not fetch the checksum $url.sha256"
    want="$(cut -d' ' -f1 "$tmp/$asset.sha256")"
    have="$(sum "$tmp/$asset")"
    [ -n "$want" ] && [ "$want" = "$have" ] || fail "the checksum of $asset does not match: $have, not $want"

    tar -xzf "$tmp/$asset" -C "$tmp" || fail "could not unpack $asset"
    [ -f "$tmp/fzn-$target/fzn" ] || fail "$asset holds no fzn-$target/fzn"

    # One rename puts the new binary in place: a running fzn or relay keeps the one it has open.
    # Each step is its own command, so that one that fails stops here and says so.
    $as_root mkdir -p "$dir" || fail "could not make $dir"
    if [ -n "$as_root" ]; then
        # shellcheck disable=SC2016
        $as_root sh -c 'cp "$1" "$2.new" && chmod 0755 "$2.new" && mv -f "$2.new" "$2"' - "$tmp/fzn-$target/fzn" "$dir/fzn" ||
            fail "could not put fzn in $dir"
    else
        cp "$tmp/fzn-$target/fzn" "$dir/fzn.new" || fail "could not write $dir/fzn.new"
        chmod 0755 "$dir/fzn.new" || fail "could not make $dir/fzn.new runnable"
        mv -f "$dir/fzn.new" "$dir/fzn" || fail "could not put fzn in $dir"
    fi
    say "installed $("$dir/fzn" --version 2>/dev/null || echo "fzn") at $dir/fzn"

    case ":$PATH:" in
        *":$dir:"*) ;;
        *)
            say "$dir is not on your PATH; add it:"
            case "${SHELL:-}" in
                */fish) say "  fish_add_path $dir" ;;
                *) say "  export PATH=\"$dir:\$PATH\"" ;;
            esac
            ;;
    esac
}

main "$@"

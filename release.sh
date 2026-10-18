#!/bin/sh
# Builds fzn for a target and lays the result out as a release, under dist/, the way GitHub
# lays releases out, so that install.sh can be pointed at dist/ as it is pointed at GitHub:
#
#   sh release.sh --target aarch64-unknown-linux-musl
#   sh release.sh --target x86_64-unknown-linux-musl --target aarch64-unknown-linux-musl
#   sh release.sh --page
#
# dist/download/v0.35.0/fzn-TARGET.tar.gz is the binary with the licence and README, and
# beside it fzn-TARGET.tar.gz.sha256; --page adds fzn-web.tar.gz, the page as the relay serves
# it, for anyone serving it some other way. dist/latest/download points at the version built.
# SHA256SUMS at the version's root lists everything there. The version is the workspace's, in
# Cargo.toml, and a tag that does not match it is refused (FZN_TAG, which the workflow sets).
#
# The relay carries the page, so the page is built here first (web/build.sh), from the engine
# as it stands, and never taken from a web/pkg left by an older build. The release workflow
# builds the page once and hands it to each job, saying so with FZN_PAGE_BUILT=1; a build
# without web/pkg is refused here, where cargo would only warn. The workflow runs this once a
# target on the machine that can build it; by hand it runs for the Linux targets on any Linux
# with the targets added (rustup target add ...), the aarch64 one linked with rust-lld.
set -eu
cd "$(dirname "$0")"

targets=
page=
while [ $# -gt 0 ]; do
    case "$1" in
        --target) [ $# -ge 2 ] || { echo "release.sh: --target needs a target" >&2; exit 1; }; targets="$targets $2"; shift ;;
        --page) page=1 ;;
        *) echo "release.sh: unknown argument $1" >&2; exit 1 ;;
    esac
    shift
done

version="$(sed -n 's/^version *= *"\([^"]*\)".*/\1/p' Cargo.toml | head -n 1)"
[ -n "$version" ] || { echo "release.sh: no version in Cargo.toml" >&2; exit 1; }
if [ -n "${FZN_TAG:-}" ] && [ "$FZN_TAG" != "v$version" ]; then
    echo "release.sh: the tag $FZN_TAG is not the workspace version v$version" >&2
    exit 1
fi
if [ -z "${FZN_PAGE_BUILT:-}" ]; then
    web/build.sh
fi
[ -f web/pkg/fzn_web_bg.wasm ] || { echo "release.sh: the page is not built; run web/build.sh first" >&2; exit 1; }

out="dist/download/v$version"
mkdir -p "$out"

# What fzn --version says past the version: the commit and the day, from git when there is
# one, else what the environment says (the workflow passes the commit it checked out).
if [ -z "${FZN_BUILD:-}" ] && command -v git >/dev/null 2>&1 && git rev-parse --short=9 HEAD >/dev/null 2>&1; then
    FZN_BUILD="$(git rev-parse --short=9 HEAD)$(git diff --quiet HEAD 2>/dev/null || echo '-dirty') $(date -u +%Y-%m-%d)"
fi
export FZN_BUILD="${FZN_BUILD:-}"
# The relay must carry the page in a release; cargo warns when it is missing, this refuses.
export FZN_PAGE=required

sum() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

# Where cargo puts what it builds, which need not be ./target.
if [ -n "$targets" ]; then
    target_dir="$(cargo metadata --format-version 1 --no-deps | sed 's/.*"target_directory":"\([^"]*\)".*/\1/')"
fi

for target in $targets; do
    case "$target" in
        # No cross gcc is needed: the musl targets carry their own startup files and
        # rust-lld links them, which is how fzn for the relay's machine has always been built.
        # The little C that ring brings for TLS is compiled by clang, which targets either
        # from any Linux, where a gcc for musl is rarely installed.
        aarch64-unknown-linux-musl)
            export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
            export CC_aarch64_unknown_linux_musl=clang ;;
        x86_64-unknown-linux-musl) export CC_x86_64_unknown_linux_musl=clang ;;
    esac
    cargo build --release --locked -p fzn --target "$target"
    name="fzn-$target"
    rm -rf "dist/stage/$name"
    mkdir -p "dist/stage/$name"
    cp "$target_dir/$target/release/fzn" LICENSE README.md "dist/stage/$name/"
    tar -C dist/stage -czf "$out/$name.tar.gz" "$name"
    (cd "$out" && sum "$name.tar.gz" > "$name.tar.gz.sha256")
    rm -rf "dist/stage/$name"
done

if [ -n "$page" ]; then
    rm -rf dist/stage/web
    mkdir -p dist/stage/web/pkg dist/stage/web/vendor/code
    # The page as the relay serves it: these files and no other. The same list is in
    # crates/fzn/src/web_files.rs, by which the relay is built with it.
    for file in web/*.html web/*.js web/*.css web/*.svg web/*.png web/*.ico web/*.webmanifest; do
        if [ -f "$file" ]; then cp "$file" dist/stage/web/; fi
    done
    cp web/pkg/*.js web/pkg/*.wasm dist/stage/web/pkg/
    cp web/vendor/*.js dist/stage/web/vendor/
    cp web/vendor/code/*.js dist/stage/web/vendor/code/
    tar -C dist/stage -czf "$out/fzn-web.tar.gz" web
    (cd "$out" && sum fzn-web.tar.gz > fzn-web.tar.gz.sha256)
    rm -rf dist/stage/web
fi
rmdir dist/stage 2>/dev/null || true

(cd "$out" && sum ./*.tar.gz > SHA256SUMS)
rm -rf dist/latest
mkdir -p dist/latest
ln -s "../download/v$version" dist/latest/download
ls -l "$out"

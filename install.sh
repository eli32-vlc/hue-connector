#!/bin/sh
# hue-connector installer: download the latest release, verify its
# checksum, and install it to ~/.local/bin (no sudo needed).
set -eu

REPO="eli32-vlc/hue-connector"
DEST_DIR="${HOME}/.local/bin"
DEST="$DEST_DIR/hue-connector"

os="$(uname -s)"
case "$os" in
  Darwin) os="darwin" ;;
  Linux) os="linux" ;;
  *) echo "unsupported OS: $os" >&2; exit 1 ;;
esac

arch="$(uname -m)"
case "$arch" in
  arm64|aarch64) arch="arm64" ;;
  x86_64|amd64) arch="amd64" ;;
  *) echo "unsupported arch: $arch" >&2; exit 1 ;;
esac

asset="hue-connector-${os}-${arch}"
echo "Downloading $asset (latest release from $REPO)…"

tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
  | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
[ -n "$tag" ] || { echo "could not resolve latest release" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsSL "https://github.com/$REPO/releases/download/$tag/$asset" -o "$tmp/$asset"
curl -fsSL "https://github.com/$REPO/releases/download/$tag/SHA256SUMS" -o "$tmp/SHA256SUMS"

# Verify the checksum of the asset we downloaded (fail closed).
expected="$(awk -v a="$asset" '$2 == a {print $1}' "$tmp/SHA256SUMS")"
[ -n "$expected" ] || { echo "asset not found in SHA256SUMS" >&2; exit 1; }
if command -v shasum >/dev/null 2>&1; then
  actual="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
elif command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
else
  echo "no checksum tool found; refusing to install unverified binary" >&2; exit 1
fi
[ "$actual" = "$expected" ] || { echo "checksum mismatch" >&2; exit 1; }

mkdir -p "$DEST_DIR"
install -m 0755 "$tmp/$asset" "$DEST"
echo "Installed $DEST (version $tag)"

case "$PATH" in
  *"$DEST_DIR"*) ;;
  *) echo "Note: $DEST_DIR is not on your PATH. Add it, e.g.:"; echo "  export PATH=\"\$HOME/.local/bin:\$PATH\"" ;;
esac

echo "Next: hue-connector pair <server-url> <code>"

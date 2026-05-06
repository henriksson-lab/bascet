#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

targets=(
  x86_64-unknown-linux-gnu
  x86_64-pc-windows-gnu
  x86_64-apple-darwin
  aarch64-apple-darwin
)

echo "Adding Rust targets..."
rustup target add "${targets[@]}"

if cargo --list | grep -q '^    zigbuild'; then
  echo "cargo-zigbuild already installed"
else
  echo "Installing cargo-zigbuild..."
  cargo install cargo-zigbuild --locked
fi

echo
echo "Checking Linux cross-compilers expected by the helper scripts..."

missing=0
for tool in \
  x86_64-w64-mingw32-gcc \
  x86_64-w64-mingw32-ar \
  x86_64-apple-darwin23-clang \
  x86_64-apple-darwin23-ar \
  aarch64-apple-darwin23-clang \
  aarch64-apple-darwin23-ar
do
  if command -v "$tool" >/dev/null 2>&1; then
    echo "  ok  $tool"
  else
    echo "  miss $tool"
    missing=1
  fi
done

if [[ "$missing" -ne 0 ]]; then
  cat <<'EOF'

Some toolchains are missing.

- Windows builds expect `mingw-w64`.
- macOS cross-builds expect an `osxcross` toolchain providing the Apple-prefixed clang/ar binaries used by `scripts/build-macos.sh`.
- Native macOS builds do not use these cross-compilers.
EOF
fi

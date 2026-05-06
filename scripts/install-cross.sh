#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

os="$(uname -s)"

targets=(
  x86_64-unknown-linux-gnu
  x86_64-pc-windows-gnu
  x86_64-apple-darwin
  aarch64-apple-darwin
)

echo "Installing Rust nightly..."
rustup toolchain install nightly

echo "Adding Rust targets..."
rustup target add "${targets[@]}"

if cargo --list | grep -q '^    zigbuild'; then
  echo "cargo-zigbuild already installed"
else
  echo "Installing cargo-zigbuild..."
  cargo install cargo-zigbuild --locked
fi

install_mingw() {
  if command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
    echo "mingw-w64 already installed"
    return 0
  fi

  case "$os" in
    Linux)
      if command -v apt-get >/dev/null 2>&1; then
        echo "Installing mingw-w64 with apt-get..."
        sudo apt-get update
        sudo apt-get install -y mingw-w64
      else
        echo "missing mingw-w64 and no supported package manager was detected" >&2
        echo "install x86_64-w64-mingw32-gcc manually for Windows cross-builds" >&2
      fi
      ;;
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        echo "Installing mingw-w64 with Homebrew..."
        brew install mingw-w64
      else
        echo "Homebrew not found; skipping mingw-w64 install on macOS" >&2
        echo "install mingw-w64 manually if you need Windows cross-builds" >&2
      fi
      ;;
    *)
      echo "unsupported host OS: $os" >&2
      ;;
  esac
}

install_mingw

echo
echo "Checking optional cross-toolchains..."

check_tool() {
  local tool="$1"
  if command -v "$tool" >/dev/null 2>&1; then
    echo "  ok  $tool"
  else
    echo "  miss $tool"
  fi
}

check_tool x86_64-w64-mingw32-gcc
check_tool x86_64-w64-mingw32-ar
check_tool x86_64-apple-darwin23-clang
check_tool x86_64-apple-darwin23-ar
check_tool aarch64-apple-darwin23-clang
check_tool aarch64-apple-darwin23-ar

cat <<'EOF'

Native builds:
- `cargo +nightly build --profile=release` uses the host toolchain on macOS and Linux.

Cross-builds:
- Windows cross-builds need `mingw-w64`.
- Linux to macOS cross-builds need an `osxcross` toolchain matching `scripts/build-macos.sh`.
- macOS hosts do not need `osxcross` for native macOS-target builds.
EOF

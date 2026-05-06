#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

profile="${CROSS_PROFILE:-release}"
package="${CROSS_PACKAGE:-bascet-cli}"
features="${CROSS_FEATURES:---all-features}"
target="${1:-x86_64-pc-windows-gnu}"

if [[ "$target" != "x86_64-pc-windows-gnu" ]]; then
  echo "usage: $0 [x86_64-pc-windows-gnu]" >&2
  exit 1
fi

if ! command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
  echo "missing x86_64-w64-mingw32-gcc; install mingw-w64 first" >&2
  exit 1
fi

echo "Building $target with: cargo build"
cargo build --profile="$profile" --target "$target" -p "$package" $features

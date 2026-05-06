#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

profile="${CROSS_PROFILE:-release}"
package="${CROSS_PACKAGE:-bascet-cli}"
features="${CROSS_FEATURES:---all-features}"
target="${1:-x86_64-unknown-linux-gnu}"
host_os="$(uname -s)"

if [[ "$target" != "x86_64-unknown-linux-gnu" ]]; then
  echo "usage: $0 [x86_64-unknown-linux-gnu]" >&2
  exit 1
fi

build_tool=()
if cargo --list | grep -q '^    zigbuild'; then
  build_tool=(cargo zigbuild)
elif [[ "$host_os" == "Linux" ]]; then
  build_tool=(cargo build)
else
  echo "cargo-zigbuild is required to build $target from $host_os" >&2
  exit 1
fi

echo "Building $target with: ${build_tool[*]}"
"${build_tool[@]}" --profile="$profile" --target "$target" -p "$package" $features

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

profile="${CROSS_PROFILE:-release}"
package="${CROSS_PACKAGE:-bascet-cli}"
features="${CROSS_FEATURES:---all-features}"
target="${1:-x86_64-unknown-linux-gnu}"
host_os="$(uname -s)"
linux_cflags="${LINUX_CFLAGS:--mno-avx512f -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX512VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX_VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_VPCLMULQDQ}"

if [[ "$target" != "x86_64-unknown-linux-gnu" ]]; then
  echo "usage: $0 [x86_64-unknown-linux-gnu]" >&2
  exit 1
fi

build_tool=()
target_args=(--target "$target")
if [[ "$host_os" == "Linux" ]]; then
  build_tool=(cargo build)
  target_args=()
elif cargo --list | grep -q '^    zigbuild'; then
  build_tool=(cargo zigbuild)
else
  echo "cargo-zigbuild is required to build $target from $host_os" >&2
  exit 1
fi

echo "Building $target with: ${build_tool[*]}"
export CFLAGS_x86_64_unknown_linux_gnu="${linux_cflags} ${CFLAGS_x86_64_unknown_linux_gnu:-}"
"${build_tool[@]}" --profile="$profile" "${target_args[@]}" -p "$package" $features

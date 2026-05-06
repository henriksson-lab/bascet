#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

profile="${CROSS_PROFILE:-release}"
package="${CROSS_PACKAGE:-bascet-cli}"
bin_name="${BASCET_BIN:-bascet}"
features="${CROSS_FEATURES:---all-features}"
target="${1:-x86_64-apple-darwin}"
target_env="${target//-/_}"
target_env_upper="$(printf '%s' "$target_env" | tr '[:lower:]' '[:upper:]')"
host_os="$(uname -s)"
build_tool=(cargo build)
use_zigbuild=0
if [[ "$host_os" == "Linux" ]] && cargo --list | grep -q '^    zigbuild'; then
  build_tool=(cargo zigbuild)
  use_zigbuild=1
fi

case "$target" in
  x86_64-apple-darwin)
    target_cc="x86_64-apple-darwin23-clang"
    target_ar="x86_64-apple-darwin23-ar"
    target_cxx="x86_64-apple-darwin23-clang++"
    extra_cflags="-DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX512VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX_VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_VPCLMULQDQ"
    ;;
  aarch64-apple-darwin)
    target_cc="aarch64-apple-darwin23-clang"
    target_ar="aarch64-apple-darwin23-ar"
    target_cxx="aarch64-apple-darwin23-clang++"
    extra_cflags=""
    ;;
  universal)
    "$(dirname "$0")/build-macos.sh" x86_64-apple-darwin
    "$(dirname "$0")/build-macos.sh" aarch64-apple-darwin
    lipo_bin="${LIPO:-$(command -v llvm-lipo 2>/dev/null || command -v llvm-lipo-14 2>/dev/null || command -v lipo 2>/dev/null || true)}"
    if [[ -z "$lipo_bin" ]]; then
      echo "missing lipo/llvm-lipo for universal build" >&2
      exit 1
    fi
    out_dir="target/universal-apple-darwin/$profile"
    mkdir -p "$out_dir"
    "$lipo_bin" -create \
      "target/x86_64-apple-darwin/$profile/$bin_name" \
      "target/aarch64-apple-darwin/$profile/$bin_name" \
      -output "$out_dir/$bin_name"
    echo "built $out_dir/$bin_name"
    exit 0
    ;;
  *)
    echo "usage: $0 [x86_64-apple-darwin|aarch64-apple-darwin|universal]" >&2
    exit 1
    ;;
esac

if [[ "$host_os" == "Linux" && "$use_zigbuild" -eq 0 ]]; then
  export "CARGO_TARGET_${target_env_upper}_LINKER=${target_cc}"
  export "CC_${target_env}=${target_cc}"
  export "AR_${target_env}=${target_ar}"
  export "CXX_${target_env}=${target_cxx}"
fi
if [[ -n "$extra_cflags" ]]; then
  export "CFLAGS_${target_env}=${extra_cflags} ${CFLAGS_x86_64_apple_darwin:-}"
fi

echo "Building $target with: ${build_tool[*]}"
"${build_tool[@]}" --profile="$profile" --target "$target" -p "$package" $features

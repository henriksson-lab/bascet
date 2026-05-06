<p align="center">
<img src="./assets/img/Bascet.png" alt="Bascet logo" style="height: 50%; width: 50%;"/>
</p>

# Bascet (Bacterial Single CEll Toolkit)

Bascet is a complete solution for single-cell analysis, with focus on microbial analysis.
It is however design to also do RNA-seq, ATAC-seq (you-name-it), agnostic from the instrument used.
It has also been designed to analyze large numbers of bulk samples in a manner analogous to single-cell analysis
(i.e. with a focus on clustering and data-driven analysis).

Bascet is an advanced command-line tool aimed primarily to be used through the Zorn R library, which offers multi-node compute capability and ease of use.

Most users will prefer to use Zorn. For more information, follow this link: https://henriksson-lab.github.io/zorn/

## Cross-compiling

Use the helper scripts instead of the `Makefile`:

```bash
make install_cross
./scripts/build-linux.sh x86_64-unknown-linux-gnu
./scripts/build-windows.sh x86_64-pc-windows-gnu
./scripts/build-macos.sh x86_64-apple-darwin
./scripts/build-macos.sh aarch64-apple-darwin
./scripts/build-macos.sh universal
```

Notes:

- Windows builds still expect `mingw-w64`.
- macOS-to-Linux builds use `cargo zigbuild` when available.
- Native macOS builds use the platform default linker when you run `cargo +nightly build --profile=release`.
- Linux-to-macOS cross-builds expect `osxcross` to provide binaries such as `x86_64-apple-darwin23-clang`.
- If `cargo zigbuild` is installed, the build script will use it automatically.
- `make install_cross` is now the portable entry point and delegates to `scripts/install-cross.sh`.

CARGO ?= cargo
ZIGBUILD ?= cargo zigbuild
RUSTUP ?= rustup
LIPO ?= $(shell command -v llvm-lipo 2>/dev/null || command -v llvm-lipo-14 2>/dev/null || command -v lipo 2>/dev/null || printf lipo)
XDG_CACHE_HOME ?= $(CURDIR)/.tmp/xdg-cache
CROSS_PROFILE ?= release
CROSS_PACKAGE ?= bascet-cli
CROSS_FEATURES ?= --all-features
BASCET_BIN ?= bascet
MAC_UNIVERSAL_OUT ?= target/universal-apple-darwin/$(CROSS_PROFILE)/$(BASCET_BIN)
LINUX_TARGET ?= x86_64-unknown-linux-gnu
LINUX_BIN_OUT ?= target/$(CROSS_PROFILE)/$(BASCET_BIN)
WINDOWS_BIN_OUT ?= target/x86_64-pc-windows-gnu/$(CROSS_PROFILE)/$(BASCET_BIN).exe
BINS_PUBLISH_DIR ?= /corgi/public_http/public/bascet/bins
LINUX_PUBLISH_BIN ?= bascet-linux-x86_64
WINDOWS_PUBLISH_BIN ?= bascet-windows-x86_64.exe
MAC_PUBLISH_BIN ?= bascet-macos-universal
LINUX_TARGETS ?= $(LINUX_TARGET)
WINDOWS_TARGETS ?= x86_64-pc-windows-gnu
MAC_TARGETS ?= \
	x86_64-apple-darwin \
	aarch64-apple-darwin
CROSS_TARGETS ?= $(LINUX_TARGETS) $(WINDOWS_TARGETS) $(MAC_TARGETS)
MACOS_X86_CFLAGS ?= -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX512VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX_VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_VPCLMULQDQ
LINUX_CFLAGS ?= -mno-avx512f -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX512VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX_VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_VPCLMULQDQ

.PHONY: all test fix install_rust install_cross install_crosscompile install_mingw loc all_linux all_win all_windows cross cross_targets all_mac linux_release mac_universal publish_bins FORCE

all:
	cargo +nightly build --profile=release

test:
	cargo +nightly test

fix:
	cargo +nightly fix --lib -p bascet --allow-dirty

install_rust:
	rustup toolchain install nightly

install_cross install_crosscompile:
	./scripts/install-cross.sh

loc:
	wc -l \
	src/*.rs \
	src/*/*.rs \
	src/*/*/*.rs \
	src/*/*/*/*.rs \
	src/*/*.sh \
	bascet*/*.rs \
	bascet*/*/*.rs \
	bascet*/*/*/*.rs \
	bascet*/*/*/*/*.rs

all_win all_windows: $(addprefix cross-,$(WINDOWS_TARGETS))

all_linux: $(addprefix cross-,$(LINUX_TARGETS))

all_mac: $(addprefix cross-,$(MAC_TARGETS))

linux_release:
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" CROSS_PROFILE="$(CROSS_PROFILE)" CROSS_PACKAGE="$(CROSS_PACKAGE)" CROSS_FEATURES="$(CROSS_FEATURES)" ./scripts/build-linux.sh x86_64-unknown-linux-gnu

mac_universal: all_mac
	CROSS_PROFILE="$(CROSS_PROFILE)" CROSS_PACKAGE="$(CROSS_PACKAGE)" CROSS_FEATURES="$(CROSS_FEATURES)" BASCET_BIN="$(BASCET_BIN)" LIPO="$(LIPO)" ./scripts/build-macos.sh universal

publish_bins: linux_release all_win mac_universal
	mkdir -p $(BINS_PUBLISH_DIR)
	cp $(LINUX_BIN_OUT) $(BINS_PUBLISH_DIR)/$(LINUX_PUBLISH_BIN)
	cp $(WINDOWS_BIN_OUT) $(BINS_PUBLISH_DIR)/$(WINDOWS_PUBLISH_BIN)
	cp $(MAC_UNIVERSAL_OUT) $(BINS_PUBLISH_DIR)/$(MAC_PUBLISH_BIN)
	cd $(BINS_PUBLISH_DIR) && md5sum $(LINUX_PUBLISH_BIN) > $(LINUX_PUBLISH_BIN).md5
	cd $(BINS_PUBLISH_DIR) && md5sum $(WINDOWS_PUBLISH_BIN) > $(WINDOWS_PUBLISH_BIN).md5
	cd $(BINS_PUBLISH_DIR) && md5sum $(MAC_PUBLISH_BIN) > $(MAC_PUBLISH_BIN).md5

cross: $(addprefix cross-,$(CROSS_TARGETS))

cross_targets:
	$(RUSTUP) target add $(CROSS_TARGETS)

cross-%: FORCE
	$(CARGO) build --profile=$(CROSS_PROFILE) --target $* -p $(CROSS_PACKAGE) $(CROSS_FEATURES)

cross-x86_64-unknown-linux-gnu: FORCE
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" CROSS_PROFILE="$(CROSS_PROFILE)" CROSS_PACKAGE="$(CROSS_PACKAGE)" CROSS_FEATURES="$(CROSS_FEATURES)" ./scripts/build-linux.sh x86_64-unknown-linux-gnu

cross-x86_64-pc-windows-gnu: FORCE
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" CROSS_PROFILE="$(CROSS_PROFILE)" CROSS_PACKAGE="$(CROSS_PACKAGE)" CROSS_FEATURES="$(CROSS_FEATURES)" ./scripts/build-windows.sh x86_64-pc-windows-gnu

cross-x86_64-apple-darwin: FORCE
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" CROSS_PROFILE="$(CROSS_PROFILE)" CROSS_PACKAGE="$(CROSS_PACKAGE)" CROSS_FEATURES="$(CROSS_FEATURES)" BASCET_BIN="$(BASCET_BIN)" ./scripts/build-macos.sh x86_64-apple-darwin

cross-aarch64-apple-darwin: FORCE
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" CROSS_PROFILE="$(CROSS_PROFILE)" CROSS_PACKAGE="$(CROSS_PACKAGE)" CROSS_FEATURES="$(CROSS_FEATURES)" BASCET_BIN="$(BASCET_BIN)" ./scripts/build-macos.sh aarch64-apple-darwin

FORCE:

publish_test:
	# find . -name .DS_Store -print0 | xargs -0 git rm -f --ignore-unmatch
	cargo publish --dry-run

install_mingw:
	./scripts/install-cross.sh

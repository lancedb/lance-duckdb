.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=lance
# The Rust crate name is different from the extension name
RUST_CRATE_NAME=lance_duckdb

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.3.2

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile

# Override the library filename since our Rust crate name differs from extension name
ifeq ($(OS),Windows_NT)
	EXTENSION_LIB_FILENAME=$(RUST_CRATE_NAME).dll
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        EXTENSION_LIB_FILENAME=lib$(RUST_CRATE_NAME).so
    endif
    ifeq ($(UNAME_S),Darwin)
        EXTENSION_LIB_FILENAME=lib$(RUST_CRATE_NAME).dylib
    endif
endif

include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug

test_debug: debug test_extension_debug
test_release: release test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean

#!/usr/bin/env bash
# Shared build configuration for every shell build entry point.
#
# This file intentionally contains configuration and discovery only. The
# caller owns error handling, dependency checks, compilation, and test
# execution policy.

if [[ -n "${DBMS_BUILD_COMMON_LOADED:-}" ]]; then
    return 0 2>/dev/null || exit 0
fi
DBMS_BUILD_COMMON_LOADED=1

dbms_init_build_config() {
    local source_dir="${1:?repository root is required}"
    DBMS_SOURCE_DIR="$source_dir"
    DBMS_MANIFEST="${DBMS_SOURCE_DIR}/cmake/dbms_sources.txt"

    DBMS_PRODUCTION_INCLUDES=(
        -Isrc
        -Isrc/common
        -Isrc/storage
        -Isrc/access
        -Isrc/transaction
        -Isrc/network
        -Isrc/utils
        -Isrc/executor
        -Isrc/commands
        -Isrc/interfaces
        -Isrc/parser
        -Isrc/catalog
        -Isrc/expression
        -Isrc/replication
        -Isrc/process
    )
    DBMS_TEST_INCLUDES=("${DBMS_PRODUCTION_INCLUDES[@]}" -Itests)
    DBMS_CXXFLAGS=(-std=c++17 -O2 -pthread -Wall -Wextra)
    DBMS_LDFLAGS=(-pthread)

    if pkg-config --exists openssl 2>/dev/null; then
        DBMS_HAS_OPENSSL=1
        DBMS_TLS_SOURCE="src/network/TLSWrapper.cpp"
        DBMS_CXXFLAGS+=(-DHAS_OPENSSL=1)
        DBMS_LDFLAGS+=(-lssl -lcrypto)
        DBMS_TLS_STATUS="OpenSSL detected, TLS support enabled"
    else
        DBMS_HAS_OPENSSL=0
        DBMS_TLS_SOURCE="src/network/TLSWrapper_stub.cpp"
        DBMS_TLS_STATUS="OpenSSL not found, using TLS stub (plain TCP)"
    fi

    mapfile -t DBMS_MANIFEST_SOURCES < <(
        sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' "$DBMS_MANIFEST"
    )
}

dbms_main_sources() {
    DBMS_MAIN_SOURCES=("${DBMS_MANIFEST_SOURCES[@]}" "$DBMS_TLS_SOURCE")
}

dbms_test_project_sources() {
    DBMS_PROJECT_SOURCES=()
    local source
    for source in "${DBMS_MANIFEST_SOURCES[@]}"; do
        [[ "$source" == "src/main.cpp" ]] || DBMS_PROJECT_SOURCES+=("$source")
    done
    DBMS_PROJECT_SOURCES+=("$DBMS_TLS_SOURCE")
}

dbms_print_tls_status() {
    local prefix="${1:-build}"
    echo "[${prefix}] ${DBMS_TLS_STATUS}"
}

dbms_newest_header() {
    local newest=""
    local header
    while IFS= read -r header; do
        if [[ -z "$newest" || "$header" -nt "$newest" ]]; then
            newest="$header"
        fi
    done < <(find "${DBMS_SOURCE_DIR}/src" -type f \( -name '*.h' -o -name '*.hpp' \) -print)
    printf '%s' "$newest"
}

dbms_cache_signature() {
    {
        printf '%s\n' "${DBMS_CXXFLAGS[@]}"
        printf '%s\n' "${DBMS_PRODUCTION_INCLUDES[@]}"
        printf '%s\n' "${DBMS_TEST_INCLUDES[@]}"
        printf '%s\n' "${DBMS_LDFLAGS[@]}"
        printf '%s\n' "$DBMS_TLS_SOURCE"
    } | sha256sum | awk '{print $1}'
}

dbms_cache_needs_rebuild() {
    local cache_dir="${1:?cache directory is required}"
    local marker="${cache_dir}/.build-config.sha256"
    [[ ! -f "$marker" || "$(<"$marker")" != "$(dbms_cache_signature)" ]]
}

dbms_write_cache_signature() {
    local cache_dir="${1:?cache directory is required}"
    mkdir -p "$cache_dir"
    printf '%s\n' "$(dbms_cache_signature)" > "${cache_dir}/.build-config.sha256"
}

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
    DBMS_E2E_TESTS=(tests/postgres_protocol_test.py tests/window_e2e_test.py)
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

    if pkg-config --exists zlib 2>/dev/null; then
        DBMS_HAS_ZLIB=1
        DBMS_CXXFLAGS+=(-DHAS_ZLIB=1)
        DBMS_LDFLAGS+=(-lz)
        DBMS_ZLIB_STATUS="zlib detected, TOAST compression enabled"
    else
        echo "[build] zlib is required for production TOAST compression" >&2
        return 1
    fi

    mapfile -t DBMS_MANIFEST_SOURCES < <(
        sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' "$DBMS_MANIFEST"
    )
}

dbms_main_sources() {
    DBMS_MAIN_SOURCES=("${DBMS_MANIFEST_SOURCES[@]}" "$DBMS_TLS_SOURCE")
}

dbms_main_needs_rebuild() {
    local binary="${DBMS_SOURCE_DIR}/dbms_main"
    local stamp="${DBMS_SOURCE_DIR}/build/.dbms_main-build-config.sha256"
    local source header

    [[ ! -x "$binary" ]] && return 0
    [[ ! -f "$stamp" || "$(<"$stamp")" != "$(dbms_cache_signature)" ]] && return 0
    [[ "${DBMS_MANIFEST}" -nt "$binary" ]] && return 0

    for source in "${DBMS_MAIN_SOURCES[@]}"; do
        [[ "${DBMS_SOURCE_DIR}/${source}" -nt "$binary" ]] && return 0
    done
    while IFS= read -r header; do
        [[ "$header" -nt "$binary" ]] && return 0
    done < <(find "${DBMS_SOURCE_DIR}/src" -type f \( -name '*.h' -o -name '*.hpp' \) -print)
    return 1
}

dbms_build_main() {
    local binary="${DBMS_SOURCE_DIR}/dbms_main"
    local stamp="${DBMS_SOURCE_DIR}/build/.dbms_main-build-config.sha256"
    local temporary_binary="${DBMS_SOURCE_DIR}/build/.dbms_main.tmp.$$"

    mkdir -p "${DBMS_SOURCE_DIR}/build"
    if ! dbms_main_needs_rebuild; then
        echo "[build] dbms_main is up to date"
        return 0
    fi

    echo "[build] Compiling production binary..."
    if ! (cd "${DBMS_SOURCE_DIR}" && \
        g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_PRODUCTION_INCLUDES[@]}" \
            "${DBMS_MAIN_SOURCES[@]}" -o "$temporary_binary" "${DBMS_LDFLAGS[@]}"); then
        rm -f -- "$temporary_binary"
        echo "[build] Production binary compilation failed" >&2
        return 1
    fi
    if ! mv -f -- "$temporary_binary" "$binary"; then
        rm -f -- "$temporary_binary"
        echo "[build] Could not publish production binary" >&2
        return 1
    fi
    printf '%s\n' "$(dbms_cache_signature)" > "${stamp}.tmp"
    if ! mv -f -- "${stamp}.tmp" "$stamp"; then
        rm -f -- "${stamp}.tmp"
        echo "[build] Could not publish production build stamp" >&2
        return 1
    fi
    echo "[build] Success: ./dbms_main"
}

dbms_run_isolated_test() {
    local name="${1:?test name is required}"
    local binary="${2:?test binary is required}"
    local work_dir
    local status=0

    work_dir="$(mktemp -d "${TMPDIR:-/tmp}/dbms-test-${name}.XXXXXX")" || return 1
    (cd "$work_dir" && "$binary") || status=$?
    if ! rm -rf -- "$work_dir"; then
        echo "[test-build] Could not clean isolated directory: ${work_dir}" >&2
        status=1
    fi
    return "$status"
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
    echo "[${prefix}] ${DBMS_ZLIB_STATUS}"
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
        printf '%s\n' "$DBMS_HAS_ZLIB"
        printf 'manifest\n'
        sha256sum -- "$DBMS_MANIFEST"

        # Timestamps are not a safe cache validity boundary: a Git checkout,
        # restore, or copied workspace can leave an older source with a newer
        # mtime than the object file. Include the actual production inputs so
        # stale objects can never be reused after source content changes.
        local source header
        for source in "${DBMS_MANIFEST_SOURCES[@]}" "$DBMS_TLS_SOURCE"; do
            printf 'source %s\n' "$source"
            sha256sum -- "${DBMS_SOURCE_DIR}/${source}"
        done
        while IFS= read -r header; do
            printf 'header %s\n' "${header#"${DBMS_SOURCE_DIR}/"}"
            sha256sum -- "$header"
        done < <(find "${DBMS_SOURCE_DIR}/src" -type f \( -name '*.h' -o -name '*.hpp' \) -print | sort)
    } | sha256sum | awk '{print $1}'
}

dbms_test_cache_signature() {
    {
        printf 'production\n%s\n' "$(dbms_cache_signature)"
        local test_file
        while IFS= read -r test_file; do
            printf 'test %s\n' "${test_file#"${DBMS_SOURCE_DIR}/"}"
            sha256sum -- "$test_file"
        done < <(find "${DBMS_SOURCE_DIR}/tests" -maxdepth 1 -type f \
            \( -name '*_test.cpp' -o -name 'test_stubs.cpp' \) -print | sort)
    } | sha256sum | awk '{print $1}'
}

dbms_cache_needs_rebuild() {
    local cache_dir="${1:?cache directory is required}"
    local marker="${cache_dir}/.build-config.sha256"
    [[ ! -f "$marker" || "$(<"$marker")" != "$(dbms_cache_signature)" ]]
}

dbms_test_cache_needs_rebuild() {
    local cache_dir="${1:?cache directory is required}"
    local marker="${cache_dir}/.test-build-config.sha256"
    [[ ! -f "$marker" || "$(<"$marker")" != "$(dbms_test_cache_signature)" ]]
}

dbms_write_cache_signature() {
    local cache_dir="${1:?cache directory is required}"
    mkdir -p "$cache_dir"
    printf '%s\n' "$(dbms_cache_signature)" > "${cache_dir}/.build-config.sha256"
}

dbms_write_test_cache_signature() {
    local cache_dir="${1:?cache directory is required}"
    mkdir -p "$cache_dir"
    printf '%s\n' "$(dbms_test_cache_signature)" > "${cache_dir}/.test-build-config.sha256"
}

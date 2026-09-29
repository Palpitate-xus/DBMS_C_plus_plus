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
    DBMS_E2E_TESTS=(tests/postgres_protocol_test.py tests/window_e2e_test.py tests/inherit_only_e2e_test.py tests/explain_analyze_e2e_test.py tests/unnest_e2e_test.py tests/timestamptz_e2e_test.py tests/multijoin_e2e_test.py tests/div14_feature_gate_test.py)
    DBMS_E2E_TESTS+=(tests/review_sql_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/cte_clause_boundary_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/fetch_clause_boundary_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/subquery_sqlstate_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/sql_literal_preservation_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/boolean_literal_boundary_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/limit_offset_boundary_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/sql_whitespace_boundary_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/quoted_alias_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/fromless_structured_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/derived_type_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/window_type_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/join_type_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/dml_command_tag_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/dml_cte_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/transaction_isolation_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/table_structured_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/values_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/set_operation_structured_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/sequence_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/bit_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/network_types_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/geometric_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/xml_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/prepared_statement_lifecycle_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/copy_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/matview_refresh_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/create_database_options_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/tablespace_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/comment_security_label_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/merge_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/update_delete_from_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/unique_update_batch_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/returning_old_new_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/function_result_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/procedure_replace_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/gap_progress_test.py tests/pg_diff_runner_test.py)
    DBMS_E2E_TESTS+=(tests/build_cache_routing_test.py)
    DBMS_E2E_TESTS+=(tests/main_build_incremental_test.py)
    DBMS_E2E_TESTS+=(tests/e2e_binary_routing_test.py)
    DBMS_E2E_TESTS+=(tests/version_consistency_test.py)
    DBMS_E2E_TESTS+=(tests/documentation_status_test.py)
    DBMS_E2E_TESTS+=(tests/compatibility_contract_test.py)
    DBMS_E2E_TESTS+=(tests/data_directory_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/advisory_lock_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/for_share_nowait_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/row_lock_timeout_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/row_lock_deadlock_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/dml_row_lock_timeout_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/dml_row_deadlock_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/table_lock_timeout_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/transaction_select_table_lock_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/transaction_ddl_upgrade_timeout_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/drop_database_idle_connection_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/alter_database_rename_connection_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/alter_database_rename_trailing_tokens_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/database_options_atomic_write_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/database_options_concurrent_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/database_options_read_failure_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/database_directory_guard_protocol_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/alter_table_set_schema_guard_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/rename_schema_guard_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/cli_error_recovery_e2e_test.py)
    DBMS_E2E_TESTS+=(tests/no_fake_compat_objects_test.py)
    DBMS_E2E_TESTS+=(tests/compat_fallback_registry_test.py)
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

    if ! pkg-config --exists icu-i18n 2>/dev/null; then
        echo "[build] ICU i18n is required for named timezone rules" >&2
        return 1
    fi
    DBMS_CXXFLAGS+=(-DHAS_ICU=1)
    local icu_cflags icu_libs
    read -r -a icu_cflags <<< "$(pkg-config --cflags icu-i18n)"
    read -r -a icu_libs <<< "$(pkg-config --libs icu-i18n)"
    DBMS_CXXFLAGS+=("${icu_cflags[@]}")
    DBMS_LDFLAGS+=("${icu_libs[@]}")

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
    [[ "${DBMS_SOURCE_DIR}/scripts/build_common.sh" -nt "$binary" ]] && return 0

    for source in "${DBMS_MAIN_SOURCES[@]}"; do
        [[ ! -f "${DBMS_SOURCE_DIR}/build/main_obj/${source}.o" ]] && return 0
        [[ "${DBMS_SOURCE_DIR}/${source}" -nt "$binary" ]] && return 0
    done
    while IFS= read -r header; do
        [[ "$header" -nt "$binary" ]] && return 0
    done < <(find "${DBMS_SOURCE_DIR}/src" -type f \( -name '*.h' -o -name '*.hpp' \) -print)
    return 1
}

dbms_main_compile_signature() {
    {
        command -v g++
        g++ -dumpfullversion -dumpversion
        printf '%s\n' "${DBMS_CXXFLAGS[@]}"
        printf '%s\n' "${DBMS_PRODUCTION_INCLUDES[@]}"
        local header
        while IFS= read -r header; do
            sha256sum -- "$header"
        done < <(find "${DBMS_SOURCE_DIR}/src" -type f \( -name '*.h' -o -name '*.hpp' \) -print | sort)
    } | sha256sum | awk '{print $1}'
}

dbms_main_object_signature() {
    local source="${1:?source is required}"
    local compile_signature="${2:?compile signature is required}"
    {
        printf '%s\n' "$compile_signature" "$source"
        sha256sum -- "${DBMS_SOURCE_DIR}/${source}"
    } | sha256sum | awk '{print $1}'
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

    local initial_signature compile_signature source object object_stamp
    local expected_signature temporary_object
    local -a objects=()
    initial_signature="$(dbms_cache_signature)"
    compile_signature="$(dbms_main_compile_signature)"
    for source in "${DBMS_MAIN_SOURCES[@]}"; do
        object="${DBMS_SOURCE_DIR}/build/main_obj/${source}.o"
        object_stamp="${object}.sha256"
        expected_signature="$(dbms_main_object_signature "$source" "$compile_signature")"
        mkdir -p -- "$(dirname "$object")"
        if [[ ! -f "$object" || ! -f "$object_stamp" ||
              "$(<"$object_stamp")" != "$expected_signature" ]]; then
            temporary_object="${object}.tmp.$$"
            echo "[build] Compiling ${source}"
            if ! (cd "${DBMS_SOURCE_DIR}" &&
                g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_PRODUCTION_INCLUDES[@]}" \
                    -c "$source" -o "$temporary_object"); then
                rm -f -- "$temporary_object"
                echo "[build] Production object compilation failed: ${source}" >&2
                return 1
            fi
            if [[ "$(dbms_main_object_signature "$source" "$compile_signature")" != "$expected_signature" ]]; then
                rm -f -- "$temporary_object"
                echo "[build] Source changed during compilation: ${source}" >&2
                return 1
            fi
            mv -f -- "$temporary_object" "$object"
            printf '%s\n' "$expected_signature" > "${object_stamp}.tmp.$$"
            mv -f -- "${object_stamp}.tmp.$$" "$object_stamp"
        fi
        objects+=("$object")
    done
    if [[ "$(dbms_cache_signature)" != "$initial_signature" ]]; then
        echo "[build] Inputs changed during compilation" >&2
        return 1
    fi
    echo "[build] Linking production binary..."
    if ! g++ "${objects[@]}" -o "$temporary_binary" "${DBMS_LDFLAGS[@]}"; then
        rm -f -- "$temporary_binary"
        echo "[build] Production binary link failed" >&2
        return 1
    fi
    if [[ "$(dbms_cache_signature)" != "$initial_signature" ]]; then
        rm -f -- "$temporary_binary"
        echo "[build] Inputs changed during linking" >&2
        return 1
    fi
    if ! mv -f -- "$temporary_binary" "$binary"; then
        rm -f -- "$temporary_binary"
        echo "[build] Could not publish production binary" >&2
        return 1
    fi
    printf '%s\n' "$initial_signature" > "${stamp}.tmp"
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

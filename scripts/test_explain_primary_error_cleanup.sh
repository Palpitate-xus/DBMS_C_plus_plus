#!/usr/bin/env bash
# Same-TU real-main driver: no copied publishExplainPlan body, program-entry
# invocation, or test stubs. --sanitize instruments the entire main TU/driver;
# the other 57 production TUs retain their audited normal compilation flags.
# The test-only same-TU driver uses explicit -O0 to avoid optimizing the unused
# CLI/server entry; the independent production main remains the official -O2.
set -euo pipefail
task_repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$task_repo"
source scripts/build_common.sh
dbms_init_build_config "$task_repo"
dbms_main_sources
dbms_build_main
task_sig=$(dbms_main_compile_signature)
task_objects=()
for task_source in "${DBMS_MAIN_SOURCES[@]}"; do
    task_object="build/main_obj/$task_source.o"
    [[ "$(<"$task_object.sha256")" == "$(dbms_main_object_signature "$task_source" "$task_sig")" ]]
    [[ "$task_source" == src/main.cpp ]] || task_objects+=("$task_object")
done
[[ "${#task_objects[@]}" == 57 ]]
[[ "$(<build/.dbms_main-build-config.sha256)" == "$(dbms_cache_signature)" ]]
task_mode=normal
task_flags=("${DBMS_CXXFLAGS[@]}" -O0)
if [[ "${1:-}" == --sanitize && "$#" == 1 ]]; then
    task_mode=scoped-san
    task_flags+=(-g1 -fsanitize=address,undefined -fno-omit-frame-pointer)
elif [[ "$#" != 0 ]]; then
    echo 'usage: bash scripts/test_explain_primary_error_cleanup.sh [--sanitize]' >&2
    exit 2
fi
task_driver=tests/frontend/explain_primary_error_cleanup.cpp
task_object="build/explain_primary_cleanup.$task_mode.o"
task_binary="build/explain_primary_cleanup.$task_mode"
task_driver_sig=$({ printf '%s\n' "$task_sig" "${task_flags[@]}"; sha256sum "$task_driver" src/main.cpp; } | sha256sum | awk '{print $1}')
if [[ ! -f "$task_object" || ! -f "$task_object.sha256" || "$(<"$task_object.sha256")" != "$task_driver_sig" ]]; then
    g++ "${task_flags[@]}" "${DBMS_TEST_INCLUDES[@]}" -c "$task_driver" -o "$task_object"
    printf '%s\n' "$task_driver_sig" > "$task_object.sha256"
fi
g++ "${task_flags[@]}" "$task_object" "${task_objects[@]}" "${DBMS_LDFLAGS[@]}" -o "$task_binary"
echo "[explain-cleanup] real main.cpp same-TU; 57 matched objects; no test_stubs; renamed program entry never invoked; mode=$task_mode"
# The real main TU validates its data directory before any global engine is
# initialized. Relative '.' resolves inside the fresh isolated test cwd.
DBMS_DATA_DIR=. dbms_run_isolated_test "explain_primary_cleanup.$task_mode" "$task_repo/$task_binary"

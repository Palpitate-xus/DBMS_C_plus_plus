#!/usr/bin/env bash
# Real Main dispatcher, no test stubs. Only the test TU uses -O0.
set -euo pipefail
task_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$task_root"
source scripts/build_common.sh
dbms_init_build_config "$task_root"
dbms_main_sources
dbms_build_main
task_signature=$(dbms_main_compile_signature)
task_objects=()
for task_source in "${DBMS_MAIN_SOURCES[@]}"; do
    task_object="$task_root/build/main_obj/$task_source.o"
    [[ "$(<"$task_object.sha256")" == "$(dbms_main_object_signature "$task_source" "$task_signature")" ]]
    [[ "$task_source" == src/main.cpp ]] || task_objects+=("$task_object")
done
[[ ${#task_objects[@]} -gt 0 ]]
task_binary="$task_root/build/values_bit_result_frontend"
g++ "${DBMS_CXXFLAGS[@]}" -O0 "${DBMS_TEST_INCLUDES[@]}" \
    tests/frontend/values_bit_result.cpp "${task_objects[@]}" \
    "${DBMS_LDFLAGS[@]}" -o "$task_binary"
DBMS_DATA_DIR=. dbms_run_isolated_test values_bit_result_frontend "$task_binary"

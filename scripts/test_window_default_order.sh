#!/usr/bin/env bash
# Link the real Main frontend with normal production objects, without stubs.
set -euo pipefail
task_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$task_root"
source scripts/build_common.sh
dbms_init_build_config "$task_root"
dbms_main_sources
dbms_test_project_sources
dbms_build_main
task_signature=$(dbms_main_compile_signature)
task_objects=()
for task_source in "${DBMS_PROJECT_SOURCES[@]}"; do
    task_object="$task_root/build/main_obj/$task_source.o"
    [[ "$(<"$task_object.sha256")" == "$(dbms_main_object_signature "$task_source" "$task_signature")" ]]
    task_objects+=("$task_object")
done
task_driver=tests/frontend/window_default_order.cpp
task_binary="$task_root/build/window_default_order_frontend"
g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" "$task_driver" \
    "${task_objects[@]}" "${DBMS_LDFLAGS[@]}" -o "$task_binary"
# Main resolves its data directory before entering the driver's main().
# Resolve '.' within the helper's owned, isolated cwd, never a user's directory.
DBMS_DATA_DIR=. dbms_run_isolated_test window_default_order_frontend "$task_binary"

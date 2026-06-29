#!/bin/zsh
# Fast batch test runner: build project objects if stale/missing, compile+link+run each test,
# report PASS/FAIL by exit code.
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"
INC=(-Isrc -Isrc/common -Isrc/storage -Isrc/access -Isrc/transaction -Isrc/network \
     -Isrc/utils -Isrc/executor -Isrc/commands -Isrc/interfaces -Isrc/parser \
     -Isrc/catalog -Isrc/expression)
CXXFLAGS=(-std=c++17 -O2 -pthread)

# Detect OpenSSL to match main build
HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null || [ -f /usr/include/openssl/ssl.h ]; then
    HAS_OPENSSL=1
fi

if [ "$HAS_OPENSSL" -eq 1 ]; then
    TLS_DEFS=(-DHAS_OPENSSL=1)
    TLS_LIBS=(-lssl -lcrypto)
else
    TLS_DEFS=()
    TLS_LIBS=()
fi

PROJECT_SOURCES=(
    src/commands/TableManage.cpp
    src/executor/ExecutionPlan.cpp
    src/storage/BufferPool.cpp
    src/storage/PageAllocator.cpp
    src/storage/Page.cpp
    src/storage/PgPage.cpp
    src/storage/PageWrapper.cpp
    src/storage/FreeSpaceMap.cpp
    src/storage/VisibilityMap.cpp
    src/storage/CommitLog.cpp
    src/storage/WAL.cpp
    src/access/BPTree.cpp
    src/access/HashIndex.cpp
    src/access/SPGiSTIndex.cpp
    src/access/BPTreeIndexAM.cpp
    src/access/HashIndexAM.cpp
    src/transaction/LockManager.cpp
    src/transaction/TxnIdGenerator.cpp
    src/network/NetworkServer.cpp
    src/common/Config.cpp
    src/common/GUC.cpp
    src/parser/parser.cpp
    src/catalog/catalog.cpp
    src/catalog/oid.cpp
    src/catalog/systables.cpp
    src/catalog/migrate.cpp
    src/catalog/CatalogService.cpp
    src/catalog/type_registry.cpp
    src/catalog/collation.cpp
    src/expression/ExprEvaluator.cpp
    src/expression/expr_helper.cpp
    src/commands/DdlExecutor.cpp
    src/commands/DdlTransaction.cpp
    src/types/numeric.cpp
)

if [ "$HAS_OPENSSL" -eq 1 ]; then
    PROJECT_SOURCES+=(src/network/TLSWrapper.cpp)
else
    PROJECT_SOURCES+=(src/network/TLSWrapper_stub.cpp)
fi

mkdir -p build/obj

# Conservative header dependency check: if any src header changed, rebuild all project objects.
NEWEST_HEADER=$(print -rl src/**/*.(h|hpp)(N.om[1]) 2>/dev/null || true)

for src in "${PROJECT_SOURCES[@]}"; do
    obj="build/obj/$(basename "$src" .cpp).o"
    if [ ! -f "$obj" ] || [ "$src" -nt "$obj" ] || { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt "$obj" ]; }; then
        if ! g++ "${CXXFLAGS[@]}" -Wall -Wextra "${TLS_DEFS[@]}" "${INC[@]}" -c "$src" -o "$obj" 2>/tmp/project_obj.err; then
            echo "COMPILE-FAIL project object $src"; exit 1
        fi
    fi
done

# Ensure shared test stubs (weak globals referenced by project objects) are available.
if [ ! -f build/obj/test_stubs.o ] || [ tests/test_stubs.cpp -nt build/obj/test_stubs.o ] || { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt build/obj/test_stubs.o ]; }; then
    if ! g++ "${CXXFLAGS[@]}" -Wall -Wextra "${TLS_DEFS[@]}" "${INC[@]}" -c tests/test_stubs.cpp -o build/obj/test_stubs.o 2>/tmp/test_stubs.err; then
        echo "COMPILE-FAIL test_stubs.cpp"; exit 1
    fi
fi

link=()
for o in build/obj/*.o; do
    case "$o" in
        *_test.o) ;;
        */main.o) ;;
        *) link+=("$o");;
    esac
done

pass=0; fail=0; failed=()
for tf in tests/*_test.cpp; do
    name=$(basename "$tf" .cpp)
    if ! g++ "${CXXFLAGS[@]}" "${INC[@]}" -c "$tf" -o "build/obj/$name.o" 2>"/tmp/${name}.cerr"; then
        echo "COMPILE-FAIL $name"; fail=$((fail+1)); failed+=("$name(compile)"); continue
    fi
    if ! g++ "${CXXFLAGS[@]}" "build/obj/$name.o" "${link[@]}" "${TLS_LIBS[@]}" -o "build/$name" 2>"/tmp/${name}.lerr"; then
        echo "LINK-FAIL $name"; fail=$((fail+1)); failed+=("$name(link)"); continue
    fi
    if ./build/$name >/dev/null 2>&1; then
        pass=$((pass+1))
    else
        echo "RUN-FAIL $name (exit $?)"; fail=$((fail+1)); failed+=("$name")
    fi
done
echo "=================================="
echo "PASS=$pass FAIL=$fail"
[[ $fail -gt 0 ]] && echo "FAILED: ${failed[@]}"

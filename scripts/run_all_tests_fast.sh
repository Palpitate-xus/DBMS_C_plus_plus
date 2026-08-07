#!/bin/zsh
# Fast batch test runner: build project objects if stale/missing, compile+link+run each test,
# report PASS/FAIL by exit code.
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"
INC=(-Isrc -Isrc/common -Isrc/storage -Isrc/access -Isrc/transaction -Isrc/network \
     -Isrc/utils -Isrc/executor -Isrc/commands -Isrc/interfaces -Isrc/parser \
     -Isrc/catalog -Isrc/expression -Isrc/replication -Isrc/process \
     -Isrc/commands -Isrc/interfaces -Itests)
CXXFLAGS=(-std=c++17 -O2 -pthread)

# Detect OpenSSL to match main build
HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null; then
    HAS_OPENSSL=1
fi

if [ "$HAS_OPENSSL" -eq 1 ]; then
    TLS_DEFS=(-DHAS_OPENSSL=1)
    TLS_LIBS=(-lssl -lcrypto)
else
    TLS_DEFS=()
    TLS_LIBS=()
fi

PROJECT_SOURCES=()
while IFS= read -r src; do
    [[ -z "$src" || "$src" == \#* || "$src" == "src/main.cpp" ]] && continue
    PROJECT_SOURCES+=("$src")
done < <(sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' cmake/dbms_sources.txt)

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
    link_for_test=("${link[@]}")
    if rg -q '(^|[[:space:]])(bool checkAdmin|bool checkDB|std::string resolveTableName|bool execute\(|void logSlowQuery|void recordSqlStat)' "$tf"; then
        link_for_test=()
        for obj in "${link[@]}"; do
            [[ "$obj" == */test_stubs.o ]] || link_for_test+=("$obj")
        done
    fi
    if ! g++ "${CXXFLAGS[@]}" "build/obj/$name.o" "${link_for_test[@]}" "${TLS_LIBS[@]}" -o "build/$name" 2>"/tmp/${name}.lerr"; then
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
if [[ $fail -gt 0 ]]; then
    echo "FAILED: ${failed[@]}"
    exit 1
fi
exit 0

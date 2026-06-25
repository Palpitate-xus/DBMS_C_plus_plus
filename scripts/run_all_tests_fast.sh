#!/bin/zsh
# Fast batch test runner: reuse build/obj/*.o, compile+link+run each test,
# report PASS/FAIL by exit code.
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"
INC=(-Isrc -Isrc/common -Isrc/storage -Isrc/access -Isrc/transaction -Isrc/network \
     -Isrc/utils -Isrc/executor -Isrc/commands -Isrc/interfaces -Isrc/parser \
     -Isrc/catalog -Isrc/expression)
CXXFLAGS=(-std=c++17 -O2 -pthread)

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
    if ! g++ "${CXXFLAGS[@]}" "build/obj/$name.o" "${link[@]}" -o "build/$name" 2>"/tmp/${name}.lerr"; then
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

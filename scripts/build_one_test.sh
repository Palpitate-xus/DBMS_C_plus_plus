#!/bin/zsh
# 增量编译并运行单个测试：复用 build/obj/ 下已编译的对象文件。
# 用法:
#   scripts/build_one_test.sh <test_name> [changed_src1.cpp changed_src2.cpp ...]
# 例:
#   scripts/build_one_test.sh policy_test src/commands/DdlExecutor.cpp
# 会先重新编译列出的源文件到 build/obj/，再编译 tests/<test_name>.cpp 并链接所有
# 非 *_test.o 的对象文件运行。

set -e
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"

INC=(-Isrc -Isrc/common -Isrc/storage -Isrc/access -Isrc/transaction -Isrc/network \
     -Isrc/utils -Isrc/executor -Isrc/commands -Isrc/interfaces -Isrc/parser \
     -Isrc/catalog -Isrc/expression)
CXXFLAGS=(-std=c++17 -O2 -pthread)

mkdir -p build/obj

test_name="$1"; shift
# 重新编译改动的源文件
for src in "$@"; do
    base=$(basename "$src" .cpp)
    echo "[inc] compiling $src -> build/obj/$base.o"
    g++ "${CXXFLAGS[@]}" "${INC[@]}" -c "$src" -o "build/obj/$base.o"
done

# 编译测试本身
echo "[inc] compiling tests/$test_name.cpp"
g++ "${CXXFLAGS[@]}" "${INC[@]}" -c "tests/$test_name.cpp" -o "build/obj/$test_name.o"

# 链接：所有非 *_test.o 的对象 + 本测试对象
link=()
for o in build/obj/*.o; do
    case "$o" in
        *_test.o) ;;
        *) link+=("$o");;
    esac
done
echo "[inc] linking build/$test_name"
g++ "${CXXFLAGS[@]}" "${link[@]}" "build/obj/$test_name.o" -o "build/$test_name" -pthread

echo "[inc] running build/$test_name"
"build/$test_name"

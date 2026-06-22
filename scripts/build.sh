#!/bin/bash
# DBMS 自动构建脚本
# 检测 OpenSSL 并自动选择 TLS 实现

set -e

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build"

# 检测 OpenSSL
HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null || [ -f /usr/include/openssl/ssl.h ]; then
    HAS_OPENSSL=1
    echo "[build] OpenSSL detected, TLS support enabled"
else
    echo "[build] OpenSSL not found, using TLS stub (plain TCP)"
fi

# 源文件列表 (与 CMakeLists.txt 保持一致)
SOURCES=(
    src/main.cpp
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
    src/expression/ExprEvaluator.cpp
    src/commands/DdlExecutor.cpp
    src/commands/DdlTransaction.cpp
)

# 选择 TLS 实现
if [ "$HAS_OPENSSL" -eq 1 ]; then
    SOURCES+=(src/network/TLSWrapper.cpp)
    TLS_DEFS="-DHAS_OPENSSL=1"
    TLS_LIBS="-lssl -lcrypto"
else
    SOURCES+=(src/network/TLSWrapper_stub.cpp)
    TLS_DEFS=""
    TLS_LIBS=""
fi

# Include 路径
INCLUDES=(
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
)

# 编译选项
CXXFLAGS="-std=c++17 -O2 -pthread -Wall -Wextra ${TLS_DEFS}"
LDFLAGS="-pthread ${TLS_LIBS}"

echo "[build] Compiling..."
cd "${SRC_DIR}"
g++ ${CXXFLAGS} "${INCLUDES[@]}" "${SOURCES[@]}" -o dbms_main ${LDFLAGS}

echo "[build] Success: ./dbms_main"

cmake_minimum_required(VERSION 3.16)
project(DBMS_C_plus_plus VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 所有子目录加入头文件搜索路径
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/common
    ${CMAKE_SOURCE_DIR}/src/storage
    ${CMAKE_SOURCE_DIR}/src/access
    ${CMAKE_SOURCE_DIR}/src/transaction
    ${CMAKE_SOURCE_DIR}/src/network
    ${CMAKE_SOURCE_DIR}/src/utils
    ${CMAKE_SOURCE_DIR}/src/catalog
    ${CMAKE_SOURCE_DIR}/src/commands
    ${CMAKE_SOURCE_DIR}/src/interfaces
    ${CMAKE_SOURCE_DIR}/src/executor
    ${CMAKE_SOURCE_DIR}/src/parser
    ${CMAKE_SOURCE_DIR}/src/expression
)

# 源文件列表 (TLSWrapper 在下方根据 OpenSSL 可用性条件添加)
set(DBMS_SOURCES
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
    src/catalog/collation.cpp
    src/expression/ExprEvaluator.cpp
    src/commands/DdlExecutor.cpp
    src/commands/DdlTransaction.cpp
    src/types/numeric.cpp
)

add_executable(dbms_main ${DBMS_SOURCES})

target_compile_options(dbms_main PRIVATE
    -O2
    -pthread
    -Wall
    -Wextra
)

target_link_libraries(dbms_main PRIVATE
    pthread
)

# ============================================================================
# TLS support: auto-detect OpenSSL
# ============================================================================
find_package(OpenSSL QUIET)
if(OpenSSL_FOUND)
    message(STATUS "OpenSSL found, enabling TLS support")
    target_sources(dbms_main PRIVATE src/network/TLSWrapper.cpp)
    target_link_libraries(dbms_main PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(dbms_main PRIVATE HAS_OPENSSL=1)
else()
    message(STATUS "OpenSSL not found, using TLS stub (plain TCP)")
endif()

# 安装目标
install(TARGETS dbms_main DESTINATION bin)

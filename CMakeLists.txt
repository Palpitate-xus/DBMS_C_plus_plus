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
    ${CMAKE_SOURCE_DIR}/src/replication
    ${CMAKE_SOURCE_DIR}/src/process
)

# Single source of truth for production translation units. The shell build and
# test scripts consume the same manifest, so adding/removing a module cannot
# silently make the build entry points disagree.
file(STRINGS "${CMAKE_SOURCE_DIR}/cmake/dbms_sources.txt" DBMS_SOURCES)

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
    target_sources(dbms_main PRIVATE src/network/TLSWrapper_stub.cpp)
endif()

# 安装目标
install(TARGETS dbms_main DESTINATION bin)

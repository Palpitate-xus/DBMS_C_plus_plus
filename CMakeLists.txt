cmake_minimum_required(VERSION 3.16)
project(DBMS_C_plus_plus VERSION 0.1.0 LANGUAGES CXX)
# Keep CMake, src/common/version.h and CHANGELOG.md in sync when cutting a
# release; scripts/package.sh re-checks the three agree before packaging.

include(CTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Single source of truth for production translation units. The shell build and
# test scripts consume the same manifest, so adding/removing a module cannot
# silently make the build entry points disagree.
file(STRINGS "${CMAKE_SOURCE_DIR}/cmake/dbms_sources.txt" DBMS_SOURCES)

list(FILTER DBMS_SOURCES EXCLUDE REGEX "^[[:space:]]*$")
list(FILTER DBMS_SOURCES EXCLUDE REGEX "^[[:space:]]*#")
list(LENGTH DBMS_SOURCES DBMS_SOURCE_COUNT)
list(REMOVE_DUPLICATES DBMS_SOURCES)
list(LENGTH DBMS_SOURCES DBMS_UNIQUE_SOURCE_COUNT)
if(NOT DBMS_SOURCE_COUNT EQUAL DBMS_UNIQUE_SOURCE_COUNT)
    message(FATAL_ERROR "cmake/dbms_sources.txt contains duplicate production sources")
endif()
list(FILTER DBMS_SOURCES INCLUDE REGEX "^src/")
if(NOT "src/main.cpp" IN_LIST DBMS_SOURCES)
    message(FATAL_ERROR "cmake/dbms_sources.txt must contain src/main.cpp")
endif()
foreach(DBMS_SOURCE IN LISTS DBMS_SOURCES)
    if(NOT EXISTS "${CMAKE_SOURCE_DIR}/${DBMS_SOURCE}")
        message(FATAL_ERROR "Manifest source does not exist: ${DBMS_SOURCE}")
    endif()
endforeach()

add_executable(dbms_main ${DBMS_SOURCES})

target_include_directories(dbms_main PRIVATE
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

target_compile_options(dbms_main PRIVATE
    -O2
    -pthread
    -Wall
    -Wextra
)

target_link_libraries(dbms_main PRIVATE
    pthread
)

# Keep one complete test orchestrator. The shell runner owns the intentionally
# standalone test linking/stub policy and the two protocol E2E tests; CMake
# exposes it as a normal build/check/CTest entry instead of reimplementing a
# second, potentially divergent test harness.
add_custom_target(check
    COMMAND "${CMAKE_SOURCE_DIR}/scripts/build_tests.sh"
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    USES_TERMINAL
    COMMENT "Building and running the complete DBMS regression suite"
)
add_custom_target(test-suite DEPENDS check)
if(BUILD_TESTING)
    add_test(
        NAME dbms_full_regression
        COMMAND "${CMAKE_SOURCE_DIR}/scripts/build_tests.sh"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    )
    set_tests_properties(dbms_full_regression PROPERTIES TIMEOUT 1800)
endif()

# TOAST compression is part of the current storage format. Old uncompressed
# TOAST files are intentionally not supported by this production format.
find_package(ZLIB REQUIRED)
target_link_libraries(dbms_main PRIVATE ZLIB::ZLIB)
target_compile_definitions(dbms_main PRIVATE HAS_ZLIB=1)
target_compile_definitions(dbms_main PRIVATE
    DBMS_VERSION_STRING="${PROJECT_VERSION}"
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

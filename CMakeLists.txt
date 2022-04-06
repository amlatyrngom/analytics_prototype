cmake_minimum_required(VERSION 3.17)
project(smart_ids)
set(CMAKE_CXX_STANDARD 20)

# CXX Flags
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -march=native")

# Sources Files
# TODO: Uncomment when execution engine fixed!
#file(GLOB_RECURSE SMART_ID_SOURCES ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)

file(GLOB_RECURSE SMART_ID_SOURCES ${PROJECT_SOURCE_DIR}/src/storage/*.cpp ${PROJECT_SOURCE_DIR}/src/common/*.cpp ${PROJECT_SOURCE_DIR}/src/include/storage/*.h ${PROJECT_SOURCE_DIR}/src/include/common/*.h)

file(GLOB_RECURSE SMART_ID_TEST_SOURCES ${PROJECT_SOURCE_DIR}/test/test_util/*.cpp ${PROJECT_SOURCE_DIR}/test/test_util/*.h)
include_directories(src/include)
include_directories(test)
include_directories(bench)
include_directories(SYSTEM third_party)
include_directories(SYSTEM third_party/benchmark/include)

message("Build Type ${CMAKE_BUILD_TYPE}")

# Set cache size and factor
set(CONFIG_CACHE_SIZE 32)
set(CONFIG_CACHE_FACTOR 4)
if( APPLE )
    execute_process(COMMAND sysctl -n hw.l3cachesize
            OUTPUT_VARIABLE CONFIG_CACHE_SIZE
            OUTPUT_STRIP_TRAILING_WHITESPACE)
endif( APPLE )

if( ${CMAKE_SYSTEM_NAME} STREQUAL "Linux" )
    execute_process(COMMAND getconf LEVEL3_CACHE_SIZE
            OUTPUT_VARIABLE CONFIG_CACHE_SIZE
            OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

message("-- Cache size size: ${CONFIG_CACHE_SIZE}")
configure_file(system_info.h.in system_info.h @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# MurmurHash
add_subdirectory(third_party/smhasher/src)
list(APPEND THIRD_PARTY_LIBS SMHasherSupport)

# Google Test
add_subdirectory(third_party/googletest)
list(APPEND THIRD_PARTY_LIBS gtest)

# Google Benchmark
add_subdirectory(third_party/benchmark)
list(APPEND THIRD_PARTY_LIBS benchmark::benchmark)

# Tessil
add_subdirectory(third_party/robin-map)
list(APPEND THIRD_PARTY_LIBS tsl::robin_map)
add_subdirectory(third_party/ordered-map)
list(APPEND THIRD_PARTY_LIBS tsl::ordered_map)

# Modern SQLite cpp
list(APPEND THIRD_PARTY_LIBS sqlite3)

if( APPLE )
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl@3)
endif( APPLE )

find_package(OpenSSL REQUIRED)


include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
message("FOUND SSL ${OPENSSL_INCLUDE_DIR}")
add_subdirectory(third_party/cuckoofilter)
list(APPEND THIRD_PARTY_LIBS cuckoofilter)


# Main Project Lib
add_library(smart_ids_shared SHARED ${SMART_ID_SOURCES})
target_link_libraries(smart_ids_shared ${THIRD_PARTY_LIBS})

add_executable(smartids_main src/main.cpp)
target_link_libraries(smartids_main smart_ids_shared)

# For Test
add_library(smart_ids_test_shared SHARED ${SMART_ID_TEST_SOURCES})
target_link_libraries(smart_ids_test_shared smart_ids_shared)
add_subdirectory(test)

# For Benchmarking
add_subdirectory(bench)
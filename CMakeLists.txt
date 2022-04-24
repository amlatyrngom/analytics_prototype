cmake_minimum_required(VERSION 3.17)
project(smart_ids)
set(CMAKE_CXX_STANDARD 20)

# CXX Flags
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -march=native")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("DEBUG MODE")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif (CMAKE_BUILD_TYPE STREQUAL "Debug")

# Sources Files
# TODO: Uncomment when execution engine fully fixed!
#file(GLOB_RECURSE SMART_ID_SOURCES ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
file(GLOB_RECURSE COMMON_SOURCES ${PROJECT_SOURCE_DIR}/src/common/*.cpp ${PROJECT_SOURCE_DIR}/src/include/common/*.h)
file(GLOB_RECURSE STORAGE_SOURCES ${PROJECT_SOURCE_DIR}/src/storage/*.cpp  ${PROJECT_SOURCE_DIR}/src/include/storage/*.h )
file(GLOB_RECURSE OPTIMIZER_SOURCES ${PROJECT_SOURCE_DIR}/src/optimizer/*.cpp  ${PROJECT_SOURCE_DIR}/src/include/optimizer/*.h )
# TODO: Remove out when execution engine fully fixed!
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/execution_types.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/expr_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/plan_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/scan_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/plan_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/expr_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/plan_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/expr_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/scan_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/nodes/scan_node.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/expr.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/binary_comp.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/init.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/reduce.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/hashing.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/gather.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/scatter.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/sort.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/select.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/execution_factory.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/static_aggregation.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/static_aggr_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/static_aggr_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/hash_aggr_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/hash_aggr_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/hash_aggr_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/sort_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/sort_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/sort_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/hash_join_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/hash_join_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/hash_join_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/projection_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/projection_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/projection_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/nodes/rowid_index_join_node.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/include/execution/executors/rowid_index_join_executor.h)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/executors/rowid_index_join_executor.cpp)
list(APPEND EXECUTION_SOURCES ${PROJECT_SOURCE_DIR}/src/execution/vector_ops/read_block.cpp)



list(APPEND SMART_ID_SOURCES ${COMMON_SOURCES} ${STORAGE_SOURCES} ${OPTIMIZER_SOURCES} ${EXECUTION_SOURCES})

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

# SQLite cpp
add_subdirectory(third_party/SQLiteCpp)
list(APPEND THIRD_PARTY_LIBS sqlite3 SQLiteCpp)

# Toml
include_directories(SYSTEM third_party/tomlplusplus/include)

# FMT
include_directories(SYSTEM third_party/fmt/include)
add_subdirectory(third_party/fmt)
list(APPEND THIRD_PARTY_LIBS fmt-header-only)

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
add_subdirectory(test/storage)

# For Benchmarking
#add_subdirectory(bench)
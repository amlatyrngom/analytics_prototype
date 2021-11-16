project(cuckoofilter)

cmake_minimum_required(VERSION 2.4)
set(CMAKE_CXX_STANDARD 11)

set(CMAKE_BUILD_TYPE Release)

file(GLOB_RECURSE CUCKOO_SOURCES src/*.cc src/include/*.h)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-strict-aliasing")

cmake_policy(SET CMP0003 NEW)
add_library(cuckoofilter ${CUCKOO_SOURCES})
target_link_libraries(cuckoofilter pthread ${OPENSSL_LIBRARIES})
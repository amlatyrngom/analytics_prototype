file(GLOB_RECURSE TEST_LIST ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)


foreach (TEST_ITEM ${TEST_LIST})
    message("Adding Test: ${TEST_ITEM}")
    add_executable(${TEST_ITEM} ${TEST_ITEM}.cpp)
    target_link_libraries(${TEST_ITEM} smart_ids_test_shared)
endforeach ()


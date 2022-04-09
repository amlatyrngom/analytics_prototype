file(GLOB_RECURSE TEST_LIST ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

foreach (TEST_ITEM ${TEST_LIST})
    message("Adding Test: ${TEST_ITEM}")
    get_filename_component(TEST_NAME ${TEST_ITEM} NAME_WLE)
    add_executable(${TEST_NAME} ${TEST_ITEM})
    target_link_libraries(${TEST_NAME} smart_ids_test_shared)
endforeach ()


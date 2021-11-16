list(APPEND TEST_LIST "table_loader_test" "scan_test" "vector_ops_int_test" "hash_join_test" "filter_repr_test"
                      "aggregate_test" "sort_test" "tpch_test" "materializer_test" "embedding_test"
                      "histogram_test" "stats_collector_test" "usage_test" "schema_graph_test" "index_test"
                      "param_test" "executor_stats_test" "tpch_embedding_test" "duckdb_test"
                      "job_light_test")

foreach (TEST_ITEM ${TEST_LIST})
    message("Adding Test: ${TEST_ITEM}")
    add_executable(${TEST_ITEM} ${TEST_ITEM}.cpp)
    target_link_libraries(${TEST_ITEM} smart_ids_test_shared)
endforeach ()


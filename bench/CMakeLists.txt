list(APPEND BENCH_LIST "q1_bench" "embedding_bench" "bloom_filter_bench" "detail_bench")


foreach (BENCH_ITEM ${BENCH_LIST})
    message("Adding Benchmark: ${BENCH_ITEM}")
    add_executable(${BENCH_ITEM} ${BENCH_ITEM}.cpp)
    target_link_libraries(${BENCH_ITEM} smart_ids_test_shared)
endforeach ()


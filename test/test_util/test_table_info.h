#pragma once

// Information about the test table
const char *test_table_name = "test3";
constexpr int num_test_rows = 1000000;
constexpr int num_test_cols = 6;
constexpr int mod_val = 10;
constexpr int int64_idx = 0;
constexpr int int32_idx = 1;
constexpr int varchar_idx = 2;
constexpr int float64_idx = 3;
constexpr int float32_idx = 4;
constexpr int date_idx = 5;
constexpr int id_idx = 6;
constexpr int embedding_idx = 7;

const char *str_values[] = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj"};
const double float_values[] = {0.37, 0.73, 3.7, 7.3, 0.037, 0.073, 0.0037, 0.0073, 0.00037, 0.00073};
const char
    *date_values[] = {"1996-01-02", "1996-12-01", "1993-10-14", "1995-10-11", "1994-07-30", "1992-02-21", "1996-01-10",
                      "1995-07-16", "1993-10-27", "1998-07-21"};


// TPCH Tables.
// lineitem
constexpr int l_orderkey_idx = 0;
constexpr int l_partkey_idx = 1;
constexpr int l_suppkey_idx = 2;
constexpr int l_linenumber_idx = 3;
constexpr int l_quantity_idx = 4;
constexpr int l_extendedprice_idx = 5;
constexpr int l_discount_idx = 6;
constexpr int l_tax_idx = 7;
constexpr int l_returnflag_idx = 8;
constexpr int l_linestatus_idx = 9;
constexpr int l_shipdate_idx = 10;
constexpr int l_commitdate_idx = 11;
constexpr int l_receiptdate_idx = 12;
constexpr int l_shipinstruct_idx = 13;
constexpr int l_shipmode_idx = 14;
constexpr int l_comment_idx = 15;
constexpr int l_id_idx = 16;
constexpr int l_embedding_idx = 17;

// orders
constexpr int o_orderkey_idx = 0;
constexpr int o_custkey_idx = 1;
constexpr int o_orderstatus_idx = 2;
constexpr int o_totalprice_idx = 3;
constexpr int o_orderdate_idx = 4;
constexpr int o_orderpriority_idx = 5;
constexpr int o_clerk_idx = 6;
constexpr int o_shippriority_idx = 7;
constexpr int o_comment_idx = 8;
constexpr int o_id_idx = 9;
constexpr int o_embedding_idx = 10;

// region
constexpr int r_regionkey_idx = 0;
constexpr int r_name_idx = 1;
constexpr int r_comment_idx = 2;

// nation
constexpr int n_nationkey_idx = 0;
constexpr int n_name_idx = 1;
constexpr int n_regionkey_idx = 2;
constexpr int n_comment_idx = 3;

// customer
constexpr int c_custkey_idx = 0;
constexpr int c_name_idx = 1;
constexpr int c_address_idx = 2;
constexpr int c_nationkey_idx = 3;
constexpr int c_phone_idx = 4;
constexpr int c_acctbal_idx = 5;
constexpr int c_mktsegment_idx = 6;
constexpr int c_comment_idx = 7;

// supplier
constexpr int s_suppkey_idx = 0;
constexpr int s_name_idx = 1;
constexpr int s_address_idx = 2;
constexpr int s_nationkey_idx = 3;
constexpr int s_phone_idx = 4;
constexpr int s_acctbal_idx = 5;
constexpr int s_comment_idx = 6;

// Join Test Tables
constexpr uint64_t small_build_size = 2100;
constexpr uint64_t small_probe_size = 4200;
constexpr uint64_t build_int64_col = 0;
constexpr uint64_t build_int32_col = 1;
constexpr uint64_t build_varchar_col = 2;
constexpr uint64_t probe_pk_col = 0;
constexpr uint64_t probe_int64_col = 1;
constexpr uint64_t probe_int32_col = 2;
constexpr uint64_t probe_varchar_col = 3;


// Embedding Test Tables
const char* embedding_build_name = "embedding_build";
const char* embedding_probe_name = "embedding_probe";
const char* embedding_interm_name = "embedding_interm";
constexpr int embedding_build_size = 2048;
constexpr int embedding_probe_size = 4096;
constexpr int embedding_build_pk_idx = 0;
constexpr int embedding_probe_pk_idx = 0;
constexpr int embedding_probe_fk_idx = 1;
constexpr int embedding_filter_idx = 2;
constexpr int embedding_interm_origin_idx = 0;
constexpr int embedding_interm_target_idx = 1;

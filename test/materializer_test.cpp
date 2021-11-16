#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
#include "execution/execution_factory.h"
using namespace smartid;

template<typename RowChecker>
void CheckRows(PlanExecutor* executor, RowChecker checker) {
  const VectorProjection *vp;
  while ((vp = executor->Next())) {
    vp->GetFilter()->Map([&](sel_t i) {
      checker(vp, i);
    });
  }
}

TEST_CASE("Simple Materialization") {
  TableLoader::LoadTestTables("tpch_data");
  auto test_table = Catalog::Instance()->GetTable(test_table_name);
  std::vector<Column> new_cols{
      Column::ScalarColumn("int64", SqlType::Int64),
      Column::ScalarColumn("int32", SqlType::Int32),
      Column::ScalarColumn("proj", SqlType::Int64),
      Column::ScalarColumn("id_col", SqlType::Int64),
  };
  Schema new_schema(std::move(new_cols));
  auto mat_table = Catalog::Instance()->CreateTable("mat_table", std::move(new_schema));
  {
    ColumnNode int64_col(int64_idx);
    ColumnNode int32_col(int32_idx);
    ColumnNode id_col(test_table->IdIdx());
    // Filter
    ConstantNode val5(Value(int32_t(5)), SqlType::Int32);
    BinaryCompNode comp(&int32_col, &val5, OpType::EQ);
    std::vector<ExprNode*> filters{&comp};
    // Projections
    BinaryArithNode proj(&int64_col, &val5, OpType::ADD, SqlType::Int64);
    std::vector<ExprNode*> projections{&int64_col, &int32_col, &proj, &id_col};
    // Scan
    ScanNode scan_node(test_table, std::move(projections), std::move(filters), false);
    // Materialize
    MaterializerNode materializer_node(&scan_node, mat_table);
    auto executor = ExecutionFactory::MakePlanExecutor(&materializer_node);
    executor->Next();
  }

  // Scan Materializer table
  {
    ColumnNode int64_col(0);
    ColumnNode int32_col(1);
    ColumnNode proj(2);
    ColumnNode id_col(3);
    std::vector<ExprNode*> projections{&int64_col, &int32_col, &proj, &id_col};
    ScanNode scan_node(mat_table, std::move(projections), {}, false);
    PrintNode print_node(&scan_node);
    auto executor = ExecutionFactory::MakePlanExecutor(&print_node);
    executor->Next();
  }
}

//TEST_CASE("Denormalization Test") {
//  TableLoader::LoadJoinTables("tpch_data");
//  {
//    // Build
//    std::string build_name("small_build_table");
//    ColumnNode build_int64(build_int64_col);
//    ColumnNode build_int32(build_int32_col);
//    ColumnNode build_varchar(build_varchar_col);
//    ScanNode build_scan
//        (build_name, std::vector<ExprNode *>{&build_int64, &build_int32, &build_varchar}, std::vector<ExprNode *>{});
//
//    // Probe
//    std::string probe_name("small_probe_table");
//    ColumnNode probe_pk(probe_pk_col);
//    ColumnNode probe_int64(probe_int64_col);
//    ColumnNode probe_int32(probe_int32_col);
//    ColumnNode probe_varchar(probe_varchar_col);
//    ScanNode probe_scan(probe_name,
//                        std::vector<ExprNode *>{&probe_pk, &probe_int64, &probe_int32, &probe_varchar},
//                        std::vector<ExprNode *>{});
//
//    // Join
//    std::vector<std::pair<uint64_t, uint64_t>> projections = {
//        {0, build_int64_col}, {0, build_int32_col}, {0, build_varchar_col},
//        {1, probe_pk_col}, {1, probe_int64_col}, {1, probe_int32_col}, {1, probe_varchar_col},
//    };
//    HashJoinNode
//        join_node(&build_scan,
//                  &probe_scan,
//                  {0},
//                  {0},
//                  std::move(projections),
//                  JoinType::INNER);
//
//    // Materialize
//    MaterializerNode materializer_node(&join_node, "mat_table");
//    materializer_node.Next();
//  }
//
//  // Scan Materializer table
//  {
//    ColumnNode build_int64(0);
//    ColumnNode build_int32(1);
//    ColumnNode build_varchar(2);
//    ColumnNode probe_pk(3);
//    ColumnNode probe_int64(4);
//    ColumnNode probe_int32(5);
//    ColumnNode probe_varchar(6);
//
//    std::vector<ExprNode*> projections{&build_int64, &build_int32, &build_varchar, &probe_pk, &probe_int64, &probe_int32, &probe_varchar};
//    ScanNode scan_node("mat_table", std::move(projections), {});
//    PrintNode print_node(&scan_node);
//    print_node.Next();
//  }
//}

//TEST_CASE("LineOrder Test") {
//  TableLoader::LoadTPCH("tpch_data");
//  {
//    // Orders scan
//    // Cols
//    ColumnNode o_orderkey(o_orderkey_idx);
//    ColumnNode o_orderdate(o_orderdate_idx);
//    ColumnNode o_orderpriority(o_orderpriority_idx);
//    // Filters
//    std::vector<ExprNode*> o_filters{};
//    // Projections
//    std::vector<ExprNode*> o_projections{&o_orderkey, &o_orderpriority, &o_orderdate};
//
//    // Scan
//    ScanNode o_scan("orders", std::move(o_projections), std::move(o_filters));
//    // Lineitem scan
//    // Cols
//    ColumnNode l_orderkey(l_orderkey_idx);
//    ColumnNode l_partkey(l_partkey_idx);
//    // Filters
//    std::vector<ExprNode*> l_filters{};
//    // Projections
//    std::vector<ExprNode*> l_projections{&l_orderkey, &l_partkey};
//    // Scan
//    ScanNode l_scan("lineitem", std::move(l_projections), std::move(l_filters));
//
//    // Join
//    std::vector<uint64_t> join_build_cols{0};
//    std::vector<uint64_t> join_probe_cols{0};
//    std::vector<std::pair<uint64_t, uint64_t>> join_projections = {
//        {0, 0},
//        {0, 1},
//        {0, 2},
//        {1, 0},
//        {1, 1},
//    };
//    HashJoinNode hash_join_node(&o_scan, &l_scan, std::move(join_build_cols), std::move(join_probe_cols), std::move(join_projections), JoinType::INNER);
//    // Materialize
//    MaterializerNode materializer_node(&hash_join_node, "lineorder");
//    materializer_node.Next();
//  }
//
//  {
//    ColumnNode o_orderkey(0);
//    ColumnNode o_orderpriority(1);
//    ColumnNode o_orderdate(2);
//    ColumnNode l_orderkey(3);
//    ColumnNode l_partkey(4);
//
//    std::vector<ExprNode*> projections{&o_orderkey, &o_orderpriority, &o_orderdate, &l_orderkey, &l_partkey};
//    ScanNode scan_node("lineorder", std::move(projections), {});
//    PrintNode print_node(&scan_node);
//    print_node.Next();
//  }
//}


//TEST_CASE("Order Embedding Test") {
//  TableLoader::LoadTPCH("tpch_data");
//  {
//    // Orders scan
//    // Cols
//    ColumnNode o_orderkey(o_orderkey_idx);
//    ColumnNode o_orderdate(o_orderdate_idx);
//    ColumnNode o_orderpriority(o_orderpriority_idx);
//    ColumnNode o_id(o_id_idx);
//    // Filters
//    std::vector<ExprNode*> o_filters{};
//    // Projections
//    std::vector<ExprNode*> o_projections{&o_orderkey, &o_orderpriority, &o_orderdate, &o_id};
//
//    // Scan
//    ScanNode o_scan("orders", std::move(o_projections), std::move(o_filters));
//    // Lineitem scan
//    // Cols
//    ColumnNode l_orderkey(l_orderkey_idx);
//    ColumnNode l_partkey(l_partkey_idx);
//    // Filters
//    ConstantNode comp_val(Value(int32_t(2)), SqlType::Int32);
//    BinaryCompNode comp(&l_partkey, &comp_val, OpType::LT);
//    std::vector<ExprNode*> l_filters{&comp};
//    // Projections
//    std::vector<ExprNode*> l_projections{&l_orderkey, &l_partkey};
//    // Scan
//    ScanNode l_scan("lineitem", std::move(l_projections), std::move(l_filters));
//
//    // Join
//    std::vector<uint64_t> join_build_cols{0};
//    std::vector<uint64_t> join_probe_cols{0};
//    std::vector<std::pair<uint64_t, uint64_t>> join_projections = {
//        {0, 3},
//    };
//    HashJoinNode hash_join_node(&o_scan, &l_scan, std::move(join_build_cols), std::move(join_probe_cols), std::move(join_projections), JoinType::LEFT_SEMI);
//    const VectorProjection* vp;
//    auto order_table = Catalog::Instance()->GetTable("orders");
//    auto embed_col_idx = order_table->GetSchema().NumCols() - 1;
//    while ((vp = hash_join_node.Next()) != nullptr) {
//      auto id_data = vp->VectorAt(0)->DataAs<int64_t>();
//      vp->GetFilter()->Map([&](sel_t i) {
//        auto id = id_data[i];
//        auto row_idx = id & 0xFFFFFF;
//        auto block_idx = id >> 32;
//        std::cout << "TupleID: (" << block_idx << ", " << row_idx << ")" << std::endl;
//        auto block = order_table->MutableBlockAt(block_idx);
//        block->MutableColumnDataAs<int64_t>(embed_col_idx)[row_idx] = 1;
//      });
//    }
//  }
//
//  {
//    ColumnNode o_orderkey(o_orderkey_idx);
//    ColumnNode o_orderdate(o_orderdate_idx);
//    ColumnNode o_orderpriority(o_orderpriority_idx);
//    ColumnNode o_id(o_id_idx);
//    ColumnNode o_embedding(o_embedding_idx);
//
//    std::vector<ExprNode*> projections{&o_orderkey, &o_orderpriority, &o_orderdate, &o_id, &o_embedding};
//    ScanNode scan_node("orders", std::move(projections), {});
//    PrintNode print_node(&scan_node);
//    print_node.Next();
//  }
//}

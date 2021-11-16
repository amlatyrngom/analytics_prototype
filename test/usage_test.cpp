#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "storage/table_loader.h"
#include "execution/execution_factory.h"
#include "test_util/test_table_info.h"
#include "stats/embedder.h"
using namespace smartid;

template<typename RowChecker>
void CheckRows(PlanExecutor * executor, RowChecker checker) {
  const VectorProjection *vp;
  while ((vp = executor->Next())) {
    vp->GetFilter()->Map([&](sel_t i) {
      checker(vp, i);
    });
  }
}


void RecordScan(Table* table, int64_t comp_val) {
  // Projection
  ColumnNode int64_col(int64_idx);
  std::vector<ExprNode *> projections{&int64_col};
  // Filter
  ConstantNode constant_node{Value(comp_val), SqlType::Int64};
  BinaryCompNode binary_comp_node(&int64_col, &constant_node, OpType::LT);
  std::vector<ExprNode *> filters{&binary_comp_node};
  // Scan
  ScanNode scan(table, std::move(projections), std::move(filters), true);
  // Run scan
  NoopOutputNode noop_output_node(&scan);
  auto executor = ExecutionFactory::MakePlanExecutor(&noop_output_node);
  executor->Next();
}

template <typename RowChecker>
void TestUsage(Table* table, RowChecker usage_checker) {
  // Projection
  ColumnNode int64_col(int64_idx);
  ColumnNode usage_col(table->UsageIdx());
  std::vector<ExprNode *> projections{&int64_col, &usage_col};
  // Filter
  std::vector<ExprNode *> filters{};
  // Scan
  ScanNode scan(table, std::move(projections), std::move(filters), false);
  // Run scan
  auto executor = ExecutionFactory::MakePlanExecutor(&scan);
  CheckRows(executor.get(), usage_checker);
}



TEST_CASE("Simple Usage Test") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  auto test_table = Catalog::Instance()->GetTable(test_table_name);
  // Run with col1 < 500, then col1 < 1000.
  // Values in [0, 500) should have usage 2. Values in [500, 1000) should have usage 1. Other value should have usage 0.
  RecordScan(test_table, 500);
  RecordScan(test_table, 1000);

  auto checker = [&](const VectorProjection* vp, sel_t i) {
    auto col1 = vp->VectorAt(0)->DataAs<int64_t>()[i];
    auto usage_val = vp->VectorAt(1)->DataAs<int64_t>()[i];
    if (col1 < 500) {
      REQUIRE(usage_val == 2);
    } else if (col1 < 1000) {
      REQUIRE(usage_val == 1);
    } else {
      REQUIRE(usage_val == 0);
    }
  };
  TestUsage(test_table, checker);
}
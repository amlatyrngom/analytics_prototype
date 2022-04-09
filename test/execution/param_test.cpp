#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "storage/table_loader.h"
#include "execution/nodes/plan_node.h"
#include "execution/execution_factory.h"
#include "test_util/test_table_info.h"
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


TEST_CASE("Simple Param Test") {
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;
  auto catalog = Catalog::Instance();
  auto table = catalog->GetTable(test_table_name);
  auto int64_col = factory.MakeCol(int64_idx);
  auto int32_col = factory.MakeCol(int32_idx);
  // Projections
  std::vector<ExprNode*> proj{int64_col, int32_col};
  // Filters
  std::string upper_param_name("upper");
  ParamNode param_node(upper_param_name);
  auto comp = factory.MakeBinaryComp(int32_col, &param_node, OpType::LT);
  std::vector<ExprNode*> filters{comp};
  // Scan
  auto scan_node = factory.MakeScan(table, std::move(proj), std::move(filters));

  for (int32_t upper = 1; upper < 10; upper++) {
    ExecutionContext context;
    context.AddParam(upper_param_name, Value(upper), SqlType::Int32);
    auto executor = ExecutionFactory::MakePlanExecutor(scan_node, &context);
    auto expected_num_rows = upper * (num_test_rows) / 10;
    uint64_t actual_num_rows = 0;
    auto checker = [&](const VectorProjection* vp, sel_t i) {
      auto int32_val = vp->VectorAt(1)->DataAs<int32_t>()[i];
      REQUIRE(int32_val < upper);
      actual_num_rows++;
    };
    CheckRows(executor.get(), checker);
    REQUIRE(actual_num_rows == expected_num_rows);
  }
}
#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include <limits>
#include "storage/table_loader.h"
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

TEST_CASE("Single Column ASC Sort") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;
  auto test_table = Catalog::Instance()->GetTable(test_table_name);
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  ColumnNode float64_col(float64_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col, &float64_col};
  auto comp = factory.MakeBinaryComp(factory.MakeCol(int64_idx),
                                      factory.MakeConst(Value(int64_t(10000)), SqlType::Int64),
                                      OpType::LT);


  std::vector<ExprNode *> filters{comp};
  ScanNode scan(test_table, std::move(scan_projections), std::move(filters), false);
  // Sort
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
      {int32_idx, SortType::ASC}
  };
  SortNode sort_node(&scan, std::move(sort_keys));
  // Test
  int num_rows = 0;
  auto curr_val = std::numeric_limits<int32_t>::min();
  auto check_fn = [&](const VectorProjection *vp, sel_t i) {
    auto int32_val = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    REQUIRE(int32_val >= curr_val);
    curr_val = int32_val;
    num_rows++;
  };
  auto executor = ExecutionFactory::MakePlanExecutor(&sort_node);
  CheckRows(executor.get(), check_fn);
  REQUIRE(num_rows == 10000);
}

TEST_CASE("Single Column DESC Sort") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;
  auto test_table = Catalog::Instance()->GetTable(test_table_name);
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  ColumnNode float64_col(float64_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col, &float64_col};
  auto comp = factory.MakeBinaryComp(factory.MakeCol(int64_idx),
                                     factory.MakeConst(Value(int64_t(10000)), SqlType::Int64),
                                     OpType::LT);
  std::vector<ExprNode *> filters{comp};
  ScanNode scan(test_table, std::move(scan_projections), std::move(filters), false);
  // Sort
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
      {int32_idx, SortType::DESC}
  };
  SortNode sort_node(&scan, std::move(sort_keys));
  // Test
  int num_rows = 0;
  auto curr_val = std::numeric_limits<int32_t>::max();
  auto check_fn = [&](const VectorProjection *vp, sel_t i) {
    auto int32_val = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    REQUIRE(int32_val <= curr_val);
    curr_val = int32_val;
    num_rows++;
  };
  auto executor = ExecutionFactory::MakePlanExecutor(&sort_node);
  CheckRows(executor.get(), check_fn);
  REQUIRE(num_rows == 10000);
}

TEST_CASE("Double ASC Sort") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;
  auto test_table = Catalog::Instance()->GetTable(test_table_name);
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  ColumnNode float64_col(float64_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col, &float64_col};
  auto comp = factory.MakeBinaryComp(factory.MakeCol(int64_idx),
                                     factory.MakeConst(Value(int64_t(10000)), SqlType::Int64),
                                     OpType::LT);
  std::vector<ExprNode *> filters{comp};
  ScanNode scan(test_table, std::move(scan_projections), std::move(filters), false);
  // Sort
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
      {int32_idx, SortType::DESC},
      {int64_idx, SortType::ASC}
  };
  SortNode sort_node(&scan, std::move(sort_keys));
  // Test
  int num_rows = 0;
  auto curr_val32 = std::numeric_limits<int32_t>::max();
  auto curr_val64 = std::numeric_limits<int64_t>::max();
  std::pair<int32_t, int64_t> curr_pair{curr_val32, curr_val64};
  auto check_fn = [&](const VectorProjection *vp, sel_t i) {
    auto int64_val = vp->VectorAt(int64_idx)->DataAs<int64_t>()[i];
    auto int32_val = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    std::pair<int32_t, int64_t> val{int32_val, -int64_val};
    REQUIRE(val < curr_pair);
    curr_pair = val;
    num_rows++;
  };
  auto executor = ExecutionFactory::MakePlanExecutor(&sort_node);
  CheckRows(executor.get(), check_fn);
  REQUIRE(num_rows == 10000);
}

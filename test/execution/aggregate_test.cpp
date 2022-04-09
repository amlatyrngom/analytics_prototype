#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "storage/table_loader.h"
#include "execution/execution_factory.h"
#include "test_util/test_table_info.h"
using namespace smartid;

template<typename RowChecker>
void CheckRows(PlanExecutor *executor, RowChecker checker) {
  const VectorProjection *vp;
  while ((vp = executor->Next())) {
    vp->GetFilter()->Map([&](sel_t i) {
      checker(vp, i);
    });
  }
}

void PrintStaticAggr(Table* table) {
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  ColumnNode float64_col(float64_idx);
  ColumnNode float32_col(float32_idx);
  ColumnNode date_col(date_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col, &float64_col, &float32_col, &date_col};
  ConstantNode bound(Value(int32_t(5)), SqlType::Int32);
  BinaryCompNode comp(&int32_col, &bound, OpType::LT);
  std::vector<ExprNode *> filters{&comp};
  ScanNode scan(table, std::move(scan_projections), std::move(filters), false);
  // Aggregate
  std::vector<std::pair<uint64_t, AggType>> aggs = {
      // 6 Count Aggregates
      {int64_idx, AggType::COUNT},
      {int32_idx, AggType::COUNT},
      {varchar_idx, AggType::COUNT},
      {float64_idx, AggType::COUNT},
      {float32_idx, AggType::COUNT},
      {date_idx, AggType::COUNT},
      // 6 Max Aggregates
      {int64_idx, AggType::MAX},
      {int32_idx, AggType::MAX},
      {varchar_idx, AggType::MAX},
      {float64_idx, AggType::MAX},
      {float32_idx, AggType::MAX},
      {date_idx, AggType::MAX},
      // 6 Min Aggregates
      {int64_idx, AggType::MIN},
      {int32_idx, AggType::MIN},
      {varchar_idx, AggType::MIN},
      {float64_idx, AggType::MIN},
      {float32_idx, AggType::MIN},
      {date_idx, AggType::MIN},
      // 4 Sum Aggregates
      {int64_idx, AggType::SUM},
      {int32_idx, AggType::SUM},
      {float64_idx, AggType::SUM},
      {float32_idx, AggType::SUM},
  };
  StaticAggregateNode static_aggregate_node(&scan, std::move(aggs));
  PrintNode node(&static_aggregate_node);
  auto executor = ExecutionFactory::MakePlanExecutor(&node);
  executor->Next();
}

TEST_CASE("Static Aggregation") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  auto table = Catalog::Instance()->GetTable(test_table_name);
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  ColumnNode float64_col(float64_idx);
  ColumnNode float32_col(float32_idx);
  ColumnNode date_col(date_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col, &float64_col, &float32_col, &date_col};
  ConstantNode bound(Value(int32_t(5)), SqlType::Int32);
  BinaryCompNode comp(&int32_col, &bound, OpType::LT);
  std::vector<ExprNode *> filters{&comp};
  ScanNode scan(table, std::move(scan_projections), std::move(filters), false);
  // Aggregate
  std::vector<std::pair<uint64_t, AggType>> aggs = {
      // 6 Count Aggregates
      {int64_idx, AggType::COUNT},
      {int32_idx, AggType::COUNT},
      {varchar_idx, AggType::COUNT},
      {float64_idx, AggType::COUNT},
      {float32_idx, AggType::COUNT},
      {date_idx, AggType::COUNT},
      // 6 Max Aggregates
      {int64_idx, AggType::MAX},
      {int32_idx, AggType::MAX},
      {varchar_idx, AggType::MAX},
      {float64_idx, AggType::MAX},
      {float32_idx, AggType::MAX},
      {date_idx, AggType::MAX},
      // 6 Min Aggregates
      {int64_idx, AggType::MIN},
      {int32_idx, AggType::MIN},
      {varchar_idx, AggType::MIN},
      {float64_idx, AggType::MIN},
      {float32_idx, AggType::MIN},
      {date_idx, AggType::MIN},
      // 4 Sum Aggregates
      {int64_idx, AggType::SUM},
      {int32_idx, AggType::SUM},
      {float64_idx, AggType::SUM},
      {float32_idx, AggType::SUM},
  };
  StaticAggregateNode static_aggregate_node(&scan, std::move(aggs));
  auto expected_num_rows = 1;
  // counts
  auto expected_count = num_test_rows / 2;
  // maxes
  auto expected_int64_max = num_test_rows - 5 - 1;
  auto expected_int32_max = 4;
  auto expected_varchar_max = Varlen(3, str_values[4]);
  auto expected_float64_max = 7.3;
  auto expected_float32_max = expected_float64_max + 1;
  auto expected_date_max = Date::FromString(date_values[1]);
  // mins
  auto expected_int64_min = 0;
  auto expected_int32_min = 0;
  auto expected_varchar_min = Varlen(3, str_values[0]);
  auto expected_float64_min = 0.037;
  auto expected_float32_min = expected_float64_min + 1;
  auto expected_date_min = Date::FromString(date_values[2]);
  // Sums
  double k = num_test_rows / 10.0;
  auto expected_int64_sum = 10 * k + 50 * (k * (k - 1) / 2);
  auto expected_int32_sum = 10 * k;
  auto expected_float64_sum =
      k * (float_values[0] + float_values[1] + float_values[2] + float_values[3] + float_values[4]);
  auto expected_float32_sum =
      5 * k + k * (float_values[0] + float_values[1] + float_values[2] + float_values[3] + float_values[4]);
  // Checker
  int num_rows = 0;
  auto check_row = [&](const VectorProjection *vp, sel_t i) {
    // There should only be one row
    REQUIRE(i == 0);
    REQUIRE(num_rows == 0);
    // Counts are all the same
    int attr_idx = 0;
    REQUIRE(expected_count == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_count == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_count == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_count == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_count == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_count == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    // Maxes are as expected
    REQUIRE(expected_int64_max == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_int32_max == vp->VectorAt(attr_idx)->DataAs<int32_t>()[i]);
    attr_idx++;
    REQUIRE(expected_varchar_max == vp->VectorAt(attr_idx)->DataAs<Varlen>()[i]);
    attr_idx++;
    REQUIRE(expected_float64_max == Approx(vp->VectorAt(attr_idx)->DataAs<double>()[i]));
    attr_idx++;
    REQUIRE(expected_float32_max == Approx(vp->VectorAt(attr_idx)->DataAs<float>()[i]));
    attr_idx++;
    REQUIRE(expected_date_max == vp->VectorAt(attr_idx)->DataAs<Date>()[i]);
    attr_idx++;
    // Mins are as expected
    REQUIRE(expected_int64_min == vp->VectorAt(attr_idx)->DataAs<int64_t>()[i]);
    attr_idx++;
    REQUIRE(expected_int32_min == vp->VectorAt(attr_idx)->DataAs<int32_t>()[i]);
    attr_idx++;
    REQUIRE(expected_varchar_min == vp->VectorAt(attr_idx)->DataAs<Varlen>()[i]);
    attr_idx++;
    REQUIRE(expected_float64_min == Approx(vp->VectorAt(attr_idx)->DataAs<double>()[i]));
    attr_idx++;
    REQUIRE(expected_float32_min == Approx(vp->VectorAt(attr_idx)->DataAs<float>()[i]));
    attr_idx++;
    REQUIRE(expected_date_min == vp->VectorAt(attr_idx)->DataAs<Date>()[i]);
    attr_idx++;
    // Sums are as expected
    REQUIRE(expected_int64_sum == Approx(vp->VectorAt(attr_idx)->DataAs<double>()[i]));
    attr_idx++;
    REQUIRE(expected_int32_sum == Approx(vp->VectorAt(attr_idx)->DataAs<double>()[i]));
    attr_idx++;
    REQUIRE(expected_float64_sum == Approx(vp->VectorAt(attr_idx)->DataAs<double>()[i]));
    attr_idx++;
    REQUIRE(expected_float32_sum == Approx(vp->VectorAt(attr_idx)->DataAs<double>()[i]));
    // Add Num Rows to check at the end
    num_rows++;
  };
  auto executor = ExecutionFactory::MakePlanExecutor(&static_aggregate_node);
  CheckRows(executor.get(), check_row);
  REQUIRE(num_rows == expected_num_rows);
  // Free allocated memory.
  expected_varchar_max.Free();
  expected_varchar_min.Free();
}

TEST_CASE("Hash Aggregation") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  auto table = Catalog::Instance()->GetTable(test_table_name);
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col};
  ConstantNode bound(Value(int32_t(5)), SqlType::Int32);
  BinaryCompNode comp(&int32_col, &bound, OpType::LT);
  std::vector<ExprNode *> filters{&comp};
  ScanNode scan(table, std::move(scan_projections), std::move(filters), false);
  // Hash Aggregate
  std::vector<uint64_t> group_bys{varchar_idx};
  std::vector<std::pair<uint64_t, AggType>> aggs{
      {int64_idx, AggType::COUNT},
      {int64_idx, AggType::MIN},
      {int64_idx, AggType::MAX},
      {varchar_idx, AggType::MAX},
      {varchar_idx, AggType::MIN},
  };
  HashAggregationNode aggregation_node(&scan, std::move(group_bys), std::move(aggs));

  uint64_t num_expected_rows = 5;
  uint64_t num_rows = 0;
  auto check_row = [&](const VectorProjection *vp, sel_t i) {
    REQUIRE(i < num_expected_rows);
    auto varchar_attr = vp->VectorAt(0)->DataAs<Varlen>()[i];
    std::string varchar_str(varchar_attr.Data(), varchar_attr.Size());
    auto int64_count = vp->VectorAt(1)->DataAs<int64_t>()[i];
    auto int64_min = vp->VectorAt(2)->DataAs<int64_t>()[i];
    auto int64_max = vp->VectorAt(3)->DataAs<int64_t>()[i];
    auto varchar_max = vp->VectorAt(4)->DataAs<Varlen>()[i];
    auto varchar_min = vp->VectorAt(5)->DataAs<Varlen>()[i];
    std::string varchar_max_str(varchar_max.Data(), varchar_max.Size());
    std::string varchar_min_str(varchar_min.Data(), varchar_min.Size());

    REQUIRE(varchar_str == str_values[i]);
    REQUIRE(int64_count == (num_test_rows / mod_val));
    REQUIRE(int64_min == i);
    REQUIRE(int64_max == (num_test_rows + i - mod_val));
    REQUIRE(varchar_str == varchar_max_str);
    REQUIRE(varchar_str == varchar_min_str);
    num_rows++;
  };

  auto executor = ExecutionFactory::MakePlanExecutor(&aggregation_node);
  CheckRows(executor.get(), check_row);
  REQUIRE(num_expected_rows == num_rows);
}

TEST_CASE("Hash + Static Aggregation") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  auto table = Catalog::Instance()->GetTable(test_table_name);
  // Scan
  ColumnNode int64_col(int64_idx);
  ColumnNode int32_col(int32_idx);
  ColumnNode varchar_col(varchar_idx);
  std::vector<ExprNode *> scan_projections{&int64_col, &int32_col, &varchar_col};
  ConstantNode bound(Value(int32_t(5)), SqlType::Int32);
  BinaryCompNode comp(&int32_col, &bound, OpType::LT);
  std::vector<ExprNode *> filters{&comp};
  ScanNode scan(table, std::move(scan_projections), std::move(filters), false);
  // Hash Aggregate
  std::vector<uint64_t> group_bys{varchar_idx};
  std::vector<std::pair<uint64_t, AggType>> aggs{
      {int64_idx, AggType::COUNT},
      {int64_idx, AggType::MIN},
      {int64_idx, AggType::MAX},
      {varchar_idx, AggType::MAX},
      {varchar_idx, AggType::MIN},
  };
  HashAggregationNode aggregation_node(&scan, std::move(group_bys), std::move(aggs));
  // Static Aggregate
  std::vector<std::pair<uint64_t, AggType>> static_aggs{
      {0, AggType::COUNT},
      {1, AggType::SUM},
      {2, AggType::SUM},
  };
  StaticAggregateNode static_aggregate_node{&aggregation_node, std::move(static_aggs)};
  // Check
  uint64_t num_expected_rows = 1;
  uint64_t num_rows = 0;
  auto check_row = [&](const VectorProjection *vp, sel_t i) {
    REQUIRE(i == 0);
    // Values
    auto total_count = vp->VectorAt(0)->DataAs<int64_t>()[i];
    auto total_sum = vp->VectorAt(1)->DataAs<double>()[i];
    auto min_sum = vp->VectorAt(2)->DataAs<double>()[i];
    // Checks
    REQUIRE(total_count == 5);
    REQUIRE(total_sum == Approx(num_test_rows / 2));
    REQUIRE(min_sum == Approx(1 + 2 + 3 + 4));
    num_rows++;
  };
  auto executor = ExecutionFactory::MakePlanExecutor(&static_aggregate_node);
  CheckRows(executor.get(), check_row);
  REQUIRE(num_expected_rows == num_rows);
}
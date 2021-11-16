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

template<typename ProjectionChecker, typename FilterChecker>
void PrintScan(ExecutionFactory* factory, std::vector<ExprNode *> &&extra_projections,
               std::vector<ExprNode *> &&filters,
               int num_expected_rows,
               ProjectionChecker projection_checker,
               FilterChecker filter_checker) {
  auto catalog = Catalog::Instance();
  auto table = catalog->GetTable(test_table_name);

  auto int64_col = factory->MakeCol(int64_idx);
  auto int32_col = factory->MakeCol(int32_idx);
  auto varchar_col = factory->MakeCol(varchar_idx);
  auto float64_col = factory->MakeCol(float64_idx);
  auto float32_col = factory->MakeCol(float32_idx);
  auto date_col = factory->MakeCol(date_idx);
  std::vector<ExprNode *> projections{int64_col, int32_col, varchar_col, float64_col, float32_col, date_col};
  projections.insert(projections.end(), extra_projections.begin(), extra_projections.end());
  auto scan = factory->MakeScan(table, std::move(projections), std::move(filters));
  auto noop = factory->MakeNoop(scan);

  // Execute.
  auto executor = ExecutionFactory::MakePlanExecutor(noop);
  executor->Next();
}

template<typename ProjectionChecker, typename FilterChecker>
void TestScan(ExecutionFactory* factory, std::vector<ExprNode *> &&extra_projections,
              std::vector<ExprNode *> &&filters,
              int num_expected_rows,
              ProjectionChecker projection_checker,
              FilterChecker filter_checker) {
  auto catalog = Catalog::Instance();
  auto table = catalog->GetTable(test_table_name);

  auto int64_col = factory->MakeCol(int64_idx);
  auto int32_col = factory->MakeCol(int32_idx);
  auto varchar_col = factory->MakeCol(varchar_idx);
  auto float64_col = factory->MakeCol(float64_idx);
  auto float32_col = factory->MakeCol(float32_idx);
  auto date_col = factory->MakeCol(date_idx);
  std::vector<ExprNode *> projections{int64_col, int32_col, varchar_col, float64_col, float32_col, date_col};
  projections.insert(projections.end(), extra_projections.begin(), extra_projections.end());
  auto scan = factory->MakeScan(table, std::move(projections), std::move(filters));

  // Output checker
  int num_rows = 0;
  auto check_row = [&](const VectorProjection *vp, sel_t i) {
    auto int64_attr = vp->VectorAt(int64_idx)->DataAs<int64_t>()[i];
    auto int32_attr = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    auto varchar_attr = vp->VectorAt(varchar_idx)->DataAs<Varlen>()[i];
    auto float64_attr = vp->VectorAt(float64_idx)->DataAs<double>()[i];
    auto float32_attr = vp->VectorAt(float32_idx)->DataAs<float>()[i];
    auto date_attr = vp->VectorAt(date_idx)->DataAs<Date>()[i];
    auto varchar_str = std::string(varchar_attr.Data(), varchar_attr.Size());
    auto date_str = date_attr.ToString();
    REQUIRE(int64_attr < num_test_rows);
    REQUIRE(int32_attr == (int64_attr % mod_val));
    REQUIRE(varchar_str == str_values[int64_attr % mod_val]);
    REQUIRE(float64_attr == Approx(float_values[int64_attr % mod_val]));
    REQUIRE(float32_attr == Approx(float_values[int64_attr % mod_val] + 1.0));
    REQUIRE(date_str == date_values[int64_attr % mod_val]);
    projection_checker(vp, i);
    filter_checker(vp, i);
    num_rows++;
  };

  // Execute
  auto executor = ExecutionFactory::MakePlanExecutor(scan);
  CheckRows(executor.get(), check_row);
  REQUIRE(num_rows == num_expected_rows);
}

//TEST_CASE("Basic Scan Perf") {
//  // Load Tables
//  TableLoader::LoadTestTables("tpch_data");
//  for (int i = 0; i < 10000; i++) {
//    // Projections
//    std::vector<ExprNode *> extra_projections{};
//    // Filters
//    ColumnNode int64_col(int64_idx);
//    ColumnNode int32_col(int32_idx);
//    ConstantCompNode comp1(&int64_col, Value(int64_t(num_test_rows / 10)), OpType::LT);
//    ConstantCompNode comp2(&int32_col, Value(5), OpType::EQ);
//    std::vector<ExprNode *> filters{&comp1, &comp2};
//    auto projection_checker = [&](const VectorProjection *vp, sel_t i) {
//    };
//    auto filter_checker = [&](const VectorProjection *vp, sel_t i) {
//      auto int64_attr = vp->VectorAt(int64_idx)->DataAs<int64_t>()[i];
//      auto int32_attr = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
//      REQUIRE(int64_attr < (num_test_rows / 10));
//      REQUIRE(int32_attr == 5);
//    };
//    int num_expected_rows = num_test_rows / (10 * mod_val);
//    PrintScan(std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
//    //TestScan(std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
//  }
//}

TEST_CASE("Basic Scan") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;

  int num_expected_rows = num_test_rows;
  std::vector<ExprNode *> extra_projections{};
  std::vector<ExprNode *> filters{};
  auto projection_checker = [&](const VectorProjection *vp, sel_t i) {
  };
  auto filter_checker = [&](const VectorProjection *vp, sel_t i) {
  };
  TestScan(&factory, std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
}

TEST_CASE("Simple Scan with Filters") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;

  // Projections
  std::vector<ExprNode *> extra_projections{};
  // Filters
  auto comp1 = factory.MakeBinaryComp(factory.MakeCol(int64_idx),
                                      factory.MakeConst(Value(int64_t(num_test_rows / 10)), SqlType::Int64),
                                      OpType::LT);
  auto comp2 = factory.MakeBinaryComp(factory.MakeCol(int32_idx),
                                      factory.MakeConst(Value(int32_t(5)), SqlType::Int32),
                                      OpType::EQ);

  std::vector<ExprNode *> filters{comp1, comp2};
  auto projection_checker = [&](const VectorProjection *vp, sel_t i) {
  };
  auto filter_checker = [&](const VectorProjection *vp, sel_t i) {
    auto int64_attr = vp->VectorAt(int64_idx)->DataAs<int64_t>()[i];
    auto int32_attr = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    REQUIRE(int64_attr < (num_test_rows / 10));
    REQUIRE(int32_attr == 5);
  };
  int num_expected_rows = num_test_rows / (10 * mod_val);
  TestScan(&factory, std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
}

TEST_CASE("Simple Scan with projection & filter") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;
  // Projections
  auto sum = factory.MakeBinaryArith(factory.MakeCol(int64_idx),
                                     factory.MakeCol(int32_idx),
                                     OpType::ADD, SqlType::Int64);
  std::vector<ExprNode *> extra_projections{sum};
  // Filters
  auto upper_bound = factory.MakeBinaryArith(factory.MakeCol(int64_idx),
                                             factory.MakeConst(Value(int64_t(5)), SqlType::Int64),
                                             OpType::ADD, SqlType::Int64);
  auto comp = factory.MakeBinaryComp(sum, upper_bound, OpType::GE);
  std::vector<ExprNode *> filters{comp};
  // Checkers
  auto projection_checker = [&](const VectorProjection *vp, sel_t i) {
    auto int64_attr = vp->VectorAt(int64_idx)->DataAs<int64_t>()[i];
    auto int32_attr = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    auto proj_attr = vp->VectorAt(num_test_cols)->DataAs<int64_t>()[i];
    REQUIRE((int64_attr + int32_attr) == proj_attr);
  };
  auto filter_checker = [&](const VectorProjection *vp, sel_t i) {
    auto int64_attr = vp->VectorAt(int64_idx)->DataAs<int64_t>()[i];
    auto int32_attr = vp->VectorAt(int32_idx)->DataAs<int32_t>()[i];
    REQUIRE((int64_attr + int32_attr) >= (int64_attr + 5));
  };
  int num_expected_rows = num_test_rows / 2;
  TestScan(&factory, std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
}

TEST_CASE("Scan with Date Filter") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;

  // Projections
  std::vector<ExprNode *> extra_projections{};
  // Filters
  auto date_val = Date::FromString(date_values[5]);
  auto comp = factory.MakeBinaryComp(factory.MakeCol(date_idx),
                                     factory.MakeConst(Value(date_val), SqlType::Date),
                                     OpType::EQ);
  std::vector<ExprNode *> filters{comp};
  // Checkers
  auto projection_checker = [&](const VectorProjection *vp, sel_t i) {
  };
  auto filter_checker = [&](const VectorProjection *vp, sel_t i) {
    auto date_attr = vp->VectorAt(date_idx)->DataAs<Date>()[i];
    REQUIRE(date_attr == date_val);
    REQUIRE(date_attr.ToString() == date_values[5]);
  };
  int num_expected_rows = num_test_rows / 10;
  TestScan(&factory, std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
}

TEST_CASE("Scan with String Filter") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  ExecutionFactory factory;

  // Projections
  std::vector<ExprNode *> extra_projections{};
  // Filters
  std::string str(str_values[5]);
  Varlen varlen(str.size(), str.data());
  auto comp = factory.MakeBinaryComp(factory.MakeCol(varchar_idx),
                                     factory.MakeConst(Value(varlen), SqlType::Varchar),
                                     OpType::LT);
  std::vector<ExprNode *> filters{comp};
  // Checkers
  auto projection_checker = [&](const VectorProjection *vp, sel_t i) {
  };
  auto filter_checker = [&](const VectorProjection *vp, sel_t i) {
    auto varchar_attr = vp->VectorAt(varchar_idx)->DataAs<Varlen>()[i];
    REQUIRE(varchar_attr < varlen);
  };
  int num_expected_rows = num_test_rows / 2;
  TestScan(&factory, std::move(extra_projections), std::move(filters), num_expected_rows, projection_checker, filter_checker);
  varlen.Free();
}

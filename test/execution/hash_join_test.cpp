#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_factory.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
using namespace smartid;

template<typename RowChecker>
void CheckRows(PlanExecutor* executor, RowChecker checker) {
  const VectorProjection *vp;
  while ((vp = executor->Next())) {
    checker(vp);
  }
}


template<typename NLMatch>
void PrintTables(std::vector<uint64_t> &&build_cols, std::vector<uint64_t> &&probe_cols, NLMatch nl_match) {
  // Build
  std::string build_name("small_build_table");
  auto build_table = Catalog::Instance()->GetTable(build_name);
  ColumnNode build_int64(build_int64_col);
  ColumnNode build_int32(build_int32_col);
  ColumnNode build_varchar(build_varchar_col);
  // Probe
  std::string probe_name("small_probe_table");
  auto probe_table = Catalog::Instance()->GetTable(probe_name);
  ColumnNode probe_pk(probe_pk_col);
  ColumnNode probe_int64(probe_int64_col);
  ColumnNode probe_int32(probe_int32_col);
  ColumnNode probe_varchar(probe_varchar_col);

  // Print Probe
  {
    std::cout << "PROBE TABLE" << std::endl;
    ScanNode probe_scan(probe_table,
                        std::vector<ExprNode *>{&probe_pk, &probe_int64, &probe_int32, &probe_varchar},
                        std::vector<ExprNode *>{}, false);
    PrintNode node(&probe_scan);
    auto executor = ExecutionFactory::MakePlanExecutor(&node);
    executor->Next();
  }

  // Print Build
  {
    std::cout << "BUILD TABLE" << std::endl;
    ScanNode build_scan
        (build_table, std::vector<ExprNode *>{&build_int64, &build_int32, &build_varchar}, std::vector<ExprNode *>{}, false);
    PrintNode node(&build_scan);
    auto executor = ExecutionFactory::MakePlanExecutor(&node);
    executor->Next();
  }

  // Print Join
  {
    std::vector<std::pair<uint64_t, uint64_t>> projections = {
        {0, build_int64_col}, {0, build_int32_col}, {0, build_varchar_col},
        {1, probe_pk_col}, {1, probe_int64_col}, {1, probe_int32_col}, {1, probe_varchar_col},
    };
    ScanNode probe_scan(probe_table,
                        std::vector<ExprNode *>{&probe_pk, &probe_int64, &probe_int32, &probe_varchar},
                        std::vector<ExprNode *>{}, false);
    ScanNode build_scan
        (build_table, std::vector<ExprNode *>{&build_int64, &build_int32, &build_varchar}, std::vector<ExprNode *>{}, false);
    HashJoinNode
        join_node(&build_scan,
                  &probe_scan,
                  std::move(build_cols),
                  std::move(probe_cols),
                  std::move(projections),
                  JoinType::INNER);
    PrintNode node(&join_node);
    auto executor = ExecutionFactory::MakePlanExecutor(&node);
    executor->Next();
  }
}

template<typename NLMatch>
void TestJoin(std::vector<uint64_t> &&build_cols, std::vector<uint64_t> &&probe_cols, NLMatch nl_match, bool use_bloom) {
  // Build
  std::string build_name("small_build_table");
  auto build_table = Catalog::Instance()->GetTable(build_name);
  ColumnNode build_int64(build_int64_col);
  ColumnNode build_int32(build_int32_col);
  ColumnNode build_varchar(build_varchar_col);
  // Probe
  std::string probe_name("small_probe_table");
  auto probe_table = Catalog::Instance()->GetTable(probe_name);
  ColumnNode probe_pk(probe_pk_col);
  ColumnNode probe_int64(probe_int64_col);
  ColumnNode probe_int32(probe_int32_col);
  ColumnNode probe_varchar(probe_varchar_col);
  // Join
  std::vector<std::pair<uint64_t, uint64_t>> projections = {
      {0, build_int64_col}, {0, build_int32_col}, {0, build_varchar_col},
      {1, probe_pk_col}, {1, probe_int64_col}, {1, probe_int32_col}, {1, probe_varchar_col},
  };


  // Do a nested loop join to find the expected results.
  uint64_t expected_num_matches = 0;
  {
    ScanNode probe_scan(probe_table,
                        std::vector<ExprNode *>{&probe_pk, &probe_int64, &probe_int32, &probe_varchar},
                        std::vector<ExprNode *>{}, false);
    auto probe_executor = ExecutionFactory::MakePlanExecutor(&probe_scan);
    const VectorProjection *vp1;
    while ((vp1 = probe_executor->Next()) != nullptr) {
      vp1->GetFilter()->Map([&](sel_t i) {
        ScanNode build_scan(build_table,
                            std::vector<ExprNode *>{&build_int64, &build_int32, &build_varchar},
                            std::vector<ExprNode *>{}, false);
        auto build_executor = ExecutionFactory::MakePlanExecutor(&build_scan);
        const VectorProjection *vp2;
        while ((vp2 = build_executor->Next()) != nullptr) {
          vp2->GetFilter()->Map([&](sel_t j) {
            if (nl_match(vp1, vp2, i, j)) {
              expected_num_matches++;
            }
          });
        }
      });
    }
  }

  // Do a hash join
  auto num_matches = 0;
  {
    ScanNode probe_scan(probe_table,
                        std::vector<ExprNode *>{&probe_pk, &probe_int64, &probe_int32, &probe_varchar},
                        std::vector<ExprNode *>{}, false);
    ScanNode build_scan(build_table,
                        std::vector<ExprNode *>{&build_int64, &build_int32, &build_varchar},
                        std::vector<ExprNode *>{}, false);
    HashJoinNode
        join_node(&build_scan,
                  &probe_scan,
                  std::move(build_cols),
                  std::move(probe_cols),
                  std::move(projections),
                  JoinType::INNER);
    if (use_bloom) {
      join_node.UseBloomFilter(2100);
    }

    // Actual hash join result.
    auto check_row = [&](const VectorProjection *vp) {
      auto build_int64_data = vp->VectorAt(0)->DataAs<int64_t>();
      auto build_int32_data = vp->VectorAt(1)->DataAs<int32_t>();
      auto build_varchar_data = vp->VectorAt(2)->DataAs<Varlen>();
      //auto probe_pk_data = vp->VectorAt(3)->DataAs<int64_t>();
      auto probe_int64_data = vp->VectorAt(4)->DataAs<int64_t>();
      auto probe_int32_data = vp->VectorAt(5)->DataAs<int32_t>();
      auto probe_varchar_data = vp->VectorAt(6)->DataAs<Varlen>();

      vp->GetFilter()->Map([&](sel_t i) {
        REQUIRE(build_int64_data[i] == probe_int64_data[i]);
        REQUIRE(build_int32_data[i] == probe_int32_data[i]);
        REQUIRE(build_varchar_data[i] == probe_varchar_data[i]);
        num_matches++;
      });
    };
    auto join_executor = ExecutionFactory::MakePlanExecutor(&join_node);
    CheckRows(join_executor.get(), check_row);
  }
  REQUIRE(num_matches == expected_num_matches);
}

TEST_CASE("Single Integer Column Join") {
  TableLoader::LoadJoinTables("tpch_data");
  std::vector<uint64_t> build_key_cols = {build_int64_col};
  std::vector<uint64_t> probe_key_cols = {probe_int64_col};
  auto nl_match = [&](const VectorProjection *probe_vp, const VectorProjection *build_vp, sel_t i, sel_t j) {
    auto probe_int64_data = probe_vp->VectorAt(probe_int64_col)->DataAs<int64_t>();
    auto build_int64_data = build_vp->VectorAt(build_int64_col)->DataAs<int64_t>();
    return (probe_int64_data[i] == build_int64_data[j]);
  };
  TestJoin(std::move(build_key_cols), std::move(probe_key_cols), nl_match, false);
}

TEST_CASE("Two Integer Columns Join") {
  TableLoader::LoadJoinTables("tpch_data");
  std::vector<uint64_t> build_key_cols = {build_int64_col, build_int32_col};
  std::vector<uint64_t> probe_key_cols = {probe_int64_col, probe_int32_col};
  auto nl_match = [&](const VectorProjection *probe_vp, const VectorProjection *build_vp, sel_t i, sel_t j) {
    auto probe_int64_data = probe_vp->VectorAt(probe_int64_col)->DataAs<int64_t>();
    auto probe_int32_data = probe_vp->VectorAt(probe_int32_col)->DataAs<int32_t>();
    auto build_int64_data = build_vp->VectorAt(build_int64_col)->DataAs<int64_t>();
    auto build_int32_data = build_vp->VectorAt(build_int32_col)->DataAs<int32_t>();
    return (probe_int64_data[i] == build_int64_data[j] && probe_int32_data[i] == build_int32_data[j]);
  };
  TestJoin(std::move(build_key_cols), std::move(probe_key_cols), nl_match, false);
}

TEST_CASE("Single Varchar Column Join") {
  TableLoader::LoadJoinTables("tpch_data");
  std::vector<uint64_t> build_key_cols = {build_varchar_col};
  std::vector<uint64_t> probe_key_cols = {probe_varchar_col};
  auto nl_match = [&](const VectorProjection *probe_vp, const VectorProjection *build_vp, sel_t i, sel_t j) {
    auto probe_varchar_data = probe_vp->VectorAt(probe_varchar_col)->DataAs<Varlen>();
    auto build_varchar_data = build_vp->VectorAt(build_varchar_col)->DataAs<Varlen>();
    return (probe_varchar_data[i] == build_varchar_data[j]);
  };
  TestJoin(std::move(build_key_cols), std::move(probe_key_cols), nl_match, false);
}

TEST_CASE("Mixed Type Columns Join") {
  TableLoader::LoadJoinTables("tpch_data");
  std::vector<uint64_t> build_key_cols = {build_varchar_col, build_int32_col, build_int64_col};
  std::vector<uint64_t> probe_key_cols = {probe_varchar_col, probe_int32_col, probe_int64_col};
  auto nl_match = [&](const VectorProjection *probe_vp, const VectorProjection *build_vp, sel_t i, sel_t j) {
    auto probe_varchar_data = probe_vp->VectorAt(probe_varchar_col)->DataAs<Varlen>();
    auto probe_int32_data = probe_vp->VectorAt(probe_int32_col)->DataAs<int32_t>();
    auto probe_int64_data = probe_vp->VectorAt(probe_int64_col)->DataAs<int64_t>();
    auto build_varchar_data = build_vp->VectorAt(build_varchar_col)->DataAs<Varlen>();
    auto build_int32_data = build_vp->VectorAt(build_int32_col)->DataAs<int32_t>();
    auto build_int64_data = build_vp->VectorAt(build_int64_col)->DataAs<int64_t>();
    return (probe_varchar_data[i] == build_varchar_data[j] && probe_int32_data[i] == build_int32_data[j]
        && probe_int64_data[i] == build_int64_data[j]);
  };
  TestJoin(std::move(build_key_cols), std::move(probe_key_cols), nl_match, false);
}

TEST_CASE("Single Integer Column Join With Bloom Filter") {
  TableLoader::LoadJoinTables("tpch_data");
  std::vector<uint64_t> build_key_cols = {build_int64_col};
  std::vector<uint64_t> probe_key_cols = {probe_int64_col};
  auto nl_match = [&](const VectorProjection *probe_vp, const VectorProjection *build_vp, sel_t i, sel_t j) {
    auto probe_int64_data = probe_vp->VectorAt(probe_int64_col)->DataAs<int64_t>();
    auto build_int64_data = build_vp->VectorAt(build_int64_col)->DataAs<int64_t>();
    return (probe_int64_data[i] == build_int64_data[j]);
  };
  TestJoin(std::move(build_key_cols), std::move(probe_key_cols), nl_match, true);
}


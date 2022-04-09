#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "storage/table_loader.h"
#include "execution/nodes/plan_node.h"
#include "execution/execution_factory.h"
#include "test_util/test_table_info.h"
using namespace smartid;

constexpr const char* build_upper_param = "build_upper";
constexpr const char* probe_upper_param = "probe_upper";

void Execute(PlanNode* node, ExecutionContext* ctx) {
  NoopOutputNode noop(node);
  auto executor = ExecutionFactory::MakePlanExecutor(&noop, ctx);
  executor->Next();
  executor->CollectStats();
}

ScanNode* MakeBuildScan(ExecutionFactory* factory) {
  auto build_table = Catalog::Instance()->GetTable(embedding_build_name);
  // Columns
  auto build_pk = factory->MakeCol(embedding_build_pk_idx);
  // Projections
  std::vector<ExprNode*> projections{build_pk};
  // Filters
  auto build_upper = factory->MakeParam(build_upper_param);
  auto comp = factory->MakeBinaryComp(build_pk, build_upper, OpType::LT);
  std::vector<ExprNode*> filters{comp};
  // Scan
  return factory->MakeScan(build_table, std::move(projections), std::move(filters));
}

ScanNode* MakeProbeScan(ExecutionFactory* factory) {
  auto probe_table = Catalog::Instance()->GetTable(embedding_probe_name);
  // Columns
  auto probe_fk = factory->MakeCol(embedding_probe_fk_idx);
  // Projections
  std::vector<ExprNode*> projections{probe_fk};
  // Filters
  auto probe_upper = factory->MakeParam(probe_upper_param);
  auto comp = factory->MakeBinaryComp(probe_fk, probe_upper, OpType::LT);
  std::vector<ExprNode*> filters{comp};
  // Scan
  return factory->MakeScan(probe_table, std::move(projections), std::move(filters));
}

HashJoinNode* MakeJoin(ExecutionFactory* factory, ScanNode* build_scan, ScanNode* probe_scan) {
  std::vector<uint64_t> build_keys{0};
  std::vector<uint64_t> probe_keys{0};
  std::vector<std::pair<uint64_t, uint64_t>> projections {
      {0, 0},
      {1, 0}
  };
  return factory->MakeHashJoin(build_scan, probe_scan, std::move(build_keys), std::move(probe_keys), std::move(projections), JoinType::INNER);
}

void CheckScan(ScanNode* scan, int32_t table_size, std::vector<int32_t> expected_params, std::vector<int32_t> expected_out_sizes) {
  // The build has one filter.
  auto filters = scan->GetFilters();
  REQUIRE(filters.size() == 1);
  auto comp = dynamic_cast<const BinaryCompNode*>(filters[0]);
  REQUIRE(comp != nullptr);

  // Check parameter values
  auto param = dynamic_cast<const ParamNode*>(comp->Child(1));
  REQUIRE(param != nullptr);
  auto param_stats = param->GetStats();
  REQUIRE(param_stats.val_type == SqlType::Int32);
  REQUIRE(param_stats.vals.size() == expected_params.size());
  for (uint64_t i = 0; i < expected_params.size(); i++) {
    REQUIRE(param_stats.vals[i] == Value(int32_t(expected_params[i])));
  }

  // Check filter selectivities.
  auto comp_stats = comp->GetStats();
  REQUIRE(comp_stats.sels_.size() == expected_out_sizes.size());
  for (uint64_t i = 0; i < expected_out_sizes.size(); i++) {
    REQUIRE(comp_stats.sels_[i].first == table_size);
    REQUIRE(comp_stats.sels_[i].second == expected_out_sizes[i]);
  }
}

void CheckJoin(HashJoinNode* join_node,
               std::vector<int32_t> expected_build_params,
               std::vector<int32_t> expected_build_out_sizes,
               std::vector<int32_t> expected_probe_params,
               std::vector<int32_t> expected_probe_out_sizes,
               std::vector<JoinStats> expected_join_stats) {
  // Check scans
  auto build_node = dynamic_cast<ScanNode*>(join_node->Child(0));
  REQUIRE(build_node != nullptr);
  CheckScan(build_node, embedding_build_size, expected_build_params, expected_build_out_sizes);
  auto probe_node = dynamic_cast<ScanNode*>(join_node->Child(1));
  CheckScan(probe_node, embedding_probe_size, expected_probe_params, expected_probe_out_sizes);

  // Check joins
  auto join_stats = join_node->GetJoinStats();
  REQUIRE(join_stats.size() == expected_join_stats.size());
  for (uint64_t i = 0; i < join_stats.size(); i++) {
    REQUIRE(join_stats[i].build_in == expected_join_stats[i].build_in);
    REQUIRE(join_stats[i].build_out == expected_join_stats[i].build_out);
    REQUIRE(join_stats[i].probe_in == expected_join_stats[i].probe_in);
    REQUIRE(join_stats[i].probe_out == expected_join_stats[i].probe_out);
  }

}


TEST_CASE("Scan with params") {
  TableLoader::LoadJoinTables("tpch_data");
  ExecutionFactory factory;
  auto build_scan = MakeBuildScan(&factory);
  auto probe_scan = MakeProbeScan(&factory);
  ExecutionContext ctx;
  // Execute with one parameter value.
  int32_t param1 = embedding_build_size / 2;
  ctx.AddParam(build_upper_param, Value(int32_t(param1)), SqlType::Int32);
  ctx.AddParam(probe_upper_param, Value(int32_t(param1)), SqlType::Int32);
  Execute(build_scan, &ctx);
  Execute(probe_scan, &ctx);
  // Execute with another parameter value.
  int32_t param2 = embedding_build_size / 4;
  ctx.AddParam(build_upper_param, Value(int32_t(param2)), SqlType::Int32);
  ctx.AddParam(probe_upper_param, Value(int32_t(param2)), SqlType::Int32);
  Execute(build_scan, &ctx);
  Execute(probe_scan, &ctx);
  // Check content
  // The build table is filter on pk. So size matches paramaters.
  CheckScan(build_scan, embedding_build_size, {param1, param2}, {param1, param2});
  // The build table is filter on fk. So size is twice paramaters.
  CheckScan(probe_scan, embedding_probe_size, {param1, param2}, {2 * param1, 2 * param2});
}

TEST_CASE("Join with params") {
  TableLoader::LoadJoinTables("tpch_data");
  ExecutionFactory factory;
  ExecutionContext ctx;
  auto build_scan = MakeBuildScan(&factory);
  auto probe_scan = MakeProbeScan(&factory);
  auto join = MakeJoin(&factory, build_scan, probe_scan);
  std::vector<int32_t> expected_build_params;
  std::vector<int32_t> expected_build_out_sizes;
  std::vector<int32_t> expected_probe_params;
  std::vector<int32_t> expected_probe_out_sizes;
  std::vector<JoinStats> expected_join_stats;

  // First, filter only on the build side.
  // The build match should be 100%. The probe match should be 50%.
  {
    int32_t build_upper = embedding_build_size / 2;
    int32_t probe_upper = embedding_build_size;
    ctx.AddParam(build_upper_param, Value(build_upper), SqlType::Int32);
    ctx.AddParam(probe_upper_param, Value(probe_upper), SqlType::Int32);
    Execute(join, &ctx);
    // Expected stats
    expected_build_params.emplace_back(build_upper);
    expected_build_out_sizes.emplace_back(build_upper);
    expected_probe_params.emplace_back(probe_upper);
    expected_probe_out_sizes.emplace_back(2*probe_upper);
    JoinStats expected_stats;
    expected_stats.build_in = expected_build_out_sizes.back();
    expected_stats.build_out = expected_stats.build_in;
    expected_stats.probe_in = expected_probe_out_sizes.back();
    expected_stats.probe_out = expected_stats.probe_in / 2;
    expected_join_stats.emplace_back(expected_stats);
  }
  // Second, filter only on the prove side.
  // The build match should be 50%. The probe match should be 100%.
  {
    int32_t build_upper = embedding_build_size;
    int32_t probe_upper = embedding_build_size / 2;
    ctx.AddParam(build_upper_param, Value(build_upper), SqlType::Int32);
    ctx.AddParam(probe_upper_param, Value(probe_upper), SqlType::Int32);
    Execute(join, &ctx);
    // Expected stats
    expected_build_params.emplace_back(build_upper);
    expected_build_out_sizes.emplace_back(build_upper);
    expected_probe_params.emplace_back(probe_upper);
    expected_probe_out_sizes.emplace_back(2*probe_upper);
    JoinStats expected_stats;
    expected_stats.build_in = expected_build_out_sizes.back();
    expected_stats.build_out = expected_stats.build_in / 2;
    expected_stats.probe_in = expected_probe_out_sizes.back();
    expected_stats.probe_out = expected_stats.probe_in;
    expected_join_stats.emplace_back(expected_stats);
  }
  // Third, select 1/4 build tuples, and 1/2 probe tuples.
  // The build match should be 100%. The probe match should be 50%.
  {
    int32_t build_upper = embedding_build_size / 4;
    int32_t probe_upper = embedding_build_size / 2;
    ctx.AddParam(build_upper_param, Value(build_upper), SqlType::Int32);
    ctx.AddParam(probe_upper_param, Value(probe_upper), SqlType::Int32);
    Execute(join, &ctx);
    // Expected stats
    expected_build_params.emplace_back(build_upper);
    expected_build_out_sizes.emplace_back(build_upper);
    expected_probe_params.emplace_back(probe_upper);
    expected_probe_out_sizes.emplace_back(2*probe_upper);
    JoinStats expected_stats;
    expected_stats.build_in = expected_build_out_sizes.back();
    expected_stats.build_out = expected_stats.build_in;
    expected_stats.probe_in = expected_probe_out_sizes.back();
    expected_stats.probe_out = expected_stats.probe_in / 2;
    expected_join_stats.emplace_back(expected_stats);
  }
  // Third, select 1/2 build tuples, and 1/4 probe tuples.
  // The build match should be 50%. The probe match should be 100%.
  {
    int32_t build_upper = embedding_build_size / 2;
    int32_t probe_upper = embedding_build_size / 4;
    ctx.AddParam(build_upper_param, Value(build_upper), SqlType::Int32);
    ctx.AddParam(probe_upper_param, Value(probe_upper), SqlType::Int32);
    Execute(join, &ctx);
    // Expected stats
    expected_build_params.emplace_back(build_upper);
    expected_build_out_sizes.emplace_back(build_upper);
    expected_probe_params.emplace_back(probe_upper);
    expected_probe_out_sizes.emplace_back(2*probe_upper);
    JoinStats expected_stats;
    expected_stats.build_in = expected_build_out_sizes.back();
    expected_stats.build_out = expected_stats.build_in / 2;
    expected_stats.probe_in = expected_probe_out_sizes.back();
    expected_stats.probe_out = expected_stats.probe_in;
    expected_join_stats.emplace_back(expected_stats);
  }



  CheckJoin(join, expected_build_params, expected_build_out_sizes, expected_probe_params, expected_probe_out_sizes, expected_join_stats);
}
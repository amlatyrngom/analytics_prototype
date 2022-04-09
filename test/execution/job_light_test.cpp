#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "stats/schema_graph.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
#include "execution/execution_factory.h"
#include <fstream>
using namespace smartid;

constexpr const char* year_hi = "year_hi";
constexpr const char* role_id_exact = "role_id_exact";
int32_t year_upper = 2000;
int32_t role_id = 11;
int16_t year_embedding = 255;
int16_t role_id_embedding = 128;

template<typename RowChecker>
void CheckRows(PlanExecutor* executor, RowChecker checker) {
  const VectorProjection *vp;
  while ((vp = executor->Next())) {
    vp->GetFilter()->Map([&](sel_t i) {
      checker(vp, i);
    });
  }
}

int64_t Execute(PlanNode* node, ExecutionContext* ctx) {
  PrintNode final_node(node);
  auto executor = ExecutionFactory::MakePlanExecutor(&final_node, ctx);
  auto start = std::chrono::high_resolution_clock::now();
  executor->Next();
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = duration_cast<std::chrono::nanoseconds>(end - start).count();
  executor->CollectStats();
  return duration;
}

int64_t Counter(PlanNode* node, ExecutionFactory* factory, ExecutionContext* ctx) {
  auto count_node = factory->MakeStaticAggregation(node, {{0, AggType::COUNT}});
  return Execute(count_node, ctx);
}

uint64_t GetCount(PlanNode* node, ExecutionFactory* factory, ExecutionContext* ctx) {
  auto count_node = factory->MakeStaticAggregation(node, {{0, AggType::COUNT}});
  auto executor = ExecutionFactory::MakePlanExecutor(count_node, ctx);
  uint64_t count = 0;
  CheckRows(executor.get(), [&] (const VectorProjection* vp, sel_t i) {
    count = vp->VectorAt(0)->DataAs<uint64_t>()[0];
  });
  return count;
}



ScanNode* ScanTitle(ExecutionFactory* factory, Table* title, bool embed, bool ideal) {
  auto t_id = factory->MakeCol(title->GetSchema().ColIdx("id"));
  auto t_year = factory->MakeCol(title->GetSchema().ColIdx("production_year"));
  auto t_embedding = factory->MakeCol(title->GetSchema().ColIdx("embedding"));
  auto t_ideal_embedding = factory->MakeCol(title->GetSchema().ColIdx("ideal_embedding"));
  // Projections
  std::vector<ExprNode*> t_projs{t_id};
  // Filters
  auto t_year_hi = factory->MakeParam(year_hi);
  auto t_comp = factory->MakeBinaryComp(t_year, t_year_hi, OpType::LT);
  std::vector<ExprNode*> t_filters{t_comp};
  if (embed) {
    auto t_check = factory->MakeEmbeddingCheck(t_embedding, role_id_embedding);
    t_filters.emplace_back(t_check);
  }
  if (ideal) {
    t_filters.pop_back();
    auto one_const = factory->MakeConst(Value(int16_t(1)), SqlType::Int16);
    auto t_check = factory->MakeBinaryComp(t_ideal_embedding, one_const, OpType::EQ);
    t_filters.emplace_back(t_check);
  }
  return factory->MakeScan(title, std::move(t_projs), std::move(t_filters));
}

ScanNode* ScanCastInfo(ExecutionFactory* factory, Table* cast_info, bool embed, bool ideal) {
  auto ci_movie_id = factory->MakeCol(cast_info->GetSchema().ColIdx("movie_id"));
  auto ci_role_id = factory->MakeCol(cast_info->GetSchema().ColIdx("role_id"));
  auto ci_embedding = factory->MakeCol(cast_info->GetSchema().ColIdx("embedding"));
  auto ci_ideal_embedding = factory->MakeCol(cast_info->GetSchema().ColIdx("ideal_embedding"));
  // Projections
  std::vector<ExprNode*> ci_projs{ci_movie_id};
  // Filters
  auto ci_role_id_eq = factory->MakeParam(role_id_exact);
  auto ci_comp = factory->MakeBinaryComp(ci_role_id, ci_role_id_eq, OpType::EQ);
  std::vector<ExprNode*> ci_filters{ci_comp};
  if (embed) {
    auto ci_check = factory->MakeEmbeddingCheck(ci_embedding, year_embedding);
    ci_filters.emplace_back(ci_check);
  }
  if (ideal) {
    ci_filters.pop_back();
    auto one_const = factory->MakeConst(Value(int16_t(1)), SqlType::Int16);
    auto ci_check = factory->MakeBinaryComp(ci_ideal_embedding, one_const, OpType::EQ);
    ci_filters.emplace_back(ci_check);
  }

  return factory->MakeScan(cast_info, std::move(ci_projs), std::move(ci_filters));
}

using StatsMap = const std::vector<JoinStats>*;


std::pair<HashJoinNode*, bool> Join(ExecutionFactory* factory, ExecutionContext* ctx, ScanNode* t_scan, ScanNode* ci_scan, bool good_order, bool bloom) {
  std::vector<uint64_t> build_keys{0};
  std::vector<uint64_t> probe_keys{0};
  // Return any of the ids for counting
  std::vector<std::pair<uint64_t, uint64_t>> join_projs {
      {0, 0}
  };

  // Join order
  auto t_input = GetCount(t_scan, factory, ctx);
  auto ci_input = GetCount(ci_scan, factory, ctx);
  auto is_t_build = (good_order && t_input < ci_input) || (!good_order && ci_input < t_input);
  ScanNode* build;
  ScanNode* probe;
  uint64_t build_in;
  if (is_t_build) {
    build = t_scan;
    probe = ci_scan;
    build_in = t_input;
  } else {
    build = ci_scan;
    probe = t_scan;
    build_in = ci_input;
  }
  auto join = factory->MakeHashJoin(build, probe,
                                    std::move(build_keys), std::move(probe_keys),
                                    std::move(join_projs), JoinType::INNER);
  if (bloom) {
    join->UseBloomFilter(build_in);
  }
  return {join, is_t_build};
}

void PrintStats(const std::string& test_name, const std::vector<double>& t_scan_times, const std::vector<double>& ci_scan_times,
                const std::vector<JoinStats>& join_times, const std::vector<int64_t>& totals, bool is_t_build) {
  auto get_probe_time = [&](const JoinStats& join_stats) {
    return join_stats.probe_time;
  };
  auto get_build_time = [&](const JoinStats& join_stats) {
    return join_stats.insert_time;
  };
  std::string filename = test_name + ".csv";
  std::ofstream file_os(filename);
  file_os << "t_count,ci_count,bloom_in,bloom_out,t_scan_time,ci_scan_time,build_time,probe_time,total" << std::endl;
  for(uint64_t i = 0; i < t_scan_times.size(); i++) {
    auto t_time = t_scan_times[i];
    auto ci_time = ci_scan_times[i];
    const auto& join_stat = join_times[i];
    auto probe_time = get_probe_time(join_stat);
    auto probe_in = join_stat.probe_in;
    auto build_time = get_build_time(join_stat);
    auto build_in = join_stat.build_in;
    auto bloom_in = join_stat.bloom_before;
    auto bloom_out = join_stat.bloom_after;
    auto total = totals[i];
    auto t_count = is_t_build ? build_in : probe_in;
    auto ci_count = is_t_build ? probe_in : build_in;
    file_os << t_count << ","
            << ci_count << ","
            << bloom_in << ","
            << bloom_out << ","
            << t_time / 1e6 << ","
            << ci_time / 1e6 << ","
            << build_time / 1e6 << ","
            << probe_time / 1e6 << ","
            << total / 1e6
            << std::endl;
  }
}

enum class TestType {
  Embed, Bloom, Ideal, None
};

enum class JoinOrder {
  Good, Bad
};

std::string MakeTestName(TestType test_type, JoinOrder join_order) {
  std::string test_type_str{}, join_order_str{};
  switch (test_type) {
    case TestType::Embed:
      test_type_str = "embed";
      break;
    case TestType::Bloom:
      test_type_str = "bloom";
      break;
    case TestType::Ideal:
      test_type_str = "ideal";
      break;
    case TestType::None:
      test_type_str = "none";
      break;
  }
  switch (join_order) {
    case JoinOrder::Good:
      join_order_str = "good";
      break;
    case JoinOrder::Bad:
      join_order_str = "bad";
  }
  std::stringstream ss;
  ss << "motivation_" << test_type_str << "_" << join_order_str;
  return ss.str();
}

void RunTest(TestType test_type, JoinOrder join_order) {
  ExecutionFactory factory;
  ExecutionContext ctx;
  std::vector<int64_t> totals;

  auto test_name = MakeTestName(test_type, join_order);
  bool embed = test_type == TestType::Embed;
  bool ideal = test_type == TestType::Ideal;
  bool bloom = test_type == TestType::Bloom;
  bool good_order = join_order == JoinOrder::Good;

  auto catalog = Catalog::Instance();
  auto title = catalog->GetTable("title");
  auto cast_info = catalog->GetTable("cast_info");
  ctx.AddParam(year_hi, Value(year_upper), SqlType::Int32);
  ctx.AddParam(role_id_exact, Value(role_id), SqlType::Int32);
  auto t_scan = ScanTitle(&factory, title, embed, ideal);
  auto t_scan_times = &t_scan->GetStats();
  auto ci_scan = ScanCastInfo(&factory, cast_info, embed, ideal);
  auto ci_scan_times = &ci_scan->GetStats();
  auto [join, is_t_build] = Join(&factory, &ctx, t_scan, ci_scan, good_order, bloom);
  auto join_times = &join->GetJoinStats();
  for (int i = 0; i < 1; i++){
    auto duration = Counter(join, &factory, &ctx);
    totals.emplace_back(duration);
  }
  PrintStats(test_name, *t_scan_times, *ci_scan_times, *join_times, totals, is_t_build);
}

TEST_CASE("Run all tests") {
  TableLoader::LoadJOBLight("job_light_mini_embedded");
//  RunTest(TestType::None, JoinOrder::Good);
//  RunTest(TestType::Embed, JoinOrder::Good);
//  RunTest(TestType::Bloom, JoinOrder::Good);
  RunTest(TestType::Ideal, JoinOrder::Good);
//  RunTest(TestType::None, JoinOrder::Bad);
//  RunTest(TestType::Embed, JoinOrder::Bad);
//  RunTest(TestType::Bloom, JoinOrder::Bad);
  RunTest(TestType::Ideal, JoinOrder::Bad);
}




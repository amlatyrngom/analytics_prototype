#define DATA_PATH "/Users/amlatyr/Code/duckdb_test/tpch_embedded"
#define DEC_BEST
#define DEC_RR
#define KD

#undef KD
#undef DEC_RR


#include "bench_util/tcph_creates.h"
#include "execution/execution_factory.h"
#include <chrono>
#include <sstream>

using namespace smartid;

// How to run this expt.
// 1. Generate TPCH data, train the trees, and dump the embeddings in "duckdb_test".
// 2. Step (1) should output the embeddings to use for the test. Use those in these experiments.


using ScanTimes = const std::vector<double>*;
auto comp_date = Date::FromYMD(1996, 4, 1);

ScanNode* ScanCustomer(ExecutionFactory* factory, ScanTimes* scan_times, const uint64_t* mask=nullptr) {
  auto customer = Catalog::Instance()->GetTable("customer");
  const auto& schema = customer->GetSchema();
  auto custkey = factory->MakeCol(schema.ColIdx("c_custkey"));
  auto mktsegment = factory->MakeCol(schema.ColIdx("c_mktsegment"));
  auto embedding = factory->MakeCol(schema.ColIdx("embedding"));
  std::vector<ExprNode*> c_projs{custkey};

  // Filters
  auto mktsegment_rhs = factory->MakeConst(Value(int32_t(1)), SqlType::Int32);
  auto mktsegment_comp = factory->MakeBinaryComp(mktsegment, mktsegment_rhs, OpType::EQ);
  std::vector<ExprNode*> c_filters;
  c_filters.emplace_back(mktsegment_comp);
  if (mask != nullptr) {
    c_filters.emplace_back(factory->MakeEmbeddingCheck(embedding, *mask));
  }

  auto c_scan = factory->MakeScan(customer, std::move(c_projs), std::move(c_filters));
  *scan_times = &c_scan->GetStats();
  return c_scan;
}

ScanNode* ScanOrders(ExecutionFactory* factory, ScanTimes* scan_times, const uint64_t* mask=nullptr) {
  auto orders = Catalog::Instance()->GetTable("orders");
  const auto& schema = orders->GetSchema();
  auto custkey = factory->MakeCol(schema.ColIdx("o_custkey"));
  auto orderkey = factory->MakeCol(schema.ColIdx("o_orderkey"));
  auto orderdate = factory->MakeCol(schema.ColIdx("o_orderdate"));
  auto embedding = factory->MakeCol(schema.ColIdx("embedding"));
  std::vector<ExprNode*> o_projs{custkey, orderkey};

  // Filters
  auto date_rhs = factory->MakeConst(Value(comp_date), SqlType::Date);
  auto date_comp = factory->MakeBinaryComp(orderdate, date_rhs, OpType::LT);
  std::vector<ExprNode*> o_filters;
  o_filters.emplace_back(date_comp);
  if (mask != nullptr) {
    o_filters.emplace_back(factory->MakeEmbeddingCheck(embedding, *mask));
  }

  auto o_scan = factory->MakeScan(orders, std::move(o_projs), std::move(o_filters));
  *scan_times = &o_scan->GetStats();
  return o_scan;
}

ScanNode* ScanLineitem(ExecutionFactory* factory, ScanTimes * scan_times, const uint64_t* mask=nullptr) {
  auto lineitem = Catalog::Instance()->GetTable("lineitem");
  const auto& schema = lineitem->GetSchema();
  auto orderkey = factory->MakeCol(schema.ColIdx("l_orderkey"));
  auto shipdate = factory->MakeCol(schema.ColIdx("l_shipdate"));
  auto embedding = factory->MakeCol(schema.ColIdx("embedding"));
  std::vector<ExprNode*> l_projs{orderkey};

  // Filters
  auto date_rhs = factory->MakeConst(Value(comp_date), SqlType::Date);
  auto date_comp = factory->MakeBinaryComp(shipdate, date_rhs, OpType::GT);
  std::vector<ExprNode*> l_filters;
  l_filters.emplace_back(date_comp);
  if (mask != nullptr) {
    l_filters.emplace_back(factory->MakeEmbeddingCheck(embedding, *mask));
  }

  auto l_scan = factory->MakeScan(lineitem, std::move(l_projs), std::move(l_filters));
  *scan_times = &l_scan->GetStats();
  return l_scan;
}

std::pair<HashJoinNode*, HashJoinNode*> JoinTables(ExecutionFactory* factory, ScanNode* c_scan, ScanNode* o_scan, ScanNode* l_scan) {
  // c, o
  HashJoinNode* join1;
  {
    std::vector<uint64_t> build_keys{0}; // c_custkey
    std::vector<uint64_t> probe_keys{0}; // o_custkey
    std::vector<std::pair<uint64_t, uint64_t>> join_projs{
        {1, 1} // o_orderkey
    };
    join1 = factory->MakeHashJoin(c_scan, o_scan, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
  }
  HashJoinNode* join2;
  {
    std::vector<uint64_t> build_keys{0}; // o_orderkey
    std::vector<uint64_t> probe_keys{0}; // l_orderkey
    std::vector<std::pair<uint64_t, uint64_t>> join_projs{
        {0, 0} // o_orderkey
    };
    join2 = factory->MakeHashJoin(join1, l_scan, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
  }
  return {join1, join2};
}

StaticAggregateNode* Count(ExecutionFactory* factory, PlanNode* child) {
  return factory->MakeStaticAggregation(child, {
      {0, AggType::COUNT},
  });
}

std::string Run(const std::string& name, uint64_t num_runs, uint64_t* c_mask, uint64_t* o_mask, uint64_t* l_mask) {
  double duration = 0;
  uint64_t c_count = 0;
  uint64_t o_count = 0;
  uint64_t l_count = 0;
  double c_time = 0;
  double o_time = 0;
  double l_time = 0;
  double join1_time = 0;
  double join2_time = 0;
  ScanTimes c_times;
  ScanTimes o_times;
  ScanTimes l_times;
  for (uint64_t i = 0; i < num_runs; i++) {
    ExecutionFactory factory;
    auto c_scan = ScanCustomer(&factory, &c_times, c_mask);
    auto o_scan = ScanOrders(&factory, &o_times, o_mask);
    auto l_scan = ScanLineitem(&factory, &l_times, l_mask);
    auto [join1, join2] = JoinTables(&factory, c_scan, o_scan, l_scan);
    auto count_node = Count(&factory, join2);
    auto print_node = factory.MakePrint(count_node);
    auto exec = ExecutionFactory::MakePlanExecutor(print_node);

    auto start = std::chrono::high_resolution_clock::now();
    exec->Next();
    exec->CollectStats();
    auto end = std::chrono::high_resolution_clock::now();
    duration += duration_cast<std::chrono::nanoseconds>(end - start).count();
    c_time += c_times->at(0);
    o_time += o_times->at(0);
    l_time += l_times->at(0);
    auto& join1_stats = join1->GetJoinStats()[0];
    auto& join2_stats = join2->GetJoinStats()[0];
    c_count += join1_stats.build_in;
    o_count += join1_stats.probe_in;
    l_count += join2_stats.probe_in;
    join1_time += join1_stats.build_hash_time + join1_stats.insert_time + join1_stats.probe_hash_time + join1_stats.probe_time;
    join2_time += join2_stats.build_hash_time + join2_stats.insert_time + join2_stats.probe_hash_time + join2_stats.probe_time;
  }

  duration /= num_runs;
  c_count /= num_runs;
  o_count /= num_runs;
  l_count /= num_runs;
  c_time /= num_runs;
  o_time /= num_runs;
  l_time /= num_runs;
  join1_time /= num_runs;
  join2_time /= num_runs;

  std::stringstream ss;
  ss << name << ": Total Time = " << duration << ", "
            << "C Time = " << c_time << ", "
            << "C Count = " << c_count << ", "
            << "O Time = " << o_time << ", "
            << "O Count = " << o_count << ", "
            << "L Time = " << l_time << ", "
            << "L Count = " << l_count << ", "
            << "J1 Time = " << join1_time << ", "
            << "J2 Time = " << join2_time << std::endl;
  return ss.str();
}


int main() {
  TpchCreates::LoadTPCH(DATA_PATH);
  uint64_t num_runs = 10;

  // Unopt
  std::string unopt, dec_best, kd, dec_kd, unif;
  uint64_t c_mask, o_mask, l_mask;
  unopt = Run("Unopt", num_runs, nullptr, nullptr, nullptr);



//  // Dec Best-First
//  c_mask = 5016428481760ull;
//  o_mask = 4278190080ull;
//  l_mask = 4186112ull;
//  dec_best = Run("DecBest", num_runs, &c_mask, &o_mask, &l_mask);
//
//  // KD-Med
//  c_mask = 3768582144ull;
//  o_mask = 6761996510855168ull;
//  l_mask = 229401ull;
//  kd = Run("KD", num_runs, &c_mask, &o_mask, &l_mask);
//
//  // Dec-KD
//  c_mask = 1081144286034001920ull;
//  o_mask = 25769803776ull;
//  l_mask = 54760833228ull;
//  dec_kd = Run("DecKD", num_runs, &c_mask, &o_mask, &l_mask);

  // KD-Mid
  c_mask = 7595426324676604ull;
  o_mask = 52ull;
  l_mask = 33554913ull;
  unif = Run("Unif", num_runs, &c_mask, &o_mask, &l_mask);

  std::cout << unopt << dec_best << kd << dec_kd << unif << std::endl;
}

#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "stats/schema_graph.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
#include "execution/execution_factory.h"
#include <fstream>
using namespace smartid;

void Execute(PlanNode* node, ExecutionContext* ctx) {
  NoopOutputNode noop(node);
  auto executor = ExecutionFactory::MakePlanExecutor(&noop, ctx);
  executor->Next();
  executor->CollectStats();
}

void Counter(PlanNode* node, ExecutionFactory* factory, ExecutionContext* ctx) {
  auto count_node = factory->MakeStaticAggregation(node, {{0, AggType::COUNT}});
  Execute(count_node, ctx);
}

using StatsMap = std::vector<const std::vector<JoinStats>*>;

constexpr const char* q4_orderdate_lower = "q4_orderdate_lower";
constexpr const char* q4_orderdate_upper = "q4_orderdate_upper";
constexpr const char* q5_orderdate_lower = "q5_orderdate_lower";
constexpr const char* q5_orderdate_upper = "q5_orderdate_upper";
constexpr const char* q3_orderdate_upper = "q3_orderdate_upper";
constexpr const char* q3_shipdate_lower = "q3_shipdate_lower";

std::string asia_str("ASIA");
Varlen asia_varlen(asia_str.size(), asia_str.data());
std::string building_str("BUILDING");
Varlen building_varlen(building_str.size(), building_str.data());

template <typename F>
void Printer(std::ostream & os, const std::string& data_name, const StatsMap& stats_map, F getter) {
  std::cout << "==================== " << data_name << " =============================" << std::endl;
  std::vector<std::string> col_names{"JoinName", "ColdRun1", "ColdRun2", "WarmRun1", "WarmRun2"};
  for (uint64_t i = 0; i < col_names.size(); i++) {
    os << col_names[i];
    if (i == col_names.size() - 1) {
      os << std::endl;
    } else {
      os << ",";
    }
  }
  for (uint64_t i = 0; i < stats_map.size(); i++) {
    os << ("Join " + std::to_string(i)) << ","
              << getter((*stats_map[i])[0]) << ","
              << getter((*stats_map[i])[1]) << ","
              << getter((*stats_map[i])[2]) << ","
              << getter((*stats_map[i])[3]) << std::endl;
  }
}

void PrintStatsMap(const std::string& name, const StatsMap& stats_map) {
  auto get_join_time = [&](const JoinStats& join_stats) {
    return join_stats.build_hash_time + join_stats.insert_time + join_stats.probe_hash_time + join_stats.probe_time;
  };
  auto get_probe_time = [&](const JoinStats& join_stats) {
    return join_stats.probe_time + join_stats.probe_hash_time;
  };
  auto get_build_time = [&](const JoinStats& join_stats) {
    return join_stats.insert_time + join_stats.build_hash_time;
  };

  auto get_probe_in = [&](const JoinStats& join_stats) {
    return join_stats.probe_in;
  };
  auto get_probe_out = [&](const JoinStats& join_stats) {
    return join_stats.probe_out;
  };
  auto get_build_in = [&](const JoinStats& join_stats) {
    return join_stats.build_in;
  };
  auto get_build_out = [&](const JoinStats& join_stats) {
    return join_stats.build_out;
  };

  {
    std::string filename = name + "_join_time.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "JOIN TIME", stats_map, get_join_time);
  }
  {
    std::string filename = name + "_probe_time.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "PROBE TIME", stats_map, get_probe_time);
  }
  {
    std::string filename = name + "_build_time.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "BUILD TIME", stats_map, get_build_time);
  }
  {
    std::string filename = name + "_probe_in.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "PROBE IN", stats_map, get_probe_in);
  }
  {
    std::string filename = name + "_probe_out.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "PROBE OUT", stats_map, get_probe_out);
  }
  {
    std::string filename = name + "_build_in.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "BUILD IN", stats_map, get_build_in);
  }
  {
    std::string filename = name + "_build_out.csv";
    std::ofstream file_os(filename);
    Printer(file_os, "BUILD OUT", stats_map, get_build_out);
  }
}

// Return c_custkey
ScanNode* Q3CustomerScan(ExecutionFactory* factory, Table* customer) {
  // Customer
  auto c_custkey = factory->MakeCol(c_custkey_idx);
  auto c_mktsegment = factory->MakeCol(c_mktsegment_idx);
  std::vector<ExprNode*> c_projections{c_custkey};
  // Filter
  auto building_const = factory->MakeConst(Value(building_varlen), SqlType::Varchar);
  auto c_comp = factory->MakeBinaryComp(c_mktsegment, building_const, OpType::EQ);
  std::vector<ExprNode*> c_filters{c_comp};
  return factory->MakeScan(customer, std::move(c_projections), std::move(c_filters));
}

// Return o_custkey, o_orderkey, o_orderdate, o_shippriority
ScanNode* Q3OrdersScan(ExecutionFactory* factory, Table* orders) {
  // Scan orders
  auto o_custkey = factory->MakeCol(o_custkey_idx);
  auto o_orderkey = factory->MakeCol(o_orderkey_idx);
  auto o_orderdate = factory->MakeCol(o_orderdate_idx);
  auto o_shippriority = factory->MakeCol(o_shippriority_idx);
  std::vector<ExprNode*> o_projs{o_custkey, o_orderkey, o_orderdate, o_shippriority};
  // Filter
  auto o_orderdate_upper = factory->MakeParam(q3_orderdate_upper);
  auto o_comp = factory->MakeBinaryComp(o_orderdate, o_orderdate_upper, OpType::LT);
  std::vector<ExprNode*> o_filters{o_comp};
  return factory->MakeScan(orders, std::move(o_projs), std::move(o_filters));
}

// Return l_orderkey, revenue
ScanNode* Q3LineitemScan(ExecutionFactory* factory, Table* lineitem) {
  // Scan lineitem
  auto l_orderkey = factory->MakeCol(l_orderkey_idx);
  auto l_shipdate = factory->MakeCol(l_shipdate_idx);
  auto l_extendedprice = factory->MakeCol(l_extendedprice_idx);
  auto l_discount = factory->MakeCol(l_discount_idx);
  // Make revenue
  auto one_const = factory->MakeConst(Value(float(1.0f)), SqlType::Float32);
  auto one_minus_discount = factory->MakeBinaryArith(one_const, l_discount, OpType::SUB, SqlType::Float32);
  auto revenue = factory->MakeBinaryArith(l_extendedprice, one_minus_discount, OpType::MUL, SqlType::Float32);
  std::vector<ExprNode*> l_projs{l_orderkey, revenue};
  // Filter
  auto l_shipdate_lower = factory->MakeParam(q3_shipdate_lower);
  auto l_comp = factory->MakeBinaryComp(l_shipdate, l_shipdate_lower, OpType::GT);
  std::vector<ExprNode*> l_filters{l_comp};
  return factory->MakeScan(lineitem, std::move(l_projs), std::move(l_filters));
}

PlanNode* Q3Join(ExecutionFactory* factory, ExecutionContext* ctx,
                 ScanNode* c_scan, ScanNode* o_scan, ScanNode* l_scan,
                 StatsMap* stats_map) {
  HashJoinNode* join;
  {
    std::vector<uint64_t> build_keys{0}; // c_custkey
    std::vector<uint64_t> probe_keys{0};
    // o_orderkey, o_orderdate, o_shippriority
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
        {1, 1}, {1, 2}, {1, 3}
    };
    join = factory->MakeHashJoin(c_scan, o_scan,
                                 std::move(build_keys), std::move(probe_keys), std::move(join_projs),
                                 JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
  }
  {
    std::vector<uint64_t> build_keys{0}; // o_orderkey
    std::vector<uint64_t> probe_keys{0}; // l_orderkey
    // l_orderkey/o_orderkey, o_orderdate, o_shippriority, revenue
    std::vector<std::pair<uint64_t, uint64_t>> join_projs{
        {0, 0}, {0, 1}, {0, 2}, {1, 1}
    };
    join = factory->MakeHashJoin(join, l_scan,
                                 std::move(build_keys), std::move(probe_keys), std::move(join_projs),
                                 JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
  }
  // TODO(Amadou): Change to sort node one multisort has been made efficient.
  PlanNode* final_node;
  {
    std::vector<uint64_t> group_bys{0, 1, 2}; // orderkey, o_orderdate, o_shippriority
    std::vector<std::pair<uint64_t, AggType>> aggs {
        {3, AggType::SUM}
    };
    final_node = factory->MakeHashAggregation(join, std::move(group_bys), std::move(aggs));
  }
  return final_node;
}

// Returns r_regionkey
ScanNode* Q5RegionScan(ExecutionFactory* factory, Table* region) {
  // Scan region
  auto r_regionkey = factory->MakeCol(r_regionkey_idx);
  auto r_name = factory->MakeCol(r_name_idx);
  std::vector<ExprNode*> r_projections{r_regionkey};
  // Region Filters
  auto r_asia_const = factory->MakeConst(Value(asia_varlen), SqlType::Varchar);
  auto r_asia_comp = factory->MakeBinaryComp(r_name, r_asia_const, OpType::EQ);
  std::vector<ExprNode*> r_filters{r_asia_comp};
  return factory->MakeScan(region, std::move(r_projections), std::move(r_filters));
}

// Returns n_regionkey, n_nationkey, n_name
ScanNode* Q5NationScan(ExecutionFactory* factory, Table* nation) {
  // Scan nation
  // Nation projection
  auto n_regionkey = factory->MakeCol(n_regionkey_idx);
  auto n_nationkey = factory->MakeCol(n_nationkey_idx);
  auto n_name = factory->MakeCol(n_name_idx);
  std::vector<ExprNode*> n_projections{n_regionkey, n_nationkey, n_name};
  return factory->MakeScan(nation, std::move(n_projections), {});
}

// Return c_nationkey, c_custkey
ScanNode* Q5CustomerScan(ExecutionFactory* factory, Table* customer) {
  // Customer
  auto c_nationkey = factory->MakeCol(c_nationkey_idx);
  auto c_custkey = factory->MakeCol(c_custkey_idx);
  std::vector<ExprNode*> c_projections{c_nationkey, c_custkey};
  return factory->MakeScan(customer, std::move(c_projections), {});
}

// Return o_custkey, o_orderkey
ScanNode* Q5OrdersScan(ExecutionFactory* factory, Table* orders) {
  // Scan orders
  auto o_custkey = factory->MakeCol(o_custkey_idx);
  auto o_orderkey = factory->MakeCol(o_orderkey_idx);
  auto o_orderdate = factory->MakeCol(o_orderdate_idx);
  std::vector<ExprNode*> o_projections{o_custkey, o_orderkey};
  // Orders filter
  auto o_orderdate_lower = factory->MakeParam(q5_orderdate_lower);
  auto o_orderdate_upper = factory->MakeParam(q5_orderdate_upper);
  auto comp_lower = factory->MakeBinaryComp(o_orderdate, o_orderdate_lower, OpType::GE);
  auto comp_upper = factory->MakeBinaryComp(o_orderdate, o_orderdate_upper, OpType::LT);
  std::vector<ExprNode*> o_filters{comp_lower, comp_upper};
  // Scan
  return factory->MakeScan(orders, std::move(o_projections), std::move(o_filters));
}

// l_orderkey, l_suppkey, revenue
ScanNode* Q5LineitemScan(ExecutionFactory* factory, Table* lineitem) {
  // Scan lineitem
  auto l_orderkey = factory->MakeCol(l_orderkey_idx);
  auto l_suppkey = factory->MakeCol(l_suppkey_idx);
  auto l_extendedprice = factory->MakeCol(l_extendedprice_idx);
  auto l_discount = factory->MakeCol(l_discount_idx);
  // Make revenue
  auto one_const = factory->MakeConst(Value(float(1.0f)), SqlType::Float32);
  auto one_minus_discount = factory->MakeBinaryArith(one_const, l_discount, OpType::SUB, SqlType::Float32);
  auto revenue = factory->MakeBinaryArith(l_extendedprice, one_minus_discount, OpType::MUL, SqlType::Float32);
  std::vector<ExprNode*> l_projs{l_orderkey, l_suppkey, revenue};
  // Scan
  return factory->MakeScan(lineitem, std::move(l_projs), {});
}

// s_suppkey, s_nationkey
ScanNode* Q5SupplierScan(ExecutionFactory* factory, Table* supplier) {
  // Scan supplier
  auto s_suppkey = factory->MakeCol(s_suppkey_idx);
  auto s_nationkey = factory->MakeCol(s_nationkey_idx);
  std::vector<ExprNode*> s_projs{s_suppkey, s_nationkey};
  return factory->MakeScan(supplier, std::move(s_projs), {});
}



PlanNode* Q5Join(ExecutionFactory* factory, ExecutionContext* ctx,
                 ScanNode* r_scan, ScanNode* n_scan, ScanNode* c_scan,
                 ScanNode* o_scan, ScanNode* l_scan, ScanNode* s_scan,
                 StatsMap* stats_map, std::vector<HashJoinNode*>* join_nodes) {
  HashJoinNode* join;
  {
    // region, nation
    std::vector<uint64_t> build_keys{0}; // r_regionkey
    std::vector<uint64_t> probe_keys{0}; // n_regionkey
    // n_nationkey, n_name
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
        {1, 1}, {1, 2}
    };
    join = factory->MakeHashJoin(r_scan, n_scan,
                                 std::move(build_keys), std::move(probe_keys),
                                 std::move(join_projs), JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
    join_nodes->emplace_back(join);
  }
  {
    // join, customer
    std::vector<uint64_t> build_keys{0}; // n_nationkey
    std::vector<uint64_t> probe_keys{0}; // c_nationkey
    // c_custkey, n_nationkey, n_name
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
      {1, 1}, {0, 0}, {0, 1}
    };
    join = factory->MakeHashJoin(join, c_scan,
                                 std::move(build_keys), std::move(probe_keys),
                                 std::move(join_projs), JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
    join_nodes->emplace_back(join);
  }
  {
    // join, orders
    std::vector<uint64_t> build_keys{0}; // c_custkey
    std::vector<uint64_t> probe_keys{0}; // o_custkey
    // o_orderkey, n_nationkey, n_name
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
        {1, 1}, {0, 1}, {0, 2}
    };
    join = factory->MakeHashJoin(join, o_scan,
                                 std::move(build_keys), std::move(probe_keys),
                                 std::move(join_projs), JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
    join_nodes->emplace_back(join);
  }
  {
    // join, lineitem
    std::vector<uint64_t> build_keys{0}; // o_orderkey
    std::vector<uint64_t> probe_keys{0}; // l_orderkey
    // l_suppkey, n_nationkey, n_name, revenue
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
        {1, 1}, {0, 1}, {0, 2}, {1, 2}
    };
    join = factory->MakeHashJoin(join, l_scan,
                                 std::move(build_keys), std::move(probe_keys),
                                 std::move(join_projs), JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
    join_nodes->emplace_back(join);
  }
  {
    // supplier, join
    std::vector<uint64_t> build_keys{0, 1}; // l_suppkey, n_nationkey
    std::vector<uint64_t> probe_keys{0, 1}; // s_suppkey, s_nationkey
    // n_name, revenue
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
        {1, 2}, {1, 3}
    };
    join = factory->MakeHashJoin(s_scan, join,
                                 std::move(build_keys), std::move(probe_keys),
                                 std::move(join_projs), JoinType::INNER);
    stats_map->emplace_back(&join->GetJoinStats());
    join_nodes->emplace_back(join);
  }
  // Aggregate sort
  SortNode* sort_node;
  {
    std::vector<uint64_t> group_bys{0}; // n_name
    std::vector<std::pair<uint64_t, AggType>> aggs {
        {1, AggType::SUM}, // revenue
    };
    auto hash_aggregation_node = factory->MakeHashAggregation(join, std::move(group_bys), std::move(aggs));

    // Sort
    std::vector<std::pair<uint64_t, SortType>> sort_keys{
        {0, SortType::ASC}, // n_name
    };
    sort_node = factory->MakeSort(hash_aggregation_node, std::move(sort_keys));
  }
  return sort_node;
}


// o_orderkey, o_orderpriority
ScanNode* Q4OrdersScan(ExecutionFactory* factory, ExecutionContext* ctx, Table* orders) {
  auto o_orderkey = factory->MakeCol(o_orderkey_idx);
  auto o_orderdate = factory->MakeCol(o_orderdate_idx);
  auto o_orderpriority = factory->MakeCol(o_orderpriority_idx);
  std::vector<ExprNode*> o_projs{o_orderkey, o_orderpriority};
  auto o_orderdate_lower = factory->MakeParam(q4_orderdate_lower);
  auto o_orderdate_upper = factory->MakeParam(q4_orderdate_upper);
  auto comp_lower = factory->MakeBinaryComp(o_orderdate, o_orderdate_lower, OpType::GE);
  auto comp_upper = factory->MakeBinaryComp(o_orderdate, o_orderdate_upper, OpType::LT);
  std::vector<ExprNode*> o_filters{comp_lower, comp_upper};
  return factory->MakeScan(orders, std::move(o_projs), std::move(o_filters));
}

// l_orderkey
ScanNode* Q4LineitemScan(ExecutionFactory* factory, ExecutionContext* ctx, Table* lineitem) {
  auto l_orderkey = factory->MakeCol(l_orderkey_idx);
  auto l_commitdate = factory->MakeCol(l_commitdate_idx);
  auto l_receiptdate = factory->MakeCol(l_receiptdate_idx);
  std::vector<ExprNode*> l_projs{l_orderkey};
  auto comp = factory->MakeBinaryComp(l_commitdate, l_receiptdate, OpType::LT);
  std::vector<ExprNode*> l_filters{comp};
  return factory->MakeScan(lineitem, std::move(l_projs), std::move(l_filters));
}

PlanNode* Q4Join(ExecutionFactory* factory, ExecutionContext* ctx, ScanNode* o_scan, ScanNode* l_scan, StatsMap* stats_map) {
  HashJoinNode* join;
  {
    std::vector<uint64_t> build_keys{0};
    std::vector<uint64_t> probe_keys{0};
    // Return o_priority
    std::vector<std::pair<uint64_t, uint64_t>> join_projs {
        {0, 1}
    };
    join = factory->MakeHashJoin(o_scan, l_scan,
                                 std::move(build_keys), std::move(probe_keys),
                                 std::move(join_projs), JoinType::LEFT_SEMI);
    stats_map->emplace_back(&join->GetJoinStats());
  }
  // Aggregate sort
  SortNode* sort_node;
  {
    std::vector<uint64_t> group_bys{0};
    std::vector<std::pair<uint64_t, AggType>> aggs {
        {0, AggType::COUNT},
    };
    auto hash_aggregation_node = factory->MakeHashAggregation(join, std::move(group_bys), std::move(aggs));

    // Sort
    std::vector<std::pair<uint64_t, SortType>> sort_keys{
        {0, SortType::ASC},
    };
    sort_node = factory->MakeSort(hash_aggregation_node, std::move(sort_keys));
  }
  return sort_node;
}

void SetQ3Params(ExecutionContext* ctx, const Date& date) {
  ctx->AddParam(q3_orderdate_upper, Value(date), SqlType::Date);
  ctx->AddParam(q3_shipdate_lower, Value(date), SqlType::Date);
}


void SetQ4Params(ExecutionContext* ctx, const Date& lower, const Date& upper) {
  ctx->AddParam(q4_orderdate_lower, Value(lower), SqlType::Date);
  ctx->AddParam(q4_orderdate_upper, Value(upper), SqlType::Date);
}

void SetQ5Params(ExecutionContext* ctx, const Date& lower, const Date& upper) {
  ctx->AddParam(q5_orderdate_lower, Value(lower), SqlType::Date);
  ctx->AddParam(q5_orderdate_upper, Value(upper), SqlType::Date);
}

void ExecuteQ3(Embedder* embedder, ExecutionFactory* factory, ExecutionContext* ctx,
               PlanNode* node,
               ScanNode* c_scan, ScanNode* o_scan, ScanNode* l_scan) {
  if (embedder != nullptr) {
    std::vector<ScanNode*> scans{c_scan, o_scan, l_scan};
    for (auto& s: scans) s->ResetEmbeddingFilter();
    for (auto& s1: scans) {
      for (auto& s2: scans) {
        if (s1 != s2) embedder->UpdateScan(factory, ctx, s1, s2);
      }
    }
  }
  Execute(node, ctx);
}

void ExecuteQ5(Embedder* embedder, ExecutionFactory* factory, ExecutionContext* ctx,
               PlanNode* node,
               ScanNode* r_scan, ScanNode* n_scan, ScanNode* c_scan,
               ScanNode* o_scan, ScanNode* l_scan, ScanNode* s_scan) {
  if (embedder != nullptr) {
    std::vector<ScanNode*> scans{r_scan, n_scan, c_scan, o_scan, l_scan, s_scan};
    for (auto& s: scans) s->ResetEmbeddingFilter();
    for (auto& s1: scans) {
      for (auto& s2: scans) {
        if (s1 != s2) embedder->UpdateScan(factory, ctx, s1, s2);
      }
    }
  }
  Execute(node, ctx);
}


void ExecuteQ4(Embedder* embedder, ExecutionFactory* factory, ExecutionContext* ctx, PlanNode* node, ScanNode* o_scan, ScanNode* l_scan) {
  if (embedder != nullptr) {
    o_scan->ResetEmbeddingFilter();
    l_scan->ResetEmbeddingFilter();
    embedder->UpdateScan(factory, ctx, o_scan, l_scan);
    embedder->UpdateScan(factory, ctx, l_scan, o_scan);
  }
  Execute(node, ctx);
}

TEST_CASE("Q4 Test") {
  TableLoader::LoadTPCH("tpch_data");
  SchemaGraph schema_graph({"part", "supplier", "partsupp", "customer", "nation", "lineitem", "region", "orders"});
  const auto& graph = schema_graph.GetTableGraph();
  ExecutionFactory factory;
  ExecutionContext ctx;
  auto catalog = Catalog::Instance();
  auto customer = catalog->GetTable("customer");
  auto nation = catalog->GetTable("nation");
  auto region = catalog->GetTable("region");
  auto orders = catalog->GetTable("orders");
  auto lineitem = catalog->GetTable("lineitem");
  auto supplier = catalog->GetTable("supplier");
  catalog->MakeTableStats(orders, {o_orderdate_idx}, {HistogramType::EquiWidth});
  catalog->MakeTableStats(lineitem, {l_shipdate_idx}, {HistogramType::EquiWidth});

  StatsMap q4_stats;
  auto q4_o_scan = Q4OrdersScan(&factory, &ctx, orders);
  auto q4_l_scan = Q4LineitemScan(&factory, &ctx, lineitem);
  auto q4_join = Q4Join(&factory, &ctx, q4_o_scan, q4_l_scan, &q4_stats);
  SetQ5Params(&ctx, Date::FromYMD(1994, 1, 1), Date::FromYMD(1995, 10, 1));
  StatsMap q5_stats;
  auto q5_r_scan = Q5RegionScan(&factory, region);
  auto q5_n_scan = Q5NationScan(&factory, nation);
  auto q5_c_scan = Q5CustomerScan(&factory, customer);
  auto q5_o_scan = Q5OrdersScan(&factory, orders);
  auto q5_l_scan = Q5LineitemScan(&factory, lineitem);
  auto q5_s_scan = Q5SupplierScan(&factory, supplier);
  std::vector<HashJoinNode*> q5_all_joins;
  auto q5_join = Q5Join(&factory, &ctx, q5_r_scan, q5_n_scan, q5_c_scan, q5_o_scan, q5_l_scan, q5_s_scan, &q5_stats, &q5_all_joins);


  SetQ3Params(&ctx, Date::FromYMD(1995, 3, 15));
  StatsMap q3_stats;
  auto q3_c_scan = Q3CustomerScan(&factory, customer);
  auto q3_o_scan = Q3OrdersScan(&factory, orders);
  auto q3_l_scan = Q3LineitemScan(&factory, lineitem);
  auto q3_join = Q3Join(&factory, &ctx, q3_c_scan, q3_o_scan, q3_l_scan, &q3_stats);

  // Initial Execution
  {
    std::cout << "INITIAL Q3" << std::endl;
    std::cout << "Q3 Run" << std::endl;
    SetQ3Params(&ctx, Date::FromYMD(1995, 3, 15));
    ExecuteQ3(nullptr, &factory, &ctx, q3_join, q3_c_scan, q3_o_scan, q3_l_scan);
    std::cout << "Q3 Run" << std::endl;
    SetQ3Params(&ctx, Date::FromYMD(1994, 3, 1));
    ExecuteQ3(nullptr, &factory, &ctx, q3_join, q3_c_scan, q3_o_scan, q3_l_scan);
  }
  {
    std::cout << "INITIAL Q4" << std::endl;
    SetQ4Params(&ctx, Date::FromYMD(1993, 7, 1), Date::FromYMD(1993, 10, 1));
    ExecuteQ4(nullptr, &factory, &ctx, q4_join, q4_o_scan, q4_l_scan);
    SetQ4Params(&ctx, Date::FromYMD(1993, 1, 1), Date::FromYMD(1993, 7, 1));
    ExecuteQ4(nullptr, &factory, &ctx, q4_join, q4_o_scan, q4_l_scan);
  }
  {
    std::cout << "INITIAL Q5" << std::endl;
    std::cout << "Q5 Run" << std::endl;
    SetQ5Params(&ctx, Date::FromYMD(1994, 1, 1), Date::FromYMD(1995, 1, 1));
    ExecuteQ5(nullptr, &factory, &ctx, q5_join, q5_r_scan, q5_n_scan, q5_c_scan, q5_o_scan, q5_l_scan, q5_s_scan);
    std::cout << "Q5 Run" << std::endl;
    SetQ5Params(&ctx, Date::FromYMD(1993, 1, 1), Date::FromYMD(1994, 1, 1));
    ExecuteQ5(nullptr, &factory, &ctx, q5_join, q5_r_scan, q5_n_scan, q5_c_scan, q5_o_scan, q5_l_scan, q5_s_scan);
  }

  // Make embeddings.
  auto embedder = schema_graph.BuildEmbeddings(&factory, {q4_join, q3_join, q5_join});


  // Second execution
  {
    std::cout << "WARM Q4" << std::endl;
    SetQ4Params(&ctx, Date::FromYMD(1993, 7, 1), Date::FromYMD(1993, 10, 1));
    ExecuteQ4(embedder, &factory, &ctx, q4_join, q4_o_scan, q4_l_scan);
    SetQ4Params(&ctx, Date::FromYMD(1993, 1, 1), Date::FromYMD(1993, 7, 1));
    ExecuteQ4(embedder, &factory, &ctx, q4_join, q4_o_scan, q4_l_scan);
  }
  {
    std::cout << "WARM Q3" << std::endl;
    std::cout << "Q3 Run" << std::endl;
    SetQ3Params(&ctx, Date::FromYMD(1995, 3, 15));
    ExecuteQ3(embedder, &factory, &ctx, q3_join, q3_c_scan, q3_o_scan, q3_l_scan);
    std::cout << "Q3 Run" << std::endl;
    SetQ3Params(&ctx, Date::FromYMD(1994, 3, 1));
    ExecuteQ3(embedder, &factory, &ctx, q3_join, q3_c_scan, q3_o_scan, q3_l_scan);
  }
  {
    std::cout << "WARM Q5" << std::endl;
    std::cout << "Q5 Run" << std::endl;
    SetQ5Params(&ctx, Date::FromYMD(1994, 1, 1), Date::FromYMD(1995, 1, 1));
    ExecuteQ5(embedder, &factory, &ctx, q5_join, q5_r_scan, q5_n_scan, q5_c_scan, q5_o_scan, q5_l_scan, q5_s_scan);
    std::cout << "Q5 Run" << std::endl;
    SetQ5Params(&ctx, Date::FromYMD(1993, 1, 1), Date::FromYMD(1994, 1, 1));
    ExecuteQ5(embedder, &factory, &ctx, q5_join, q5_r_scan, q5_n_scan, q5_c_scan, q5_o_scan, q5_l_scan, q5_s_scan);
  }
  std::cout << "============================ Q5 Stats =========================" << std::endl;
  PrintStatsMap("q5", q5_stats);
  std::cout << "============================ Q4 Stats =========================" << std::endl;
  PrintStatsMap("q4", q4_stats);
  std::cout << "============================ Q3 Stats =========================" << std::endl;
  PrintStatsMap("q3", q3_stats);
}
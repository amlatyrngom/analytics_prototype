#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "stats/schema_graph.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
#include "execution/execution_factory.h"
using namespace smartid;

constexpr const char* build_upper_param = "build_upper";
constexpr const char* build_lower_param = "build_lower";
constexpr const char* probe_upper_param = "probe_upper";
constexpr const char* probe_lower_param = "probe_lower";


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
  auto build_lower = factory->MakeParam(build_lower_param);
  auto comp_upper = factory->MakeBinaryComp(build_pk, build_upper, OpType::LT);
  auto comp_lower = factory->MakeBinaryComp(build_pk, build_lower, OpType::GE);
  std::vector<ExprNode*> filters{comp_upper, comp_lower};
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
  auto probe_lower = factory->MakeParam(probe_lower_param);
  auto comp_upper = factory->MakeBinaryComp(probe_fk, probe_upper, OpType::LT);
  auto comp_lower = factory->MakeBinaryComp(probe_fk, probe_lower, OpType::GE);
  std::vector<ExprNode*> filters{comp_upper, comp_lower};
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

std::tuple<FilterInfo, FilterInfo, FilterInfo, FilterInfo> SetParams(ExecutionContext* ctx, int32_t build_upper, int32_t build_lower, int32_t probe_upper, int32_t probe_lower) {
  // Set params
  ctx->AddParam(build_upper_param, Value(build_upper), SqlType::Int32);
  ctx->AddParam(build_lower_param, Value(build_lower), SqlType::Int32);
  ctx->AddParam(probe_upper_param, Value(probe_upper), SqlType::Int32);
  ctx->AddParam(probe_lower_param, Value(probe_lower), SqlType::Int32);

  JoinStats expected_join_stats;
  expected_join_stats.build_in = build_upper - build_lower;
  expected_join_stats.probe_in = 2*(probe_upper - probe_lower);
  auto out_tuples = std::min(probe_upper, build_upper) - std::max(build_lower, probe_lower);
  out_tuples = std::max(0, out_tuples);
  expected_join_stats.build_out = out_tuples;
  expected_join_stats.probe_out = 2*out_tuples;
  FilterInfo probe_info_upper;
  FilterInfo probe_info_lower;
  // Make expected probe-to-build info
  probe_info_upper.selectivity = static_cast<double>(2*probe_upper) / embedding_probe_size;
  probe_info_lower.selectivity = static_cast<double>(probe_upper - probe_lower) / (probe_upper);
  probe_info_upper.comp_type = OpType::LT;
  probe_info_lower.comp_type = OpType::GE;
  probe_info_upper.col_idx = embedding_probe_fk_idx;
  probe_info_lower.col_idx = embedding_probe_fk_idx;
  probe_info_upper.comp_vals.emplace_back(Value(probe_upper));
  probe_info_lower.comp_vals.emplace_back(Value(probe_lower));
  probe_info_upper.join_stats = expected_join_stats;
  probe_info_lower.join_stats = expected_join_stats;
  // Make expected probe-to-build info
  FilterInfo build_info_upper;
  FilterInfo build_info_lower;
  build_info_upper.selectivity = static_cast<double>(build_upper) / embedding_build_size;
  build_info_lower.selectivity = static_cast<double>(build_upper - build_lower) / build_upper;
  build_info_upper.comp_type = OpType::LT;
  build_info_lower.comp_type = OpType::GE;
  build_info_upper.col_idx = embedding_build_pk_idx;
  build_info_lower.col_idx = embedding_build_pk_idx;
  build_info_upper.comp_vals.emplace_back(Value(build_upper));
  build_info_lower.comp_vals.emplace_back(Value(build_lower));
  build_info_upper.join_stats = expected_join_stats;
  build_info_lower.join_stats = expected_join_stats;
  return {probe_info_upper, probe_info_lower, build_info_upper, build_info_lower};
}

const SchemaGraph::FilterList& CheckGraphStructure(const SchemaGraph::FilterGraph& filter_graph, const Table* table1, const Table* table2) {
  REQUIRE(filter_graph.size() == 1);
  REQUIRE(filter_graph.contains(table1));
  REQUIRE(filter_graph.at(table1).size() == 1);
  REQUIRE(filter_graph.at(table1).contains(table2));
  return filter_graph.at(table1).at(table2);
}

void CheckFilterGraph(const SchemaGraph::FilterGraph& filter_graph, const Table* table1, const Table* table2, const std::vector<FilterInfo>& expected_infos) {
  auto actual_infos = CheckGraphStructure(filter_graph, table1, table2);
  REQUIRE(actual_infos.size() == expected_infos.size());
  for (uint64_t i = 0; i < expected_infos.size(); i++) {
    const auto& actual_info = actual_infos[i];
    const auto& expected_info = expected_infos[i];
    REQUIRE(actual_info.col_idx == expected_info.col_idx);
    REQUIRE(actual_info.selectivity == Approx(expected_info.selectivity));
    REQUIRE(actual_info.comp_type == expected_info.comp_type);
    REQUIRE(actual_info.comp_vals == expected_info.comp_vals);
    REQUIRE(actual_info.join_stats.probe_in == expected_info.join_stats.probe_in);
    REQUIRE(actual_info.join_stats.probe_out == expected_info.join_stats.probe_out);
    REQUIRE(actual_info.join_stats.build_in == expected_info.join_stats.build_in);
    REQUIRE(actual_info.join_stats.build_out == expected_info.join_stats.build_out);
  }
}

void CheckSingularRankGraph(const SchemaGraph::RankGraph& rank_graph, const Table* table1, const Table* table2, uint64_t col_idx) {
  REQUIRE(rank_graph.size() == 1);
  REQUIRE(rank_graph.contains(table1));
  REQUIRE(rank_graph.at(table1).size() == 1);
  REQUIRE(rank_graph.at(table1).contains(table2));
  REQUIRE(rank_graph.at(table1).at(table2).size() == 1);
  REQUIRE(rank_graph.at(table1).at(table2).contains(col_idx));
  REQUIRE(rank_graph.at(table1).at(table2).at(col_idx) == Approx(64));
}

//TEST_CASE("Simple Join statistics") {
//  TableLoader::LoadJoinTables("tpch_data");
//  SchemaGraph schema_graph({embedding_probe_name, embedding_build_name});
//  const auto& graph = schema_graph.GetTableGraph();
//  auto probe_table = Catalog::Instance()->GetTable(embedding_probe_name);
//  auto build_table = Catalog::Instance()->GetTable(embedding_build_name);
//
//  ExecutionFactory factory;
//  ExecutionContext ctx;
//  auto build_scan = MakeBuildScan(&factory);
//  auto probe_scan = MakeProbeScan(&factory);
//  auto join = MakeJoin(&factory, build_scan, probe_scan);
//  // Set params
//  int32_t build_lower = embedding_build_size / 16;
//  int32_t build_upper = embedding_build_size / 4;
//  int32_t probe_lower = embedding_build_size / 8;
//  int32_t probe_upper = embedding_build_size / 2;
//  auto [expected_probe_upper, expected_probe_lower, expected_build_upper, expected_build_lower] = SetParams(&ctx, build_upper, build_lower, probe_upper, probe_lower);
//  // Execute
//  Execute(join, &ctx);
//  // Make filter graph.
//  schema_graph.MakeFilterGraphs({join});
//  // Probe filter
//  // Should contain: build_table --> probe_table --> <expected_probe_upper, expected_probe_lower>
//  auto probe_graph = schema_graph.GetProbeFilterGraph();
//  CheckFilterGraph(probe_graph, build_table, probe_table, {expected_probe_upper, expected_probe_lower});
//
//  // Build filter
//  // Should contain: probe_table --> build_table --> <expected_build_upper, expected_probe_lower>
//  auto build_graph = schema_graph.GetBuildFilterGraph();
//  CheckFilterGraph(build_graph, probe_table, build_table, {expected_build_upper, expected_build_lower});
//
//  // Compute num bits.
//  schema_graph.MakeTableRank(&probe_graph, true);
//  schema_graph.MakeTableRank(&build_graph, false);
//  CheckSingularRankGraph(schema_graph.GetProbeRankGraph(), build_table, probe_table, embedding_probe_fk_idx);
//  CheckSingularRankGraph(schema_graph.GetBuildRankGraph(), probe_table, build_table, embedding_build_pk_idx);
//}


//TEST_CASE("TPCH Table Graph Test") {
//  TableLoader::LoadTPCH("tpch_data");
//  SchemaGraph schema_graph({"part", "supplier", "partsupp", "customer", "nation", "lineitem", "region", "orders"});
//  const auto& graph = schema_graph.GetTableGraph();
//
//  // Check lineitem fks.
//  auto lineitem = Catalog::Instance()->GetTable("lineitem");
//  auto partsupp = Catalog::Instance()->GetTable("partsupp");
//  auto orders = Catalog::Instance()->GetTable("orders");
//  auto customer = Catalog::Instance()->GetTable("customer");
//
//  // Check existence of connection.
//  REQUIRE(graph.contains(lineitem));
//  REQUIRE(graph.at(lineitem).contains(partsupp));
//  REQUIRE(graph.at(lineitem).contains(orders));
//  REQUIRE(!graph.at(lineitem).contains(customer)); // no fk to customer
//  // Check content of connections.
//  // l_orderkey --> o_orderkey
//  const auto& o_fks = graph.at(lineitem).at(orders);
//  REQUIRE(o_fks.size() == 1);
//  REQUIRE(o_fks[0].first == 0);
//  REQUIRE(o_fks[0].second == 0);
//  // (l_partkey, l_suppkey) --> (ps_partkey, ps_suppkey)
//  auto ps_fks = graph.at(lineitem).at(partsupp);
//  REQUIRE(ps_fks.size() == 2);
//  REQUIRE(ps_fks[0].first == 1);
//  REQUIRE(ps_fks[0].second == 0);
//  REQUIRE(ps_fks[1].first == 2);
//  REQUIRE(ps_fks[1].second == 1);
//
//  // Find path from lineitem to orders.
//  EmbeddingInfo embedding_info;
//  auto region = Catalog::Instance()->GetTable("region");
//  schema_graph.MakeJoinPath(lineitem, region, &embedding_info);
//  schema_graph.MakeEmbeddingInfo();
//}

//TEST_CASE("TPCH Filter Graph Test Scan Only") {
//  TableLoader::LoadTPCH("tpch_data");
//  SchemaGraph schema_graph({"part", "supplier", "partsupp", "customer", "nation", "lineitem", "region", "orders"});
//  const auto& graph = schema_graph.GetTableGraph();
//
//  // Make a scan node. Because there are no joins, no embedding should be detected.
//  // Lineitem scan
//  // Cols
//  ColumnNode l_orderkey(l_orderkey_idx);
//  // Filters
//  ColumnNode l_partkey(l_partkey_idx);
//  ConstantNode comp_val(Value(int32_t(5000)), SqlType::Int32);
//  BinaryCompNode comp(&l_partkey, &comp_val, OpType::LE);
//  std::vector<ExprNode*> l_filters{&comp};
//  // Projections
//  std::vector<ExprNode*> l_projections{&l_orderkey};
//  // Scan
//  auto lineitem = Catalog::Instance()->GetTable("lineitem");
//  ScanNode l_scan(lineitem, std::move(l_projections), std::move(l_filters), false);
//
//  schema_graph.MakeFilterGraphs({&l_scan});
//
//  REQUIRE(schema_graph.GetProbeFilterGraph().empty());
//  REQUIRE(schema_graph.GetBuildFilterGraph().empty());
//}


// Returns r_regionkey, r_name
ScanNode* MakeRegionScan(ExecutionFactory* factory, Table* region, Value asia_val) {
  // Scan region
  auto r_regionkey = factory->MakeCol(r_regionkey_idx);
  auto r_name = factory->MakeCol(r_name_idx);
  std::vector<ExprNode*> r_projections{r_regionkey, r_name};
  // Region Filters
  auto r_asia_const = factory->MakeConst(asia_val, SqlType::Varchar);
  auto r_asia_comp = factory->MakeBinaryComp(r_name, r_asia_const, OpType::EQ);
  std::vector<ExprNode*> r_filters{r_asia_comp};
  return factory->MakeScan(region, std::move(r_projections), std::move(r_filters));
}

// Returns n_regionkey, n_nationkey, n_name
ScanNode* MakeNationScan(ExecutionFactory* factory, Table* nation) {
  // Scan nation
  // Nation projection
  auto n_regionkey = factory->MakeCol(n_regionkey_idx);
  auto n_nationkey = factory->MakeCol(n_nationkey_idx);
  auto n_name = factory->MakeCol(n_name_idx);
  std::vector<ExprNode*> n_projections{n_regionkey, n_nationkey, n_name};
  return factory->MakeScan(nation, std::move(n_projections), {});
}

// Returns n_regionkey, n_nationkey, n_name, n_embedding
ScanNode* MakeNationScanWithEmbedding(ExecutionFactory* factory, Table* nation) {
  // Scan nation
  // Nation projection
  auto n_regionkey = factory->MakeCol(n_regionkey_idx);
  auto n_nationkey = factory->MakeCol(n_nationkey_idx);
  auto n_name = factory->MakeCol(n_name_idx);
  auto n_embedding = factory->MakeCol(nation->EmbeddingIdx());
  std::vector<ExprNode*> n_projections{n_regionkey, n_nationkey, n_name, n_embedding};
  return factory->MakeScan(nation, std::move(n_projections), {});
}


// Return c_nationkey, c_custkey, c_name
ScanNode* MakeCustomerScan(ExecutionFactory* factory, Table* customer) {
  // Customer
  auto c_nationkey = factory->MakeCol(c_nationkey_idx);
  auto c_custkey = factory->MakeCol(c_custkey_idx);
  auto c_name = factory->MakeCol(c_name_idx);
  std::vector<ExprNode*> c_projections{c_nationkey, c_custkey, c_name};
  return factory->MakeScan(customer, std::move(c_projections), {});
}

// Return c_nationkey, c_custkey, c_name, c_embedding
ScanNode* MakeCustomerScanWithEmbedding(ExecutionFactory* factory, Table* customer) {
  // Customer
  auto c_nationkey = factory->MakeCol(c_nationkey_idx);
  auto c_custkey = factory->MakeCol(c_custkey_idx);
  auto c_name = factory->MakeCol(c_name_idx);
  auto c_embedding = factory->MakeCol(customer->EmbeddingIdx());
  std::vector<ExprNode*> c_projections{c_nationkey, c_custkey, c_name, c_embedding};
  return factory->MakeScan(customer, std::move(c_projections), {});
}

// Return o_custkey, o_orderdate, o_orderpriority
ScanNode* MakeOrdersScan(ExecutionFactory* factory, Table* orders, Value date_upper) {
  // Scan orders
  auto o_custkey = factory->MakeCol(o_custkey_idx);
  auto o_orderdate = factory->MakeCol(o_orderdate_idx);
  auto o_orderpriority = factory->MakeCol(o_orderpriority_idx);
  std::vector<ExprNode*> o_projections{o_custkey, o_orderdate, o_orderpriority};
  // Orders filter
  auto o_date_const = factory->MakeConst(date_upper, SqlType::Date);
  auto o_date_comp = factory->MakeBinaryComp(o_orderdate, o_date_const, OpType::LT);
  auto o_custkey_const = factory->MakeConst(Value(int32_t(100)), SqlType::Int32);
  auto o_custkey_comp = factory->MakeBinaryComp(o_custkey, o_custkey_const, OpType::LT);
  std::vector<ExprNode*> o_filters{o_date_comp, o_custkey_comp};
  // Scan
  return factory->MakeScan(orders, std::move(o_projections), std::move(o_filters));
}

// Return o_custkey, o_orderdate, o_orderpriority, o_embedding
ScanNode* MakeOrdersScanWithEmbedding(ExecutionFactory* factory, Table* orders, Value date_upper) {
  // Scan orders
  auto o_custkey = factory->MakeCol(o_custkey_idx);
  auto o_orderdate = factory->MakeCol(o_orderdate_idx);
  auto o_orderpriority = factory->MakeCol(o_orderpriority_idx);
  auto o_embedding = factory->MakeCol(orders->EmbeddingIdx());
  std::vector<ExprNode*> o_projections{o_custkey, o_orderdate, o_orderpriority, o_embedding};
  // Orders filter
  auto o_date_const = factory->MakeConst(date_upper, SqlType::Date);
  auto o_date_comp = factory->MakeBinaryComp(o_orderdate, o_date_const, OpType::LT);
  auto o_custkey_const = factory->MakeConst(Value(int32_t(100)), SqlType::Int32);
  auto o_custkey_comp = factory->MakeBinaryComp(o_custkey, o_custkey_const, OpType::LT);
  std::vector<ExprNode*> o_filters{o_date_comp, o_custkey_comp};
  // Scan
  return factory->MakeScan(orders, std::move(o_projections), std::move(o_filters));
}

HashJoinNode* JoinTables(ExecutionFactory* factory, ScanNode* region, ScanNode* nation, ScanNode* customer, ScanNode* orders) {
  // Join1: region, nation.
  std::vector<uint64_t> r_build_keys{0}; // r_regionkey
  std::vector<uint64_t> n_probe_keys{0}; // n_regionkey
  std::vector<std::pair<uint64_t, uint64_t>> proj1 {
      {0, 1}, // r_name
      {1, 1}, {1, 2}, // n_nationkey, n_name
  };
  // Output r_name, n_nationkey, n_name
  auto join1 = factory->MakeHashJoin(region, nation,
                                     std::move(r_build_keys), std::move(n_probe_keys),
                                     std::move(proj1), JoinType::INNER);
  // Join2: With customer
  std::vector<uint64_t> n_build_keys{1}; // n_nationkey
  std::vector<uint64_t> c_probe_keys{0}; // c_nationkey
  std::vector<std::pair<uint64_t, uint64_t>> proj2 {
      {0, 0}, {0, 2}, // r_name, n_name
      {1, 1}, {1, 2}, // c_custkey, c_name
  };
  // Output r_name, n_name, c_custkey, c_name
  auto join2 = factory->MakeHashJoin(join1, customer,
                                     std::move(n_build_keys), std::move(c_probe_keys),
                                     std::move(proj2), JoinType::INNER);

  // Join3: With orders
  std::vector<uint64_t> c_build_keys{2};
  std::vector<uint64_t> o_probe_keys{0};
  std::vector<std::pair<uint64_t, uint64_t>> proj3 {
      {0, 0}, {0, 1}, {0, 3}, // r_name, n_name, c_name
      {1, 1}, {1, 2}, // o_orderdate, o_orderpriority
  };
  auto join3 = factory->MakeHashJoin(join2, orders,
                                     std::move(c_build_keys), std::move(o_probe_keys),
                                     std::move(proj3), JoinType::INNER);
  return join3;
}

void Retry(ExecutionFactory* factory) {

}

TEST_CASE("TPCH Intermediate Join & Aggregation") {
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
  catalog->MakeTableStats(region, {r_name_idx}, {HistogramType::EquiWidth});
  catalog->MakeTableStats(orders, {o_orderdate_idx, o_custkey_idx}, {HistogramType::EquiWidth});
  // asia
  std::string asia_str("ASIA");
  Varlen asia_varlen(asia_str.size(), asia_str.data());
  Value asia_val(asia_varlen);
  // date upper
  Value date_upper_val(Date::FromYMD(1993, 1, 1));
  //Value date_upper_val1(Date::FromYMD(1993, 1, 1));

  // Scans
  auto r_scan = MakeRegionScan(&factory, region, asia_val);
  auto n_scan = MakeNationScan(&factory, nation);
  auto c_scan = MakeCustomerScan(&factory, customer);
  auto c_scan_e = MakeCustomerScanWithEmbedding(&factory, customer);
  auto o_scan = MakeOrdersScan(&factory, orders, date_upper_val);

  auto join = JoinTables(&factory, r_scan, n_scan, c_scan, o_scan);

  // Exec
  Execute(join, &ctx);

  // Build embedding
  auto embedder = schema_graph.BuildEmbeddings(&factory, {join});
  auto origin_scan = o_scan;
  auto target_scan = c_scan_e;
  embedder->UpdateScan(&factory, &ctx, origin_scan, target_scan);
  auto print_node = factory.MakePrint(target_scan);
  Execute(print_node, &ctx);

}


//TEST_CASE("TPCH Filter Graph Join & Aggregation") {
//  // SELECT * FROM lineitem JOIN orders ON l_orderkey=o_orderkey
//  // WHERE val1 < o_orderkey
//  // AND l_partkey <= val2 AND l_orderkey >= val3
//  TableLoader::LoadTPCH("tpch_data");
//  SchemaGraph schema_graph({"part", "supplier", "partsupp", "customer", "nation", "lineitem", "region", "orders"});
//  const auto& graph = schema_graph.GetTableGraph();
//  auto lineitem = Catalog::Instance()->GetTable("lineitem");
//  auto orders = Catalog::Instance()->GetTable("orders");
//
//  // Orders scan
//  // Cols
//  ColumnNode o_orderkey(o_orderkey_idx);
//  // Filters
//  ConstantNode o_comp_val(Value(int32_t(1000)), SqlType::Int32);
//  // Note reversed order of comparison
//  BinaryCompNode o_comp(&o_comp_val, &o_orderkey, OpType::LT);
//  std::vector<ExprNode*> o_filters{&o_comp};
//  // Projections
//  std::vector<ExprNode*> o_projections{&o_orderkey};
//
//  // Scan
//  ScanNode o_scan(orders, std::move(o_projections), std::move(o_filters), false);
//  // Lineitem scan
//  // Cols
//  ColumnNode l_orderkey(l_orderkey_idx);
//  // Filters
//  ColumnNode l_partkey(l_partkey_idx);
//  ConstantNode l_comp_val1(Value(int32_t(5000)), SqlType::Int32);
//  ConstantNode l_comp_val2(Value(int32_t(10000)), SqlType::Int32);
//  BinaryCompNode l_comp1(&l_partkey, &l_comp_val1, OpType::LE);
//  BinaryCompNode l_comp2(&l_orderkey, &l_comp_val2, OpType::GE);
//  std::vector<ExprNode*> l_filters{&l_comp1, &l_comp2};
//  // Projections
//  std::vector<ExprNode*> l_projections{&l_orderkey};
//  // Scan
//  ScanNode l_scan(lineitem, std::move(l_projections), std::move(l_filters), false);
//
//  // Join
//  std::vector<uint64_t> join_build_cols{0};
//  std::vector<uint64_t> join_probe_cols{0};
//  std::vector<std::pair<uint64_t, uint64_t>> join_projections = {
//      {0, 0},
//  };
//  HashJoinNode hash_join_node(&o_scan, &l_scan, std::move(join_build_cols), std::move(join_probe_cols), std::move(join_projections), JoinType::INNER);
//
//  // Agg
//  std::vector<std::pair<uint64_t, AggType>> aggs {
//      {0, AggType::COUNT},
//  };
//  StaticAggregateNode static_aggregate_node(&hash_join_node, std::move(aggs));
//
//  schema_graph.MakeFilterGraphs({&static_aggregate_node});
//
//  // Check graph content
//  // Probe graph
//  // In the probe graph, because lineitem is the probe table, the graph should contain:
//  // orders --> lineitem --> <(l_partkey, LE), (l_orderkey, GE)>
//  const auto& probe_graph = schema_graph.GetProbeFilterGraph();
//  SchemaGraph::FilterList expected_probe_filters{
//      {l_partkey_idx, OpType::LE},
//      {l_orderkey_idx, OpType::GE},
//  };
//  REQUIRE(!probe_graph.contains(lineitem));
//  REQUIRE(probe_graph.contains(orders));
//  REQUIRE(probe_graph.at(orders).contains(lineitem));
//  REQUIRE(!probe_graph.at(orders).contains(orders));
//  REQUIRE(probe_graph.at(orders).at(lineitem) == expected_probe_filters);
//
//  // Build Graph
//  // In the build graph, because orders is the build table, it should contain:
//  // lineitem --> orders --> <(o_orderkey, GT)>
//  // Note the flip in the comparison sign.
//  const auto& build_graph = schema_graph.GetBuildFilterGraph();
//  SchemaGraph::FilterList expected_build_filters{
//      {l_orderkey_idx, OpType::GT},
//  };
//  REQUIRE(!build_graph.contains(orders));
//  REQUIRE(build_graph.contains(lineitem));
//  REQUIRE(build_graph.at(lineitem).contains(orders));
//  REQUIRE(!build_graph.at(lineitem).contains(lineitem));
//  REQUIRE(build_graph.at(lineitem).at(orders) == expected_build_filters);
//}
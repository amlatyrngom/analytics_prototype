#include "benchmark/benchmark.h"

#include "test_util/test_table_info.h"
#include "execution/execution_factory.h"
#include "storage/table_loader.h"
#include "stats/embedder.h"
#include <set>
using namespace smartid;

#define ORDERS_SIZE (SCALE_FACTOR * 1500000)


void Execute(PlanNode* node, ExecutionContext* ctx) {
  PrintNode noop(node);
  auto executor = ExecutionFactory::MakePlanExecutor(&noop, ctx);
  executor->Next();
  executor->CollectStats();
}

const char* l_quantity_param = "l_quantity_param";
const char* o_orderdate_param = "o_orderdate_param";
Date o_orderdate_val = Date::FromYMD(1993,4, 29);
Date::NativeType o_orderdate_native = o_orderdate_val.ToNative();
float l_quantity_val = 11.0f;

using ScanStats = std::vector<const BinaryCompNode::Stats*>;
bool first_run = false;

class EmbeddingFixture : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State& state) override {
    TableLoader::LoadTPCH("tpch_data");
    if (!first_run) {
      first_run = true;
//      DenormLineorder();
      MakeExactEmbeddings();
    }
  }

//  void ResetEmbedding() {
//    ColumnNode o_embedding(o_embedding_idx);
//    ScanNode o_scan("orders", {&o_embedding},{});
//    const VectorProjection* vp;
//    auto order_table = Catalog::Instance()->GetTable("orders");
//    for (uint64_t b_idx = 0; b_idx < order_table->NumBlocks(); b_idx++) {
//      auto block = order_table->MutableBlockAt(b_idx);
//      auto data = order_table->MutableBlockAt(b_idx)->MutableColumnDataAs<int64_t>(o_embedding_idx);
//      std::memset(data, 0, sizeof(int64_t)*block->NumElems());
//    }
//  }

  static void MakeExactEmbeddings(ExecutionFactory* factory, const char* qty_upper, const char* orderdate_upper, ExecutionContext* ctx) {
    TableLoader::LoadTPCH("tpch_data");
    auto orders = Catalog::Instance()->GetTable("orders");
    auto lineitem = Catalog::Instance()->GetTable("lineitem");

    auto join_node = MakeJoin(factory, qty_upper, orderdate_upper, nullptr, true);
    uint64_t o_idx = 2;
    uint64_t l_idx = 3;

    const VectorProjection* vp;
    auto o_embed_idx = orders->EmbeddingIdx();
    auto l_embed_idx = lineitem->EmbeddingIdx();
    auto join_exec = ExecutionFactory::MakePlanExecutor(join_node, ctx);
    while ((vp = join_exec->Next()) != nullptr) {
      auto o_id_data = vp->VectorAt(o_idx)->DataAs<int64_t>();
      auto l_id_data = vp->VectorAt(l_idx)->DataAs<int64_t>();
      vp->GetFilter()->Map([&](sel_t i) {
        {
          // Orders
          auto id = o_id_data[i];
          auto row_idx = id & 0xFFFFFFull;
          auto block_idx = id >> 32ull;
          auto block = orders->MutableBlockAt(block_idx);
          block->MutableColumnDataAs<int64_t>(o_embed_idx)[row_idx] = 1;
        }
        {
          // Lineitem
          auto id = l_id_data[i];
          auto row_idx = id & 0xFFFFFFull;
          auto block_idx = id >> 32ull;
          auto block = lineitem->MutableBlockAt(block_idx);
          block->MutableColumnDataAs<int64_t>(l_embed_idx)[row_idx] = 1;
        }
      });
    }
  }

//  static PlanNode* ScanLineorder(ExecutionFactory* factory, const char* qty_upper) {
//    auto lineorder = Catalog::Instance()->GetTable("lineorder");
//    auto lo_orderkey = factory->MakeCol(0);
//    auto lo_shipdate = factory->MakeCol(1);
//    std::vector<ExprNode*> lo_projs{lo_orderkey, lo_shipdate};
//    // Filters
//    auto date_const = factory->MakeParam(qty_upper);
//    auto date_comp = factory->MakeBinaryComp(lo_shipdate, date_const, OpType::LE);
//    std::vector<ExprNode*> lo_filters{date_comp};
//
//    auto lo_scan = factory->MakeScan(lineorder, std::move(lo_projs), std::move(lo_filters));
//    return lo_scan;
//  }

  static HashJoinNode* MakeJoin(ExecutionFactory* factory, const char* quantity_upper, const char* orderdate_upper, ScanStats* scan_stats, bool embed = false, bool use_embed = false) {
    TableLoader::LoadTPCH("tpch_data");
    auto orders = Catalog::Instance()->GetTable("orders");
    auto lineitem = Catalog::Instance()->GetTable("lineitem");
    // Orders scan
    // Cols
    auto o_orderkey = factory->MakeCol(o_orderkey_idx);
    auto o_orderdate = factory->MakeCol(o_orderdate_idx);
    // Filters
    std::vector<ExprNode*> o_filters{};
    if (orderdate_upper != nullptr) {
      auto date_const = factory->MakeParam(orderdate_upper);
      auto date_comp = factory->MakeBinaryComp(o_orderdate, date_const, OpType::LT);
      if (scan_stats != nullptr) scan_stats->emplace_back(&date_comp->GetStats());
      o_filters.emplace_back(date_comp);
    }
    if (use_embed) {
      auto o_embed = factory->MakeCol(orders->EmbeddingIdx());
      auto zero_const = factory->MakeConst(Value(int64_t(0)), SqlType::Int64);
      auto embed_comp = factory->MakeBinaryComp(o_embed, zero_const, OpType::NE);
      if (scan_stats != nullptr) scan_stats->emplace_back(&embed_comp->GetStats());
      o_filters.emplace_back(embed_comp);
    }
    // Projections
    std::vector<ExprNode*> o_projections{o_orderkey};
    if (embed) o_projections.emplace_back(factory->MakeCol(orders->IdIdx()));
    // Scan
    auto o_scan = factory->MakeScan(orders, std::move(o_projections), std::move(o_filters));

    // Lineitem scan
    // Cols
    auto l_orderkey = factory->MakeCol(l_orderkey_idx);
    auto l_qty = factory->MakeCol(l_quantity_idx);
    // Filters
    std::vector<ExprNode*> l_filters{};
    if (quantity_upper != nullptr) {
      auto qty_const = factory->MakeParam(quantity_upper);
      auto qty_comp = factory->MakeBinaryComp(l_qty, qty_const, OpType::LT);
      if (scan_stats != nullptr) scan_stats->emplace_back(&qty_comp->GetStats());
      l_filters.emplace_back(qty_comp);
    }
    if (use_embed) {
      auto l_embed = factory->MakeCol(lineitem->EmbeddingIdx());
      auto zero_const = factory->MakeConst(Value(int64_t(0)), SqlType::Int64);
      auto embed_comp = factory->MakeBinaryComp(l_embed, zero_const, OpType::NE);
      if (scan_stats != nullptr) scan_stats->emplace_back(&embed_comp->GetStats());
      l_filters.emplace_back(embed_comp);
    }
    // Projections
    std::vector<ExprNode*> l_projections{l_orderkey};
    if (embed) l_projections.emplace_back(factory->MakeCol(lineitem->IdIdx()));
    // Scan
    auto l_scan = factory->MakeScan(lineitem, std::move(l_projections), std::move(l_filters));

    // Join
    std::vector<uint64_t> join_build_cols{0};
    std::vector<uint64_t> join_probe_cols{0};
    std::vector<std::pair<uint64_t, uint64_t>> join_projections = {
        {0, 0}, // o_orderkey
        {1, 0}, // l_orderkey
    };
    if (embed) {
      join_projections.emplace_back(0, 1); // o_id
      join_projections.emplace_back(1, 1); // l_id
    }
    auto hash_join_node = factory->MakeHashJoin(o_scan, l_scan, std::move(join_build_cols), std::move(join_probe_cols), std::move(join_projections), JoinType::INNER);
    return hash_join_node;
  }

  static void MakeExactEmbeddings() {
    ExecutionFactory factory;
    ExecutionContext ctx;
    ctx.AddParam(o_orderdate_param, Value(o_orderdate_val), SqlType::Date);
    ctx.AddParam(l_quantity_param, Value(l_quantity_val), SqlType::Float32);
    MakeExactEmbeddings(&factory, l_quantity_param, o_orderdate_param, &ctx);
  }

//  static void DenormLineorder() {
//    ExecutionFactory factory;
//    ExecutionContext ctx;
//    auto join_node = MakeJoin(&factory, nullptr, nullptr, nullptr);
//    // Materialize
//    std::vector<Column> cols{
//        Column::ScalarColumn("o_orderkey", SqlType::Int32),
//        Column::ScalarColumn("l_orderkey", SqlType::Int32),
//    };
//    Schema schema(std::move(cols));
//    auto lineorder = Catalog::Instance()->CreateTable("lineorder", std::move(schema));
//    auto materializer_node = factory.MakeMaterializer(join_node, lineorder);
//    Execute(materializer_node, &ctx);
//  }

  std::set<int64_t> curr_exact_partkeys;
  std::map<int, double> results;
  std::unordered_map<int64_t, std::unordered_set<int64_t>> curr_hist_partkeys;
};

BENCHMARK_DEFINE_F(EmbeddingFixture, RegularBench)(benchmark::State& st) {
  ExecutionFactory factory;
  ExecutionContext ctx;
  ScanStats scan_stats;
  auto join_node = MakeJoin(&factory, l_quantity_param, o_orderdate_param, &scan_stats);
  ctx.AddParam(o_orderdate_param, Value(o_orderdate_val), SqlType::Date);
  ctx.AddParam(l_quantity_param, Value(l_quantity_val), SqlType::Float32);
  // Agg
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {0, AggType::COUNT},
  };
  auto static_aggregate_node = factory.MakeStaticAggregation(join_node, std::move(aggs));

  for (auto _ : st) {
    Execute(static_aggregate_node, &ctx);
    auto p = scan_stats[0]->sels_.back();
    double sel = static_cast<double>(p.second) / p.first;
    results[o_orderdate_native] = sel;
  }
  // Print Results
  std::cerr << "sels" << std::endl;
  for (const auto& p: results) {
    std::cerr << p.second << std::endl;
  }
}

BENCHMARK_DEFINE_F(EmbeddingFixture, CuckooFilterBench)(benchmark::State& st) {
  auto expected_bloom_size = 0.21 * ORDERS_SIZE;
  ExecutionFactory factory;
  ExecutionContext ctx;
  ScanStats scan_stats;
  auto join_node = MakeJoin(&factory, l_quantity_param, o_orderdate_param, &scan_stats);
  join_node->UseBloomFilter(expected_bloom_size);
  ctx.AddParam(o_orderdate_param, Value(o_orderdate_val), SqlType::Date);
  ctx.AddParam(l_quantity_param, Value(l_quantity_val), SqlType::Float32);
  // Agg
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {0, AggType::COUNT},
  };
  auto static_aggregate_node = factory.MakeStaticAggregation(join_node, std::move(aggs));

  for (auto _ : st) {
    Execute(static_aggregate_node, &ctx);
    auto p = scan_stats[0]->sels_.back();
    double sel = static_cast<double>(p.second) / p.first;
    results[o_orderdate_native] = sel;
  }
  // Print Results
  std::cerr << "sels" << std::endl;
  for (const auto& p: results) {
    std::cerr << p.second << std::endl;
  }
}

BENCHMARK_DEFINE_F(EmbeddingFixture, ExactBench)(benchmark::State& st) {
  ExecutionFactory factory;
  ExecutionContext ctx;
  ScanStats scan_stats;
  auto join_node = MakeJoin(&factory, nullptr, o_orderdate_param, &scan_stats, false, true);
  ctx.AddParam(o_orderdate_param, Value(o_orderdate_val), SqlType::Date);
  // Agg
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {0, AggType::COUNT},
  };
  auto static_aggregate_node = factory.MakeStaticAggregation(join_node, std::move(aggs));

  for (auto _ : st) {
    Execute(static_aggregate_node, &ctx);
    auto p = scan_stats[0]->sels_.back();
    double sel = static_cast<double>(p.second) / p.first;
    results[o_orderdate_native] = sel;
  }
  // Print Results
  std::cerr << "sels" << std::endl;
  for (const auto& p: results) {
    std::cerr << p.second << std::endl;
  }
}

BENCHMARK_DEFINE_F(EmbeddingFixture, CheckEmbeddings)(benchmark::State& st) {
  auto lineitem = Catalog::Instance()->GetTable("lineitem");
  ExecutionFactory factory;
  ExecutionContext ctx;
  auto l_orderkey = factory.MakeCol(l_orderkey_idx);
  auto l_id = factory.MakeCol(lineitem->IdIdx());
  auto l_embed = factory.MakeCol(lineitem->EmbeddingIdx());
  std::vector<ExprNode*> l_filters{};
  std::vector<ExprNode*> l_projs{l_orderkey, l_id, l_embed};
  auto l_scan = factory.MakeScan(lineitem, std::move(l_projs), std::move(l_filters), false);

  for (auto _ : st) {
    Execute(l_scan, &ctx);
  }
}



//BENCHMARK_DEFINE_F(EmbeddingFixture, FullDenormalizationBench)(benchmark::State& st) {
//  int l_shipdate_max = st.range(0);
//  ExecutionFactory factory;
//  ExecutionContext ctx;
//  ScanStats scan_stats;
//  auto lo_scan = ScanLineorder(&factory, l_shipdate_param);
//  ctx.AddParam(l_shipdate_param, Value(Date::FromNative(l_shipdate_max)), SqlType::Date);
//  // Agg
//  std::vector<std::pair<uint64_t, AggType>> aggs {
//      {0, AggType::COUNT},
//  };
//  auto static_aggregate_node = factory.MakeStaticAggregation(lo_scan, std::move(aggs));
//
//
//  for (auto _ : st) {
//    Execute(static_aggregate_node, &ctx);
//  }
//}
//



// Register
BENCHMARK_REGISTER_F(EmbeddingFixture, RegularBench);
BENCHMARK_REGISTER_F(EmbeddingFixture, CuckooFilterBench);
BENCHMARK_REGISTER_F(EmbeddingFixture, ExactBench);
//BENCHMARK_REGISTER_F(EmbeddingFixture, CheckEmbeddings);
//BENCHMARK_REGISTER_F(EmbeddingFixture, FullDenormalizationBench)->DenseRange(2448624, 2451149, 101);
//BENCHMARK_REGISTER_F(EmbeddingFixture, EmbeddingBench)->DenseRange(0, 200000, 5000);

//static void CustomHistArguments(benchmark::internal::Benchmark* b) {
//  for (const int& i: {8, 16, 32, 64})
//    for (int j = 0; j <= 200000; j += 5000)
//      b->Args({i, j});
//}
//BENCHMARK_REGISTER_F(EmbeddingFixture, HistEmbeddingBench)->Apply(CustomHistArguments);


// Main
BENCHMARK_MAIN();
#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
#include "stats/embedder.h"
#include "execution/execution_factory.h"
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

template<typename RowChecker>
void CheckRows(PlanNode* node, RowChecker checker) {
  auto executor = ExecutionFactory::MakePlanExecutor(node);
  CheckRows(executor.get(), checker);
}


TEST_CASE("Test Content of Embedding Row") {
  TableLoader::LoadJoinTables("tpch_data");
  auto catalog = Catalog::Instance();
  auto build_table = catalog->GetTable(embedding_build_name);
  auto probe_table = catalog->GetTable(embedding_probe_name);
  catalog->MakeTableStats(probe_table, {embedding_filter_idx}, {HistogramType::EquiWidth});

  // Embed same column at different offsets, with a different number of bits.
  // For simplicity assume both are divisors of 64.
  uint64_t num_bits1 = 64;
  uint64_t offset1 = 0;
  uint64_t num_bits2 = 16;
  uint64_t offset2 = num_bits1;

  // Build embedding info.
  EmbeddingInfo embedding_info;
  embedding_info.target_table = build_table;

  // Build info about the origin table.
  OriginInfo origin_info;
  origin_info.target_pk = embedding_build_pk_idx;
  origin_info.origin_fk = embedding_probe_fk_idx;
  EmbeddingColInfo embedding_col_info1{embedding_filter_idx, HistogramType::EquiWidth, num_bits1, offset1};
  EmbeddingColInfo embedding_col_info2{embedding_filter_idx, HistogramType::EquiWidth, num_bits2, offset2};
  origin_info.col_infos.emplace_back(embedding_col_info1);
  origin_info.col_infos.emplace_back(embedding_col_info2);
  embedding_info.origins.emplace(probe_table, origin_info);

  Embedder embedder(std::move(embedding_info));

  // Now iterate through embedding column.
  ColumnNode embedding_node(build_table->EmbeddingIdx());
  ScanNode scan_node(build_table, {&embedding_node}, {}, false);
  uint64_t curr_num_rows = 0;
  // The filter column values are in [0, embedding_build_size). So bin_size is computed as:
  auto bin_size = embedding_build_size / 64;
  auto checker = [&](const VectorProjection* vp, sel_t i) {
    auto val = vp->VectorAt(0)->DataAs<uint64_t>()[i];
    // Build Row at index k is matched with Probe row at index k and embedding_build_size + k.
    // At these indices, the filter value is k and embedding_build_size - 1 - k.
    // So the following bins will match.
    uint64_t bin_idx1 = curr_num_rows / bin_size;
    uint64_t bin_idx2 = (embedding_build_size - 1 - curr_num_rows) / bin_size;
    // Now apply the offset and number of bits in each of the two of the two embedding.
    uint64_t expected_val = 0;
    // First embedding
    {
      uint64_t scale = 64 / num_bits1;
      uint64_t bit_idx1 = (bin_idx1 / scale) + offset1;
      uint64_t bit_idx2 = (bin_idx2 / scale) + offset1;
      expected_val |= (1ull << bit_idx1) | (1ull << bit_idx2);
    }
    // Second embedding
    {
      uint64_t scale = 64 / num_bits2;
      uint64_t bit_idx1 = (bin_idx1 / scale) + offset2;
      uint64_t bit_idx2 = (bin_idx2 / scale) + offset2;
      expected_val |= (1ull << bit_idx1) | (1ull << bit_idx2);
    }
    REQUIRE(val == expected_val);
    curr_num_rows++;
  };
  CheckRows(&scan_node, checker);
  REQUIRE(curr_num_rows == embedding_build_size);
}


TEST_CASE("Test Content of Embedding Row with Intermidiary table") {
  TableLoader::LoadJoinTables("tpch_data");
  auto catalog = Catalog::Instance();
  auto build_table = catalog->GetTable(embedding_build_name);
  auto interm_table = catalog->GetTable(embedding_interm_name);
  auto probe_table = catalog->GetTable(embedding_probe_name);
  catalog->MakeTableStats(probe_table, {embedding_filter_idx}, {HistogramType::EquiWidth});

  // Embed same column at different offsets, with a different number of bits.
  // For simplicity assume both are divisors of 64.
  uint64_t num_bits1 = 32;
  uint64_t offset1 = 0;
  uint64_t num_bits2 = 16;
  uint64_t offset2 = num_bits1;

  // Build embedding info.
  EmbeddingInfo embedding_info;
  embedding_info.target_table = build_table;

  // Build info about the origin table.
  OriginInfo origin_info;
  origin_info.target_pk = embedding_build_pk_idx;
  origin_info.origin_fk = embedding_probe_fk_idx;
  EmbeddingColInfo embedding_col_info1{embedding_filter_idx, HistogramType::EquiWidth, num_bits1, offset1};
  EmbeddingColInfo embedding_col_info2{embedding_filter_idx, HistogramType::EquiWidth, num_bits2, offset2};
  origin_info.col_infos.emplace_back(embedding_col_info1);
  origin_info.col_infos.emplace_back(embedding_col_info2);
  // Interm Join
  EmbeddingTableInfo embedding_table_info(interm_table, embedding_interm_origin_idx, embedding_interm_target_idx);
  origin_info.interm_tables.emplace_back(embedding_table_info);
  embedding_info.origins.emplace(probe_table, origin_info);

  Embedder embedder(std::move(embedding_info));

  // Now iterate through embedding column.
  ColumnNode embedding_node(build_table->GetSchema().NumCols() - 1);
  ScanNode scan_node(build_table, {&embedding_node}, {}, false);
  uint64_t curr_num_rows = 0;
  // The filter column values are in [0, embedding_build_size). So bin_size is computed as:
  auto bin_size = embedding_build_size / 64;
  auto checker = [&](const VectorProjection* vp, sel_t i) {
    auto val = vp->VectorAt(0)->DataAs<uint64_t>()[i];
    // Build Row at index k is matched with Probe row at index k and embedding_build_size + k.
    // At these indices, the filter value is k and embedding_build_size - 1 - k.
    // So the following bins will match.
    uint64_t bin_idx1 = curr_num_rows / bin_size;
    uint64_t bin_idx2 = (embedding_build_size - 1 - curr_num_rows) / bin_size;
    // Now apply the offset and number of bits in each of the two of the two embedding.
    uint64_t expected_val = 0;
    // First embedding
    {
      uint64_t scale = 64 / num_bits1;
      uint64_t bit_idx1 = (bin_idx1 / scale) + offset1;
      uint64_t bit_idx2 = (bin_idx2 / scale) + offset1;
      expected_val |= (1ull << bit_idx1) | (1ull << bit_idx2);
    }
    // Second embedding
    {
      uint64_t scale = 64 / num_bits2;
      uint64_t bit_idx1 = (bin_idx1 / scale) + offset2;
      uint64_t bit_idx2 = (bin_idx2 / scale) + offset2;
      expected_val |= (1ull << bit_idx1) | (1ull << bit_idx2);
    }
    REQUIRE(val == expected_val);
    curr_num_rows++;
  };
  CheckRows(&scan_node, checker);
  REQUIRE(curr_num_rows == embedding_build_size);
}


TEST_CASE("Simple Join Test") {
  // Try: SELECT build_pk, probe_pk FROM build, probe WHERE build_pk = build_fk AND a <= probe_filter <= b
  TableLoader::LoadJoinTables("tpch_data");
  auto catalog = Catalog::Instance();
  auto build = catalog->GetTable(embedding_build_name);
  auto probe = catalog->GetTable(embedding_probe_name);
  // Embed filter_col
  catalog->MakeTableStats(probe, {embedding_filter_idx}, {HistogramType::EquiWidth});
  // Embed same column at different offsets, with a different number of bits.
  // For simplicity assume both are divisors of 64.
  uint64_t num_bits = 64;
  uint64_t offset = 0;

  // Build embedding info.
  EmbeddingInfo embedding_info;
  embedding_info.target_table = build;

  // Build info about the origin table.
  OriginInfo origin_info;
  origin_info.target_pk = embedding_build_pk_idx;
  origin_info.origin_fk = embedding_probe_fk_idx;
  EmbeddingColInfo embedding_col_info{embedding_filter_idx, HistogramType::EquiWidth, num_bits, offset};
  origin_info.col_infos.emplace_back(embedding_col_info);
  embedding_info.origins.emplace(probe, origin_info);

  Embedder embedder(std::move(embedding_info));

  // Get mask to check embedding.
  Value comp_lo(int32_t(embedding_build_size / 8));
  Value comp_hi(int32_t(embedding_build_size / 4));
  ConstantNode comp_lo_node(Value(comp_lo), SqlType::Int32); // Copy value for reuse.
  ConstantNode comp_hi_node(Value(comp_hi), SqlType::Int32); // Copy value for reuse.
  auto comp_mask = embedder.GetEmbedding(probe, embedding_filter_idx, comp_lo, OpType::GE);
  comp_mask &= embedder.GetEmbedding(probe, embedding_filter_idx, comp_hi, OpType::LE);
  std::cout << "Comp Mask: " << std::hex << comp_mask << std::dec << std::endl;

  // Make common objects
  // Build
  ColumnNode build_pk(embedding_build_pk_idx);
  std::vector<ExprNode*> build_projs{&build_pk};
  ColumnNode build_embedding_col(build->GetSchema().NumCols() - 1);
  EmbeddingCheckNode embedding_check_node(&build_embedding_col, comp_mask);
  std::vector<ExprNode*> build_filters{/*&embedding_check_node*/};
  // Probe
  ColumnNode probe_pk(embedding_probe_pk_idx);
  ColumnNode probe_fk(embedding_probe_fk_idx);
  ColumnNode probe_filter_col(embedding_filter_idx);
  std::vector<ExprNode*> probe_projs{&probe_pk, &probe_fk, &probe_filter_col};
  BinaryCompNode probe_comp_lo(&probe_filter_col, &comp_lo_node, OpType::GE);
  BinaryCompNode probe_comp_hi(&probe_filter_col, &comp_hi_node, OpType::LE);
  std::vector<ExprNode*> probe_filters{&probe_comp_lo, &probe_comp_hi};

  // Join
  std::vector<uint64_t> build_keys{0}; // build_pk
  std::vector<uint64_t> probe_keys{1}; // probe_fk
  std::vector<std::pair<uint64_t, uint64_t>> join_projs{
      {0, 0}, // build_pk
      {1, 0}, // probe_pk
      {1, 2}, // probe_filter
  };

  // Sorting for deterministic order.
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
    {1, SortType::ASC} // probe_pk
  };


  // Case 1: Join without using embeddings.
  std::vector<int32_t> expected_col1;
  std::vector<int32_t> expected_col2;
  std::vector<int32_t> expected_col3;
  {
    // Copy to avoid moving
    // Build
    ScanNode build_scan(build, std::vector<ExprNode*>(build_projs), std::vector<ExprNode*>(), false);
    ScanNode probe_scan(probe, std::vector<ExprNode*>(probe_projs), std::vector<ExprNode*>(probe_filters), false);
    HashJoinNode hash_join_node(&build_scan, &probe_scan, std::vector<uint64_t>(build_keys), std::vector<uint64_t>(probe_keys),
                                std::vector<std::pair<uint64_t, uint64_t>>(join_projs), JoinType::INNER);
    SortNode sort_node(&hash_join_node, std::vector<std::pair<uint64_t, SortType>>(sort_keys));
    auto checker = [&](const VectorProjection* vp, sel_t i) {
      expected_col1.emplace_back(vp->VectorAt(0)->DataAs<int32_t>()[i]);
      expected_col2.emplace_back(vp->VectorAt(1)->DataAs<int32_t>()[i]);
      expected_col3.emplace_back(vp->VectorAt(2)->DataAs<int32_t>()[i]);
    };
    CheckRows(&sort_node, checker);
  }
  // Case 2: Join using embeddings
  std::vector<int32_t> actual_col1;
  std::vector<int32_t> actual_col2;
  std::vector<int32_t> actual_col3;
  {
    // Copy to avoid moving
    // Build
    ScanNode build_scan(build, std::vector<ExprNode*>(build_projs), std::vector<ExprNode*>(build_filters), false);
    ScanNode probe_scan(probe, std::vector<ExprNode*>(probe_projs), std::vector<ExprNode*>(probe_filters), false);
    HashJoinNode hash_join_node(&build_scan, &probe_scan, std::vector<uint64_t>(build_keys), std::vector<uint64_t>(probe_keys),
                                std::vector<std::pair<uint64_t, uint64_t>>(join_projs), JoinType::INNER);
    SortNode sort_node(&hash_join_node, std::vector<std::pair<uint64_t, SortType>>(sort_keys));
    auto checker = [&](const VectorProjection* vp, sel_t i) {
      actual_col1.emplace_back(vp->VectorAt(0)->DataAs<int32_t>()[i]);
      actual_col2.emplace_back(vp->VectorAt(1)->DataAs<int32_t>()[i]);
      actual_col3.emplace_back(vp->VectorAt(2)->DataAs<int32_t>()[i]);
    };
    CheckRows(&sort_node, checker);
  }

  REQUIRE(actual_col1.size() == expected_col1.size());
  REQUIRE(actual_col2.size() == expected_col2.size());
  REQUIRE(actual_col3.size() == expected_col3.size());
  REQUIRE(actual_col1 == expected_col1);
  REQUIRE(actual_col2 == expected_col2);
  REQUIRE(actual_col3 == expected_col3);
}

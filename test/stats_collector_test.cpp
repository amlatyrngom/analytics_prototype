#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "test_util/test_table_info.h"
#include "storage/table_loader.h"
#include "stats/histogram.h"
#include "stats/table_stats.h"
using namespace smartid;


TEST_CASE("Stats Collection Test") {
  // Load Tables
  TableLoader::LoadTestTables("tpch_data");
  Table* table = Catalog::Instance()->GetTable(test_table_name);
  std::vector<uint64_t> wanted_cols{int32_idx};
  std::vector<HistogramType> wanted_hists{HistogramType::EquiWidth};
  TableStats table_stats{table, wanted_cols, wanted_hists};

  auto hist = table_stats.GetHistogram(int32_idx, HistogramType::EquiWidth);
  const auto& parts = hist->GetParts();
  // Here, all values have the same frequency. Also, there are less values than buckets. So each value should have its own bucket.
  std::vector<std::pair<int32_t, int32_t>> expected_parts = {
      {0, 0},
      {1, 1},
      {2, 2},
      {3, 3},
      {4, 4},
      {5, 5},
      {6, 6},
      {7, 7},
      {8, 8},
      {9, 9},
  };
  REQUIRE(expected_parts.size() == parts.size());
  for (uint64_t i = 0; i < parts.size(); i++) {
    auto lo = std::get<int32_t>(parts[i].first);
    auto hi = std::get<int32_t>(parts[i].second);
    REQUIRE(expected_parts[i].first == lo);
    REQUIRE(expected_parts[i].second == hi);
  }

  // Try reading with 5 bits allocated.
  int num_bits = expected_parts.size() / 2;
  int bit_offset = 30;
  std::vector<int32_t> in_values{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  Vector in_vec(SqlType::Int32);
  Vector out_vec(SqlType::Int64);
  Filter filter;
  filter.Reset(in_values.size(), FilterMode::SelVecFull);
  in_vec.ShallowReset(in_values.size(), reinterpret_cast<char*>(in_values.data()));
  hist->BitIndices(&filter, &in_vec, &out_vec, bit_offset, num_bits);
  auto out_data = out_vec.DataAs<int64_t>();
  for (uint64_t i = 0; i < in_values.size(); i++) {
    REQUIRE(out_data[i] == (bit_offset + (in_values[i] / 2)));
  }
}

TEST_CASE("Stats Collection Test 2") {
  // Load Tables
  TableLoader::LoadJoinTables("tpch_data");
  Table* table = Catalog::Instance()->GetTable("small_probe_table");
  std::vector<uint64_t> wanted_cols{probe_pk_col, probe_int64_col};
  std::vector<HistogramType> wanted_hists{HistogramType::EquiWidth};
  TableStats table_stats{table, wanted_cols, wanted_hists};

  auto hist = table_stats.GetHistogram(probe_pk_col, HistogramType::EquiWidth);
  const auto& parts = hist->GetParts();
  // The is column has sequential values.
  std::vector<std::pair<int64_t, int64_t>> expected_parts;
  auto width = (small_probe_size / 64) + 1;
  for (int i = 0; i < 64; i++) {
    expected_parts.emplace_back(i*width, (i+1)*width-1);
  }
  // Adjust last bucket
  expected_parts.back().second = small_probe_size - 1;

  REQUIRE(expected_parts.size() == parts.size());
  for (uint64_t i = 0; i < parts.size(); i++) {
    auto lo = std::get<int64_t>(parts[i].first);
    auto hi = std::get<int64_t>(parts[i].second);
    REQUIRE(expected_parts[i].first == lo);
    REQUIRE(expected_parts[i].second == hi);
  }
}
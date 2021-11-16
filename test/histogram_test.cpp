#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "stats/histogram.h"
using namespace smartid;

namespace {
std::ostream &operator<<(std::ostream &os, const smartid::Date &d) {
  return os << d.ToString();
}
}

template<typename T, typename Op>
void GenVecData(Op gen_fn, Vector *out) {
  auto data = out->MutableDataAs<T>();
  for (int i = 0; i < out->NumElems(); i++) {
    data[i] = gen_fn(i);
  }
}

template<typename T>
void GenVecData(const std::vector<T>& vec, Vector* out) {
  out->ShallowReset(vec.size(), reinterpret_cast<const char*>(vec.data()));
}

template <typename T>
void CheckBuckets(const Histogram& hist, const std::vector<std::pair<T, T>>& expected_buckets, SqlType sql_type) {
  const auto& parts = hist.GetParts();
  for (const auto& p: parts) {
    auto lo = std::get<T>(p.first);
    auto hi = std::get<T>(p.second);
    std::cout << lo << ", " << hi << std::endl;
  }
  REQUIRE(parts.size() == expected_buckets.size());
  for (uint64_t i = 0; i < expected_buckets.size(); i++) {
    const auto& expected_p = expected_buckets[i];
    auto lo = std::get<T>(parts[i].first);
    auto hi = std::get<T>(parts[i].second);
    std::cout << lo << ", " << hi << std::endl;
    REQUIRE(expected_p.first == lo);
    REQUIRE(expected_p.second == hi);
  }

  std::vector<T> check_vals;
  std::vector<int64_t> expected_idx;
  for (int64_t i = 0; i < static_cast<int64_t>(expected_buckets.size()); i++) {
    auto lo = std::get<T>(parts[i].first);
    auto hi = std::get<T>(parts[i].second);
    // Before
    check_vals.emplace_back(lo - 1);
    expected_idx.emplace_back(i - 1);
    // Lo
    check_vals.emplace_back(lo);
    expected_idx.emplace_back(i);
    // Hi
    check_vals.emplace_back(hi);
    expected_idx.emplace_back(i);
    // After
    check_vals.emplace_back(hi+1);
    // Expected depends on next value
    if (i == parts.size() - 1 || expected_buckets[i+1].first == hi + 1) {
      // Should return next index.
      expected_idx.emplace_back(i+1);
    } else {
      // Fall back to this bucket
      expected_idx.emplace_back(i);
    }
    // Middle
    if constexpr (std::is_arithmetic_v<T>) {
      check_vals.emplace_back((lo + hi) / 2);
      expected_idx.emplace_back(i);
    } else if constexpr (std::is_same_v<T, Date>) {
      auto mid = Date::FromNative((lo.ToNative() + hi.ToNative()) / 2);
      check_vals.emplace_back(mid);
      expected_idx.emplace_back(i);
    }
    // Test Borders
    if (lo + 1 < hi) {
      // Beginning
      check_vals.emplace_back(lo + 1);
      expected_idx.emplace_back(i);
      // Ending
      check_vals.emplace_back(hi - 1);
      expected_idx.emplace_back(i);
    }
  }

  // Make input and output vectors
  Vector input_values(sql_type);
  Vector output_indices(SqlType::Int64);
  GenVecData(check_vals, &input_values);
  Filter filter;
  filter.Reset(input_values.NumElems(), FilterMode::SelVecFull);
  hist.BitIndices(&filter, &input_values, &output_indices, 0, 64);

  // Check results
  auto output_data = output_indices.DataAs<int64_t>();
  filter.Map([&](sel_t i) {
    std::cout << "Val, Output, Expected: "
              << check_vals[i] << ", "
              << output_data[i] << ", "
              << expected_idx[i] << std::endl;
    REQUIRE(output_data[i] == expected_idx[i]);
  });
}

template <typename T>
void TestEquiWithHistogram(int num_distinct_vals, T scale, int num_parts, int bit_offset, SqlType sql_type, const std::vector<std::pair<T, T>>& expected_buckets) {
  ColumnStats stats;
  stats.min_ = Value(T(0));
  stats.max_ = Value(scale*(num_distinct_vals - 1));
  stats.count_distinct_ = num_distinct_vals;

  auto gen_vals = [&](int i) {
    return scale*i;
  };
  Vector vals(sql_type, num_distinct_vals);
  GenVecData<T>(gen_vals, &vals);
  stats.by_freq_.vals_ = &vals;
  EquiWidthHistogram equi_width_histogram(num_parts, sql_type, stats);
  CheckBuckets<T>(equi_width_histogram, expected_buckets, sql_type);
}

template <typename T>
void TestEquiDepthHistogram(bool use_freq, int num_distinct_vals, int num_parts, int bit_offset, SqlType sql_type, int64_t singularity, const std::vector<std::pair<T, T>>& expected_buckets) {
  ColumnStats stats;
  stats.min_ = Value(T(0));
  stats.max_ = Value(T(num_distinct_vals - 1));
  stats.count_distinct_ = num_distinct_vals;
  stats.count_ = 0;

  auto gen_vals = [&](int i) {
    return i;
  };
  auto gen_freqs = [&](int i) {
    auto f = i == singularity ? num_distinct_vals + 1 : 1;
    stats.count_ += f;
    return f;
  };
  Vector vals(sql_type, num_distinct_vals);
  Vector weights(SqlType::Int64, num_distinct_vals);


  GenVecData<T>(gen_vals, &vals);
  GenVecData<int64_t>(gen_freqs, &weights);
  auto& val_weigths = use_freq ? stats.by_freq_ : stats.by_usage_;
  val_weigths.vals_ = &vals;
  val_weigths.weights_ = &weights;

  EquiDepthHistogram equi_depth_histogram(num_parts, sql_type, stats, use_freq);
  CheckBuckets<T>(equi_depth_histogram, expected_buckets, sql_type);
}


TEST_CASE("EquiWidth Consecutive Values Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int32_t scale = 1;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<int32_t>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Int32, expected_buckets);
}

TEST_CASE("EquiWidth Consecutive Values Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int32_t scale = 1;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<int64_t>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Int64, expected_buckets);
}

TEST_CASE("EquiWidth Consecutive Values F32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr float scale = 1.0;
  std::vector<std::pair<float, float>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<float>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Float32, expected_buckets);
}


TEST_CASE("EquiWidth Consecutive Values F64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr double scale = 1.0;
  std::vector<std::pair<double, double>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<double>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Float64, expected_buckets);
}


TEST_CASE("EquiWidth Jumping Values Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int32_t scale = 10;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<int32_t>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Int32, expected_buckets);
}

TEST_CASE("EquiWidth Jumping Values Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int32_t scale = 10;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<int64_t>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Int64, expected_buckets);
}

TEST_CASE("EquiWidth Jumping Values F32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr float scale = 10.0;
  std::vector<std::pair<float, float>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<float>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Float32, expected_buckets);
}


TEST_CASE("EquiWidth Jumping Values F64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr double scale = 10.0;
  std::vector<std::pair<double, double>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(scale*i*width, scale*((i+1)*width - 1));
  }
  TestEquiWithHistogram<double>(num_distinct_vals, scale, num_parts, bit_offset, SqlType::Float64, expected_buckets);
}

TEST_CASE("EquiDepth Uniform Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = -1;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(i*width, (i+1)*width - 1);
  }
  TestEquiDepthHistogram<int32_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
  TestEquiDepthHistogram<int32_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Uniform Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = -1;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(i*width, (i+1)*width - 1);
  }
  TestEquiDepthHistogram<int64_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
  TestEquiDepthHistogram<int64_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Uniform F32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = -1;
  std::vector<std::pair<float, float>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(i*width, (i+1)*width - 1);
  }
  TestEquiDepthHistogram<float>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
  TestEquiDepthHistogram<float>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Uniform F64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = -1;
  std::vector<std::pair<double, double>> expected_buckets;
  int width = num_distinct_vals / num_parts;
  for (int i = 0; i < num_parts; i++) {
    expected_buckets.emplace_back(i*width, (i+1)*width - 1);
  }
  TestEquiDepthHistogram<double>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
  TestEquiDepthHistogram<double>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Singularity at 0 Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestEquiDepthHistogram<int32_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
  TestEquiDepthHistogram<int32_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Singularity at 0 Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestEquiDepthHistogram<int64_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
  TestEquiDepthHistogram<int64_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
}


TEST_CASE("EquiDepth Singularity at 0 Float32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<float, float>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestEquiDepthHistogram<float>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
  TestEquiDepthHistogram<float>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
}


TEST_CASE("EquiDepth Singularity at 0 Float64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<double, double>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestEquiDepthHistogram<double>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
  TestEquiDepthHistogram<double>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Singularity at middle Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestEquiDepthHistogram<int32_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
  TestEquiDepthHistogram<int32_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
}

TEST_CASE("EquiDepth Singularity at middle Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestEquiDepthHistogram<int64_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
  TestEquiDepthHistogram<int64_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
}


TEST_CASE("EquiDepth Singularity at middle F32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<float, float>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestEquiDepthHistogram<float>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
  TestEquiDepthHistogram<float>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
}


TEST_CASE("EquiDepth Singularity at middle F64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<double, double>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestEquiDepthHistogram<double>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
  TestEquiDepthHistogram<double>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
}

template <typename T>
void TestMaxDiff(SqlType sql_type) {
  // Input values 0, 1, 3, 100, 110, 150, 300, 360, 430, 500
  // Buckets should be [0, 3], [100, 150], [300, 500].
  int32_t num_buckets = 3;
  std::vector<T> input_vals {0, 1, 3, 100, 110, 150, 300, 360, 430, 500};
  std::vector<std::pair<T, T>> expected_buckets {
      {0, 3},
      {100, 150},
      {300, 500}
  };
  ColumnStats stats;
  Vector input_vec(sql_type);
  input_vec.ShallowReset(input_vals.size(), reinterpret_cast<const char*>(input_vals.data()));
  stats.by_freq_.vals_ = &input_vec;
  MaxDiffHistogram max_diff_histogram(num_buckets, sql_type, stats);
  CheckBuckets<T>(max_diff_histogram, expected_buckets, sql_type);
}

TEST_CASE("MaxDiff Simple Test I32") {
  TestMaxDiff<int32_t>(SqlType::Int32);
}

TEST_CASE("MaxDiff Simple Test I64") {
  TestMaxDiff<int64_t>(SqlType::Int64);
}

TEST_CASE("MaxDiff Simple Test F32") {
  TestMaxDiff<float>(SqlType::Float32);
}

TEST_CASE("MaxDiff Simple Test F64") {
  TestMaxDiff<double>(SqlType::Float64);
}

TEST_CASE("Simple EquiWidth Date") {
  // Year/month is 2000/1.
  // Days are in [1, 12]
  // Buckets should be [1, 3], [4, 6], [7, 9], [10, 12]
  int32_t num_buckets = 4;
  int32_t num_dates = 12;
  std::vector<Date> input_vals;
  for (int32_t i = 0; i < num_dates; i++) {
    input_vals.emplace_back(Date::FromYMD(2000, 1, i + 1));
  }
  std::vector<std::pair<Date, Date>> expected_buckets;
  auto width = num_dates / num_buckets;
  for (int32_t i = 0; i < num_buckets; i ++) {
    auto lo = Date::FromYMD(2000, 1, i*width + 1);
    auto hi = Date::FromYMD(2000, 1, (i+1)*width);
    expected_buckets.emplace_back(lo, hi);
  }
  ColumnStats stats;
  stats.min_ = Value(input_vals.front());
  stats.max_ = Value(input_vals.back());
  stats.count_distinct_ = num_dates;
  Vector input_vec(SqlType::Date);
  input_vec.ShallowReset(num_dates, reinterpret_cast<const char*>(input_vals.data()));
  stats.by_freq_.vals_ = &input_vec;

  EquiWidthHistogram equi_width_histogram(num_buckets, SqlType::Date, stats);
  CheckBuckets<Date>(equi_width_histogram, expected_buckets, SqlType::Date);
}

template <typename T>
void TestCompressedHistogram(bool use_freq, int num_distinct_vals, int num_parts, int bit_offset, SqlType sql_type, int64_t singularity, const std::vector<std::pair<T, T>>& expected_buckets) {
  ColumnStats stats;
  stats.min_ = Value(T(0));
  stats.max_ = Value(T(num_distinct_vals - 1));
  stats.count_distinct_ = num_distinct_vals;
  stats.count_ = 0;

  auto gen_vals = [&](int i) {
    return i;
  };
  auto gen_freqs = [&](int i) {
    auto f = i == singularity ? num_distinct_vals + 1 : 1;
    stats.count_ += f;
    return f;
  };
  Vector vals(sql_type, num_distinct_vals);
  Vector weights(SqlType::Int64, num_distinct_vals);


  GenVecData<T>(gen_vals, &vals);
  GenVecData<int64_t>(gen_freqs, &weights);
  auto& val_weigths = use_freq ? stats.by_freq_ : stats.by_usage_;
  val_weigths.vals_ = &vals;
  val_weigths.weights_ = &weights;

  CompressedHistogram compressed_histogram(num_parts, sql_type, stats, use_freq);
  CheckBuckets<T>(compressed_histogram, expected_buckets, sql_type);
}


TEST_CASE("Compressed Singularity at 0 Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestCompressedHistogram<int32_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
  TestCompressedHistogram<int32_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
}

TEST_CASE("Compressed Singularity at 0 Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestCompressedHistogram<int64_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
  TestCompressedHistogram<int64_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
}


TEST_CASE("Compressed Singularity at 0 Float32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<float, float>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestCompressedHistogram<float>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
  TestCompressedHistogram<float>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
}


TEST_CASE("Compressed Singularity at 0 Float64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 0;
  std::vector<std::pair<double, double>> expected_buckets {
      {0, 0},
      {1, 200},
      {201, 400},
      {401, 600},
      {601, 800},
      {801, 999},
  };
  TestCompressedHistogram<double>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
  TestCompressedHistogram<double>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
}

TEST_CASE("Compressed Singularity at middle Int32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<int32_t, int32_t>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 499},
      {500, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestCompressedHistogram<int32_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
  TestCompressedHistogram<int32_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int32, singularity, expected_buckets);
}

TEST_CASE("Compressed Singularity at middle Int64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<int64_t, int64_t>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 499},
      {500, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestCompressedHistogram<int64_t>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
  TestCompressedHistogram<int64_t>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Int64, singularity, expected_buckets);
}


TEST_CASE("Compressed Singularity at middle F32 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<float, float>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 499},
      {500, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestCompressedHistogram<float>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
  TestCompressedHistogram<float>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float32, singularity, expected_buckets);
}


TEST_CASE("Compressed Singularity at middle F64 Test") {
  constexpr int num_distinct_vals = 1000;
  constexpr int num_parts = 10;
  constexpr int bit_offset = 0;
  constexpr int64_t singularity = 500;
  std::vector<std::pair<double, double>> expected_buckets {
      {0, 199},
      {200, 399},
      {400, 499},
      {500, 500},
      {501, 700},
      {701, 900},
      {901, 999},
  };
  TestCompressedHistogram<double>(true, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
  TestCompressedHistogram<double>(false, num_distinct_vals, num_parts, bit_offset, SqlType::Float64, singularity, expected_buckets);
}

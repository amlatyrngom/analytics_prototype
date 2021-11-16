#include "benchmark/benchmark.h"

#include "execution/execution_common.h"
#include "execution/bloom_filter.h"
#include "common/util.h"
using namespace smartid;

constexpr int num_elems = 1000000;

class BloomFilterBenchFixture : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State& state) override {
    if (hashes_.empty()) {
      MakeTable();
    }
  }

  void MakeTable() {
    filter_ = std::make_unique<CuckooFilter>(num_elems, 0.01);
    table_ = std::make_unique<JoinTable>();
    hashes_.resize(num_elems);
    auto& table = *table_;
    for (int64_t i = 0; i < num_elems; i++) {
      auto h = ArithHashMurmur2(i, 0);
      hashes_[i] = h;
      table[h] = nullptr;
      filter_->Insert(h);
    }

  }

  std::vector<int64_t> hashes_;
  std::unique_ptr<CuckooFilter> filter_;
  std::unique_ptr<JoinTable> table_;
};



BENCHMARK_DEFINE_F(BloomFilterBenchFixture, HashTableOnly)(benchmark::State& st) {
  for (auto _ : st) {
    for (const auto &h: hashes_) {
      if (const auto &it = table_->find(h); it != table_->end()) {
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

BENCHMARK_DEFINE_F(BloomFilterBenchFixture, BloomFilterOnly)(benchmark::State& st) {
  for (auto _ : st) {
    for (const auto &h: hashes_) {
      benchmark::DoNotOptimize(filter_->Test(h));
    }
  }
}

BENCHMARK_REGISTER_F(BloomFilterBenchFixture, HashTableOnly);
BENCHMARK_REGISTER_F(BloomFilterBenchFixture, BloomFilterOnly);

BENCHMARK_MAIN();
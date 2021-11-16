#pragma once
#include "cuckoofilter/src/cuckoofilter.h"
#include "cuckoofilter/src/simd-block.h"
//#include "bloomfilter-bsd/src/dtl/filter/blocked_bloomfilter/blocked_bloomfilter.hpp"
#include "common/util.h"
#include <cmath>

namespace smartid {


struct IdHash {
  uint64_t operator()(uint64_t k) const {
    return k;
  }
};

class DummyBloomFilter {
 public:
  explicit DummyBloomFilter(int64_t expected_count, double fpr) {}

  void Insert(int64_t k) {
  }

  bool Test(int64_t k) {
    return true;
  }
};

class BlockedBloomFilter {
 public:
  explicit BlockedBloomFilter(int64_t expected_count, double fpr): filter_(LogSpace(expected_count)) {}

  static uint64_t LogSpace(uint64_t count) {
//    std::cout << "Count: " << count << ", Log: " << std::ceil(std::log2(count)) << std::endl;
    return std::ceil(std::log2(count));
  }

  void Insert(int64_t k) {
    filter_.Add(k);
  }

  bool Test(int64_t k) {
    return filter_.Find(k);
  }


 private:
  SimdBlockFilter<IdHash> filter_;
};

class CuckooFilter {
 public:
  CuckooFilter(int64_t expected_count, double fpr) : filter_(expected_count), expected_count_(expected_count) {
  }

  void Insert(int64_t k) {
    auto ret = filter_.Add(k);
//    ASSERT(ret == cuckoofilter::Ok, "Bloom Add Failed!!!!");
  }

  bool Test(int64_t k) {
    return filter_.Contain(k) == cuckoofilter::Ok;
  }

 private:
  cuckoofilter::CuckooFilter<int64_t, 12> filter_;
  uint64_t expected_count_;
};

}
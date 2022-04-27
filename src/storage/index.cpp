#include "storage/index.h"
#include <fmt/core.h>
#include <fmt/ranges.h>

namespace smartid {

void RowIDIndex::PrinTop64(uint64_t n) {
  uint64_t num_printed{0};
  for (const auto& [k, vs]: index64_) {
    if (num_printed == n) return;
    fmt::print("Index64 key={}, vals={}\n", k, vs);
    num_printed++;
  }
}

void RowIDIndex::PrinTop32(uint64_t n) {
  uint64_t num_printed{0};
  for (const auto& [k, vs]: index32_) {
    if (num_printed == n) return;
    fmt::print("Index32 key={}, vals={}\n", k, vs);
    num_printed++;
  }
}

//void RowIDIndex::Insert(int64_t key, int64_t val) {
//  key &= KEY_MASK;
//  index_[key].emplace_back(val);
//}
//
//const std::vector<int64_t> &RowIDIndex::GetVals(int64_t key) {
//  key &= KEY_MASK;
//  if (auto it = index_.find(key); it != index_.end()) {
//    return it->second;
//  }
//  return empty_vec;
//}
}
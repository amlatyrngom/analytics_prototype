#pragma once

#include "common/util.h"
#include "common/types.h"
#include "execution/execution_common.h"

namespace smartid {
class Catalog;

using RowIDIndexTable64 = tsl::robin_map<int64_t, std::vector<int64_t>>;
using RowIDIndexTable32 = tsl::robin_map<int32_t, std::vector<int64_t>>;


class RowIDIndex {
 public:
  static constexpr int64_t KEY_MASK64{0xFFFFFFFFFFFFll};
  static constexpr int32_t KEY_MASK32{0xFFFFFF};
  RowIDIndex() = default;

  RowIDIndexTable64 *GetIndex64() {
    return &index64_;
  }

  [[nodiscard]] const RowIDIndexTable64 *GetIndex64() const {
    return &index64_;
  }

  RowIDIndexTable32 *GetIndex32() {
    return &index32_;
  }

  [[nodiscard]] const RowIDIndexTable32 *GetIndex32() const {
    return &index32_;
  }

//  int64_t MakeKey(int64_t key) const {
//    return key & KEY_MASK64;
//  }

//  void Insert(int64_t key, int64_t val);
//
//  const std::vector<int64_t>& GetVals(int64_t key);

  void PrinTop64(uint64_t n);
  void PrinTop32(uint64_t n);

 private:
  RowIDIndexTable64 index64_;
  RowIDIndexTable32 index32_;
  std::vector<int64_t> empty_vec;
};
}


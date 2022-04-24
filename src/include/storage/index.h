#pragma once

#include "common/util.h"
#include "execution/execution_common.h"

namespace smartid {
class Catalog;

class RowIDIndex {
 public:
  static constexpr int64_t KEY_MASK{0xFFFFFFFFFFFFll};

  RowIDIndex() = default;

  RowIDIndexTable *GetIndex() {
    return &index_;
  }

  [[nodiscard]] const RowIDIndexTable *GetIndex() const {
    return &index_;
  }

  int64_t MakeKey(int64_t key) const {
    return key & KEY_MASK;
  }

  void Insert(int64_t key, int64_t val);

  const std::vector<int64_t>& GetVals(int64_t key);

  void PrinTop(uint64_t n);

 private:
  RowIDIndexTable index_;
  std::vector<int64_t> empty_vec;
};
}


#pragma once

#include "common/util.h"
#include "execution/execution_common.h"

namespace smartid {
class RowIDIndex {
 public:
  RowIDIndex(uint64_t key_idx, uint64_t val_idx) {};


  RowIDIndexTable *GetIndex() {
    return &index_;
  }

 private:
  RowIDIndexTable index_;

};
}


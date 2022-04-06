#pragma once

#include "common/util.h"
#include "execution/execution_common.h"
#include "plan_node.h"


namespace smartid {
class BuildRowIDIndexNode : public PlanNode {
 public:
  BuildRowIDIndexNode(PlanNode *child, uint64_t key_idx, uint64_t val_idx, RowIDIndexTable* index_table)
  : PlanNode(PlanType::BuildRowIDIndex, {child})
  , key_idx_(key_idx)
  , val_idx_(val_idx)
  , index_table_(index_table)
  {}

  [[nodiscard]] const auto& KeyIdx() const {
    return key_idx_;
  }

  [[nodiscard]] const auto& ValIdx() const {
    return val_idx_;
  }

  [[nodiscard]] const auto& IndexTable() const {
    return index_table_;
  }
 private:
  uint64_t key_idx_;
  uint64_t val_idx_;
  RowIDIndexTable *index_table_;
};
}


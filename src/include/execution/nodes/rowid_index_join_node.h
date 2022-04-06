#pragma once

#include "common/util.h"
#include "execution/execution_common.h"
#include "plan_node.h"

namespace smartid {

class RowIDIndexJoinNode : public PlanNode {
 public:
  RowIDIndexJoinNode(PlanNode *key_side, RowIDIndexTable * index_table, uint64_t key_idx, Table* lookup_side, std::vector<std::pair<uint64_t, uint64_t>> &&projections)
  : PlanNode(PlanType::RowIDIndexJoin, {key_side})
  , index_table_(index_table)
  , key_idx_(key_idx)
  , lookup_side_(lookup_side)
  , projections_(std::move(projections))
  {}

  [[nodiscard]] const auto& IndexTable() const {
    return index_table_;
  }

  [[nodiscard]] const auto& KeyIdx() const {
    return key_idx_;
  }

  Table* LookupSide() {
    return lookup_side_;
  }

  [[nodiscard]] const auto& LookupSide() const {
    return lookup_side_;
  }

  [[nodiscard]] const auto& Projections() const {
    return projections_;
  }

 private:
  RowIDIndexTable * index_table_;
  uint64_t key_idx_;
  Table* lookup_side_;
  std::vector<std::pair<uint64_t, uint64_t>> projections_;
};

}
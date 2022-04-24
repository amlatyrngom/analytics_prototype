#pragma once

#include "common/util.h"
#include "plan_node.h"

namespace smartid {
class RowIDIndex;
class Table;
class ScanNode;

class RowIDIndexJoinNode : public PlanNode {
 public:
  RowIDIndexJoinNode(PlanNode *key_side, RowIDIndex * index, uint64_t key_idx, ScanNode* lookup_side, std::vector<std::pair<uint64_t, uint64_t>> &&projections)
  : PlanNode(PlanType::RowIDIndexJoin, {key_side})
  , index_(index)
  , key_idx_(key_idx)
  , lookup_side_(lookup_side)
  , projections_(std::move(projections))
  {}

  [[nodiscard]] const auto& IndexTable() const {
    return index_;
  }

  [[nodiscard]] const auto& KeyIdx() const {
    return key_idx_;
  }

  ScanNode* LookupSide() {
    return lookup_side_;
  }

  [[nodiscard]] const auto& LookupSide() const {
    return lookup_side_;
  }

  [[nodiscard]] const auto& Projections() const {
    return projections_;
  }

 private:
  RowIDIndex * index_;
  uint64_t key_idx_;
  ScanNode* lookup_side_;
  std::vector<std::pair<uint64_t, uint64_t>> projections_;
};

}
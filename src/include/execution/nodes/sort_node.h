#pragma once

#include "execution/execution_common.h"
#include "execution/vector_ops.h"
#include "plan_node.h"

namespace smartid {
class SortNode : public PlanNode {
 public:
  SortNode(PlanNode *child, std::vector<std::pair<uint64_t, SortType>> &&sort_keys, int64_t limit = -1)
      : PlanNode(PlanType::Sort, {child})
      , sort_keys_(std::move(sort_keys))
      , limit_(limit) {}

  [[nodiscard]] const auto& SortKeys() const {
    return sort_keys_;
  }

  [[nodiscard]] int64_t Limit() const {
    return limit_;
  }
 private:
  std::vector<std::pair<uint64_t, SortType>> sort_keys_;
  int64_t limit_;
};
}

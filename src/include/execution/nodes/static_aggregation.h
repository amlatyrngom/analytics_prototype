#pragma once

#include "execution/execution_common.h"
#include "execution/vector_ops.h"
#include "plan_node.h"

namespace smartid {
/**
 * Aggregation without group by.
 * Does not yet support empty input, because NULL not implemented.
 */
class StaticAggregateNode : public PlanNode {
 public:
  /**
   * Constructor
   */
  StaticAggregateNode(PlanNode *child, std::vector<std::pair<uint64_t, AggType>> &&aggs)
      : PlanNode(PlanType::StaticAgg, {child}), aggs_(std::move(aggs)) {}

  [[nodiscard]] const auto& GetAggs() const {
    return aggs_;
  }

 private:
  // The vector of {col_idx, aggregate type} pairs.
  std::vector<std::pair<uint64_t, AggType>> aggs_;
};
}
#pragma once

#include "plan_node.h"


namespace smartid {
class HashAggregationNode : public PlanNode {
 public:
  /**
   * Constructor.
   */
  HashAggregationNode(PlanNode *child,
                      std::vector<uint64_t> &&group_by_keys,
                      std::vector<std::pair<uint64_t, AggType>> &&aggs)
      : PlanNode(PlanType::HashAgg, {child}),
        group_by_keys_(std::move(group_by_keys)),
        aggs_(std::move(aggs)) {}

  [[nodiscard]] const auto& GetGroupBys() const {
    return group_by_keys_;
  }

  [[nodiscard]] const auto& GetAggs() const {
    return aggs_;
  }
 private:
  // The GROUP BY keys.
  std::vector<uint64_t> group_by_keys_;
  // The aggregates.
  std::vector<std::pair<uint64_t, AggType>> aggs_;
};
}
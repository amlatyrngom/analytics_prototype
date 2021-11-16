#pragma once
#include "execution/execution_common.h"
#include "common/catalog.h"
#include "expr_node.h"
#include "execution/nodes/plan_node.h"

namespace smartid {
class ProjectionNode : public PlanNode {
 public:
  ProjectionNode(PlanNode* child, std::vector<ExprNode*>&& projections)
      : PlanNode(PlanType::Projection, {child}), projections_(std::move(projections)) {}

  [[nodiscard]] const auto& GetProjections() const {
    return projections_;
  }
 private:
  std::vector<ExprNode *> projections_;
};


}
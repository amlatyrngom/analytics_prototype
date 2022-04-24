#pragma once
#include "execution/execution_common.h"
#include "common/catalog.h"
#include "expr_node.h"
#include "execution/nodes/plan_node.h"

namespace smartid {
class ProjectionNode : public PlanNode {
 public:
  ProjectionNode(PlanNode* child, std::vector<ExprNode*>&& projections, std::vector<ExprNode*> filters)
      : PlanNode(PlanType::Projection, {child}), projections_(std::move(projections)) {}

  [[nodiscard]] const auto& GetProjections() const {
    return projections_;
  }

  [[nodiscard]] const auto& GetFilter() const {
    return filters_;
  }
 private:
  std::vector<ExprNode *> projections_;
  std::vector<ExprNode *> filters_;
};


}
#pragma once
#include "execution/execution_common.h"
#include "common/catalog.h"
#include "expr_node.h"

namespace smartid {

enum class PlanType {
  Noop,
  Print,
  Scan,
  Projection,
  HashJoin,
  HashAgg,
  StaticAgg,
  Sort,
  Materialize,
};

class PlanNode {
 public:
  explicit PlanNode(PlanType plan_type, std::vector<PlanNode*>&& children) : plan_type_(plan_type), children_(std::move(children)) {}

  uint64_t NumChildren() const {
    return children_.size();
  }

  [[nodiscard]] const PlanNode* Child(uint64_t idx) const {
    return children_[idx];
  }

  [[nodiscard]] PlanNode* Child(uint64_t idx) {
    return children_[idx];
  }


  virtual ~PlanNode() = default;

  [[nodiscard]] PlanType GetPlanType() const {
    return plan_type_;
  }

 private:
  PlanType plan_type_;
  std::vector<PlanNode*> children_;
};

class NoopOutputNode : public PlanNode {
 public:
  explicit NoopOutputNode(PlanNode* child) : PlanNode(PlanType::Noop, {child}) {}
};

class PrintNode : public PlanNode {
 public:
  explicit PrintNode(PlanNode *child) : PlanNode(PlanType::Print, {child}) {}
};
}
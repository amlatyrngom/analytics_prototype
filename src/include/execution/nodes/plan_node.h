#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include "execution/execution_types.h"

namespace smartid {

class PlanNode {
 public:
  explicit PlanNode(PlanType plan_type, std::vector<PlanNode*>&& children) : plan_type_(plan_type), children_(std::move(children)) {}

  [[nodiscard]] uint64_t NumChildren() const {
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
  explicit PrintNode(PlanNode *child, std::vector<std::string>&& col_names)
  : PlanNode(PlanType::Print, {child})
  , col_names_(std::move(col_names)) {}

  [[nodiscard]] const auto& ColNames() const {
    return col_names_;
  }

 private:
  std::vector<std::string> col_names_;
};
}
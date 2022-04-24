#pragma once

#include "execution/nodes/plan_node.h"

namespace smartid {
class VectorProjection;

class PlanExecutor {
 public:
  /**
   * Instantiate executor
   */
  explicit PlanExecutor(std::vector<std::unique_ptr<PlanExecutor>>&& children);

  virtual const VectorProjection *Next() = 0;

  [[nodiscard]] const PlanExecutor* Child(uint64_t idx) const {
    return children_[idx].get();
  }

  [[nodiscard]] PlanExecutor* Child(uint64_t idx) {
    return children_[idx].get();
  }

  virtual ~PlanExecutor();

  VectorProjection *Result() {
    return result_.get();
  }

 protected:
  std::unique_ptr<VectorProjection> result_{nullptr};
  std::vector<std::unique_ptr<PlanExecutor>> children_;
};

class NoopOutputExecutor: public PlanExecutor {
 public:
  explicit NoopOutputExecutor(std::vector<std::unique_ptr<PlanExecutor>> && children) : PlanExecutor(std::move(children)) {}

  const VectorProjection * Next() override;
};

class PrintExecutor: public PlanExecutor {
 public:
  explicit PrintExecutor(PrintNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children)), node_(node) {}

  const VectorProjection * Next() override;

 private:
  PrintNode* node_;
};

}
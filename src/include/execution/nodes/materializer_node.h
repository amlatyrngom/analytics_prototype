#pragma once

#include "execution/execution_common.h"
#include "execution/vector_ops.h"
#include "execution/nodes/plan_node.h"

namespace smartid {
class MaterializerNode : public PlanNode {
 public:
  explicit MaterializerNode(PlanNode* child, Table* table) : PlanNode(PlanType::Materialize, {child}), table_(table) {}

  Table* GetTable() {
    return table_;
  }

 private:
  Table* table_;
};

}
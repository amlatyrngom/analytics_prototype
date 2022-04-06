#pragma once

#include "execution/nodes/build_rowid_index_node.h"
#include "execution/executors/plan_executor.h"

namespace smartid {

class BuildRowIDIndexExecutor : public PlanExecutor {
 public:
  BuildRowIDIndexExecutor(BuildRowIDIndexNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children))
  , node_(node)
  {}

  const VectorProjection * Next() override;

 private:
  BuildRowIDIndexNode* node_;
};

}
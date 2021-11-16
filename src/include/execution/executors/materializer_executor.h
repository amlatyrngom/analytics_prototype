#include "execution/nodes/materializer_node.h"
#include "execution/executors/plan_executor.h"

namespace smartid {
class MaterializerExecutor: public PlanExecutor {
 public:
  MaterializerExecutor(MaterializerNode* node, std::vector<std::unique_ptr<PlanExecutor>>&& children) : PlanExecutor(std::move(children)), node_(node){}

  const VectorProjection * Next() override;
 private:
  MaterializerNode* node_;
  bool done_{false};
};

}
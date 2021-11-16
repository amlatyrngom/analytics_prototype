#include "execution/nodes/projection_node.h"
#include "execution/executors/plan_executor.h"
#include "execution/executors/expr_executor.h"

namespace smartid {
class ProjectionExecutor: public PlanExecutor {
 public:
  ProjectionExecutor(ProjectionNode* projection_node, std::vector<std::unique_ptr<ExprExecutor>>&& projections, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children))
  , projection_node_(projection_node)
  , projections_(std::move(projections)) {
    result_ = std::make_unique<VectorProjection>(&result_filter_);
  }

  const VectorProjection * Next() override;
 private:
  ProjectionNode* projection_node_;
  std::vector<std::unique_ptr<ExprExecutor>> projections_;
  std::vector<std::unique_ptr<Vector>> result_vecs_{};
  bool init_{false};
  Filter result_filter_;
};
}
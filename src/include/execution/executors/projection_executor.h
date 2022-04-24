#include "execution/nodes/projection_node.h"
#include "execution/executors/plan_executor.h"
#include "execution/executors/expr_executor.h"

namespace smartid {
class VectorProjection;
class Bitmap;


class ProjectionExecutor: public PlanExecutor {
 public:
  ProjectionExecutor(ProjectionNode* projection_node, std::vector<std::unique_ptr<ExprExecutor>>&& projections, std::vector<std::unique_ptr<ExprExecutor>>&& filters, std::vector<std::unique_ptr<PlanExecutor>> && children);

  const VectorProjection * Next() override;
 private:
  ProjectionNode* projection_node_;
  std::vector<std::unique_ptr<ExprExecutor>> projections_;
  std::vector<std::unique_ptr<ExprExecutor>> filters_;
  bool init_{false};
  std::unique_ptr<Bitmap> result_filter_;
};
}
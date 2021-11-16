#include "execution/nodes/static_aggregation.h"
#include "execution/executors/plan_executor.h"

namespace smartid {
class StaticAggregateExecutor: public PlanExecutor {
 public:
  StaticAggregateExecutor(StaticAggregateNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children)), node_(node) {}

  const VectorProjection * Next() override;

 private:
  /**
   * Setup the return types and the result VP.
   */
  void Prepare(const VectorProjection *vp);

  /**
   * Accumulate the input of the child node.
   */
  void Accumulate();

 private:
  StaticAggregateNode* node_;
  // Whether accumulation has been done.
  bool accumulated_{false};
  // The resulting vectors.
  std::vector<std::unique_ptr<Vector>> result_vecs_;
  // The result filter (contains only one item).
  Filter result_filter_;
};
}
#include "execution/executors/plan_executor.h"
#include "execution/executors/expr_executor.h"
#include "execution/nodes/static_aggregation.h"
#include "execution/execution_types.h"


namespace smartid {
class TableIterator;
class VectorProjection;
class Bitmap;

class StaticAggregateExecutor: public PlanExecutor {
 public:
  StaticAggregateExecutor(StaticAggregateNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children);

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
  std::unique_ptr<Bitmap> result_filter_;
};
}
#include "execution/executors/plan_executor.h"
#include "execution/executors/expr_executor.h"
#include "execution/nodes/scan_node.h"

namespace smartid {
class TableIterator;
class VectorProjection;
class Bitmap;

class ScanExecutor: public PlanExecutor {
 public:
  explicit ScanExecutor(ScanNode* scan_node, std::vector<std::unique_ptr<ExprExecutor>> && filters, std::vector<std::unique_ptr<ExprExecutor>>&& projections);

  const VectorProjection * Next() override;
 private:
  std::vector<std::unique_ptr<ExprExecutor>> filters_;
  std::vector<std::unique_ptr<ExprExecutor>> projections_;
  std::unique_ptr<TableIterator> ti_;
  std::unique_ptr<VectorProjection> table_vp_;
  std::unique_ptr<Bitmap> filter_;
  bool init_{false};

 public:
  // For experiments
  uint64_t scan_in{0};
  uint64_t scan_out{0};
  double scan_time{0};
  // Hack for expts
  ScanNode* scan_node_;
};

}
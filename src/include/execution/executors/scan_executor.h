#include "execution/executors/plan_executor.h"
#include "execution/executors/expr_executor.h"
#include "execution/nodes/scan_node.h"

namespace smartid {
class ScanExecutor: public PlanExecutor {
 public:
  explicit ScanExecutor(ScanNode* scan_node, std::vector<std::unique_ptr<ExprExecutor>> && filters, std::vector<std::unique_ptr<ExprExecutor>> && embedding_filters, std::vector<std::unique_ptr<ExprExecutor>>&& projections);

  const VectorProjection * Next() override;
 protected:
  void ReportStats() override {
    scan_node_->ReportStats(scan_time_);
    for (auto& f: filters_) f->CollectStats();
  }
 private:
  ScanNode* scan_node_;
  std::vector<std::unique_ptr<ExprExecutor>> filters_;
  std::vector<std::unique_ptr<ExprExecutor>> embedding_filters_;
  std::vector<std::unique_ptr<ExprExecutor>> projections_;
  std::unique_ptr<TableIterator> ti_;
  std::unique_ptr<VectorProjection> table_vp_;
  Filter filter_;
  bool init_{false};
  double scan_time_{0};
};

}
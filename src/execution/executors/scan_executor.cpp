#include "execution/executors/scan_executor.h"
#include "storage/table.h"
#include "storage/vector.h"
#include "storage/filter.h"
#include "storage/vector_projection.h"

namespace smartid {
ScanExecutor::ScanExecutor(ScanNode *scan_node,
                           std::vector<std::unique_ptr<ExprExecutor>> &&filters,
                           std::vector<std::unique_ptr<ExprExecutor>> &&projections)
    : PlanExecutor({})
    , scan_node_(scan_node)
    , filters_(std::move(filters))
    , projections_(std::move(projections)) {
  filter_ = std::make_unique<Bitmap>();
  ti_ = std::make_unique<TableIterator>(scan_node_->GetTable(), scan_node_->GetTable()->BM(), scan_node_->GetColsToRead());
  result_ = std::make_unique<VectorProjection>(filter_.get());
  table_vp_ = std::make_unique<VectorProjection>(ti_.get());
}


const VectorProjection * ScanExecutor::Next() {
  auto start = std::chrono::high_resolution_clock::now();
  if (!ti_->Advance()) {
    return nullptr;
  }
  filter_->SetFrom(table_vp_->GetFilter());
  scan_in += filter_->NumOnes();
  for (auto &filter_expr: filters_) {
    filter_expr->Evaluate(table_vp_.get(), filter_.get());
  }
  for (auto &proj: projections_) {
    auto vec = proj->Evaluate(table_vp_.get(), filter_.get());
    if (!init_) result_->AddVector(vec);
  }
  scan_out += filter_->NumOnes();
  init_ = true;
  auto end = std::chrono::high_resolution_clock::now();
  scan_time += duration_cast<std::chrono::nanoseconds>(end - start).count();
  return result_.get();
}
}
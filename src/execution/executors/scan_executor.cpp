#include "execution/executors/scan_executor.h"

namespace smartid {
ScanExecutor::ScanExecutor(ScanNode *scan_node,
                           std::vector<std::unique_ptr<ExprExecutor>> &&filters,
                           std::vector<std::unique_ptr<ExprExecutor>> &&embedding_filters,
                           std::vector<std::unique_ptr<ExprExecutor>> &&projections)
    : PlanExecutor({})
    , scan_node_(scan_node)
    , filters_(std::move(filters))
    , embedding_filters_(std::move(embedding_filters))
    , projections_(std::move(projections)) {
  ti_ = std::make_unique<TableIterator>(scan_node->GetTable());
  result_ = std::make_unique<VectorProjection>(&filter_);
  table_vp_ = std::make_unique<VectorProjection>(ti_.get(), &filter_);
}


namespace {
void RecordUsage(Table* table, const Filter *filter, const VectorProjection *vp) {
  auto usage_col_idx = table->UsageIdx();
  auto id_col_idx = table->IdIdx();
  auto id_data = vp->VectorAt(id_col_idx)->DataAs<uint64_t>();
  filter->Map([&](uint64_t i) {
    auto id = id_data[i];
    auto row_idx = id & 0xFFFFFFull;
    auto block_idx = id >> 32ull;
    auto block = table->MutableBlockAt(block_idx);
    block->MutableColumnDataAs<int64_t>(usage_col_idx)[row_idx]++;
  });
}

}

const VectorProjection * ScanExecutor::Next() {
  auto start = std::chrono::high_resolution_clock::now();
  if (ti_->Advance()) {
    filter_.Reset(table_vp_->NumRows(), FilterMode::BitmapFull);
    for (auto &filter_expr: filters_) {
      filter_expr->Evaluate(table_vp_.get(), &filter_);
      if (filter_.Selectivity() < 0.25) {
        filter_.SwitchFilterMode(FilterMode::SelVecSelective);
      }
    }
    for (auto& embedding_expr: embedding_filters_) {
      embedding_expr->Evaluate(table_vp_.get(), &filter_);
      if (filter_.Selectivity() < 0.25) {
        filter_.SwitchFilterMode(FilterMode::SelVecSelective);
      }
    }
    if (scan_node_->RecordRows()) {
      RecordUsage(scan_node_->GetTable(), &filter_, table_vp_.get());
    }
    for (auto &proj: projections_) {
      auto vec = proj->Evaluate(table_vp_.get(), &filter_);
      if (!init_) result_->AddVector(vec);
    }
    init_ = true;
    auto end = std::chrono::high_resolution_clock::now();
    scan_time_ += duration_cast<std::chrono::nanoseconds>(end - start).count();
    return result_.get();
  }
  auto end = std::chrono::high_resolution_clock::now();
  scan_time_ += duration_cast<std::chrono::nanoseconds>(end - start).count();
  return nullptr;
}
}
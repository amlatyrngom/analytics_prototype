#include "execution/executors/static_aggr_executor.h"

namespace smartid {
void StaticAggregateExecutor::Prepare(const VectorProjection *vp) {
  // Create result vectors with the right types.
  result_filter_.Reset(1, FilterMode::BitmapFull);
  result_ = std::make_unique<VectorProjection>(&result_filter_);
  for (const auto &agg: node_->GetAggs()) {
    auto[agg_idx, agg_type] = agg;
    std::unique_ptr<Vector> out_vec;
    switch (agg_type) {
      case AggType::COUNT: {
        out_vec = std::make_unique<Vector>(SqlType::Int64, 1);
        out_vec->MutableDataAs<int64_t>()[0] = 0.0;
        break;
      }
      case AggType::SUM: {
        out_vec = std::make_unique<Vector>(SqlType::Float64, 1);
        out_vec->MutableDataAs<double>()[0] = 0.0;
        break;
      }
      case AggType::MAX:
      case AggType::MIN: {
        // TODO(Amadou): This should be null.
        out_vec = std::make_unique<Vector>(vp->VectorAt(agg_idx)->ElemType(), 1);
        std::memcpy(out_vec->MutableData(), vp->VectorAt(agg_idx)->Data(), out_vec->ElemSize());
        break;
      }
    }
    result_->AddVector(out_vec.get());
    result_vecs_.emplace_back(std::move(out_vec));
  }
}

void StaticAggregateExecutor::Accumulate() {
  const VectorProjection *vp;
  bool first{true};
  while ((vp = Child(0)->Next()) != nullptr) {
    if (first) {
      first = false;
      Prepare(vp);
    }
    // Reduce into result_vec;
    uint64_t res_idx = 0;
    for (const auto &agg: node_->GetAggs()) {
      auto[agg_idx, agg_type] = agg;
      auto vec = vp->VectorAt(agg_idx);
      auto res = result_vecs_[res_idx].get();
      VectorOps::ReduceVector(vp->GetFilter(), vec, res, agg_type);
      res_idx++;
    }
  }
}

const VectorProjection *StaticAggregateExecutor::Next() {
  // Return nullptr if already called.
  if (accumulated_) return nullptr;
  // Accumulate input and return.
  Accumulate();
  accumulated_ = true;
  return result_.get();
}
}
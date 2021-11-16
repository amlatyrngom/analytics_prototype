#include "execution/executors/projection_executor.h"

namespace smartid {
const VectorProjection * ProjectionExecutor::Next() {
  const VectorProjection* vp = Child(0)->Next();
  if (vp == nullptr) return nullptr;
  result_filter_.SetFrom(vp->GetFilter());
  for (auto &proj: projections_) {
    auto vec = proj->Evaluate(vp, &result_filter_);
    if (!init_) result_->AddVector(vec);
  }
  init_ = true;
  return result_.get();
}
}
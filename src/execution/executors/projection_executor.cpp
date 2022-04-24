#include "execution/executors/projection_executor.h"
#include "storage/filter.h"
#include "storage/vector_projection.h"

namespace smartid {

ProjectionExecutor::ProjectionExecutor(ProjectionNode *projection_node,
                                       std::vector<std::unique_ptr<ExprExecutor>> &&projections,
                                       std::vector<std::unique_ptr<ExprExecutor>> &&filters,
                                       std::vector<std::unique_ptr<PlanExecutor>> &&children)
: PlanExecutor(std::move(children))
, projection_node_(projection_node)
, projections_(std::move(projections))
, filters_(std::move(filters)) {
  result_filter_ = std::make_unique<Bitmap>();
  result_ = std::make_unique<VectorProjection>(result_filter_.get());
}


const VectorProjection * ProjectionExecutor::Next() {
  const VectorProjection* vp = Child(0)->Next();
  if (vp == nullptr) return nullptr;
  result_filter_->SetFrom(vp->GetFilter());
  for (auto &filt: filters_) {
    filt->Evaluate(vp, result_filter_.get());
  }
  for (auto &proj: projections_) {
    auto vec = proj->Evaluate(vp, result_filter_.get());
    if (!init_) result_->AddVector(vec);
  }
  init_ = true;
  return result_.get();
}
}
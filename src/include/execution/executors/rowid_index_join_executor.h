#pragma once
#include "execution/nodes/rowid_index_join_node.h"
#include "execution/executors/plan_executor.h"

namespace smartid {

class RowIDIndexJoinExecutor : public PlanExecutor {
 public:
  RowIDIndexJoinExecutor(RowIDIndexJoinNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children))
  , node_(node)
  , result_filter_(FilterMode::BitmapFull) {
    result_ = std::make_unique<VectorProjection>(&result_filter_);
    for (const auto& _: node_->Projections()) {
      result_->AddVector(nullptr);
    }
  }

  const VectorProjection * Next() override;


 private:
  void PrepareKeySide(const VectorProjection* vp);
  void PrepareLookupSide();

  void GatherKeySide(const VectorProjection* vp, uint64_t col_idx, uint64_t projection_idx, const std::vector<uint64_t>& matches);
  void GatherLookupSide(const Block* block, const std::vector<std::pair<uint64_t, uint64_t>>& row_idxs, uint64_t col_idx, uint64_t projection_idx, uint64_t num_matches);

  RowIDIndexJoinNode* node_;
  bool first_call_{true};
  // Final filter. Always on.
  Filter result_filter_;

  // Helpers to gather results
  std::unordered_map<uint64_t, SqlType> key_side_types_;
  std::unordered_map<uint64_t, SqlType> lookup_side_types_;
  std::unordered_map<uint64_t, std::unique_ptr<Vector>> key_side_results_;
  std::unordered_map<uint64_t, std::unique_ptr<Vector>> lookup_side_results_;
};

}
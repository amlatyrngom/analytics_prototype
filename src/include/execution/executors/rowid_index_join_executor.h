#pragma once
#include "execution/nodes/rowid_index_join_node.h"
#include "execution/executors/plan_executor.h"
#include "common/types.h"

namespace smartid {
class Table;
class Bitmap;
class RawTableBlock;
class Vector;
class RowIDIndex;
class ExprExecutor;


class RowIDIndexJoinExecutor : public PlanExecutor {
 public:
  RowIDIndexJoinExecutor(RowIDIndexJoinNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children, std::vector<std::unique_ptr<ExprExecutor>> && lookup_filters, std::vector<std::unique_ptr<ExprExecutor>>&& lookup_projections);

  const VectorProjection * Next() override;


 private:
  void PrepareKeySide(const VectorProjection* vp);
  void PrepareLookupSide();

  void GatherKeySide(const VectorProjection* vp, uint64_t col_idx, uint64_t projection_idx, const std::vector<uint64_t>& matches);
  void ResizeLookupSide(uint64_t num_matches);
  void ReadLookupSide(const RawTableBlock* raw_table_block, const std::vector<std::pair<uint64_t, uint64_t>>& row_idxs, uint64_t col_to_read, uint64_t read_idx);

  RowIDIndexJoinNode* node_;
  std::vector<std::unique_ptr<ExprExecutor>> lookup_filters_;
  std::vector<std::unique_ptr<ExprExecutor>> lookup_projections_;
  bool first_call_{true};
  // Final filter. Always on.
  std::unique_ptr<Bitmap> result_filter_;

  // Helpers to gather results
  std::unordered_map<uint64_t, SqlType> key_side_types_;
  std::unordered_map<uint64_t, SqlType> lookup_side_types_;
  std::unordered_map<uint64_t, std::unique_ptr<Vector>> key_side_results_;
  std::unique_ptr<VectorProjection> lookup_input_vp_;
  std::vector<std::unique_ptr<Vector>> lookup_input_vectors_;
  std::unique_ptr<VectorProjection> lookup_output_vp_;
};

}
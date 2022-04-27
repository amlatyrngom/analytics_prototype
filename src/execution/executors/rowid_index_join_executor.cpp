#include "execution/executors/rowid_index_join_executor.h"
#include "execution/executors/expr_executor.h"
#include "execution/nodes/scan_node.h"
#include "execution/vector_ops.h"
#include "storage/table.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "storage/index.h"
#include <fmt/ranges.h>
#include <fmt/core.h>

namespace smartid {

RowIDIndexJoinExecutor::RowIDIndexJoinExecutor(RowIDIndexJoinNode *node, std::vector<std::unique_ptr<PlanExecutor>> &&children, std::vector<std::unique_ptr<ExprExecutor>> && lookup_filters, std::vector<std::unique_ptr<ExprExecutor>>&& lookup_projections)
: PlanExecutor(std::move(children))
, node_(node)
, lookup_filters_(std::move(lookup_filters))
, lookup_projections_(std::move(lookup_projections)) {
  result_filter_ = std::make_unique<Bitmap>();
  result_ = std::make_unique<VectorProjection>(result_filter_.get());
  for (const auto& _: node_->Projections()) {
    result_->AddVector(nullptr);
  }
  lookup_input_vp_ = std::make_unique<VectorProjection>(result_filter_.get());
  for (const auto& _: node_->LookupSide()->GetColsToRead()) {
    lookup_input_vp_->AddVector(nullptr);
  }
  lookup_input_vectors_.resize(node_->LookupSide()->GetColsToRead().size());
  lookup_output_vp_ = std::make_unique<VectorProjection>(result_filter_.get());
}


void LookupByType(const VectorProjection* vp, RowIDIndex* index, uint64_t key_idx, SqlType key_type, std::vector<uint64_t>& key_side_matches, std::vector<int64_t>& lookup_side_matches) {
  if (key_type == SqlType::Int32) {
    auto index_table = index->GetIndex32();
    auto filter = vp->GetFilter();
    filter->Map([&](sel_t i) {
      auto key_col = vp->VectorAt(key_idx)->DataAs<int32_t>();
      auto key = key_col[i] & RowIDIndex::KEY_MASK32;
      if (auto it =  index_table->find(key); it != index_table->end()) {
        for (const auto& val: it->second) {
          key_side_matches.emplace_back(i);
          lookup_side_matches.emplace_back(val);
        }
      }
    });
  }
  if (key_type == SqlType::Int64) {
    auto index_table = index->GetIndex64();
    auto filter = vp->GetFilter();
    filter->Map([&](sel_t i) {
      auto key_col = vp->VectorAt(key_idx)->DataAs<int64_t>();
      auto key = key_col[i] & RowIDIndex::KEY_MASK64;
      if (auto it =  index_table->find(key); it != index_table->end()) {
        for (const auto& val: it->second) {
          key_side_matches.emplace_back(i);
          lookup_side_matches.emplace_back(val);
        }
      }
    });
  }
}

const VectorProjection *RowIDIndexJoinExecutor::Next() {
  auto key_child = Child(0);
  const VectorProjection* vp = key_child->Next();
  if (vp == nullptr) {
    return nullptr;
  }
  if (first_call_) {
    PrepareKeySide(vp);
  }
  std::vector<uint64_t> key_side_matches; // Contains indexes in vp.
  std::vector<int64_t> lookup_side_matches; // Contains rowids.
  LookupByType(vp, node_->IndexTable(), node_->KeyIdx(), key_side_types_.at(node_->KeyIdx()), key_side_matches, lookup_side_matches);
  if (key_side_matches.empty()) {
    return Next(); // No matches. Try next vector.
  }
  if (first_call_) {
    PrepareLookupSide();
  }

  // Reset filter.
  result_filter_->Reset(key_side_matches.size()); // Number of matches.
  // Gather key side values.
  uint64_t projection_index = 0;
  for (const auto& proj: node_->Projections()) {
    if (proj.first == 1) {
      GatherKeySide(vp, proj.second, projection_index, key_side_matches);
    }
    projection_index++;
  }

  // Gather lookup side
  auto lookup_table = node_->LookupSide()->GetTable();
  // Map from block_id to row_idx, match_idx (index in matches vector).
  std::unordered_map<int64_t, std::vector<std::pair<uint64_t, uint64_t>>> block_rows;

  uint64_t i = 0;
  uint64_t log_block_size = Settings::Instance()->LogBlockSize();
  uint64_t ones = ~(0ull);
  uint64_t row_id_mask = ~(ones << log_block_size);
  for (const auto & lookup_row_id: lookup_side_matches) {
    auto row_idx = lookup_row_id & row_id_mask;
    auto block_id = lookup_row_id >> log_block_size;
    block_rows[int64_t(block_id)].emplace_back(row_idx, i);
    i++;
  }
  // Resize output vectors.
  ResizeLookupSide(lookup_side_matches.size());
  // Read all the needed columns.
  for (const auto& [block_id, row_idxs]: block_rows) {
    uint64_t read_idx{0};
    // Get Block.
    auto block_info = lookup_table->BM()->Pin(block_id);
    auto raw_table_block = TableBlock::FromBlockInfo(block_info);
    // Read data.
    for (const auto& col_to_read: node_->LookupSide()->GetColsToRead()) {
      ReadLookupSide(raw_table_block, row_idxs, col_to_read, read_idx);
      read_idx++;
    }
    lookup_table->BM()->Unpin(block_info, false);
  }
  // Run the filters.
  for (auto& f: lookup_filters_) {
    f->Evaluate(lookup_input_vp_.get(), result_filter_.get());
  }
  // Run the projections
  for (auto& p: lookup_projections_) {
    auto vec = p->Evaluate(lookup_input_vp_.get(), result_filter_.get());
    if (first_call_) {
      lookup_output_vp_->AddVector(vec);
    }
  }
  // Gather in final vector.
  projection_index = 0;
  for (const auto& proj: node_->Projections()) {
    if (proj.first == 0) {
      result_->SetVector(lookup_output_vp_->VectorAt(proj.second), projection_index);
    }
    projection_index++;
  }

  first_call_ = false;
  return result_.get();
}

void RowIDIndexJoinExecutor::PrepareKeySide(const VectorProjection* vp) {
  for (const auto& proj: node_->Projections()) {
    if (proj.first == 1) {
      key_side_types_[proj.second] = vp->VectorAt(proj.second)->ElemType();
    }
  }
}

void RowIDIndexJoinExecutor::PrepareLookupSide() {
  for (const auto& col_idx: node_->LookupSide()->GetColsToRead()) {
    lookup_side_types_[col_idx] = node_->LookupSide()->GetTable()->GetSchema().GetColumn(col_idx).Type();
  }
}

void RowIDIndexJoinExecutor::GatherKeySide(const VectorProjection *vp,
                                           uint64_t col_idx,
                                           uint64_t projection_idx,
                                           const std::vector<uint64_t> &matches) {
  auto &vec = key_side_results_[col_idx];
  auto orig_vec = vp->VectorAt(col_idx);
  if (vec == nullptr) {
    vec = std::make_unique<Vector>(orig_vec->ElemType());
  }
  VectorOps::SelectVector(orig_vec, matches, vec.get());
  if (first_call_) {
    result_->SetVector(vec.get(), projection_idx);
  }
}

void RowIDIndexJoinExecutor::ResizeLookupSide(uint64_t num_matches) {
  uint64_t read_idx = 0;
  for (const auto& col_idx: node_->LookupSide()->GetColsToRead()) {
    auto vec = lookup_input_vectors_.at(read_idx).get();
    if (vec == nullptr) {
      lookup_input_vectors_[read_idx] = std::make_unique<Vector>(lookup_side_types_.at(col_idx));
      vec = lookup_input_vectors_.at(read_idx).get();
      lookup_input_vp_->SetVector(vec, read_idx);
    }
    vec->Resize(num_matches);
    read_idx++;
  }
}

void RowIDIndexJoinExecutor::ReadLookupSide(const RawTableBlock *raw_table_block,
                                            const std::vector<std::pair<uint64_t, uint64_t>> &row_idxs,
                                            uint64_t col_to_read,
                                            uint64_t read_idx) {
  auto &vec = lookup_input_vectors_.at(read_idx);
  VectorOps::ReadBlock(raw_table_block, row_idxs, col_to_read, vec.get());
}


}
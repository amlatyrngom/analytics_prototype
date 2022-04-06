#include "execution/executors/rowid_index_join_executor.h"
#include "execution/vector_ops.h"

namespace smartid {

const VectorProjection *RowIDIndexJoinExecutor::Next() {
  auto key_child = Child(0);
  const VectorProjection* vp = key_child->Next();
  if (vp == nullptr) {
    return nullptr;
  }
  std::vector<uint64_t> key_side_matches;
  std::vector<uint64_t> lookup_side_matches;
  auto index_table_ = node_->IndexTable();
  auto filter = vp->GetFilter();
  auto key_col = vp->VectorAt(node_->KeyIdx())->DataAs<uint64_t>();
  filter->Map([&](sel_t i) {
    auto key = key_col[i];
    if (auto it =  index_table_->find(key); it != index_table_->end()) {
      for (const auto& val: it->second) {
        key_side_matches.emplace_back(i);
        lookup_side_matches.emplace_back(val);
      }
    }
  });
  if (key_side_matches.empty()) {
    return nullptr;
  }
  if (first_call_) {
    PrepareKeySide(vp);
    PrepareLookupSide();
  }

  // Gather key side values
  uint64_t projection_index = 0;
  for (const auto& proj: node_->Projections()) {
    if (proj.first == 0) {
      GatherKeySide(vp, proj.second, projection_index, key_side_matches);
    }
    projection_index++;
  }

  // Gather lookup side
  auto lookup_table = node_->LookupSide();
  std::unordered_map<const Block*, std::vector<std::pair<uint64_t, uint64_t>>> block_rows;

  uint64_t i = 0;
  for (const auto & lookup_row_id: lookup_side_matches) {
    auto row_idx = lookup_row_id & 0xFFFFFFull;
    auto block_idx = lookup_row_id >> 32ull;
    auto block = lookup_table->MutableBlockAt(block_idx);
    block_rows[block].emplace_back(row_idx, i);
    i++;
  }
  for (const auto& [block, row_idxs]: block_rows) {
    projection_index = 0;
    for (const auto& proj: node_->Projections()) {
      if (proj.first == 0) {
        GatherLookupSide(block, row_idxs, proj.second, projection_index, lookup_side_matches.size());
      }
      projection_index++;
    }
  }
  first_call_ = false;
  return result_.get();
}

void RowIDIndexJoinExecutor::PrepareKeySide(const VectorProjection* vp) {
  for (const auto& proj: node_->Projections()) {
    if (proj.first == 0) {
      key_side_types_[proj.second] = vp->VectorAt(proj.second)->ElemType();
    }
  }
}

void RowIDIndexJoinExecutor::PrepareLookupSide() {
  for (const auto& proj: node_->Projections()) {
    if (proj.first == 1) {
      lookup_side_types_[proj.second] = node_->LookupSide()->GetSchema().GetColumn(proj.second).Type();
    }
  }
}

void RowIDIndexJoinExecutor::GatherKeySide(const VectorProjection *vp,
                                           uint64_t col_idx,
                                           uint64_t projection_idx,
                                           const std::vector<uint64_t> &matches) {
  auto &vec = key_side_results_[col_idx];
  auto orig_vec = vp->VectorAt(col_idx);
  if (vec == nullptr) {
    vec = std::make_unique<Vector>(orig_vec->ElemType(), matches.size());
  }
  VectorOps::SelectVector(orig_vec, matches, vec.get());
  if (first_call_) {
    result_->SetVector(vec.get(), projection_idx);
  }
}

void RowIDIndexJoinExecutor::GatherLookupSide(const Block *block,
                                              const std::vector<std::pair<uint64_t, uint64_t>>& row_idxs,
                                              uint64_t col_idx,
                                              uint64_t projection_idx, uint64_t num_matches) {
  auto &vec = key_side_results_[col_idx];
  if (vec == nullptr) {
    vec = std::make_unique<Vector>(lookup_side_types_[col_idx], num_matches);
  }
  // This will be a noop is the size hasn't changed since the last call.
  vec->Resize(num_matches);
  VectorOps::ReadBlock(block, row_idxs, col_idx, vec.get());

  if (first_call_) {
    result_->SetVector(vec.get(), projection_idx);
  }
}

}
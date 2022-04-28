#include "execution/executors/hash_join_executor.h"
#include "execution/vector_ops.h"

namespace smartid {
void HashJoinExecutor::PrepareBuild(const VectorProjection *vp) {
  build_offsets_.resize(vp->NumCols() + 1); // +1 for mark.
  std::vector<std::pair<uint64_t, uint64_t>> idx_sizes;
  for (uint64_t i = 0; i < vp->NumCols(); i++) {
    auto elem_type = vp->VectorAt(i)->ElemType();
    idx_sizes.emplace_back(i, TypeUtil::TypeSize(elem_type));
    build_types_.emplace_back(elem_type);
  }
  // Used for semi-joins and statistics collection.
  idx_sizes.emplace_back(vp->NumCols(), TypeUtil::TypeSize(SqlType::Char));
  build_types_.emplace_back(SqlType::Char);

  std::sort(idx_sizes.begin(), idx_sizes.end(), [](const auto &x1, const auto &x2) {
    return x1.second > x2.second;
  });
  uint64_t curr_offset = 0;
  for (const auto &item: idx_sizes) {
    build_offsets_[item.first] = curr_offset + offsetof(HTEntry, payload);
    curr_offset += item.second;
  }
  entry_size_ = curr_offset + sizeof(HTEntry);
}

void HashJoinExecutor::VectorHash(const VectorProjection *vp, const std::vector<uint64_t> &key_cols) {
  auto vec = vp->VectorAt(key_cols[0]);
  VectorOps::HashVector(vec, vp->GetFilter(), &hashes_);
  for (uint64_t i = 1; i < key_cols.size(); i++) {
    vec = vp->VectorAt(key_cols[i]);
    VectorOps::HashCombineVector(vec, vp->GetFilter(), &hashes_);
  }
}

void HashJoinExecutor::MakeEntries(const VectorProjection *vp) {
  // Resize entries vector.
  build_entries_.Resize(vp->NumRows());
  auto entries_data = build_entries_.MutableDataAs<HTEntry *>();
  // Allocate buffer where hash table entries live.
  // We only need entries for selected indexes.
  auto vp_filter = vp->GetFilter();
  auto num_new_entries = vp_filter->ActiveSize();
  std::vector<char> alloc_space(num_new_entries * entry_size_);
  // Initialize the entries to point within the buffer.
  uint64_t curr_alloc_idx = 0;
  vp_filter->Map([&](int i) {
    auto entry = reinterpret_cast<HTEntry *>(alloc_space.data() + curr_alloc_idx * entry_size_);
    entries_data[i] = entry;
    curr_alloc_idx++;
  });
  build_alloc_space_.emplace_back(std::move(alloc_space));
  // Write attributes into hash table entries.
  for (uint64_t i = 0; i < vp->NumCols(); i++) {
    auto vec = vp->VectorAt(i);
    VectorOps::ScatterVector(vec, vp_filter, vec->ElemType(), build_offsets_[i], &build_entries_);
  }
  // For left semi joins and statistics, set mark = 0;
  build_marks_.Resize(vp->NumRows()); // relies on default vector initialization.
  auto i = vp->NumCols();
  VectorOps::ScatterVector(&build_marks_, vp_filter, build_types_[i], build_offsets_[i], &build_entries_);
}

void HashJoinExecutor::VectorInsert(const VectorProjection *vp) {
  // Build a vector of hash table entries.
  MakeEntries(vp);
  // Insert entries into the hash table.
  auto hash_data = hashes_.DataAs<int64_t>();
  auto entries = build_entries_.DataAs<HTEntry *>();
  vp->GetFilter()->Map([&](sel_t i) {
    auto last_entry = join_table_[hash_data[i]];
    auto new_entry = entries[i];
    new_entry->next = last_entry;
    join_table_[hash_data[i]] = new_entry;
  });
  // Bloom filter
  if (node_->UseBloom()) {
    vp->GetFilter()->Map([&](sel_t i) {
      bloom_->Insert(hash_data[i]);
    });
  }
}

void HashJoinExecutor::Build() {
  const VectorProjection *vp;
  bool first{true};
  while ((vp = Child(0)->Next())) {
    // Build timer start
    auto start = std::chrono::high_resolution_clock::now();
    if (first) {
      // On first call, compute column types and offsets.
      PrepareBuild(vp);
      first = false;
    }
    // Hash.
    VectorHash(vp, node_->BuildKeys());
    // Insert
    VectorInsert(vp);
    // Insertion timer end
    auto end = std::chrono::high_resolution_clock::now();
    build_time += duration_cast<std::chrono::nanoseconds>(end - start).count();
  }
  built_ = true;
}

void HashJoinExecutor::FindCandidates(const VectorProjection *vp) {
  auto hash_data = hashes_.DataAs<int64_t>();
  cand_filter_.SetFrom(vp->GetFilter());
  candidates_.Resize(cand_filter_.TotalSize());
  auto cand_data = candidates_.MutableDataAs<const HTEntry *>();
  // Only select elements with a match;
  // Check bloom filter first if needed.
  if (node_->UseBloom()) {
    cand_filter_.Update([&](sel_t i) {
      return bloom_->Test(hash_data[i]);
    });
  }
  probe_in += cand_filter_.NumOnes();
  cand_filter_.Update([&](sel_t i) {
    if (const auto &it = join_table_.find(hash_data[i]); it != join_table_.end()) {
      cand_data[i] = it->second;
      return true;
    }
    return false;
  });
}

void HashJoinExecutor::TestSetMarks(Bitmap *filter, Vector *mark) {
  auto mark_data = mark->MutableDataAs<char>();
  filter->Update([&](sel_t i) {
    if (mark_data[i] == 0) {
      mark_data[i] = 1;
      return true;
    }
    return false;
  });
}


void HashJoinExecutor::CheckKeys(const VectorProjection *vp) {
  match_filter_.SetFrom(&cand_filter_);
  // Do a match for all columns.
  for (uint64_t i = 0; i < node_->ProbeKeys().size(); i++) {
    auto probe_col = node_->ProbeKeys()[i];
    auto probe_vec = vp->VectorAt(probe_col);
    auto build_col = node_->BuildKeys()[i];
    auto build_offset = build_offsets_[build_col];
    auto build_col_type = build_types_[build_col];
    VectorOps::GatherCompareVector(&candidates_, probe_vec, &match_filter_, build_col_type, build_offset);
  }
  // Set mark for semi joins.
  if (node_->GetJoinType() == JoinType::LEFT_SEMI) {
    VectorOps::TestSetMarkVector(&match_filter_, build_offsets_.back(), &candidates_);
  }
  if (node_->GetJoinType() == JoinType::RIGHT_SEMI) {
    TestSetMarks(&match_filter_, &probe_marks_);
  }
  // Add all matching columns.
  auto cand_data = candidates_.DataAs<const HTEntry *>();
  match_filter_.Map([&](sel_t i) {
    probe_matches_.emplace_back(i);
    build_matches_.emplace_back(cand_data[i]);
  });
}

void HashJoinExecutor::AdvanceChains() {
  if (node_->GetJoinType() == JoinType::RIGHT_SEMI) {
    cand_filter_.SubsetDifference(&match_filter_, &cand_filter_);
  }
  auto cand_data = candidates_.MutableDataAs<const HTEntry *>();
  // Only select non null entries.
  cand_filter_.Update([&](sel_t i) {
    return (cand_data[i] = cand_data[i]->next) != nullptr;
  });
}

void HashJoinExecutor::GatherBuildCol(uint64_t col_idx, uint64_t vec_idx) {
  auto &vec = build_vecs_[vec_idx];
  if (vec == nullptr) {
    // Initial allocation.
    vec = std::make_unique<Vector>(build_types_[col_idx]);
    vec->Resize(build_matches_.size());
  }
  // Place HT Entries into a vector.
  build_entries_.ShallowReset(build_matches_.size(), reinterpret_cast<char *>(build_matches_.data()), result_filter_.Words());

  // Gather the right values
  auto build_offset = build_offsets_[col_idx];
  auto build_type = build_types_[col_idx];
  VectorOps::GatherVector(&build_entries_, &result_filter_, build_type, build_offset, vec.get());

  // On first probe, add this vector to the result set.
  if (first_probe_) {
    result_->AddVector(vec.get());
  }
}

void HashJoinExecutor::GatherProbeCol(const VectorProjection *vp, uint64_t col_idx, uint64_t vec_idx) {
  auto &vec = probe_vecs_[vec_idx];
  auto orig_vec = vp->VectorAt(col_idx);
  if (vec == nullptr) {
    vec = std::make_unique<Vector>(orig_vec->ElemType());
    vec->Resize(probe_matches_.size());
  }
  VectorOps::SelectVector(orig_vec, probe_matches_, vec.get());
  if (first_probe_) {
    result_->AddVector(vec.get());
  }
}

void HashJoinExecutor::GatherMatches(const VectorProjection *vp) {
  result_filter_.Reset(build_matches_.size());
  auto build_vec_idx = 0;
  auto probe_vec_idx = 0;

  for (const auto &proj: node_->Projections()) {
    auto join_side = proj.first;
    auto col_idx = proj.second;
    if (join_side == 0) {
      GatherBuildCol(col_idx, build_vec_idx);
      build_vec_idx++;
    } else {
      GatherProbeCol(vp, col_idx, probe_vec_idx);
      probe_vec_idx++;
    }
  }
}

const VectorProjection *HashJoinExecutor::Next() {
  // Build only once.
  {
    if (!built_) {
      Build();
    }
  }
  // Probe
  auto vp = Child(1)->Next();
  if (vp == nullptr) {
    Clear();
    return nullptr;
  }
  auto start = std::chrono::high_resolution_clock::now();
  // For right-semi-join and statistics, set probe marks to 0;
  if (node_->GetJoinType() == JoinType::RIGHT_SEMI) {
    probe_marks_.Resize(vp->NumRows());
    VectorOps::InitVector(vp->GetFilter(), Value(char(0)), SqlType::Char, &probe_marks_);
  }
  VectorHash(vp, node_->ProbeKeys());

  FindCandidates(vp);
  build_matches_.clear();
  probe_matches_.clear();
  while (cand_filter_.ActiveSize() != 0) {
    CheckKeys(vp);
    AdvanceChains();
  }
  if (build_matches_.empty()) {
    auto end = std::chrono::high_resolution_clock::now();
    probe_time += duration_cast<std::chrono::nanoseconds>(end - start).count();
    return Next();
  }
  GatherMatches(vp);
  first_probe_ = false;
  auto end = std::chrono::high_resolution_clock::now();
  probe_time += duration_cast<std::chrono::nanoseconds>(end - start).count();
  join_out += probe_matches_.size();
  return result_.get();
}

void HashJoinExecutor::Clear() {
  join_table_.clear();
  build_alloc_space_.clear();
  bloom_ = nullptr;
}

}
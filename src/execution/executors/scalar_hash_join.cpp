#include "execution/executors/scalar_hash_join_executor.h"
#include "execution/vector_ops.h"
#include <fmt/core.h>

namespace smartid {
void ScalarHashJoinExecutor::PrepareBuild(const VectorProjection *vp) {
  build_offsets_.resize(vp->NumCols() + 1); // +1 for mark.
  std::vector<std::pair<uint64_t, uint64_t>> idx_sizes;
  for (uint64_t i = 0; i < vp->NumCols(); i++) {
    auto elem_type = vp->VectorAt(i)->ElemType();
    idx_sizes.emplace_back(i, TypeUtil::TypeSize(elem_type));
    build_types_.emplace_back(elem_type);
    if (i == node_->BuildKeys()[0]) {
      is32 = elem_type == SqlType::Int32;
      fmt::print("SCALAR HT BUILD TYPE: {}\n", is32 ? "32" : "64");
    }
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

void ScalarHashJoinExecutor::VectorHash(const VectorProjection *vp, const std::vector<uint64_t> &key_cols) {
  auto vec = vp->VectorAt(key_cols[0]);
  VectorOps::HashVector(vec, vp->GetFilter(), &hashes_);
  for (uint64_t i = 1; i < key_cols.size(); i++) {
    vec = vp->VectorAt(key_cols[i]);
    VectorOps::HashCombineVector(vec, vp->GetFilter(), &hashes_);
  }
}

void ScalarHashJoinExecutor::MakeEntries(const VectorProjection *vp) {
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

void ScalarHashJoinExecutor::VectorInsert(const VectorProjection *vp) {
  uint64_t key_col = node_->BuildKeys()[0];
  // Build a vector of hash table entries.
  MakeEntries(vp);
  // Insert entries into the hash table.
  if (is32) {
    auto key_data = vp->VectorAt(key_col)->DataAs<int32_t>();
    auto entries = build_entries_.DataAs<HTEntry *>();
    vp->GetFilter()->Map([&](sel_t i) {
      auto key = key_data[i] & Settings::KEY_MASK32;
      join_table32_[key].emplace_back(entries[i]);
//      fmt::print("Inserting key32 {}\n", key);
    });
    // Bloom filter
    if (node_->UseBloom()) {
      vp->GetFilter()->Map([&](sel_t i) {
        auto key = key_data[i] & Settings::KEY_MASK32;
        bloom_->Insert(key);
      });
    }
  } else {
    auto key_data = vp->VectorAt(key_col)->DataAs<int64_t>();
    auto entries = build_entries_.DataAs<HTEntry *>();
    vp->GetFilter()->Map([&](sel_t i) {
      auto key = key_data[i] & Settings::KEY_MASK64;
      join_table64_[key].emplace_back(entries[i]);
//      fmt::print("Inserting key64 {}\n", key);
    });
    // Bloom filter
    if (node_->UseBloom()) {
      vp->GetFilter()->Map([&](sel_t i) {
        auto key = key_data[i] & Settings::KEY_MASK64;
        bloom_->Insert(key);
      });
    }
  }

}

void ScalarHashJoinExecutor::Build() {
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
//    VectorHash(vp, node_->BuildKeys());
    // Insert
    VectorInsert(vp);
    // Insertion timer end
    auto end = std::chrono::high_resolution_clock::now();
    build_time += duration_cast<std::chrono::nanoseconds>(end - start).count();
  }
  built_ = true;
}

void ScalarHashJoinExecutor::GatherBuildCol(uint64_t col_idx, uint64_t vec_idx) {
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

void ScalarHashJoinExecutor::GatherProbeCol(const VectorProjection *vp, uint64_t col_idx, uint64_t vec_idx) {
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

void ScalarHashJoinExecutor::GatherMatches(const VectorProjection *vp) {
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

const VectorProjection *ScalarHashJoinExecutor::Next() {
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

  build_matches_.clear();
  probe_matches_.clear();
  uint64_t probe_key = node_->ProbeKeys()[0];
  cand_filter_.SetFrom(vp->GetFilter());
  if (is32) {
    auto key_data = vp->VectorAt(probe_key)->DataAs<int32_t>();
    if (node_->UseBloom()) {
      cand_filter_.Update([&](sel_t i) {
        auto key = key_data[i] & Settings::KEY_MASK32;
        return bloom_->Test(key);
      });
    }
    probe_in += cand_filter_.NumOnes();
    cand_filter_.Map([&](sel_t i) {
      auto key = key_data[i] & Settings::KEY_MASK32;
      if (const auto& it = join_table32_.find(key); it != join_table32_.end()) {
        for (const auto& entry: it->second) {
//          fmt::print("Looking up32 {}\n", key);
          probe_matches_.emplace_back(i);
          build_matches_.emplace_back(entry);
        }
      }
    });
  } else {
    auto key_data = vp->VectorAt(probe_key)->DataAs<int64_t>();
    if (node_->UseBloom()) {
      cand_filter_.Update([&](sel_t i) {
        auto key = key_data[i] & Settings::KEY_MASK64;
        return bloom_->Test(key);
      });
    }
    probe_in += cand_filter_.NumOnes();
    cand_filter_.Map([&](sel_t i) {
      auto key = key_data[i] & Settings::KEY_MASK64;
      if (const auto& it = join_table64_.find(key); it != join_table64_.end()) {
        for (const auto& entry: it->second) {
//          fmt::print("Looking up64 {}\n", key);
          probe_matches_.emplace_back(i);
          build_matches_.emplace_back(entry);
        }
      }
    });
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

void ScalarHashJoinExecutor::Clear() {
//  join_table32_.clear();
//  join_table64_.clear();
//  build_alloc_space_.clear();
  bloom_ = nullptr;
}

}
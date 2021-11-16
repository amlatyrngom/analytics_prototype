#include "execution/executors/hash_aggr_executor.h"

namespace smartid {
const VectorProjection *HashAggregationExecutor::Next() {
  if (accumulated_) return nullptr;
  Accumulate();
  accumulated_ = true;
  IterateTable();
  return result_.get();
}

void HashAggregationExecutor::Prepare(const VectorProjection *vp) {
  build_offsets_.resize(node_->GetGroupBys().size() + node_->GetAggs().size());
  // Sort input columns by type size.
  std::vector<std::pair<uint64_t, uint64_t>> idx_sizes;
  uint64_t attr_idx = 0;
  // Add group by columns.
  for (const auto &i: node_->GetGroupBys()) {
    auto elem_type = vp->VectorAt(i)->ElemType();
    // Create output vector.
    auto result_vec = std::make_unique<Vector>(elem_type);
    result_->AddVector(result_vec.get());
    result_vecs_.emplace_back(std::move(result_vec));
    // Append size and index for sorting
    idx_sizes.emplace_back(attr_idx, TypeUtil::TypeSize(elem_type));
    build_types_.emplace_back(elem_type);
    attr_idx++;
  }
  // Add aggregate columns.
  for (const auto &agg: node_->GetAggs()) {
    // Identify type of aggregate value.
    auto[agg_idx, agg_type] = agg;
    SqlType elem_type;
    switch (agg_type) {
      case AggType::COUNT: {
        elem_type = SqlType::Int64;
        break;
      }
      case AggType::SUM: {
        elem_type = SqlType::Float64;
        break;
      }
      case AggType::MIN:
      case AggType::MAX: {
        elem_type = vp->VectorAt(agg_idx)->ElemType();
        break;
      }
    }
    // Create output vector.
    auto result_vec = std::make_unique<Vector>(elem_type);
    result_->AddVector(result_vec.get());
    result_vecs_.emplace_back(std::move(result_vec));
    // Append size and index for sorting
    idx_sizes.emplace_back(attr_idx, TypeUtil::TypeSize(elem_type));
    build_types_.emplace_back(elem_type);
    attr_idx++;
  }
  // Sort by decreasing size.
  std::sort(idx_sizes.begin(), idx_sizes.end(), [](const auto &x1, const auto &x2) {
    return x1.second > x2.second;
  });
  // Write offsets according to sort orders.
  uint64_t curr_offset = 0;
  for (const auto &item: idx_sizes) {
    build_offsets_[item.first] = curr_offset + offsetof(HTEntry, payload);
    curr_offset += item.second;
  }
  entry_size_ = curr_offset + sizeof(HTEntry);
}

void HashAggregationExecutor::VectorHash(const VectorProjection *vp, const std::vector<uint64_t> &key_cols) {
  auto vec = vp->VectorAt(key_cols[0]);
  VectorOps::HashVector(vec, vp->GetFilter(), &hashes_);
  for (uint64_t i = 1; i < key_cols.size(); i++) {
    vec = vp->VectorAt(key_cols[i]);
    VectorOps::HashCombineVector(vec, vp->GetFilter(), &hashes_);
  }
}

void HashAggregationExecutor::FindCandidates(const VectorProjection *vp) {
  auto hash_data = hashes_.DataAs<int64_t>();
  auto cand_data = candidates_.MutableDataAs<const HTEntry *>();
  cand_filter_.SetFrom(&non_match_filter_);
  // Only select elements with a match;
  cand_filter_.Update([&](sel_t i) {
    if (const auto &it = agg_table_.find(hash_data[i]); it != agg_table_.end()) {
      cand_data[i] = it->second;
      return true;
    }
    return false;
  });
}

void HashAggregationExecutor::UpdateMatches(const VectorProjection *vp) {
  match_filter_.SetFrom(&cand_filter_);
  // Do a match for all columns.
  uint64_t attr_idx = 0;
  for (const auto &i : node_->GetGroupBys()) {
    auto group_by_vec = vp->VectorAt(i);
    auto key_offset = build_offsets_[attr_idx];
    auto key_type = build_types_[attr_idx];
    VectorOps::GatherCompareVector(&candidates_, group_by_vec, &match_filter_, key_type, key_offset);
    attr_idx++;
  }
  // Accumulate values into matches.
  for (const auto &agg: node_->GetAggs()) {
    auto[agg_idx, agg_type] = agg;
    auto agg_offset = build_offsets_[attr_idx];
    VectorOps::ScatterReduceVector(&match_filter_, vp->VectorAt(agg_idx), &candidates_, agg_offset, agg_type);
    attr_idx++;
  }
  // Remove matching elements to later initialize them.
  non_match_filter_.SubsetDifference(&match_filter_, &non_match_filter_);
}

void HashAggregationExecutor::AdvanceChains(const VectorProjection *vp) {
  cand_filter_.SubsetDifference(&match_filter_, &cand_filter_);
  // Advance and remove null entries.
  auto cand_data = candidates_.MutableDataAs<const HTEntry *>();
  cand_filter_.Update([&](sel_t i) {
    return (cand_data[i] = cand_data[i]->next) != nullptr;
  });
}

void HashAggregationExecutor::InitNewEntry(const VectorProjection *vp, sel_t i) {
  // Allocate buffer where hash table entries live.
  // This is to (1) avoid manual freeing, and (2) easily compute used space.
  std::vector<char> alloc_space(entry_size_);
  // Initialize the entries to point within the buffer.
  auto entry = reinterpret_cast<HTEntry *>(alloc_space.data());
  entry->next = nullptr;
  entries_.emplace_back(entry);
  build_alloc_space_.emplace_back(std::move(alloc_space));
  // Write keys into hash table entries.
  uint64_t attr_idx = 0;
  for (const auto &k: node_->GetGroupBys()) {
    auto vec = vp->VectorAt(k);
    VectorOps::ScatterScalar(vec, i, reinterpret_cast<char *>(entry), build_offsets_[attr_idx]);
    attr_idx++;
  }
  // Initialize aggregates.
  for (const auto &agg: node_->GetAggs()) {
    auto[agg_idx, agg_type] = agg;
    auto vec = vp->VectorAt(agg_idx);
    VectorOps::ScatterInitReduceScalar(vec, i, reinterpret_cast<char *>(entry), build_offsets_[attr_idx], agg_type);
    attr_idx++;
  }
  // Insert
  auto hash = hashes_.DataAs<uint64_t>()[i];
  auto last_entry = agg_table_[hash];
  entry->next = last_entry;
  agg_table_[hash] = entry;
}

void HashAggregationExecutor::Accumulate() {
  const VectorProjection *vp;
  bool first{true};
  while ((vp = Child(0)->Next()) != nullptr) {
    if (first) {
      // On first call, compute value offsets.
      first = false;
      Prepare(vp);
    }
    // Hash values.
    VectorHash(vp, node_->GetGroupBys());
    // Loop while not all values have been matched.
    non_match_filter_.SetFrom(vp->GetFilter());
    while (non_match_filter_.ActiveSize() > 0) {
      // Find candidates matches (exact + collision)
      FindCandidates(vp);
      // Advances all chains until exact match or end.
      while (cand_filter_.ActiveSize() > 0) {
        UpdateMatches(vp);
        AdvanceChains(vp);
      }
      // At this point, non_match_filer_ contains elements with brand new keys.
      // Insert the only first item in the table (the others might conflict with the first).
      // Then go back to the top and repeat.
      bool first_idx = true;
      non_match_filter_.Update([&](sel_t i) {
        if (first_idx) {
          InitNewEntry(vp, i);
          first_idx = false;
          return false;
        }
        return true;
      });
    }
  }
}

void HashAggregationExecutor::IterateTable() {
  uint64_t attr_idx = 0;
  build_entries_.ShallowReset(entries_.size(), reinterpret_cast<const char *>(entries_.data()));
  result_filter_.Reset(entries_.size(), FilterMode::BitmapFull);
  for (const auto &i: node_->GetGroupBys()) {
    auto build_offset = build_offsets_[attr_idx];
    auto build_type = build_types_[attr_idx];
    auto vec = result_vecs_[attr_idx].get();
    VectorOps::GatherVector(&build_entries_, &result_filter_, build_type, build_offset, vec);
    attr_idx++;
  }
  for (const auto &agg: node_->GetAggs()) {
    auto build_offset = build_offsets_[attr_idx];
    auto build_type = build_types_[attr_idx];
    auto vec = result_vecs_[attr_idx].get();
    VectorOps::GatherVector(&build_entries_, &result_filter_, build_type, build_offset, vec);
    attr_idx++;
  }
}
}
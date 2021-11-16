#include "execution/executors/sort_executor.h"

namespace smartid {
const VectorProjection *SortExecutor::Next() {
  if (sorted_) return nullptr;
  sorted_ = true;
  const VectorProjection *vp;
  bool first{true};
  while ((vp = Child(0)->Next())) {
    if (first) {
      first = false;
      Prepare(vp);
    }
    if (node_->Limit() != -1) {
      AppendLimit(vp, node_->Limit());
    } else {
      Append(vp);
    }
  }
  Sort();
  Collect();
  return result_.get();
}

void SortExecutor::Prepare(const VectorProjection *vp) {
  sort_offsets_.resize(vp->NumCols());
  result_ = std::make_unique<VectorProjection>(&result_filter_);
  // Sort input columns by type size.
  std::vector<std::pair<uint64_t, uint64_t>> idx_sizes;
  for (uint64_t i = 0; i < vp->NumCols(); i++) {
    auto elem_type = vp->VectorAt(i)->ElemType();
    idx_sizes.emplace_back(i, TypeUtil::TypeSize(elem_type));
    sort_types_.emplace_back(elem_type);
    // Result vector.
    auto result_vec = std::make_unique<Vector>(elem_type);
    result_->AddVector(result_vec.get());
    result_vecs_.emplace_back(std::move(result_vec));
  }
  std::sort(idx_sizes.begin(), idx_sizes.end(), [](const auto &x1, const auto &x2) {
    return x1.second > x2.second;
  });
  uint64_t curr_offset = 0;
  for (const auto &item: idx_sizes) {
    sort_offsets_[item.first] = curr_offset;
    curr_offset += item.second;
  }
  sort_size_ = curr_offset;
}

void SortExecutor::AppendLimit(const VectorProjection *vp, int64_t limit) {
  sort_entries_.Resize(vp->NumRows());
  auto entries_data = sort_entries_.MutableDataAs<char *>();
  // Allocate buffer where hash table entries live.
  // We only need entries for selected indexes.
  auto vp_filter = vp->GetFilter();
  std::vector<char> alloc_space(vp_filter->ActiveSize() * sort_size_);

  // Create new sort entries.
  uint64_t curr_alloc_idx = 0;
  vp_filter->Map([&](sel_t i) {
    auto entry = alloc_space.data() + curr_alloc_idx * sort_size_;
    entries_data[i] = entry;
    curr_alloc_idx++;
  });
  sort_alloc_space_.emplace_back(std::move(alloc_space));

  // Write attributes into sort entries.
  for (uint64_t i = 0; i < vp->NumCols(); i++) {
    auto vec = vp->VectorAt(i);
    VectorOps::ScatterVector(vec, vp_filter, vec->ElemType(), sort_offsets_[i], &sort_entries_);
  }

  // Perform a heap insert
  VectorOps::HeapInsertVector(vp_filter, limit, entries_data, sort_vector_,
                              node_->SortKeys(), sort_types_, sort_offsets_);
}

void SortExecutor::Append(const VectorProjection *vp) {
  sort_entries_.Resize(vp->NumRows());
  auto entries_data = sort_entries_.MutableDataAs<char *>();
  // Allocate buffer where hash table entries live.
  // We only need entries for selected indexes.
  auto vp_filter = vp->GetFilter();
  std::vector<char> alloc_space(vp_filter->ActiveSize() * sort_size_);
  uint64_t curr_alloc_idx = 0;
  vp_filter->Map([&](sel_t i) {
    auto entry = alloc_space.data() + curr_alloc_idx * sort_size_;
    entries_data[i] = entry;
    curr_alloc_idx++;
    sort_vector_.emplace_back(entry);
  });
  sort_alloc_space_.emplace_back(std::move(alloc_space));
  // Write attributes into sort entries.
  for (uint64_t i = 0; i < vp->NumCols(); i++) {
    auto vec = vp->VectorAt(i);
    VectorOps::ScatterVector(vec, vp_filter, vec->ElemType(), sort_offsets_[i], &sort_entries_);
  }
}

void SortExecutor::Sort() {
  if (node_->SortKeys().size() == 1) {
    auto[key, type] = node_->SortKeys()[0];
    VectorOps::SortVector(&sort_vector_, type, sort_types_[key], sort_offsets_[key]);
  } else {
    VectorOps::MultiSortVector(&sort_vector_, node_->SortKeys(), sort_types_, sort_offsets_);
  }
}

void SortExecutor::Collect() {
  sort_entries_.ShallowReset(sort_vector_.size(), reinterpret_cast<char *>(sort_vector_.data()));
  result_filter_.Reset(sort_vector_.size(), FilterMode::BitmapFull);
  for (uint64_t i = 0; i < sort_types_.size(); i++) {
    auto col_type = sort_types_[i];
    auto col_offset = sort_offsets_[i];
    auto vec = result_vecs_[i].get();
    vec->Resize(sort_vector_.size());
    VectorOps::GatherVector(&sort_entries_, &result_filter_, col_type, col_offset, vec);
  }
}
}
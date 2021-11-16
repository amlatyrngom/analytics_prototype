#include "stats/histogram.h"

namespace smartid {

//////////////////////////////////////////////////
//// Vectorized Bit Indices
///////////////////////////////////////////////////

/**
 * Scale and offset satisfy given offset and number of bits.
 */
void ScaleAndOffset(const Filter* filter, Vector* out, uint64_t scale, uint64_t offset) {
  auto out_data = out->MutableDataAs<uint64_t>();
  // If scale is power of 2, faster index computation.
  if (((scale != 0) && !(scale & (scale - 1)))) {
    auto shift_amount = static_cast<uint64_t>(__builtin_ctzll(scale));
    filter->SafeMap([&](sel_t i) {
      out_data[i] = (out_data[i] >> shift_amount) + offset;
    });
  } else {
    // Slow division
    // TODO(Amadou): If two slow, specialize with most likely values:  3, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15.
    filter->SafeMap([&](sel_t i) {
      out_data[i] = (out_data[i] / scale) + offset;
    });
  }
}

/**
 * Templated index computation.
 */
template <typename T>
void TemplatedBitIndices(const Filter* filter, const Vector *in, Vector *out, const Histogram* hist, uint64_t offset, uint64_t num_bits) {
  // How to map final results to bit indices.
  uint64_t scale = 1;
  if (hist->GetParts().size() > num_bits) {
    // Round up division.
    scale = ((hist->GetParts().size() + num_bits - 1) / num_bits);
  }

  // Make typed bounds
  const auto& part_bounds = hist->GetParts();
  std::vector<std::pair<T, T>> typed_part_bounds(part_bounds.size());
  for (uint64_t i = 0; i < part_bounds.size(); i++) {
    const auto& p = part_bounds[i];
    typed_part_bounds[i] = {std::get<T>(p.first), std::get<T>(p.second)};
  }

  // Fill output
  auto in_data = in->DataAs<T>();
  out->Resize(in->NumElems());
  auto out_data = out->MutableDataAs<int64_t>();
  filter->SafeMap([&](sel_t i) {
    const auto& val = in_data[i];
    for (int64_t k = 0; k < typed_part_bounds.size(); k++) {
      if (val < typed_part_bounds[k].first) {
         out_data[i] = k-1;
         return;
      }
      if (val >= typed_part_bounds[k].first && val <= typed_part_bounds[k].second) {
        out_data[i] = k;
        return;
      }
    }
    out_data[i] = static_cast<int64_t>(typed_part_bounds.size());
  });

  if (scale != 0) {
    ScaleAndOffset(filter, out, scale, offset);
  }
}

#define TEMPLATED_BIT_INDICES(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    TemplatedBitIndices<cpp_type>(filter, in, out, this, offset, num_bits);     \
    break;

void Histogram::BitIndices(const Filter* filter, const Vector *in, Vector *out, uint64_t offset, uint64_t num_bits)  const {
  switch (val_type_) {
    SQL_TYPE(TEMPLATED_BIT_INDICES, NOOP);
    case SqlType::Date:
      TemplatedBitIndices<Date>(filter, in, out, this, offset, num_bits);
      break;
    default:
      std::cerr << "Non Numeric BitIndices Histogram Not Yet Supported!!!!!!!!!!" << std::endl;

  }
}

///////////////////////////////////////////////////////
/// Vectorized build of EquiWidth
//////////////////////////////////////////////////////


template <typename T>
void TemplatedEquiWidth(const ColumnStats &stats, int32_t num_parts, std::vector<std::pair<Value, Value>> * part_bounds) {
  auto max_val = std::get<T>(stats.max_);
  auto min_val = std::get<T>(stats.min_);
  // Bucket width
  auto width = (max_val - min_val) / num_parts;
  const auto& vals = stats.by_freq_.vals_->DataAs<T>();
  auto lo = vals[0];
  int curr_num_parts = 0;
  for (uint64_t i = 1; i < stats.count_distinct_ && curr_num_parts < num_parts; i++) {
    if (vals[i] > lo + width) {
      curr_num_parts++;
      part_bounds->emplace_back(Value(lo), Value(vals[i-1]));
      lo = vals[i];
    }
  }

  // Add last interval if necessary
  if (curr_num_parts < num_parts) {
    part_bounds->emplace_back(Value(lo), stats.max_);
  } else {
    part_bounds->back().second = stats.max_;
  }
}

#define TEMPLATED_EQUIWIDTH(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    TemplatedEquiWidth<cpp_type>(stats, num_parts, &part_bounds_); \
    break;

EquiWidthHistogram::EquiWidthHistogram(int32_t num_parts,
                                       SqlType val_type,
                                       const ColumnStats &stats) : Histogram(num_parts, val_type) {

  switch (val_type) {
    SQL_TYPE(TEMPLATED_EQUIWIDTH, NOOP);
    case SqlType::Date:
      TemplatedEquiWidth<Date>(stats, num_parts, &part_bounds_);
      break;
    default:
    std::cerr << "Non Numeric EquiWidth Histogram Not Yet Supported!!!!!!!!!!" << std::endl;
  }
}

/////////////////////////////////////////////////////////
/// Vectorized Build of EquiDepth
//////////////////////////////////////////////////////

template <typename T>
void TemplatedEquiDepth(const ColumnStats &stats, const ValWeights& val_weights, int32_t num_parts, std::vector<std::pair<Value, Value>> * part_bounds) {
  double approx_bucket_fill = static_cast<double>(stats.count_) / num_parts;
  std::cout << "Approximate Fill Factor: " << approx_bucket_fill << std::endl;
  int64_t curr_bucket_fill = 0;
  const auto& vals = val_weights.vals_->DataAs<T>();
  const auto& weigths = val_weights.weights_->DataAs<int64_t>();
  auto lo_idx = 0;
  int curr_num_parts = 0;
  for (uint64_t i = 0; i < stats.count_distinct_ && curr_num_parts < num_parts; i++) {
    curr_bucket_fill += weigths[i];
    if (curr_bucket_fill >= approx_bucket_fill) {
      curr_bucket_fill = 0;
      curr_num_parts++;
      part_bounds->emplace_back(Value(vals[lo_idx]), Value(vals[i]));
      lo_idx = i + 1;
    }
  }

  if (curr_num_parts == num_parts) {
    // Set last value to upper bound.
    part_bounds->back().second = stats.max_;
  } else if (curr_bucket_fill > 0) {
    // Add Last Bucket
    part_bounds->emplace_back(Value(vals[lo_idx]), stats.max_);
  }
}

#define TEMPLATED_EQUIDEPTH(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    TemplatedEquiDepth<cpp_type>(stats, val_weights, num_parts, &part_bounds_); \
    break;


EquiDepthHistogram::EquiDepthHistogram(int32_t num_parts,
                                       SqlType sql_type,
                                       const ColumnStats &stats, bool use_frequency) : Histogram(num_parts, sql_type) {
 const auto& val_weights = use_frequency ? stats.by_freq_ : stats.by_usage_;
 switch (sql_type) {
    SQL_TYPE(TEMPLATED_EQUIDEPTH, NOOP);
    case SqlType::Date:
      TemplatedEquiDepth<Date>(stats, val_weights, num_parts, &part_bounds_);
      break;
    default:
      std::cerr << "Non Numeric EquiDepth Histogram Not Yet Supported!!!!!!!!!!" << std::endl;
  }
}


////////////////////////////////////////////////////////////////
/// Max Diff
///////////////////////////////////////////////////////////////

namespace {
std::ostream &operator<<(std::ostream &os, const smartid::Date &d) {
  return os << d.ToString();
}
}


template <typename T, typename DiffType=T>
void TemplatedMaxDiff(const ColumnStats& stats, int32_t num_parts, std::vector<std::pair<Value, Value>>* part_bounds) {
  const auto& vals_vec = stats.by_freq_.vals_;
  const auto& vals = stats.by_freq_.vals_->DataAs<T>();
  // Special case: one element. Should never happen anyway
  if (vals_vec->NumElems() <= 1) {
    part_bounds->emplace_back(Value(vals[0]), Value(vals[0]));
    return;
  }
  // Compute diffs

  std::vector<std::pair<DiffType, uint64_t>> diffs;
  diffs.reserve(vals_vec->NumElems());
  diffs.emplace_back(0, 0);
  for (uint64_t i = 1; i < vals_vec->NumElems(); i++) {
    diffs.emplace_back(vals[i] - vals[i-1], i);
  }
  // Get top diffs.
  std::sort(diffs.begin(), diffs.end(), [](const auto& x, const auto& y) {
    return x.first > y.first;
  });
  std::vector<uint64_t> top_indices;
  top_indices.reserve(num_parts);
  top_indices.emplace_back(0);
  // Given num_parts buckets, there are num_parts - 1 boundaries.
  for (uint64_t i = 0; i < num_parts - 1 && i < vals_vec->NumElems(); i++) {
    top_indices.emplace_back(diffs[i].second);
  }
  // Reorder
  std::sort(top_indices.begin(), top_indices.end());
  // Make bounds.
  for (uint64_t i = 1; i < top_indices.size(); i++) {
    auto lo = vals[top_indices[i-1]];
    auto hi = vals[top_indices[i] - 1];
    part_bounds->emplace_back(Value(lo), Value(hi));
    std::cout << "Making Bounds: "
              << lo << ", "
              << hi << std::endl;
  }
  // Last buckets.
  auto lo = vals[top_indices.back()];
  auto hi = vals[vals_vec->NumElems() - 1];
  part_bounds->emplace_back(Value(lo), Value(hi));
  std::cout << "Making Bounds: "
            << lo << ", "
            << hi << std::endl;
}

#define TEMPLATED_MAXDIFF(sql_type, cpp_type, ...) \
  case SqlType::sql_type: \
    TemplatedMaxDiff<cpp_type>(stats, num_parts, &part_bounds_); \
    break;


MaxDiffHistogram::MaxDiffHistogram(int32_t num_parts, SqlType sql_type, const ColumnStats &stats) : Histogram(num_parts, sql_type) {
  switch (sql_type) {
    SQL_TYPE(TEMPLATED_MAXDIFF, NOOP);
    case SqlType::Date:
      TemplatedMaxDiff<Date, Date::NativeType>(stats, num_parts, &part_bounds_);
      break;
    default:
      std::cerr << "Non Numeric MaxDiff Histogram Not Yet Supported!!!!!!!!!!" << std::endl;
  }
}


/////////////////////////////////////////////
/// Compressed Histogram
/////////////////////////////////////////////

template <typename T>
void TemplatedCompressed(const ColumnStats &stats, const ValWeights& val_weights, int32_t num_parts, std::vector<std::pair<Value, Value>> * part_bounds) {
  double approx_bucket_fill = static_cast<double>(stats.count_) / num_parts;
  std::cout << "Approximate Compressed Fill Factor: " << approx_bucket_fill << std::endl;
  int64_t curr_bucket_fill = 0;
  const auto& vals = val_weights.vals_->DataAs<T>();
  const auto& weights = val_weights.weights_->DataAs<int64_t>();
  auto lo_idx = 0;
  for (uint64_t i = 0; i < stats.count_distinct_ && part_bounds->size() < num_parts; i++) {
    // Like equidepth, but allow high frequencies to be singletons.
    bool full = false;
    if (i == 500) {
      std::cout << "Compress Debug:"
                << curr_bucket_fill << ", "
                << approx_bucket_fill << ", "
                << weights[i]
                << std::endl;
    }
    if (curr_bucket_fill == 0 && static_cast<double>(weights[i]) > approx_bucket_fill) {
      // When this is true, the current bucket is a singleton.
      part_bounds->emplace_back(Value(vals[i]), Value(vals[i]));
      lo_idx = i + 1;
    } else if (static_cast<double>(curr_bucket_fill + weights[i]) > approx_bucket_fill) {
      // When this is true, a new should bucket should be create that does not include the current value.
      part_bounds->emplace_back(Value(vals[lo_idx]), Value(vals[i-1]));
      // Current element is part of next bucket.
      curr_bucket_fill = weights[i];
      lo_idx = i;
    } else {
      curr_bucket_fill += weights[i];
      full = false;
    }
  }

  if (part_bounds->size() == num_parts) {
    // Set last value to upper bound.
    part_bounds->back().second = stats.max_;
  } else if (curr_bucket_fill > 0) {
    // Add Last Bucket
    part_bounds->emplace_back(Value(vals[lo_idx]), stats.max_);
  }
}

#define TEMPLATED_COMPRESSED(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    TemplatedCompressed<cpp_type>(stats, val_weights, num_parts, &part_bounds_); \
    break;


CompressedHistogram::CompressedHistogram(int32_t num_parts,
                                         SqlType sql_type,
                                         const ColumnStats &stats, bool use_frequency) : Histogram(num_parts, sql_type) {
  const auto& val_weights = use_frequency ? stats.by_freq_ : stats.by_usage_;
  switch (sql_type) {
    SQL_TYPE(TEMPLATED_COMPRESSED, NOOP);
    case SqlType::Date:
      TemplatedCompressed<Date>(stats, val_weights, num_parts, &part_bounds_);
      break;
    default:
      std::cerr << "Non Numeric EquiDepth Histogram Not Yet Supported!!!!!!!!!!" << std::endl;
  }
}

}
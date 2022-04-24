#include "optimizer/histogram.h"
#include "optimizer/table_statistics.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "storage/filter.h"

namespace smartid {

template<typename T>
int64_t TemplatedPartIdx(const Value& val, const EquiDepthHistogram* hist) {
  const auto& parts = hist->GetParts();
  auto raw_val = std::get<T>(val);
  for (int64_t i = 0; i < int64_t(parts.size()); i++) {
    auto lo = std::get<T>(parts[i].first);
    auto hi = std::get<T>(parts[i].second);
    if (raw_val < lo) {
      ASSERT(i > 0, "Value out of distribution! Likely bug in code.");
      return i - 1;
    }
    if (lo <= raw_val && raw_val <= hi) {
      return i;
    }
  }
  ASSERT(false, "Value out of distribution! Likely bug in code");
}

#define TEMPLATED_PART_IDX(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    return TemplatedPartIdx<cpp_type>(val, this);

uint64_t EquiDepthHistogram::PartIdx(const Value &val) const {
  switch (val_type_) {
    SQL_TYPE(TEMPLATED_PART_IDX, NOOP);
    case SqlType::Date:
      return TemplatedPartIdx<Date>(val, this);
    default:{
      ASSERT(false, "Unsupported hist type!");
    }
  }
}

//////////////////////////////////////////////////
//// Vectorized Bit Indices
///////////////////////////////////////////////////

/**
 * Scale and offset satisfy given offset and number of bits.
 */
void ScaleAndOffset(const Bitmap* filter, Vector* out, uint64_t scale, uint64_t offset) {
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
void TemplatedBitIndices(const Bitmap* filter, const Vector *in, Vector *out, const EquiDepthHistogram* hist, uint64_t offset, uint64_t num_bits) {
  out->Resize(filter->TotalSize());
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
        out_data[i] = k == 0 ? 0 : k-1;
        return;
      }
      if (val >= typed_part_bounds[k].first && val <= typed_part_bounds[k].second) {
        out_data[i] = k;
        return;
      }
    }
    // Put in last bucket.
    out_data[i] = static_cast<int64_t>(typed_part_bounds.size()) - 1;
  });

  if (scale != 0) {
    ScaleAndOffset(filter, out, scale, offset);
  }
}

#define TEMPLATED_BIT_INDICES(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    TemplatedBitIndices<cpp_type>(filter, in, out, this, offset, num_bits);     \
    break;

void EquiDepthHistogram::BitIndices(const Bitmap* filter, const Vector *in, Vector *out, uint64_t offset, uint64_t num_bits)  const {
  switch (val_type_) {
    SQL_TYPE(TEMPLATED_BIT_INDICES, NOOP);
    case SqlType::Date:
      TemplatedBitIndices<Date>(filter, in, out, this, offset, num_bits);
      break;
    default: {
      ASSERT(false, "Non Numeric BitIndices Histogram Not Yet Supported!!!!!!!!!!");
    }
  }
}

template <typename T>
void TemplatedEquiDepth(const ColumnStats &stats, const Vector* vals_vec, const Vector* freqs_vec, uint64_t num_parts, std::vector<std::pair<Value, Value>> * part_bounds, std::vector<double> * part_cards) {
  double approx_bucket_fill = static_cast<double>(stats.count_) / num_parts;
  if (stats.count_distinct_ <= num_parts) {
    approx_bucket_fill = 1.0; // Force one item per bucket.
  }
  std::cout << "Approximate Fill Factor: " << approx_bucket_fill << std::endl;
  uint64_t curr_bucket_fill = 0;
  const auto& vals = vals_vec->DataAs<T>();
  const auto& weigths = freqs_vec->DataAs<int64_t>();
  auto lo_idx = 0;
  uint64_t curr_num_parts = 0;
  for (uint64_t i = 0; i < stats.count_distinct_ && curr_num_parts < num_parts; i++) {
    curr_bucket_fill += weigths[i];
    if (curr_bucket_fill >= approx_bucket_fill) {
      part_bounds->emplace_back(Value(vals[lo_idx]), Value(vals[i]));
      part_cards->emplace_back(curr_bucket_fill);
      curr_bucket_fill = 0;
      curr_num_parts++;
      lo_idx = i + 1;
    }
  }

  if (curr_num_parts == num_parts) {
    // Set last value to upper bound.
    part_bounds->back().second = stats.max_;
  } else if (curr_bucket_fill > 0) {
    // Add Last Bucket
    part_bounds->emplace_back(Value(vals[lo_idx]), stats.max_);
    part_cards->emplace_back(curr_bucket_fill);
  }
}

#define TEMPLATED_EQUIDEPTH(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                            \
    TemplatedEquiDepth<cpp_type>(stats, vals, freqs, num_parts, &part_bounds_, &part_cards_); \
    break;


EquiDepthHistogram::EquiDepthHistogram(uint64_t num_parts, const ColumnStats& stats, const Vector* vals, const Vector* freqs) : num_parts_(num_parts), val_type_(stats.col_type) {
  if (vals == nullptr) return;
  switch (stats.col_type) {
    SQL_TYPE(TEMPLATED_EQUIDEPTH, NOOP);
    case SqlType::Date:
      TemplatedEquiDepth<Date>(stats, vals, freqs, num_parts, &part_bounds_, &part_cards_);
      break;
    default:
      std::cerr << "Non Numeric EquiDepth Histogram Not Yet Supported!!!!!!!!!!" << std::endl;
  }
}

EquiDepthHistogram::EquiDepthHistogram(std::vector<std::pair<Value, Value>> &&part_bounds,
                                       std::vector<double> &&part_cards,
                                       SqlType val_type) : val_type_(val_type), part_bounds_(std::move(part_bounds)), part_cards_(std::move(part_cards)) {}

}
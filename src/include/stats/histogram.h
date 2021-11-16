#pragma once

#include "common/util.h"
#include "execution/vector_projection.h"

namespace smartid {

/**
 * List of histogram types.
 */
enum class HistogramType {
  EquiWidth,
  EquiDepthFreq,
  EquiDepthUsage,
  MaxDiff,
  CompressedFreq,
  CompressedUsage,
};

/**
 * Convenience class. Pair of value,weight vectors.
 * Weight can either be frequency or usage.
 */
struct ValWeights {
  const Vector* vals_;
  const Vector* weights_;
};

/**
 * Stores column statistics.
 */
struct ColumnStats {
  Value min_;
  Value max_;
  int64_t count_;
  int64_t count_distinct_;
  // <value, frequency> sorted by value.
  ValWeights by_freq_;
  // <value, usage> sorted by value.
  ValWeights by_usage_;
};

/**
 * Parent histogram class.
 */
class Histogram {
 public:
  /**
   * Constructor
   */
  Histogram(int32_t num_parts, SqlType val_type)
  : num_parts_(num_parts), val_type_(val_type) {}

  /**
   * Compute the indices of the elements in the input array. The indices are scaled and shifted accordingly.
   */
  void BitIndices(const Filter* filter, const Vector* in, Vector* out, uint64_t offset, uint64_t num_bits) const;

  /**
   * @return The partition bounds.
   */
  [[nodiscard]] const auto& GetParts() const {
    return part_bounds_;
  }

 protected:
  int32_t num_parts_;
  SqlType val_type_;
  std::vector<std::pair<Value, Value>> part_bounds_;
};

/////////////////////////////////////////////////////
/// Various histogram types.
/// See History of histograms.
////////////////////////////////////////////////////


class EquiWidthHistogram : public Histogram {
 public:
  EquiWidthHistogram(int32_t num_parts, SqlType val_type, const ColumnStats& stats);
};


class EquiDepthHistogram : public Histogram {
 public:
  EquiDepthHistogram(int32_t num_parts, SqlType sql_type, const ColumnStats& stats, bool use_frequency);
};

class MaxDiffHistogram : public Histogram {
 public:
  MaxDiffHistogram(int32_t num_parts, SqlType sql_type, const ColumnStats& stats);
};

class CompressedHistogram : public Histogram {
 public:
  CompressedHistogram(int32_t num_parts, SqlType sql_type, const ColumnStats& stats, bool use_frequency);
};

}
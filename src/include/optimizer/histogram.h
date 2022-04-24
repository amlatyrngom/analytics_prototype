#pragma once

#include "common/types.h"


namespace smartid {
class Vector;
class Bitmap;
class ColumnStats;

class EquiDepthHistogram {
 public:
  /**
   * Constructor
   */
  EquiDepthHistogram(uint64_t num_parts, const ColumnStats& stats, const Vector* vals, const Vector* freqs);
  EquiDepthHistogram(std::vector<std::pair<Value, Value>>&& part_bounds, std::vector<double> && part_cards, SqlType val_type);

  /**
   * Compute the indices of the elements in the input array. The indices are scaled and shifted accordingly.
   */
  void BitIndices(const Bitmap* filter, const Vector* in, Vector* out, uint64_t offset, uint64_t num_bits) const;

  uint64_t PartIdx(const Value& val) const;

  /**
   * @return The partition bounds.
   */
  [[nodiscard]] const auto& GetParts() const {
    return part_bounds_;
  }

  [[nodiscard]] const auto& GetPartCards() const {
    return part_cards_;
  }

 protected:
  uint64_t num_parts_; // Use to  GetParts().size() to actually get num parts. This is only used at build time.
  SqlType val_type_;
  std::vector<std::pair<Value, Value>> part_bounds_;
  std::vector<double> part_cards_;
};
}
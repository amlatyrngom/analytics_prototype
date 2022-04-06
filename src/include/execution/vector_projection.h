#pragma once
#include "storage/table.h"
#include "storage/filter.h"

namespace smartid {
/**
 * A VectorProjection is a collection of vectors return by a plan node.
 */
class VectorProjection {
 public:
  /**
   * Create a VP from a table iterator with the given filter.
   */
  VectorProjection(const TableIterator *ti, const Filter *filter) : vectors_(ti->Vectors()), filter_{filter} {}

  /**
   * Create a blank VP.
   */
  VectorProjection() = default;

  /**
   * Create a VP from with the given filter.
   */
  explicit VectorProjection(const Filter *filter) : filter_(filter) {}

  /**
   * Return the vector at the given index.
   */
  [[nodiscard]] const Vector *VectorAt(uint64_t idx) const {
    return vectors_[idx];
  }

  /**
   * Return the number of columns of the VP.
   */
  [[nodiscard]] uint64_t NumCols() const {
    return vectors_.size();
  }

  /**
   * Return the number of (active+inactive) rows in a VP.
   */
  [[nodiscard]] uint64_t NumRows() const {
    return vectors_.empty() ? 0 : vectors_[0]->NumElems();
  }

  /**
   * Return the filter associated with this vector.
   */
  [[nodiscard]] const Filter *GetFilter() const {
    return filter_;
  }

  /**
   * Add a vector to this VP.
   * Should only be done once per plan node, since the VP only stores pointers.
   */
  void AddVector(const Vector *vec) {
    vectors_.emplace_back(vec);
  }

  void SetVector(const Vector* vec, uint64_t idx) {
    vectors_[idx] = vec;
  }
 private:
  std::vector<const Vector *> vectors_;
  const Filter *filter_{nullptr};
};

}
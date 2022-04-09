#pragma once
#include "common/types.h"

namespace smartid {
class Bitmap;
class RawTableBlock;

/**
 * Represents a vector of data.
 */
class Vector {
 public:
  /**
   * Construct a Vector.
   */
  explicit Vector(SqlType type);

  /**
   * @return Raw Bytes
   */
  [[nodiscard]] const char *Data() const {
    return data_;
  }

  [[nodiscard]] char *MutableData() {
    return owned_data_.data();
  }

  template<typename T>
  [[nodiscard]] T *MutableDataAs() {
    return reinterpret_cast<T *>(MutableData());
  }

  [[nodiscard]] const Bitmap* NullBitmap() const {
    return null_bitmap_.get();
  }

  Bitmap* MutableNullBitmap() {
    return null_bitmap_.get();
  }

  /**
   * @tparam T C++ element type
   * @return Element array.
   */
  template<typename T>
  const T *DataAs() const {
    return reinterpret_cast<const T *>(Data());
  }

  void SetNumElems(uint64_t num_elems) {
    num_elems_ = num_elems;
  }

  /**
   * @return Number of elements in vector.
   */
  [[nodiscard]] uint64_t NumElems() const {
    return num_elems_;
  }

  /**
   * @return Size of elements.
   */
  [[nodiscard]] uint64_t ElemSize() const {
    return elem_size_;
  }

  /**
   * @return Size of elements.
   */
  [[nodiscard]] SqlType ElemType() const {
    return elem_type_;
  }

  /**
   * Reset vector with new elements.
   */
  void Reset(uint64_t num_elems, const char *data, const uint64_t* null_bitmap, const RawTableBlock* block=nullptr);

  void Resize(uint64_t new_size);

 private:
  std::vector<char> owned_data_;
  std::unique_ptr<Bitmap> null_bitmap_;
  // For varchars.
  std::vector<std::vector<char>> allocated_strings_;
  // Info.
  const char *data_;
  uint64_t num_elems_;
  SqlType elem_type_;
  uint64_t elem_size_;
  uint64_t settings_vec_size_;
};
}
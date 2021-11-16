#pragma once

#include <iostream>
#include "common/util.h"

namespace smartid {
/**
 * The filtering mode. The performance of each depends on selectivity and operation.
 */
enum class FilterMode {
  SelVecSelective,
  SelVecFull,
  BitmapFull,
  BitmapSelective,
};

// Predeclare
class SelVec;

// Type of selected indexes.
using sel_t = int32_t;

/**
 * A Bitmap filter.
 */
class Bitmap {
 public:
  /**
   * Constructor
   */
  explicit Bitmap(uint64_t size = 0)
      : arr_(ComputeNumWords(size)), num_bits_(size), num_words_(ComputeNumWords(size)) {}

  /**
   * Set the size and set all bits to 1.
   */
  void Reset(sel_t s);

  /**
   * Copy other bitmap into this one.
   */
  void SetFrom(const Bitmap *other);

  /**
   * Return the number active elements.
   */
  [[nodiscard]] sel_t ActiveSize() const {
    return NumOnes();
  }

  /**
   * Return the number of active+inactive elements.
   */
  [[nodiscard]] sel_t TotalSize() const {
    return num_bits_;
  }

  /**
   * Return the number of words.
   */
  [[nodiscard]] sel_t NumWords() const {
    return num_words_;
  }

  /**
   * Return the array of words.
   */
  [[nodiscard]] const uint64_t *Words() const {
    return arr_.data();
  }

  /**
   * Call a function on every set index.
   */
  template<typename F>
  inline void Map(F f) const {
    for (uint64_t i = 0; i < num_words_; i++) {
      uint64_t word = arr_[i];
      while (word != 0) {
        const auto t = word & -word;
        const auto r = __builtin_ctzl(word);
        const auto idx = static_cast<sel_t>(i * BITS_PER_WORD + r);
        f(idx);
        word ^= t;
      }
    }
  }

  /**
   * Selective compute update of Bitmap.
   */
  template<typename P>
  inline void Update(P p) {
    for (uint64_t i = 0; i < num_words_; i++) {
      uint64_t word = arr_[i];
      uint64_t word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = __builtin_ctzl(word);
        const auto idx = static_cast<sel_t>(i * BITS_PER_WORD + r);
        word_result |= static_cast<uint64_t>(p(idx)) << static_cast<uint64_t>(r);
        word ^= t;
      }
      arr_[i] &= word_result;
    }
  }

  /**
   * Full compute update of Bitmap. The function should be without side-effects.
   */
  template<typename F>
  inline void UpdateFull(F f) {
    auto num_full_words = (num_bits_ % BITS_PER_WORD == 0) ? num_words_ : num_words_ - 1;
    // This first loop processes all FULL words in the bit vector. It should be
    // fully vectorized if the predicate function can also vectorized.
    for (uint64_t i = 0; i < num_full_words; i++) {
      uint64_t word_result = 0;
      for (uint64_t j = 0; j < BITS_PER_WORD; j++) {
        auto idx = i * BITS_PER_WORD + j;
        word_result |= static_cast<uint64_t>(f(idx)) << j;
      }
      arr_[i] &= word_result;
    }

    // If the last word isn't full, process it using a scalar loop.
    if (num_full_words != num_words_) {
      auto word = arr_[num_words_ - 1];
      uint64_t word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = __builtin_ctzl(word);
        const auto idx = static_cast<sel_t>((num_words_ - 1) * BITS_PER_WORD + r);
        word_result |= static_cast<uint64_t>(f(idx)) << static_cast<uint64_t>(r);
        word ^= t;
      }
      arr_[num_words_ - 1] &= word_result;
    }
  }

  /**
   * @return The selectivity of the filter.
   */
  [[nodiscard]] double Selectivity() const {
    return static_cast<double>(NumOnes()) / num_bits_;
  }

  /**
   * Remove elements from bitmap2 out of *this* and store the result in the output variable.
   */
  void SubsetDifference(const Bitmap *bitmap2, Bitmap *out) const;

 private:
  static constexpr uint64_t BITS_PER_WORD = 64;
  static constexpr uint64_t ONES = ~static_cast<uint64_t>(0);

  [[nodiscard]] uint64_t NumOnes() const;

  static uint64_t WordIdx(uint64_t i) {
    return i / BITS_PER_WORD;
  }

  static uint64_t BitIdx(uint64_t i) {
    return i % BITS_PER_WORD;
  }

  static sel_t ComputeNumWords(sel_t s) {
    return (s + BITS_PER_WORD - 1) / BITS_PER_WORD;
  }

  std::vector<uint64_t> arr_;
  sel_t num_bits_;
  sel_t num_words_;
};

/**
 * A selection vector class.
 */
class SelVec {
 public:
  /**
   * Constructor
   */
  explicit SelVec(uint64_t size = 0) : sel_(size), size_(size) {}

  /**
   * Reset size and set all elements as active.
   */
  void Reset(sel_t s) {
    size_ = s;
    sel_.resize(size_);
    for (sel_t i = 0; i < s; i++) sel_[i] = i;
  }

  /**
   * Set selvec from another.
   */
  void SetFrom(const SelVec *other) {
    size_ = other->size_;
    sel_ = other->sel_;
  }
  /**
   * Set selvec from bitmap.
   */
  void SetFrom(const Bitmap *other);

  /**
   * Update using provided function. Allows for side-effects.
   */
  template<typename F>
  inline void Update(F f) {
    sel_t r_idx = 0;
    sel_t w_idx = 0;
    for (; r_idx < size_; r_idx++) {
      auto i = sel_[r_idx];
      bool cond = f(i);
      sel_[w_idx] = i;
      w_idx += cond;
    }
    size_ = w_idx;
  }

  /**
   * Map with provided function. Allows for side-effects.
   */
  template<typename F>
  inline void Map(F f) const {
    sel_t r_idx = 0;
    for (; r_idx < size_; r_idx++) {
      auto i = sel_[r_idx];
      f(i);
    }
  }

  /**
   * @return The selection vector.
   */
  [[nodiscard]] const std::vector<sel_t> &Sel() const {
    return sel_;
  }

  /**
   * @return The number of active elements.
   */
  [[nodiscard]] sel_t ActiveSize() const {
    return size_;
  }

  /**
   * @return The number of active+inactive elements.
   */
  [[nodiscard]] sel_t TotalSize() const {
    return sel_.size();
  }

  /**
   * @return The selectivity.
   */
  [[nodiscard]] double Selectivity() const {
    return static_cast<double>(ActiveSize()) / TotalSize();
  }

  /**
   * Perform set difference, assuming selvec2 is a subset of *this*.
   */
  void SubsetDifference(const SelVec *selvec2, SelVec *out) const;

 private:
  std::vector<sel_t> sel_;
  sel_t size_{0};
};

/**
 * Wrapper class around selection vectors and bitmaps.
 */
class Filter {
 public:
  /**
   * Constructor for filter mode.
   */
  explicit Filter(FilterMode filter_mode = FilterMode::SelVecSelective) : filter_mode_{filter_mode} {}

  /**
   * @return The mode of computation
   */
  [[nodiscard]] FilterMode Mode() const {
    return filter_mode_;
  }

  /**
   * Whether the filtering mode is for a bitmap.
   */
  static bool IsBitmap(FilterMode filter_mode) {
    return filter_mode == FilterMode::BitmapFull && filter_mode == FilterMode::BitmapSelective;
  }

  /**
   * Whether the filtering mode is for selvecs.
   */
  static bool IsSelVec(FilterMode filter_mode) {
    return filter_mode == FilterMode::SelVecFull && filter_mode == FilterMode::SelVecSelective;
  }

  /**
   * Change filtering mode.
   * Does not allow for selvec->bitmap changes (too slow).
   */
  void SwitchFilterMode(FilterMode new_mode) {
    if (new_mode == filter_mode_) return;
    if ((IsBitmap(new_mode) && IsBitmap(filter_mode_)) || (IsSelVec(new_mode) && IsSelVec(filter_mode_))) {
      filter_mode_ = new_mode;
      return;
    }
    switch (new_mode) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull: {
        filter_mode_ = new_mode;
        selvec_.SetFrom(&bitmap_);
        return;
      }
      case FilterMode::BitmapSelective:
      case FilterMode::BitmapFull: {
        std::cerr << "Conversion SelVec->Bitmap not allowed!" << std::endl;
        return;
      }
    }
  }

  /**
   * Set size, filtering mode, and mark all elements as active.
   */
  void Reset(sel_t s, FilterMode filter_mode) {
    filter_mode_ = filter_mode;
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:selvec_.Reset(s);
        return;
      case FilterMode::BitmapSelective:
      case FilterMode::BitmapFull:bitmap_.Reset(s);
        return;
    }
  }

  void SetFrom(const Filter *other) {
    filter_mode_ = other->filter_mode_;
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:selvec_.SetFrom(&other->selvec_);
        return;
      case FilterMode::BitmapSelective:
      case FilterMode::BitmapFull:bitmap_.SetFrom(&other->bitmap_);
        return;
    }
  }

  /**
   * Update for side-effect-free operations.
   */
  template<typename F>
  inline void SafeUpdate(F f) {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:selvec_.Update<F>(f);
        return;
      case FilterMode::BitmapFull:bitmap_.UpdateFull<F>(f);
        return;
      case FilterMode::BitmapSelective:bitmap_.Update<F>(f);
        return;
    }
  }

  /**
   * Update for side-effect-full operations.
   */
  template<typename F>
  inline void Update(F f) {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:selvec_.Update<F>(f);
        return;
      case FilterMode::BitmapSelective:
      case FilterMode::BitmapFull:bitmap_.Update<F>(f);
        return;
    }
  }

  /**
   * Map for side-effect-free operations.
   */
  template<typename F>
  inline void SafeMap(F f) const {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:selvec_.Map<F>(f);
        return;
      case FilterMode::BitmapSelective:bitmap_.Map<F>(f);
        return;
      case FilterMode::SelVecFull:
      case FilterMode::BitmapFull: {
        auto size = TotalSize();
        for (sel_t i = 0; i < size; i++) {
          f(i);
        }
        return;
      }
    }
  }

  /**
   * Map for side-effect-full operations.
   */
  template<typename F>
  inline void Map(F f) const {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:selvec_.Map<F>(f);
        return;
      case FilterMode::BitmapSelective:
      case FilterMode::BitmapFull:bitmap_.Map<F>(f);
        return;
    }
  }

  /**
   * @return The number of active elements.
   */
  [[nodiscard]] sel_t ActiveSize() const {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:return selvec_.ActiveSize();
      case FilterMode::BitmapFull:
      case FilterMode::BitmapSelective:return bitmap_.ActiveSize();
    }
  }

  /**
   * @return The number of active+inactive elements.
   */
  [[nodiscard]] sel_t TotalSize() const {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:return selvec_.TotalSize();
      case FilterMode::BitmapFull:
      case FilterMode::BitmapSelective:return bitmap_.TotalSize();
    }
  }

  /**
   * @return The selectivity.
   */
  [[nodiscard]] double Selectivity() const {
    switch (filter_mode_) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:return selvec_.Selectivity();
      case FilterMode::BitmapFull:
      case FilterMode::BitmapSelective:return bitmap_.Selectivity();
    }
  };

  /**
   * @return The underlying selvec.
   */
  [[nodiscard]] const SelVec *GetSelVec() const {
    return &selvec_;
  }

  /**
   * @return The underlying bitmap.
   */
  [[nodiscard]] const Bitmap *GetBitmap() const {
    return &bitmap_;
  }

  /**
   * Perform set difference assuming filter2 assuming it is a subset of *this*.
   */
  void SubsetDifference(const Filter *filter2, Filter *out) const {
    // Check types.
    if (filter2->Mode() != Mode()) {
      std::cerr << "Incompatible types for subset difference!" << std::endl;
      return;
    }
    // Set output mode.
    out->filter_mode_ = Mode();
    switch (Mode()) {
      case FilterMode::SelVecSelective:
      case FilterMode::SelVecFull:return selvec_.SubsetDifference(&filter2->selvec_, &out->selvec_);
      case FilterMode::BitmapFull:
      case FilterMode::BitmapSelective:return bitmap_.SubsetDifference(&filter2->bitmap_, &out->bitmap_);
    }
  }

 private:
  SelVec selvec_{};
  Bitmap bitmap_{};
  FilterMode filter_mode_;
};
}
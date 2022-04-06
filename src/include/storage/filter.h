#pragma once

#include <iostream>
#include "common/util.h"

namespace smartid {
// Type of selected indexes.
using sel_t = uint64_t;


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
  void Reset(sel_t s, const uint64_t* words);

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

  static uint64_t AllocSize(uint64_t num_bits) {
    return ComputeNumWords(num_bits) * sizeof(uint64_t);
  }

  template<typename F>
  static inline void Map(const uint64_t* words, uint64_t num_bits, F f) {
    auto num_words = ComputeNumWords(num_bits);
    for (uint64_t i = 0; i < num_words; i++) {
      uint64_t word = words[i];
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
   * Call a function on every set index.
   */
  template<typename F>
  inline void Map(F f) const {
    Bitmap::Map(Words(), num_words_, f);
  }

  template<typename P>
  static inline void Update(uint64_t* words, uint64_t num_bits, P p) {
    auto num_words = ComputeNumWords(num_bits);
    for (uint64_t i = 0; i < num_words; i++) {
      uint64_t word = words[i];
      uint64_t word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = __builtin_ctzl(word);
        const auto idx = static_cast<sel_t>(i * BITS_PER_WORD + r);
        word_result |= static_cast<uint64_t>(p(idx)) << static_cast<uint64_t>(r);
        word ^= t;
      }
      words[i] &= word_result;
    }
  }

  template<typename F>
  static inline void UpdateFull(F f, uint64_t* words, uint64_t num_bits) {
    auto num_words = ComputeNumWords(num_bits);
    auto num_full_words = (num_bits % BITS_PER_WORD == 0) ? num_words : num_words - 1;
    // This first loop processes all FULL words in the bit vector. It should be
    // fully vectorized if the predicate function can also vectorized.
    for (uint64_t i = 0; i < num_full_words; i++) {
      uint64_t word_result = 0;
      for (uint64_t j = 0; j < BITS_PER_WORD; j++) {
        auto idx = i * BITS_PER_WORD + j;
        word_result |= static_cast<uint64_t>(f(idx)) << j;
      }
      words[i] &= word_result;
    }

    // If the last word isn't full, process it using a scalar loop.
    if (num_full_words != num_words) {
      auto word = words[num_words - 1];
      uint64_t word_result = 0;
      while (word != 0) {
        const auto t = word & -word;
        const auto r = __builtin_ctzl(word);
        const auto idx = static_cast<sel_t>((num_words - 1) * BITS_PER_WORD + r);
        word_result |= static_cast<uint64_t>(f(idx)) << static_cast<uint64_t>(r);
        word ^= t;
      }
      words[num_words - 1] &= word_result;
    }
  }


  /**
   * Selective compute update of Bitmap.
   */
  template<typename P>
  inline void Update(P p) {
    Bitmap::Update(arr_.data(), num_bits_, p);
  }

  /**
   * Full compute update of Bitmap. The function should be without side-effects.
   */
  template<typename F>
  inline void UpdateFull(F f) {
    Bitmap::UpdateFull(arr_.data(), num_bits_, f);
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

  static void Intersect(const uint64_t* b1, const uint64_t* b2, uint64_t size, uint64_t* out) {
    // All inputs must have same size
    for (uint64_t i = 0; i < size; i++) {
      out[i] = b1[i] & b2[i];
    }
  }

  static void SetBit(uint64_t* b, uint64_t idx) {
    b[WordIdx(idx)] |= (1ull << BitIdx(idx));
  }

  static bool IsSet(const uint64_t* b, uint64_t idx) {
    return (b[WordIdx(idx)] >> BitIdx(idx)) & 1;
  }

  static uint64_t NumOnes(const uint64_t* b, uint64_t num_bits);

  [[nodiscard]] uint64_t NumOnes() const {
    return NumOnes(arr_.data(), num_bits_);
  }

  static bool HasUnset(const uint64_t* b, uint64_t num_bits) {
    return NumOnes(b, num_bits) < num_bits;
  }

  static uint64_t SetAndReturnNext(uint64_t*b, uint64_t num_bits);

 private:
  static constexpr uint64_t BITS_PER_WORD = 64;
  static constexpr uint64_t ONES = ~static_cast<uint64_t>(0);


  static uint64_t WordIdx(uint64_t i) {
    return i / BITS_PER_WORD;
  }

  static uint64_t BitIdx(uint64_t i) {
    return i % BITS_PER_WORD;
  }

  static uint64_t ComputeNumWords(uint64_t s) {
    return (s + BITS_PER_WORD - 1) / BITS_PER_WORD;
  }

  std::vector<uint64_t> arr_;
  sel_t num_bits_;
  sel_t num_words_;
};
}
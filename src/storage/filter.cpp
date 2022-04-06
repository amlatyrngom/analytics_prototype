#include "storage/filter.h"

namespace smartid {
void Bitmap::Reset(sel_t s) {
  // Reset Size
  num_bits_ = s;
  num_words_ = ComputeNumWords(s);
  // Reset Ones
  arr_.resize(num_words_);
  for (sel_t i = 0; i < num_words_; i++) {
    arr_[i] = ONES;
  }
  // Zero last bits
  const uint32_t extra_bits = num_bits_ % BITS_PER_WORD;
  if (extra_bits != 0) {
    arr_[num_words_ - 1] &= ~(ONES << extra_bits);
  }
}

void Bitmap::Reset(sel_t s, const uint64_t* data) {
  // Reset Size.
  num_bits_ = s;
  num_words_ = ComputeNumWords(s);
  arr_.resize(num_words_);

  // Set full words.
  for (sel_t i = 0; i < num_words_; i++) {
    arr_[i] = data[i];
  }

  // Zero last bits
  const uint32_t extra_bits = num_bits_ % BITS_PER_WORD;
  if (extra_bits != 0) {
    arr_[num_words_ - 1] &= ~(ONES << extra_bits);
  }
}

void Bitmap::SetFrom(const Bitmap *other) {
  num_bits_ = other->num_bits_;
  num_words_ = other->num_words_;
  arr_ = other->arr_;
}

uint64_t Bitmap::NumOnes(const uint64_t* b, uint64_t num_bits) {
  uint64_t num_ones = 0;
  uint64_t num_words = ComputeNumWords(num_bits);
  for (uint64_t i = 0; i < num_words; i++) {
    num_ones += __builtin_popcountll(b[i]);
  }
  return num_ones;
}

uint64_t Bitmap::SetAndReturnNext(uint64_t *b, uint64_t num_bits) {
  uint64_t bit_idx;
  uint64_t num_words = ComputeNumWords(num_bits);
  for (uint64_t i = 0; i < num_words; i++) {
    auto num_ones = __builtin_popcountll(b[i]);
    if (num_ones < BITS_PER_WORD) {
      // Count trailing ones.
      auto reversed = ~b[i];
      auto next_unset = __builtin_ctzl(reversed);
      SetBit(b, next_unset);
      return next_unset;
    }
  }
  ASSERT(false, "Unreachable");
}

void Bitmap::SubsetDifference(const Bitmap *bitmap2, Bitmap *out) const {
  for (sel_t i = 0; i < num_words_; i++) {
    out->arr_[i] = arr_[i] & (~bitmap2->arr_[i]);
  }
}

}

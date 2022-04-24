#include "storage/filter.h"

namespace smartid {
void Bitmap::ResetAll(sel_t s, uint64_t *b) {
  auto num_words = ComputeNumWords(s);
  for (sel_t i = 0; i < num_words; i++) {
    b[i] = ONES;
  }
  // Zero last bits
  const uint32_t extra_bits = s % BITS_PER_WORD;
  if (extra_bits != 0) {
    b[num_words - 1] &= ~(ONES << extra_bits);
  }
}

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
  words_ = arr_.data();
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
  words_ = arr_.data();
}

void Bitmap::ShallowReset(sel_t s, const uint64_t* words) {
  arr_.clear();
  num_bits_ = s;
  num_words_ = ComputeNumWords(s);
  words_ = words;
}

void Bitmap::SetFrom(const Bitmap *other) {
  num_bits_ = other->num_bits_;
  num_words_ = other->num_words_;
  arr_.resize(num_words_);
  std::memcpy(arr_.data(), other->words_, num_words_ * sizeof(uint64_t));
  words_ = arr_.data();
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
    out->arr_[i] = words_[i] & (~bitmap2->words_[i]);
  }
}

void Bitmap::Intersect(const uint64_t *b1, const uint64_t *b2, uint64_t size, uint64_t *out) {
  // All inputs must have same size
  auto num_words = ComputeNumWords(size);
  for (uint64_t i = 0; i < num_words; i++) {
    out[i] = b1[i] & b2[i];
  }
}

void Bitmap::Union(const Bitmap *bitmap2) {
  auto num_words = ComputeNumWords(TotalSize());
  uint64_t *out = MutableWords();
  const uint64_t* in = bitmap2->Words();
  for (uint64_t i = 0; i < num_words; i++) {
    out[i] = out[i] | in[i];
  }
}

}

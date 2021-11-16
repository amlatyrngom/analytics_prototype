#include "execution/filter.h"

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

void Bitmap::SetFrom(const Bitmap *other) {
  num_bits_ = other->num_bits_;
  num_words_ = other->num_words_;
  arr_ = other->arr_;
}

uint64_t Bitmap::NumOnes() const {
  uint64_t num_ones = 0;
  for (const auto &w: arr_) {
    num_ones += __builtin_popcountll(w);
  }
  return num_ones;
}

void Bitmap::SubsetDifference(const Bitmap *bitmap2, Bitmap *out) const {
  for (sel_t i = 0; i < num_words_; i++) {
    out->arr_[i] = arr_[i] & (~bitmap2->arr_[i]);
  }
}

void SelVec::SetFrom(const Bitmap *other) {
  auto num_words = other->NumWords();
  auto words = other->Words();
  sel_t k = 0;
  sel_.resize(other->TotalSize());
  size_ = other->ActiveSize();
  for (sel_t i = 0; i < num_words; i++) {
    uint64_t word = words[i];
    while (word != 0) {
      const uint64_t t = word & -word;
      const uint32_t r = __builtin_ctzl(word);
      sel_[k++] = i * 64 + r;
      word ^= t;
    }
  }
}

void SelVec::SubsetDifference(const SelVec *selvec2, SelVec *out) const {
  // LHS
  auto arr1 = sel_.data();
  auto s1 = size_;
  // RHS
  auto arr2 = selvec2->sel_.data();
  auto s2 = selvec2->size_;
  // Output
  out->sel_.resize(s1);
  auto out_arr = out->sel_.data();
  // Loop variables
  sel_t i1 = 0;
  sel_t i2 = 0;
  sel_t w_idx = 0;
  // SIMD Loop.
  while (i1 + 7 < s1 && i2 < s2) {
    auto elem1 = arr1[i1];
    auto elem2 = arr2[i2];
    if (elem1 == elem2) {
      i2++;
      i1++;
    } else {
      auto end1 = arr1[i1 + 7];
      if (end1 < elem2) {
        auto elems = _mm256_maskload_epi32(arr1 + i1, MASK_ALL_256);
        _mm256_maskstore_epi32(out_arr + w_idx, MASK_ALL_256, elems);
        w_idx += 8;
        i1 += 8;
        continue;
      }
      end1 = arr1[i1 + 3];
      if (end1 < elem2) {
        auto elems = _mm_maskload_epi32(arr1 + i1, MASK_ALL_128);
        _mm_maskstore_epi32(out_arr + w_idx, MASK_ALL_128, elems);
        w_idx += 4;
        i1 += 4;
        continue;
      }
      out_arr[w_idx] = elem1;
      w_idx++;
      i1++;
    }
  }
  // Scalar Loop.
  while (i1 < s1 && i2 < s2) {
    auto elem1 = arr1[i1];
    auto elem2 = arr2[i2];
    if (elem1 == elem2) {
      i2++;
      i1++;
    } else {
      out_arr[w_idx] = elem1;
      w_idx++;
      i1++;
    }
  }
  // Copy last few items
  if (i2 == s2 && i1 < s1) {
    std::memmove(out_arr + w_idx, arr1 + i1, (s1 - i1) * sizeof(sel_t));
    w_idx += s1 - i1;
  }
  out->size_ = w_idx;
}
}

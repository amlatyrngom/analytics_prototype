#pragma once
#include "common/types.h"
#include <system_info.h>
#include <immintrin.h>
#include <cstring>

namespace smartid {

template<class Tp>
inline void DoNotOptimize(Tp const &value) {
  asm volatile("" : : "r,m"(value) : "memory");
}

template<class Tp>
inline void DoNotOptimize(Tp &value) {
#if defined(__clang__)
  asm volatile("" : "+r,m"(value) : : "memory");
#else
  asm volatile("" : "+m,r"(value) : : "memory");
#endif
}

static constexpr uint64_t K_MURMUR2_PRIME = 0xc6a4a7935bd1e995;
static constexpr int32_t K_MURMUR2_R = 47;

template <typename T>
static auto ArithHashMurmur2(const T val, uint64_t seed) -> std::enable_if_t<std::is_arithmetic_v<T>, uint64_t> {
  // MurmurHash64A
  auto k = static_cast<uint64_t>(val);
  uint64_t h = seed ^ 0x8445d61a4e774912 ^ (8 * K_MURMUR2_PRIME);
  k *= K_MURMUR2_PRIME;
  k ^= k >> K_MURMUR2_R;
  k *= K_MURMUR2_PRIME;
  h ^= k;
  h *= K_MURMUR2_PRIME;
  h ^= h >> K_MURMUR2_R;
  h *= K_MURMUR2_PRIME;
  h ^= h >> K_MURMUR2_R;
  return h;
}



#define NOOP(...)
const __m256i MASK_ALL_256 = _mm256_set1_epi32(int(0xFFFFFFFF));
const __m128i MASK_ALL_128 = _mm_set1_epi32(int(0xFFFFFFFF));
constexpr double EMA_ALPHA = 0.2;
constexpr uint64_t STATS_HORIZON = 10;
constexpr uint64_t CACHE_SIZE = CONFIG_CACHE_SIZE;
constexpr uint64_t CACHE_FACTOR = CONFIG_CACHE_FACTOR;

#ifndef CONFIG_SCALE_FACTOR
#define CONFIG_SCALE_FACTOR 0.001
#endif
constexpr double SCALE_FACTOR = CONFIG_SCALE_FACTOR;


// Assertion. From stack overflow.
#ifndef NDEBUG
#   define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::terminate(); \
        } \
    } while (false)
#else
#   define ASSERT(condition, message) do { } while (false)
#endif
}
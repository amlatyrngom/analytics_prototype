#include "execution/vector_ops.h"
#include "smhasher/src/MurmurHash2.h"
#include "smhasher/src/City.h"
#include "storage/filter.h"
#include "storage/vector.h"

namespace smartid {
//////////////////////////////////////////////////////
//// Hashing
////////////////////////////////////////////////////////

template <typename T, typename FN>
void DoMap(FN fn, const Bitmap* filter) {
  if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, Date>) {
    filter->SafeMap(fn);
  } else {
    filter->Map(fn);
  }
}

template<typename in_cpp_type>
void TemplatedHashVector(const Vector *in, const Bitmap *filter, Vector *out) {
  out->Resize(in->NumElems());
  auto in_data = in->DataAs<in_cpp_type>();
  auto out_data = out->MutableDataAs<uint64_t>();
  auto hash_fn = [&](sel_t i) {
    if constexpr (std::is_same_v<in_cpp_type, Varlen>) {
      const auto &varlen = in_data[i];
      out_data[i] = CityHash64(varlen.Data(), varlen.Info().NormalSize());
    } else if constexpr (std::is_arithmetic_v<in_cpp_type> && std::is_same_v<int64_t, in_cpp_type>){
      out_data[i] = ArithHashMurmur2(in_data[i] & 0xFFFFFFFFFFFF, 0); // Hack.
    } else if constexpr (std::is_arithmetic_v<in_cpp_type> && std::is_same_v<int32_t, in_cpp_type>){
      out_data[i] = ArithHashMurmur2(in_data[i] & 0xFFFFFF, 0); // Hack.
    } else if constexpr (std::is_arithmetic_v<in_cpp_type>) {
      out_data[i] = ArithHashMurmur2(in_data[i], 0);
    } else {
      out_data[i] = MurmurHash64A(&in_data[i], sizeof(in_cpp_type), 0);
    }
  };
  DoMap<in_cpp_type>(hash_fn, filter);
}

#define TEMPLATED_HASH_VECTOR(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type: {                          \
            TemplatedHashVector<in_cpp_type>(in, filter, out);       \
            return; \
        }

void VectorOps::HashVector(const Vector *in, const Bitmap *filter, Vector *out) {
  // TODO: Implement null hashing.
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_HASH_VECTOR, TEMPLATED_HASH_VECTOR)
  }
}


//////////////////////////////////////////////////////
//// Hashing Combine
////////////////////////////////////////////////////////

template<typename in_cpp_type>
__attribute__((noinline))
void TemplatedHashCombineVector(const Vector *in, const Bitmap *filter, Vector *out) {
  out->Resize(in->NumElems());
  auto in_data = in->DataAs<in_cpp_type>();
  auto out_data = out->MutableDataAs<uint64_t>();
  auto hash_combine_fn = [&](sel_t i) {
    if constexpr (std::is_same_v<in_cpp_type, Varlen>) {
      const auto &varlen = in_data[i];
      out_data[i] = CityHash64WithSeed(varlen.Data(), varlen.Info().NormalSize(), out_data[i]);
    } else if constexpr (std::is_arithmetic_v<in_cpp_type>){
      out_data[i] = ArithHashMurmur2(in_data[i], out_data[i]); // TODO: support with embeddings.
    } else {
      out_data[i] = MurmurHash64A(&in_data[i], sizeof(in_cpp_type), out_data[i]);
    }
  };
  // Full compute only on arithmetic types.
  DoMap<in_cpp_type>(hash_combine_fn, filter);
}

#define TEMPLATED_HASH_COMBINE_VECTOR(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type: {                          \
            TemplatedHashCombineVector<in_cpp_type>(in, filter, out);       \
            return;                                                  \
        }

void VectorOps::HashCombineVector(const Vector *in, const Bitmap *filter, Vector *out) {
  // TODO: Implement null hashing.
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_HASH_COMBINE_VECTOR, TEMPLATED_HASH_COMBINE_VECTOR)
  }
}
}
#include "execution/vector_ops.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "storage/filter.h"

namespace smartid {
/////////////////////////////////////////////////
//// Scatter
////////////////////////////////////////////////

template<typename in_cpp_type, typename out_cpp_type>
__attribute__((noinline))
void TemplatedScatterVector(const Vector *in, const Bitmap *filter, uint64_t val_offset, Vector *out) {
  out->Resize(in->NumElems());
  auto in_data = in->DataAs<in_cpp_type>();
  auto out_data = out->MutableDataAs<char *>();
  auto scatter_fn = [&](sel_t i) {
    if constexpr (std::is_same_v<in_cpp_type, out_cpp_type>) {
      *reinterpret_cast<out_cpp_type *>(out_data[i] + val_offset) = in_data[i];
    } else if constexpr (std::is_arithmetic_v<in_cpp_type> && std::is_arithmetic_v<out_cpp_type>) {
      *reinterpret_cast<out_cpp_type *>(out_data[i] + val_offset) = static_cast<out_cpp_type>(in_data[i]);
    } else {
      std::cout << "BAD CASTING" << std::endl;
    }
  };
  filter->Map(scatter_fn);
}

#define TEMPLATED_SCATTER(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type: {                     \
            TemplatedScatterVector<in_cpp_type, out_cpp_type>(in, filter, val_offset, out); \
            break;\
        }

template<typename out_cpp_type>
void ScatterByOutType(const Vector *in, const Bitmap *filter, uint64_t val_offset, Vector *out) {
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_SCATTER, TEMPLATED_SCATTER)
  }
}

#define SCATTER_BY_OUT_TYPE(out_sql_type, out_cpp_type, ...) \
        case SqlType::out_sql_type: {                         \
            ScatterByOutType<out_cpp_type>(in, filter, val_offset, out);                 \
            break; \
        }

void VectorOps::ScatterVector(const Vector *in,
                              const Bitmap *filter,
                              SqlType out_type,
                              uint64_t val_offset,
                              Vector *out) {
  switch (out_type) {
    SQL_TYPE(SCATTER_BY_OUT_TYPE, SCATTER_BY_OUT_TYPE)
  }
}


///////////////////////////////////////////////
//// Gather and Compare
///////////////////////////////////////////////

/**
 * Gathers values from in1 and compares them to values in in2 according to the types.
 */
template<typename cpp_type1, typename cpp_type2>
__attribute__((noinline))
void TemplatedGatherCompareVector(const Vector *in1, const Vector *in2, Bitmap *filter, uint64_t val_offset) {
  auto in_data1 = in1->DataAs<char *>();
  auto in_data2 = in2->DataAs<cpp_type2>();
  auto gather_compare_fn = [&](sel_t i) {
    if constexpr (std::is_same_v<cpp_type1, int64_t> && std::is_same_v<cpp_type1, cpp_type2>) {
      // Hack for smartids.
      return (in_data2[i] & Settings::KEY_MASK64) == ((*reinterpret_cast<const cpp_type1 *>(in_data1[i] + val_offset)) & Settings::KEY_MASK64);
    }else if constexpr (std::is_same_v<cpp_type1, int32_t> && std::is_same_v<cpp_type1, cpp_type2>) {
      // Hack for smartids.
      return (in_data2[i] & Settings::KEY_MASK32) == ((*reinterpret_cast<const cpp_type1 *>(in_data1[i] + val_offset)) & Settings::KEY_MASK32);
    } else if constexpr (std::is_same_v<cpp_type1, cpp_type2>) {
      return in_data2[i] == *reinterpret_cast<const cpp_type1 *>(in_data1[i] + val_offset);
    } else if constexpr (std::is_arithmetic_v<cpp_type1> && std::is_arithmetic_v<cpp_type2>) {
      return in_data2[i] == *reinterpret_cast<const cpp_type1 *>(in_data1[i] + val_offset);
    } else {
      ASSERT(false, "Bad casting");
      return false;
    }
  };
  filter->Update(gather_compare_fn);
}

#define TEMPLATED_GATHER_COMPARE(sql_type2, cpp_type2, ...) \
        case SqlType::sql_type2: {                     \
            TemplatedGatherCompareVector<cpp_type1, cpp_type2>(in1, in2, filter, val_offset); \
            break;\
        }

/**
 * Intermediate templated step.
 */
template<typename cpp_type1>
void GatherCompareByInType(const Vector *in1, const Vector *in2, Bitmap *filter, uint64_t val_offset) {
  auto vec_type2 = in2->ElemType();
  switch (vec_type2) {
    SQL_TYPE(TEMPLATED_GATHER_COMPARE, TEMPLATED_GATHER_COMPARE)
  }
}

#define GATHER_COMPARE_BY_IN_TYPE(sql_type1, cpp_type1, ...) \
        case SqlType::sql_type1: {                         \
            GatherCompareByInType<cpp_type1>(in1, in2, filter, val_offset);                 \
            break; \
        }

void VectorOps::GatherCompareVector(const Vector *in1, const Vector *in2, Bitmap *filter, SqlType in_type,
                                    uint64_t val_offset) {
  switch (in_type) {
    SQL_TYPE(GATHER_COMPARE_BY_IN_TYPE, GATHER_COMPARE_BY_IN_TYPE)
  }
}


/////////////////////////////////////////////////
//// Scatter Scalar
////////////////////////////////////////////////

template<typename cpp_type>
__attribute__((noinline))
void TemplatedScatterScalar(const Vector *in, sel_t i, char *out, uint64_t val_offset) {
  auto in_data = in->DataAs<cpp_type>();
  auto curr_val = reinterpret_cast<cpp_type *>(out + val_offset);
  *curr_val = in_data[i];
}

#define TEMPLATED_SCATTER_SCALAR(sql_type, cpp_type, ...) \
        case SqlType::sql_type: {                         \
            TemplatedScatterScalar<cpp_type>(in, i, out, val_offset);                 \
            break; \
        }

void VectorOps::ScatterScalar(const Vector *in, sel_t i, char *out, uint64_t val_offset) {
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_SCATTER_SCALAR, TEMPLATED_SCATTER_SCALAR)
  }
}


////////////////////////////////////////////////////////////////////
/// Test Set Mark
/////////////////////////////////////////////////////////////////

void VectorOps::TestSetMarkVector(Bitmap *filter, uint64_t val_offset, Vector *out) {
  auto out_data = out->MutableDataAs<char*>();
  filter->Update([&](sel_t i) {
    auto val_ptr = out_data[i] + val_offset;
    if (*val_ptr == 0) {
      *val_ptr = 1;
      return true;
    }
    return false;
  });
}
}
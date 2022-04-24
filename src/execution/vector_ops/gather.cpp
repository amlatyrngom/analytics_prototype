#include "execution/vector_ops.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "storage/filter.h"

namespace smartid {
//////////////////////////////////
/// Gather
//////////////////////////////////


/**
 * Perform the gather according to the input and output types.
 */
template<typename in_cpp_type, typename out_cpp_type>
__attribute__((noinline))
void TemplatedGatherVector(const Vector *in, const Bitmap *filter, uint64_t val_offset, Vector *out) {
  out->Resize(in->NumElems());
  auto in_data = in->DataAs<char *>();
  auto out_data = out->MutableDataAs<out_cpp_type>();
  auto gather_fn = [&](sel_t i) {
    if constexpr (std::is_same_v<in_cpp_type, out_cpp_type>) {
      out_data[i] = *reinterpret_cast<const in_cpp_type *>(in_data[i] + val_offset);
    } else if constexpr (std::is_arithmetic_v<in_cpp_type> && std::is_arithmetic_v<out_cpp_type>) {
      out_data[i] = static_cast<out_cpp_type>(*reinterpret_cast<const in_cpp_type *>(in_data[i] + val_offset));
    } else {
      ASSERT(false, "Bad Casting!");
    }
  };
  filter->Map(gather_fn);
}

#define TEMPLATED_GATHER(out_sql_type, out_cpp_type, ...) \
        case SqlType::out_sql_type: {                     \
            TemplatedGatherVector<in_cpp_type, out_cpp_type>(in, filter, val_offset, out); \
            break;\
        }

/**
 * Intermediary Helper for the gather function. The in_cpp_type param is the raw type of the input value.
 */
template<typename in_cpp_type>
void GatherByInType(const Vector *in, const Bitmap *filter, uint64_t val_offset, Vector *out) {
  auto out_vec_type = out->ElemType();
  switch (out_vec_type) {
    SQL_TYPE(TEMPLATED_GATHER, TEMPLATED_GATHER)
  }
}

#define GATHER_BY_IN_TYPE(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type: {                         \
            GatherByInType<in_cpp_type>(in, filter, val_offset, out);                 \
            break; \
        }

void VectorOps::GatherVector(const Vector *in,
                             const Bitmap *filter,
                             SqlType in_type,
                             uint64_t val_offset,
                             Vector *out) {
  switch (in_type) {
    SQL_TYPE(GATHER_BY_IN_TYPE, GATHER_BY_IN_TYPE)
  }
}
}
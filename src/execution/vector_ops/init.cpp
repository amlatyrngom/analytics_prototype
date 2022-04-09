#include "execution/vector_ops.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"

namespace smartid {

template <typename cpp_type>
void TemplatedInitVector(const Bitmap* filter, const Value& val, Vector* vec) {
  auto cpp_val = std::get<cpp_type>(val);
  auto out_data = vec->MutableDataAs<cpp_type>();
  auto init_fn = [&](sel_t i) {
    out_data[i] = cpp_val;
  };
  filter->SafeMap(init_fn);
}


#define TEMPLATED_INIT_VECTOR(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                              \
    TemplatedInitVector<cpp_type>(filter, val, out); \
    break;


void VectorOps::InitVector(const Bitmap* filter, const Value &val, SqlType val_type, Vector *out) {
  switch (val_type) {
    SQL_TYPE(TEMPLATED_INIT_VECTOR, TEMPLATED_INIT_VECTOR)
  }
}
}
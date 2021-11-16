#include "execution/vector_ops.h"

namespace smartid {

template <typename cpp_type>
void TemplatedCopyVector(const Filter* filter, const Vector* in, char* out) {
  auto in_data = in->DataAs<cpp_type>();
  auto out_data = reinterpret_cast<cpp_type*>(out);
  sel_t w_idx = 0;
  auto copy_fn = [&](sel_t i) {
    out_data[w_idx] = in_data[i];
    w_idx++;
  };
  filter->Map(copy_fn);
}

#define TEMPLATED_COPY(sql_type, cpp_type, ...) \
  case SqlType::sql_type: \
    TemplatedCopyVector<cpp_type>(filter, in, out); \
    break;


void VectorOps::CopyVector(const Filter *filter, const Vector *in, char *out) {
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_COPY, TEMPLATED_COPY)
  }
}
}
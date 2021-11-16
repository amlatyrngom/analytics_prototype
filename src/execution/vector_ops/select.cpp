#include "execution/vector_ops.h"

namespace smartid {
/////////////////////////////////////////////////
//// Select
////////////////////////////////////////////////
template<typename cpp_type>
__attribute__((noinline))
void TemplatedSelectVector(const Vector *in,
                           const std::vector<uint64_t> &indices,
                           Vector *out) {
  out->Resize(indices.size());
  auto in_data = in->DataAs<cpp_type>();
  auto out_data = out->MutableDataAs<cpp_type>();
  for (uint64_t i = 0; i < indices.size(); i++) {
    out_data[i] = in_data[indices[i]];
  }
}

#define SELECT_BY_TYPE(sql_type, cpp_type, ...) \
        case SqlType::sql_type: {               \
          TemplatedSelectVector<cpp_type>(in, indices, out); \
          break;\
        }

void VectorOps::SelectVector(const Vector *in,
                             const std::vector<uint64_t> &indices,
                             Vector *out) {
  switch (in->ElemType()) {
    SQL_TYPE(SELECT_BY_TYPE, SELECT_BY_TYPE)
  }
}

}
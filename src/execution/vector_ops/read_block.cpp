#include "execution/vector_ops.h"

namespace smartid {


template<typename T>
void ReadBlockByType(const Block *block, const std::vector<std::pair<uint64_t, uint64_t>>& row_idxs, uint64_t col_idx, Vector *out) {
  for (const auto& row_out_idx: row_idxs) {
    auto row_idx = row_out_idx.first;
    auto out_idx = row_out_idx.first;
    out->MutableDataAs<T>()[out_idx] = block->ColumnDataAs<T>(col_idx)[row_idx];
  }
}

#define REAB_BLOCK_BY_TYPE(sql_type, cpp_type, ...) \
        case SqlType::sql_type: {                         \
            ReadBlockByType<cpp_type>(block, row_idxs, col_idx, out);                 \
            break; \
        }

void VectorOps::ReadBlock(const Block *block, const std::vector<std::pair<uint64_t, uint64_t>>& row_idxs, uint64_t col_idx, Vector *out) {
  switch(out->ElemType()) {
    SQL_TYPE(REAB_BLOCK_BY_TYPE, REAB_BLOCK_BY_TYPE)
  }
}
}
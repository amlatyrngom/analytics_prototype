#include "execution/executors/build_rowid_index_executor.h"

namespace smartid {

const VectorProjection *BuildRowIDIndexExecutor::Next() {
  auto child = Child(0);
  const VectorProjection *vp;
  auto index_table = node_->IndexTable();
  while ((vp = child->Next())) {
    auto filter = vp->GetFilter();
    auto key_col = vp->VectorAt(node_->KeyIdx())->DataAs<uint64_t>();
    auto val_col = vp->VectorAt(node_->ValIdx())->DataAs<uint64_t>();
    filter->Map([&](sel_t i) {
      auto key = key_col[i];
      auto val = val_col[i];
      (*index_table)[key].emplace_back(val);
    });
  }
  return nullptr;
}

}
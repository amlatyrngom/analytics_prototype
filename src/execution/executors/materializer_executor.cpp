#include "execution/executors/materializer_executor.h"

namespace smartid {

namespace {
void ResizeBatch(const Schema& schema, std::vector<std::vector<char>> *cols) {
  cols->resize(schema.NumCols());
  for (uint64_t i = 0; i < schema.NumCols(); i++) {
    auto elem_size = TypeUtil::TypeSize(schema.GetColumn(i).Type());
    (*cols)[i].resize(BLOCK_SIZE * elem_size);
  }
}
}

const VectorProjection * MaterializerExecutor::Next() {
  if (done_) return nullptr;
  done_ = true;
  std::vector<std::vector<char>> cols;
  bool first{true};
  const VectorProjection* vp;

  uint64_t curr_batch = 0;
  const auto& schema = node_->GetTable()->GetSchema();
  while ((vp = Child(0)->Next()) != nullptr) {
    if (first) {
      first = false;
      ResizeBatch(schema, &cols);
    }
    if (curr_batch + vp->GetFilter()->ActiveSize() > BLOCK_SIZE) {
      // Insert current batch in table.
      node_->GetTable()->InsertBlock(Block{std::move(cols), curr_batch});
      // Reset batch.
      curr_batch = 0;
      cols.clear();
      ResizeBatch(schema, &cols);
    }
    for (uint64_t i = 0; i < vp->NumCols(); i++) {
      VectorOps::CopyVector(vp->GetFilter(), vp->VectorAt(i), cols[i].data() + curr_batch * vp->VectorAt(i)->ElemSize());
    }
    curr_batch += vp->GetFilter()->ActiveSize();
  }
  if (curr_batch > 0) {
    node_->GetTable()->InsertBlock(Block{std::move(cols), curr_batch});
  }
  return nullptr;
}
}

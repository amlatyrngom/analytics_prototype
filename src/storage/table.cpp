#include "storage/table.h"

namespace smartid {
BlockIterator::BlockIterator(const Table *table, const Block *block)
    : table_(table), block_(block), vecs_(block->NumCols()) {
  for (uint64_t i = 0; i < block->NumCols(); i++) {
    auto schema_col = table->GetSchema().GetColumn(i);
    vecs_[i] = std::make_unique<Vector>(schema_col.Type());
  }
}

bool BlockIterator::Advance() {
  if (curr_idx_ >= block_->NumElems()) {
    return false;
  }
  auto read_size = std::min(VEC_SIZE, block_->NumElems() - curr_idx_);
  for (uint64_t i = 0; i < block_->NumCols(); i++) {
    auto read_data = block_->ColumnData(i) + curr_idx_ * vecs_[i]->ElemSize();
    vecs_[i]->ShallowReset(read_size, read_data);
  }
  curr_idx_ += read_size;
  return true;
}

void Block::Free(uint64_t col_idx) {
  auto varlens = reinterpret_cast<Varlen *>(data_[col_idx].data());
  for (uint64_t i = 0; i < num_elems_; i++) varlens[i].Free();
}

Table::~Table() {
  const auto &schema = GetSchema();
  std::vector<uint64_t> varchar_cols{};
  for (uint64_t i = 0; i < schema.NumCols(); i++) {
    const auto &col = schema.GetColumn(i);
    if (col.Type() == SqlType::Varchar && col.OwnsVarchar()) varchar_cols.emplace_back(i);
  }
  if (varchar_cols.empty()) return;

  for (auto &block: blocks_) {
    for (auto col_idx: varchar_cols) {
      block.Free(col_idx);
    }
  }
}

std::vector<const Vector *> BlockIterator::Vectors() const {
  std::vector<const Vector *> res{vecs_.size()};
  for (uint64_t i = 0; i < block_->NumCols(); i++) {
    res[i] = vecs_[i].get();
  }
  return res;
}

TableIterator::TableIterator(const Table *table) : table_(table) {
  if (table->NumBlocks() > 0) {
    curr_block_ = table_->BlockAt(curr_block_idx_);
    block_iter_ = std::make_unique<BlockIterator>(table_, curr_block_);
  }
}

bool TableIterator::Advance() {
  // Empty Table.
  if (block_iter_ == nullptr) return false;
  // Read Vector from current block if possible.
  if (block_iter_->Advance()) return true;
  // Move to next block otherwise.
  curr_block_idx_++;
  // Check if this end of blocks reached.
  if (curr_block_idx_ >= table_->NumBlocks()) return false;
  // Load next blocks.
  curr_block_ = table_->BlockAt(curr_block_idx_);
  block_iter_->Reset(curr_block_);
  return block_iter_->Advance();
}
}
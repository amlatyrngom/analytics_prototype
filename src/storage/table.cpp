#include "storage/table.h"
#include "storage/buffer_manager.h"
#include "storage/buffer_manager.h"

namespace smartid {

void Table::InsertTableBlock(std::unique_ptr<TableBlock> &&table_block) {
  auto buffer_manager = BufferManager::Instance();
  auto settings = Settings::Instance();
  auto block_id = buffer_manager->NewBlockID();
  table_block->PrintBlock();
  table_block->RawBlock()->SetBlockID(block_id);
  // Write row_id
  if (id_idx_ != -1) {
    auto row_id_col = table_block->RawBlock()->MutableColDataAs<int64_t>(id_idx_);
    auto raw_id_bitmap = table_block->RawBlock()->MutableBitmapData(id_idx_);
    // Set all to non-null. The presence bitmap will compensate.
    std::memset(reinterpret_cast<char*>(row_id_col), 0xFF, settings->BlockSize() / Settings::BITMAP_BYTES_PER_WORD);
    for (int64_t i = 0; i < settings->BlockSize(); i++) {
      auto row_id = (block_id << settings->LogBlockSize()) | i;
      row_id_col[i] = row_id;
    }
  }

  buffer_manager->AddBlock(table_block.get());
  block_ids_.emplace_back(block_id);
}


BlockIterator::BlockIterator(const Table *table, const BlockInfo *block, const std::vector<uint64_t>& cols_to_read)
    : table_(table), block_(block), cols_to_read_(cols_to_read), presence_(nullptr), vecs_(cols_to_read_.size()) {
  for (uint64_t i = 0; i < cols_to_read.size(); i++) {
    auto schema_col = table->GetSchema().GetColumn(cols_to_read_[i]);
    vecs_[i] = std::make_unique<Vector>(schema_col.Type());
  }
}

bool BlockIterator::Advance() {
  auto table_block = TableBlock::FromBlockInfo(block_);
  auto raw_block = table_block->RawBlock();
  auto settings = Settings::Instance();
  auto block_size = settings->BlockSize();
  auto vec_size = settings->VecSize();

  bool any_present = false;

  while (curr_idx_ >= block_size && !any_present) {
    presence_ = raw_block->PresenceBitmap() + (curr_idx_ / Settings::BITMAP_BITS_PER_WORD);
    any_present = Bitmap::NumOnes(raw_block->PresenceBitmap(), vec_size) > 0;
    if (any_present) {
      for (uint64_t i = 0; i < cols_to_read_.size(); i++) {
        auto col_idx = cols_to_read_[i];
        auto null_bitmap = raw_block->BitmapData(col_idx) + (curr_idx_ / Settings::BITMAP_BITS_PER_WORD);
        // No need for deep copy of integer
        auto read_data = raw_block->ColData(col_idx) + curr_idx_ * vecs_[i]->ElemSize();
        vecs_[i]->Reset(vec_size, read_data, null_bitmap, raw_block);
      }
    }
    curr_idx_ += vec_size;
  }

  return any_present;
}

Table::~Table() {
  for (auto &block_id: block_ids_) {
    auto block_info = BufferManager::Instance()->Pin(block_id);
    BufferManager::Instance()->Unpin(block_info, false, true);
  }
}

std::vector<const Vector *> BlockIterator::Vectors() const {
  std::vector<const Vector *> res{vecs_.size()};
  for (uint64_t i = 0; i < cols_to_read_.size(); i++) {
    res[i] = vecs_[i].get();
  }
  return res;
}

TableIterator::TableIterator(const Table *table, const std::vector<uint64_t>& cols_to_read)
: table_(table)
, cols_to_read_(cols_to_read)
, buffer_manager_(BufferManager::Instance())
, curr_block_idx_(0) {
  if (curr_block_idx_ < table->NumBlocks()) {
    auto block_id = table_->BlockIDS()[curr_block_idx_];
    curr_block_ = buffer_manager_->Pin(block_id);
    block_iter_ = std::make_unique<BlockIterator>(table_, curr_block_, cols_to_read);
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
  buffer_manager_->Unpin(curr_block_, false);
  auto block_id = table_->BlockIDS()[curr_block_idx_];
  curr_block_ = buffer_manager_->Pin(block_id);
  block_iter_->Reset(curr_block_);
  return block_iter_->Advance();
}
}
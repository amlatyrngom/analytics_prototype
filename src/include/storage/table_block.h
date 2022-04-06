#pragma once

#include "common/util.h"
#include "storage/filter.h"
#include "storage/buffer_manager.h"

namespace smartid {
class RawTableBlock {
  // BEGIN LAYOUT
  // Header(block_id, total_size, num_cols=N, variable_data_offset)
  // data_offset_1, ..., data_offset_N
  // bitmap_offset_1, ..., bitmap_offset_N
  // [data_1, bitmap_1], ..., [data_N, bitmap_N]
  // variable_data
  // END LAYOUT

 public:
  static RawTableBlock* AllocateAndSetupRawBlock(const std::vector<SqlType>& col_types, uint64_t variable_data_size);

  // Get Block ID
  [[nodiscard]] int64_t BlockID() const {
    return block_id;
  }

  // Set Block ID
  void SetBlockID(int64_t new_block_id) {
    block_id = new_block_id;
  }

  // Get total size in bytes.
  [[nodiscard]] uint64_t TotalSize() const {
    return total_size;
  }

  // Get number of columns in bytes.
  [[nodiscard]] uint64_t NumCols() const {
    return num_cols;
  }

  [[nodiscard]] const char* BlockDataStart() const {
    return block_data;
  }

  // Get column offsets.
  uint64_t* ColOffsets() {
    return reinterpret_cast<uint64_t*>(block_data);
  }

  // Get a column offset.
  uint64_t ColOffset(uint64_t col_idx) {
    return ColOffsets()[col_idx];
  }

  // Const version.
  [[nodiscard]] const uint64_t* ColOffsets() const {
    return reinterpret_cast<const uint64_t*>(block_data);
  }

  // Const version.
  [[nodiscard]] uint64_t ColOffset(uint64_t col_idx) const {
    return ColOffsets()[col_idx];
  }

  // Return mutable column data.
  char* MutableColData(uint64_t col_idx) {
    return block_data + ColOffset(col_idx);
  }

  // Const version.
  [[nodiscard]] const char* ColData(uint64_t col_idx) const {
    return block_data + ColOffset(col_idx);
  }

  // Typed version.
  template<typename T>
  T* MutableColDataAs(uint64_t col_idx) {
    return reinterpret_cast<T*>(MutableColData(col_idx));
  }

  // Const version.
  template<typename T>
  const T* ColDataAs(uint64_t col_idx) const {
    return reinterpret_cast<const T*>(ColData(col_idx));
  }

  // Return bitmap offsets.
  uint64_t* BitmapOffsets() {
    // ColOffsets() is already uint64_t* so need for num_cols*sizeof(uint64_t);
    return ColOffsets() + num_cols;
  }

  // Const version.
  [[nodiscard]] const uint64_t* BitmapOffsets() const {
    return ColOffsets() + num_cols;
  }

  // Return a bitmap offset.
  uint64_t BitmapOffset(uint64_t col_idx) {
    return BitmapOffsets()[col_idx];
  }

  // Const version.
  [[nodiscard]] uint64_t BitmapOffset(uint64_t col_idx) const {
    return BitmapOffsets()[col_idx];
  }

  // Return a bitmap data.
  uint64_t* MutableBitmapData(uint64_t col_idx) {
    return reinterpret_cast<uint64_t*>(block_data + BitmapOffset(col_idx));
  }

  // Const version.
  [[nodiscard]] const uint64_t* BitmapData(uint64_t col_idx) const {
    return reinterpret_cast<const uint64_t*>(block_data + BitmapOffset(col_idx));
  }

  // Return a presence bitmap.
  uint64_t* MutablePresenceBitmap() {
    // ColOffsets() is already uint64_t* so need for num_cols*sizeof(uint64_t);
    return BitmapOffsets() + num_cols;
  }

  // Const version.
  [[nodiscard]] const uint64_t* PresenceBitmap() const {
    // ColOffsets() is already uint64_t* so need for num_cols*sizeof(uint64_t);
    return BitmapOffsets() + num_cols;
  }

  // Return a variable buffer.
  char* MutableVariableData() {
    return block_data + variable_data_offset;
  }

  // Const version.
  [[nodiscard]] const char* VariableData() const {
    return block_data + variable_data_offset;
  }

 private:

  // Disallow construction.
  RawTableBlock() = default;

  int64_t block_id{-1};
  uint64_t total_size;
  uint64_t num_cols;
  uint64_t variable_data_offset;
  char block_data[0];
};

// Held in memory when block is actively being modified.
// Takes ownership of raw_block until written to disk.
class TableBlock : public BufferBlock {
 public:
  TableBlock(std::vector<SqlType>&& col_types, RawTableBlock* raw_block) : col_types_(std::move(col_types)), raw_block_(raw_block){}

  explicit TableBlock(std::vector<SqlType>&& col_types)
      : col_types_(std::move(col_types)) {
    std::cout << "Num Cols: " << col_types_.size() << std::endl;
    raw_block_ = RawTableBlock::AllocateAndSetupRawBlock(col_types_, 0);
  }

  void PrintBlock();

  RawTableBlock* RawBlock() {
    return raw_block_;
  }

  [[nodiscard]] RawTableBlock* RawBlock() const {
    return raw_block_;
  }

  // Give to buffer.
  char * Finalize(uint64_t *data_size) override {
    if (hot_) {
      // Place all pointers within block.
      Consolidate();
    }
    auto ret = raw_block_;
    raw_block_ = nullptr;
    *data_size = ret->TotalSize();
    return reinterpret_cast<char*>(ret);
  }

  [[nodiscard]] int64_t BlockID() const override {
    return raw_block_->BlockID();
  }

  // Consolidate a hot block into a cold block;
  void Consolidate();

  void SetHot() {
    hot_ = true;
  }

  [[nodiscard]] bool Hot() const {
    return hot_;
  }

  static TableBlock* FromBlockInfo(BlockInfo* block_info) {
    return dynamic_cast<TableBlock*>(block_info->buffer_block);
  }

  static const TableBlock* FromBlockInfo(const BlockInfo* block_info) {
    return dynamic_cast<const TableBlock*>(block_info->buffer_block);
  }


 private:
  bool hot_{false};
  RawTableBlock* raw_block_;
  std::vector<SqlType> col_types_;
};


}
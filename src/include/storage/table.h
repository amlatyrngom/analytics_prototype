#pragma once

#include <iostream>
#include <unordered_set>
#include <shared_mutex>
#include "common/util.h"
#include "storage/filter.h"
#include "storage/table_block.h"

// TODO(Amadou): Add support for NULL later.
namespace smartid {
class VectorProjection;
class Vector;
class BufferManager;
class InfoStore;
class DictEncoding;
class TableStatistics;


/**
 * The Table class.
 */
class Table {
 public:
  /**
   * Create a new table or restores it from db is schema is empty.
   */
  Table(int64_t table_id, std::string name, Schema &&schema, BufferManager* buffer_manager, InfoStore* info_store, bool add_row_id=true);

  void InsertTableBlock(std::unique_ptr<TableBlock> && table_block);
  void ClearBlocks();
  void DeleteSelf();

  ~Table();

  [[nodiscard]] uint64_t IdIdx() const {
    return id_idx_;
  }

  /**
   * @return Table Name.
   */
  [[nodiscard]] const std::string &Name() const {
    return name_;
  }

  /**
   * @return Schema of the table.
   */
  [[nodiscard]] const Schema &GetSchema() const {
    return schema_;
  }

  /**
   * @return Number of blocks.
   */
  [[nodiscard]] uint64_t NumBlocks() const {
    return block_ids_.size();
  }

  [[nodiscard]] const auto& BlockIDS() const {
    return block_ids_;
  }

  BufferManager* BM() {
    return buffer_manager_;
  }

  [[nodiscard]] int64_t TableID() const {
    return table_id_;
  }

  InfoStore* IS() {
    return info_store_;
  }

  DictEncoding* GetDictEncoding(uint64_t col_idx) {
    return dict_encodings_.at(col_idx).get();
  }

  void FinalizeEncodings();

  void SetStatistics(std::unique_ptr<TableStatistics>&& table_stats);
  [[nodiscard]] const TableStatistics* GetStatistics() const;

 private:
  void StoreCreateTableInfo();
  void StoreBlockInfo(int64_t block_id);
  void DeleteBlockInfo(int64_t block_id);
  void RestoreFromDB();
  void RestoreSchema();
  void RestoreBlocks();

  // Persisted somehow.
  int64_t table_id_;
  std::vector<int64_t> block_ids_;
  std::string name_;
  Schema schema_;
  int64_t id_idx_{-1};
  std::unordered_map<uint64_t, std::unique_ptr<DictEncoding>> dict_encodings_;

  // In mem
  BufferManager* buffer_manager_{nullptr};
  InfoStore* info_store_{nullptr};
  std::unique_ptr<TableStatistics> table_statistics_;
  std::shared_mutex table_lock_;
  std::mutex table_modif_lock_; // TODO: Support concurrency.
};

/**
 * Iterator through blocks.
 */
class BlockIterator {
 public:
  /**
   * Constructor
   */
  BlockIterator(const Table *table, const BlockInfo *block, const std::vector<uint64_t>& cols_to_read);

  /**
   * Advance to next set of vectors.
   * @return True if successfully advanced.
   */
  bool Advance();

  /**
   * Reset iterator with new block.
   */
  void Reset(const BlockInfo *block) {
    block_ = block;
    curr_idx_ = 0;
  }

  /**
   * Return vector at given index.
   */
  [[nodiscard]] const Vector *VectorAt(uint64_t idx) const {
    return vecs_[idx].get();
  }

  /**
   * Return set of vectors.
   */
  [[nodiscard]] std::vector<const Vector *> Vectors() const;

  [[nodiscard]] const Bitmap* Filter() const {
    return presence_.get();
  }

 private:
  const Table *table_{nullptr};
  const BlockInfo *block_{nullptr};
  std::vector<uint64_t> cols_to_read_;
  uint64_t curr_idx_{0};
  std::unique_ptr<Bitmap> presence_;
  std::vector<std::unique_ptr<Vector>> vecs_;
};

/**
 * Helper class to iterator through tables.
 */
class TableIterator {
 public:
  /**
   * Constructor for full scan.
   */
  explicit TableIterator(const Table *table, BufferManager* bm, const std::vector<uint64_t>& cols_to_read);

  /**
   * Advance to next set of vectors.
   * @return True if successful.
   */
  bool Advance();

  /**
   * Return vector at given index.
   */
  [[nodiscard]] const Vector *VectorAt(uint64_t idx) const {
    return block_iter_->VectorAt(idx);
  }

  /**
   * Return set of vectors.
   */
  [[nodiscard]] std::vector<const Vector *> Vectors() const {
    return block_iter_->Vectors();
  }

  [[nodiscard]] const Bitmap* Filter() const {
    return block_iter_->Filter();
  }


 private:
  const Table *table_;
  std::vector<uint64_t> cols_to_read_;
  BufferManager* buffer_manager_;
  BlockInfo* curr_block_;
  uint64_t curr_block_idx_;
  std::unique_ptr<BlockIterator> block_iter_{nullptr};
};



}

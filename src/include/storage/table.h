#pragma once

#include <iostream>
#include <unordered_set>
#include <shared_mutex>
#include "common/util.h"
#include "storage/filter.h"
#include "storage/buffer_manager.h"
#include "storage/table_block.h"

// TODO(Amadou): Add support for NULL later.
namespace smartid {
class VectorProjection;

/**
 * Represents a vector of data.
 */
class Vector {
 public:
  /**
   * Construct a Vector.
   */
  explicit Vector(SqlType type)
      : elem_type_(type),
        num_elems_(0),
        owned_data_(0),
        data_(owned_data_.data()),
        allocated_strings_(0),
        elem_size_(TypeUtil::TypeSize(type)),
        settings_vec_size_(Settings::Instance()->VecSize()){}

  /**
   * @return Raw Bytes
   */
  [[nodiscard]] const char *Data() const {
    return data_;
  }

  [[nodiscard]] char *MutableData() {
    return owned_data_.data();
  }

  template<typename T>
  [[nodiscard]] T *MutableDataAs() {
    return reinterpret_cast<T *>(MutableData());
  }

  /**
   * @tparam T C++ element type
   * @return Element array.
   */
  template<typename T>
  const T *DataAs() const {
    return reinterpret_cast<const T *>(Data());
  }

  void SetNumElems(uint64_t num_elems) {
    num_elems_ = num_elems;
  }

  /**
   * @return Number of elements in vector.
   */
  [[nodiscard]] uint64_t NumElems() const {
    return num_elems_;
  }

  /**
   * @return Size of elements.
   */
  [[nodiscard]] uint64_t ElemSize() const {
    return elem_size_;
  }

  /**
   * @return Size of elements.
   */
  [[nodiscard]] SqlType ElemType() const {
    return elem_type_;
  }

  /**
   * Reset vector with new elements.
   */
  void Reset(uint64_t num_elems, const char *data, const uint64_t* null_bitmap, const RawTableBlock* block=nullptr) {
    Resize(num_elems);
    null_bitmap_.Reset(num_elems, null_bitmap);
    if (elem_type_ == SqlType::Varchar) {
      allocated_strings_.clear();
      auto src_varlens = reinterpret_cast<const Varlen*>(data);
      auto dst_varlens = reinterpret_cast<Varlen*>(owned_data_.data());
      for (uint64_t i = 0; i < num_elems_; i++) {
        if (src_varlens[i].Info().IsCompact()) {
          ASSERT(block != nullptr, "Reset with compact varlen without block!");
          auto offset = src_varlens[i].Info().CompactOffset();
          auto size = src_varlens[i].Info().CompactSize();
          auto src_data = block->VariableData() + offset;
          dst_varlens[i] = Varlen::MakeNormal(size, src_data);
        } else {
          // Assumes normal varlens. Vector class should not have access to compact ones.
          dst_varlens[i] = Varlen::MakeNormal(src_varlens[i].Info().NormalSize(), src_varlens[i].Data());
        }
      }
    } else {
      std::memcpy(owned_data_.data(), data, num_elems * elem_size_);
    }
  }

  void Resize(uint64_t new_size) {
    if (new_size == num_elems_) return;
    auto alloc_size = std::max(settings_vec_size_, new_size) * elem_size_;
    owned_data_.resize(alloc_size);
    if (elem_type_ == SqlType::Varchar) {
      allocated_strings_.resize(new_size);
    }
    data_ = owned_data_.data();
    num_elems_ = new_size;
  }

 private:
  std::vector<char> owned_data_;
  Bitmap null_bitmap_;
  // For varchars.
  std::vector<std::vector<char>> allocated_strings_;
  const char *data_;
  uint64_t num_elems_;
  SqlType elem_type_;
  uint64_t elem_size_;
  uint64_t settings_vec_size_;
};

/**
 * The Table class.
 */
class Table {
 public:
  /**
   * Create a new table.
   */
  Table(std::string name, Schema &&schema, bool add_row_id) : name_(std::move(name)), schema_(std::move(schema)) {
    if (add_row_id) {
      id_idx_ = schema_.NumCols();
      schema_.AddColumn(Column::ScalarColumn("row_id", SqlType::Int64));
    }
  }

  void InsertTableBlock(std::unique_ptr<TableBlock> && table_block);

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

  const auto& BlockIDS() const {
    return block_ids_;
  }

 private:
  // Persisted somehow.
  std::vector<int64_t> block_ids_;
  std::string name_;
  Schema schema_;
  int64_t id_idx_{-1};

  // In mem
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

 private:
  const Table *table_{nullptr};
  const BlockInfo *block_{nullptr};
  std::vector<uint64_t> cols_to_read_;
  uint64_t curr_idx_{0};
  const uint64_t* presence_;
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
  explicit TableIterator(const Table *table, const std::vector<uint64_t>& cols_to_read);

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

 private:
  const Table *table_;
  std::vector<uint64_t> cols_to_read_;
  BufferManager* buffer_manager_;
  BlockInfo* curr_block_;
  uint64_t curr_block_idx_;
  std::unique_ptr<BlockIterator> block_iter_{nullptr};
};



}

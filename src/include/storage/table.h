#pragma once

#include <iostream>
#include "common/util.h"

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
  explicit Vector(SqlType type, uint64_t num_elems = VEC_SIZE)
      : elem_type_(type),
        num_elems_(num_elems),
        owned_data_(num_elems * TypeUtil::TypeSize(type)),
        data_(owned_data_.data()),
        elem_size_(TypeUtil::TypeSize(type)) {}

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
  void DeepReset(uint64_t num_elems, const char *data) {
    Resize(num_elems);
    std::memcpy(owned_data_.data(), data, num_elems * elem_size_);
  }

  void ShallowReset(uint64_t num_elems, const char *data) {
    num_elems_ = num_elems;
    data_ = data;
  }

  void Resize(uint64_t new_size) {
    if (new_size == num_elems_) return;
    auto alloc_size = std::max(VEC_SIZE, new_size) * elem_size_;
    owned_data_.resize(alloc_size * elem_size_);
    data_ = owned_data_.data();
    num_elems_ = new_size;
  }

 private:
  std::vector<char> owned_data_;
  const char *data_;
  uint64_t num_elems_;
  SqlType elem_type_;
  uint64_t elem_size_;
};

/**
 * A block of data. Only used to prevent large contiguous allocation.
 */
class Block {
 public:
  /**
   * Create a column oriented block of data.
   */
  Block(std::vector<std::vector<char>> &&data, uint64_t num_elems) : data_(std::move(data)), num_elems_(num_elems) {}

  void Free(uint64_t col_idx);

  void AddIds(int32_t block_id, uint64_t id_idx) {
    block_id_ = block_id;
    auto & id_col = data_[id_idx];
    auto id_data = reinterpret_cast<int64_t*>(id_col.data());
    for (uint64_t i = 0; i < num_elems_; i++) {
      id_data[i] = i | (static_cast<int64_t>(block_id) << 32ull);
    }
  }

  void AddCustomerIds(int32_t block_id, uint64_t id_idx) {
    block_id_ = block_id;
    auto & id_col = data_[id_idx];
    auto id_data = reinterpret_cast<int64_t*>(id_col.data());
    for (uint64_t i = 0; i < num_elems_; i++) {
      id_data[i] = i | (static_cast<int64_t>(block_id) << 32ull);
    }
  }

  /**
   * @param col_idx index of column to read.
   * @return raw data array.
   */
  [[nodiscard]] const char *ColumnData(uint64_t col_idx) const {
    return data_[col_idx].data();
  }

  /**
   * @tparam T C++ type of column elements.
   * @param col_idx index of the column
   * @return data array of the given type.
   */
  template<typename T>
  const T *ColumnDataAs(uint64_t col_idx) const {
    return reinterpret_cast<const T *>(data_[col_idx].data());
  }

  /**
   * @param col_idx index of the column
   * @return mutable data array.
   */
  char* MutableColumnData(uint64_t col_idx) {
    return data_[col_idx].data();
  }

  /**
   * @tparam T C++ type of column elements.
   * @param col_idx index of the column
   * @return mutable data array of the given type.
   */
  template<typename T>
  T *MutableColumnDataAs(uint64_t col_idx) {
    return reinterpret_cast<T *>(data_[col_idx].data());
  }

  /**
   * @return Number of elements in this block.
   */
  [[nodiscard]] uint64_t NumElems() const {
    return num_elems_;
  }

  /**
   * @return Number of columns in this block.
  */
  [[nodiscard]] uint64_t NumCols() const {
    return data_.size();
  }

 private:
  std::vector<std::vector<char>> data_;
  uint64_t num_elems_;
  int32_t block_id_{-1};
};

/**
 * The Table class.
 */
class Table {
 public:
  /**
   * Create a new table.
   */
  Table(std::string name, Schema &&schema) : name_(std::move(name)), schema_(std::move(schema)) {
//    usage_idx_ = schema_.NumCols();
//    schema_.AddColumn(Column::ScalarColumn("usage", SqlType::Int64));
//    id_idx_ = schema_.NumCols();
//    schema_.AddColumn(Column::ScalarColumn("id", SqlType::Int64));
//    embedding_idx_ = schema_.NumCols();
//    schema_.AddColumn(Column::ScalarColumn("embedding_col", SqlType::Int64));
  }

  ~Table();

  [[nodiscard]] uint64_t IdIdx() const {
    return id_idx_;
  }

  [[nodiscard]] uint64_t UsageIdx() const {
    return usage_idx_;
  }

  [[nodiscard]] uint64_t EmbeddingIdx() const {
    return embedding_idx_;
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
   * Insert block of data in table.
   * @param block Block to insert.
   */
  void InsertBlock(Block &&block) {
//    uint32_t block_id = blocks_.size();
//    if (Name() == "customer") {
//      block.AddCustomerIds(block_id, IdIdx());
//    } else {
//      block.AddIds(block_id, IdIdx());
//    }
    blocks_.emplace_back(std::move(block));
  }

  /**
   * @return Number of blocks.
   */
  [[nodiscard]] uint64_t NumBlocks() const {
    return blocks_.size();
  }

  /**
   * @param idx Index of the wanted block.
   * @return Block at given index.
   */
  [[nodiscard]] const Block *BlockAt(uint64_t idx) const {
    return &blocks_[idx];
  }

  /**
   * @param idx Index of the wanted block.
   * @return Mutable Block at given index.
   */
  [[nodiscard]] Block *MutableBlockAt(uint64_t idx) {
    return &blocks_[idx];
  }

 private:
  std::string name_;
  Schema schema_;
  std::vector<Block> blocks_;
  uint64_t id_idx_;
  uint64_t embedding_idx_;
  uint64_t usage_idx_;
//  // Primary keys.
//  std::vector<uint64_t> pks_;
//  // Pairs of (this.fk, other.pk)
//  using TableFK = std::vector<std::pair<uint64_t, uint64_t>>;
//  std::unordered_map<const Table*, TableFK> fks_;
};

/**
 * Iterator through blocks.
 */
class BlockIterator {
 public:
  /**
   * Constructor
   */
  BlockIterator(const Table *table, const Block *block);

  /**
   * Advance to next set of vectors.
   * @return True if successfully advanced.
   */
  bool Advance();

  /**
   * Reset iterator with new block.
   */
  void Reset(const Block *block) {
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
  const Block *block_{nullptr};
  uint64_t curr_idx_{0};
  std::vector<std::unique_ptr<Vector>> vecs_;
};

/**
 * Helper class to iterator through tables.
 */
class TableIterator {
 public:
  /**
   * Constructor
   */
  explicit TableIterator(const Table *table);

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
  const Block *curr_block_;
  uint64_t curr_block_idx_{0};
  std::unique_ptr<BlockIterator> block_iter_{nullptr};
};

}

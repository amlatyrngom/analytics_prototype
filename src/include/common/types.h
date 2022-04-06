#pragma once
#include <cstdint>
#include <iostream>
#include <string>
#include <sstream>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <unordered_map>
#include "date/date.h"
#include "common/date_type.h"
#include <cstring>

namespace smartid {
/**
 * Supported types.
 */
#define SQL_TYPE(ARITH, OTHER, ...) \
  OTHER(Char, char, __VA_ARGS__)                  \
  ARITH(Int16, int16_t, __VA_ARGS__)                  \
  ARITH(Int32, int32_t, __VA_ARGS__)                  \
  ARITH(Int64, int64_t, __VA_ARGS__)                  \
  ARITH(Float32, float, __VA_ARGS__)                  \
  ARITH(Float64, double, __VA_ARGS__)                  \
  OTHER(Date, Date, __VA_ARGS__)                  \
  OTHER(Varchar, Varlen, __VA_ARGS__)             \
  OTHER(Pointer, uintptr_t, __VA_ARGS__)

#define ENUM_DEFINER(entry, ...) entry,
enum class SqlType {
  SQL_TYPE(ENUM_DEFINER, ENUM_DEFINER)
};
#undef ENUM_DEFINER


struct VarlenInfo {

  [[nodiscard]] bool IsCompact() const {
    return (info & 1) == 1;
  }

  // Can only be set once.
  void SetCompact(bool is_compact) {
    info |= is_compact;
  }

  // Remaining bits after compact.
  [[nodiscard]] uint64_t Rest() const {
    return info >> 1;
  }

  [[nodiscard]] uint64_t CompactSize() const {
    return (Rest() & 0xFFFF);
  }

  // Can only be set once.
  // Assumes s is less than 2**16.
  void SetCompactSize(uint64_t s) {
    info |= (s << 1);
  }

  [[nodiscard]] uint64_t CompactOffset() const {
    return (Rest() >> 16);
  }

  // Can only be set once.
  // Assumes s is less than 2**32.
  void SetCompactOffset(uint64_t o) {
    info |= (o << 17);
  }

  [[nodiscard]] uint64_t NormalSize() const {
    return Rest();
  }

  // Can only be set once
  void SetNormalSize(uint64_t s) {
    info |= (s << 1);
  }

  // First bit: is_compact.
  // If is_compact: next 16 bits is size. After that is offset within block.
  // If !is_compact: rest is size.
  uint64_t info{0};
};

/**
 * Class for variable length data.
 */
class Varlen {
 public:
  /**
   * Constructor
   */
//  Varlen(int64_t size, const char *data) : size_(size) {
//    if (size == 0) {
//      data_ = nullptr;
//      return;
//    }
//    if (size > 0) {
//      data_ = reinterpret_cast<char *>(std::malloc(size));
//      if (data != nullptr) {
//        std::memcpy(data_, data, size);
//      }
//    }
//    if (size < 0) {
//      // User is responsible for setting string that does not belong to the varlen.
//      data_ = nullptr;
//    }
//  }

  /**
   * Should be called explicitly by table deletor to free allocated memory.
   */
  void Free() {
    if (!info_.IsCompact() && data_ != nullptr) {
      std::free(data_);
      data_ = nullptr;
    }
  }

  [[nodiscard]] int Comp(const Varlen &other) const {
    // Only works with normal varlens.
    auto this_size = info_.NormalSize();
    auto other_size = info_.NormalSize();
    auto min_size = std::min(this_size, other_size);
    auto cmp = std::memcmp(data_, other.data_, min_size);
    if (cmp != 0) return cmp;
    if (this_size == other_size) return 0;
    return this_size < other_size ? -1 : 1;
  }

  [[nodiscard]] bool operator<(const Varlen &other) const {
    return Comp(other) < 0;
  }
  [[nodiscard]] bool operator<=(const Varlen &other) const {
    return Comp(other) <= 0;
  }
  [[nodiscard]] bool operator==(const Varlen &other) const {
    return Comp(other) == 0;
  }
  [[nodiscard]] bool operator!=(const Varlen &other) const {
    return Comp(other) != 0;
  }
  [[nodiscard]] bool operator>(const Varlen &other) const {
    return Comp(other) > 0;
  }
  [[nodiscard]] bool operator>=(const Varlen &other) const {
    return Comp(other) >= 0;
  }

  /**
   * @return Raw data array.
   */
  [[nodiscard]] const char *Data() const {
    return data_;
  }

  static Varlen MakeCompact(uint64_t offset, uint64_t size) {
    Varlen v;
    v.info_.SetCompact(true);
    v.info_.SetCompactOffset(offset);
    v.info_.SetCompactSize(size);
    v.data_ = nullptr;
    return v;
  }

  static Varlen MakeNormal(uint64_t size, const char* data) {
    Varlen v;
    v.info_.SetCompact(false);
    v.info_.SetNormalSize(size);
    if (size == 0) {
      v.data_ = nullptr;
      return v;
    }
    v.data_ = reinterpret_cast<char *>(std::malloc(size));
    if (data != nullptr) {
      std::memcpy(v.data_, data, size);
    } else {
      std::memset(v.data_, 0, size);
    }
    return v;
  }

  const auto& Info() const {
    return info_;
  }

 private:
  VarlenInfo info_; // Negative when data does not belong to the varlen.
  char *data_;
};

/**
 * Set of helper functions.
 */
struct TypeUtil {
  /**
   * Return raw size of a given type.
   */
  static uint64_t TypeSize(SqlType type) {
    switch (type) {
      case SqlType::Char: return 1;
      case SqlType::Int16: return 2;
      case SqlType::Int32: return 4;
      case SqlType::Int64: return 8;
      case SqlType::Float32: return 4;
      case SqlType::Float64: return 8;
      case SqlType::Date: return 4;
      case SqlType::Varchar: return 16;
      case SqlType::Pointer: return 8;
    }
  }

  static bool IsArithmetic(SqlType t) {
    return t == SqlType::Int64 || t == SqlType::Int32 || t == SqlType::Int16 || t == SqlType::Float32 || t == SqlType::Float64;
  }

  static bool AreCompatible(SqlType t1, SqlType t2) {
    // Same type
    if (t1 == t2) return true;
    if (IsArithmetic(t1) && IsArithmetic(t2)) {
      return true;
    }
    return false;
  }

  static SqlType Promote(SqlType t1, SqlType t2) {
    // Return if types are equal
    if (!AreCompatible(t1, t2)) {
      // TODO(Amadou): Throw some kind of error.
      std::cerr << "Cannot promote incompatible type!!!!" << std::endl;
      return t1;
    }
    if (t1 == t2) return t1;
    if (t1 == SqlType::Float64 || t2 == SqlType::Float64) return SqlType::Float64;
    if (t1 == SqlType::Float32 || t2 == SqlType::Float32) return SqlType::Float32;
    if (t1 == SqlType::Int64 || t2 == SqlType::Int64) return SqlType::Int64;
    return SqlType::Int32;
  }
};

/**
 * The a singlar Value type.
 */
using Value = std::variant<char, int16_t, int32_t, int64_t, float, double, Date, Varlen, uintptr_t>;

struct FKConstraint {
  FKConstraint() = default;

  FKConstraint(const std::string& ref_table, const std::string& ref_col) {
    refs_[ref_table] = ref_col;
  }

  explicit FKConstraint(const std::vector<std::pair<std::string, std::string>>& refs) {
    for (const auto& p: refs) {
      refs_[p.first] = p.second;
    }
  }

  std::unordered_map<std::string, std::string> refs_;
};

/**
 * A SQL Table Column.
 */
class Column {
 public:
  /**
   * Constructor
   */
  Column(std::string name, SqlType type, bool owns_varchar, bool is_pk, FKConstraint && fk_constraint)
  : name_(std::move(name))
  , type_(type)
  , owns_varchar_(owns_varchar)
  , is_pk_(is_pk)
  , fk_constraint_(std::move(fk_constraint)) {}

  ///////////////////////////
  /// Static Constructors
  ///////////////////////////
  static Column ScalarColumn(const std::string& name, SqlType type) {
    return Column(name, type, false, false, {});
  }

  static Column ScalarColumnWithConstraint(const std::string& name, SqlType type, bool is_pk, FKConstraint && fk_constraint) {
    return Column(name, type, false, is_pk, std::move(fk_constraint));
  }

  static Column VarcharColumn(const std::string& name, SqlType type, bool owns_varchar) {
    return Column(name, type, owns_varchar, false, {});
  }

  static Column VarcharColumnWithConstraint(const std::string& name, SqlType type, bool owns_varchar, bool is_pk, FKConstraint && fk_constraint) {
    return Column(name, type, owns_varchar, is_pk, std::move(fk_constraint));
  }


  [[nodiscard]] bool IsPK() const {
    return is_pk_;
  }

  [[nodiscard]] bool IsFK() const {
    return !fk_constraint_.refs_.empty();
  }

  [[nodiscard]] const FKConstraint& FK() const {
    return fk_constraint_;
  }

  /**
   * @return Name of column.
   */
  [[nodiscard]] const std::string &Name() const {
    return name_;
  }

  /**
   * @return Type of the column.
   */
  [[nodiscard]] SqlType Type() const {
    return type_;
  }

  bool OwnsVarchar() const {
    return owns_varchar_;
  }

 private:
  std::string name_;
  SqlType type_;
  bool owns_varchar_;
  bool is_pk_;
  FKConstraint fk_constraint_;
};

/**
 * The schema of a SQL table.
 */
class Schema {
 public:
  /**
   * Constructor
   */
  explicit Schema(std::vector<Column> &&cols) : cols_(std::move(cols)) {}

  /**
   * Return the column at the given index.
   */
  [[nodiscard]] const Column &GetColumn(uint64_t idx) const {
    return cols_[idx];
  }

  /**
   * Return the index of the column with the given name.
   * Name should be valid.
   */
  [[nodiscard]] uint64_t ColIdx(const std::string &col_name) const {
    uint64_t i;
    for (i = 0; i < cols_.size(); i++) {
      if (cols_[i].Name() == col_name) return i;
    }
    // Should never happen. Throwing exception not worth it.
    std::cerr << "Unknown column " << col_name << std::endl;
    std::terminate();
  }

  /**
   * @return Number of columns in this schema.
   */
  [[nodiscard]] uint64_t NumCols() const {
    return cols_.size();
  }

  void AddColumn(Column&& col) {
    cols_.emplace_back(std::move(col));
  }

 private:
  std::vector<Column> cols_;
};
}
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
 * Supported types. (char is uint8_t to disambiguate from int8_t.)
 */
#define SQL_TYPE(ARITH, OTHER, ...) \
  OTHER(Char, uint8_t, __VA_ARGS__)                  \
  ARITH(Int8, int8_t, __VA_ARGS__)                  \
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
  // Should be called explicitly to delete memory.
  void Free();

  // Make
  static Varlen MakeCompact(uint64_t offset, uint64_t size);

  static Varlen MakeNormal(uint64_t size, const char* data);

  static Varlen MakeManagedNormal(uint64_t size, const char* data, std::vector<char> & alloc_buffer);

  /**
   * @return Raw data array.
   */
  [[nodiscard]] const char *Data() const {
    return data_;
  }


  [[nodiscard]] const auto& Info() const {
    return info_;
  }

  // Compare to varlen.
  [[nodiscard]] int Comp(const Varlen &other) const;

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



 private:
  VarlenInfo info_; // Negative when data does not belong to the varlen.
  char *data_;
};

/**
 * Set of helper functions.
 */
struct TypeUtil {

  // Return the size of type.
  static uint64_t TypeSize(SqlType type);

  // Convert type to name
  static std::string TypeToName(SqlType type);

  // Convert name to type.
  static SqlType NameToType(const std::string& name);

  // Check if type is arithmetic.
  static bool IsArithmetic(SqlType t);

  // Check if two types are compatible.
  static bool AreCompatible(SqlType t1, SqlType t2);

  // Promote to common type.
  static SqlType Promote(SqlType t1, SqlType t2);
};

// A singular value.
using Value = std::variant<uint8_t, int8_t, int16_t, int32_t, int64_t, float, double, Date, Varlen, uintptr_t>;

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
  Column(std::string name, SqlType type, bool is_pk, bool is_fk, std::string fk_name, bool dict_encode)
  : name_(std::move(name))
  , type_(type)
  , is_pk_(is_pk)
  , is_fk_(is_fk)
  , fk_name_(std::move(fk_name))
  , dict_encode_(dict_encode) {}

  Column(std::string name, SqlType type): Column(std::move(name), type, false, false, "", false) {}

  Column(std::string name, SqlType type, bool is_pk): Column(std::move(name), type, is_pk, false, "", false) {}

  /**
   * Hack to allow default initialization in vectors.
   */
  Column() = default;

  [[nodiscard]] bool IsPK() const {
    return is_pk_;
  }

  [[nodiscard]] bool IsFK() const {
    return is_fk_;
  }

  [[nodiscard]] const std::string FKName() const {
    return fk_name_;
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

  [[nodiscard]] bool DictEncode() const {
    return dict_encode_;
  }

 private:
  std::string name_;
  SqlType type_;
  bool is_pk_;
  bool is_fk_;
  std::string fk_name_; // Name in the central table.
  bool dict_encode_;
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

  [[nodiscard]] const auto& Cols() const {
    return cols_;
  }

 private:
  std::vector<Column> cols_;
};
}
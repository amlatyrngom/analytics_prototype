#include "common/types.h"
#include "common/util.h"

namespace smartid {

void Varlen::Free() {
  if (!info_.IsCompact() && data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

int Varlen::Comp(const Varlen &other) const {
  // Only works with normal varlens.
  auto this_size = info_.NormalSize();
  auto other_size = info_.NormalSize();
  auto min_size = std::min(this_size, other_size);
  auto cmp = std::memcmp(data_, other.data_, min_size);
  if (cmp != 0) return cmp;
  if (this_size == other_size) return 0;
  return this_size < other_size ? -1 : 1;
}

Varlen Varlen::MakeCompact(uint64_t offset, uint64_t size) {
  Varlen v;
  v.info_.SetCompact(true);
  v.info_.SetCompactOffset(offset);
  v.info_.SetCompactSize(size);
  v.data_ = nullptr;
  return v;
}

Varlen Varlen::MakeNormal(uint64_t size, const char *data) {
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

Varlen Varlen::MakeManagedNormal(uint64_t size, const char *data, std::vector<char> &alloc_buffer) {
  Varlen v;
  v.info_.SetCompact(false);
  v.info_.SetNormalSize(size);
  if (size == 0) {
    v.data_ = nullptr;
    return v;
  }
  alloc_buffer.resize(size);
  v.data_ = alloc_buffer.data();
  if (data != nullptr) {
    std::memcpy(v.data_, data, size);
  } else {
    std::memset(v.data_, 0, size);
  }
  return v;
}

uint64_t TypeUtil::TypeSize(SqlType type) {
  switch (type) {
    case SqlType::Char: return 1;
    case SqlType::Int8: return 1;
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

std::string TypeUtil::TypeToName(SqlType type) {
  switch (type) {
    case SqlType::Char: return "char";
    case SqlType::Int8: return "tinyint";
    case SqlType::Int16: return "smallint";
    case SqlType::Int32: return "int";
    case SqlType::Int64: return "bigint";
    case SqlType::Float32: return "float";
    case SqlType::Float64: return "double";
    case SqlType::Date: return "date";
    case SqlType::Varchar: return "text";
    case SqlType::Pointer: return "pointer";
  }
}

SqlType TypeUtil::NameToType(const std::string &name) {
  if (name == "char") return SqlType::Char;
  if (name == "tinyint") return SqlType::Int8;
  if (name == "smallint") return SqlType::Int16;
  if (name == "int") return SqlType::Int32;
  if (name == "bigint") return SqlType::Int64;
  if (name == "float") return SqlType::Float32;
  if (name == "double") return SqlType::Float64;
  if (name == "date") return SqlType::Date;
  if (name == "text") return SqlType::Varchar;
  if (name == "pointer") return SqlType::Pointer;
  ASSERT(false, "Unknown type name!");
}

bool TypeUtil::IsArithmetic(SqlType t) {
  return t == SqlType::Int64 || t == SqlType::Int32 || t == SqlType::Int16 || t == SqlType::Float32 || t == SqlType::Float64;
}

bool TypeUtil::AreCompatible(SqlType t1, SqlType t2) {
  // Same type
  if (t1 == t2) return true;
  if (IsArithmetic(t1) && IsArithmetic(t2)) {
    return true;
  }
  return false;
}

SqlType TypeUtil::Promote(SqlType t1, SqlType t2) {
  // Return if types are equal
  ASSERT(AreCompatible(t1, t2), "Cannot promote incompatible type!!");
  if (t1 == t2) return t1;
  if (t1 == SqlType::Float64 || t2 == SqlType::Float64) return SqlType::Float64;
  if (t1 == SqlType::Float32 || t2 == SqlType::Float32) return SqlType::Float32;
  if (t1 == SqlType::Int64 || t2 == SqlType::Int64) return SqlType::Int64;
  return SqlType::Int32;
}

Value ValueUtil::ReadVal(const std::string &s, SqlType val_type) {
  std::stringstream ss;
  ss << s;
  return ReadVal(ss, val_type);
}

Value ValueUtil::ReadVal(std::istream& is, SqlType val_type) {
  Value val;
  switch (val_type) {
    case SqlType::Char: {
      int raw_val; // uint8_t and int8_t behave weirdly with IO.
      is >> raw_val;
      val = uint8_t(raw_val);
      break;
    }
    case SqlType::Int8: {
      int raw_val; // uint8_t and int8_t behave weirdly with IO.
      is >> raw_val;
      val = int8_t(raw_val);
      break;
    }
    case SqlType::Int16: {
      int16_t raw_val;
      is >> raw_val;
      val = raw_val;
      break;
    }
    case SqlType::Int32: {
      int32_t raw_val;
      is >> raw_val;
      val = raw_val;
      break;
    }
    case SqlType::Int64: {
      int64_t raw_val;
      is >> raw_val;
      val = raw_val;
      break;
    }
    case SqlType::Float32: {
      float raw_val;
      is >> raw_val;
      val = raw_val;
      break;
    }
    case SqlType::Float64: {
      double raw_val;
      is >> raw_val;
      val = raw_val;
      break;
    }
    case SqlType::Date: {
      std::string raw_val;
      is >> raw_val;
      val = Date::FromString(raw_val);
      break;
    }
    default: {
      ASSERT(false, "Unsupported value type!");
    }
  }
  return val;
}

void ValueUtil::WriteVal(const Value& val, std::ostream& os, SqlType val_type) {
  switch (val_type) {
    case SqlType::Char: {
      os << int(std::get<uint8_t>(val));
      return;
    }
    case SqlType::Int8: {
      os << int(std::get<int8_t>(val));
      return;
    }
    case SqlType::Int16: {
      os << std::get<int16_t>(val);
      return;
    }
    case SqlType::Int32: {
      os << std::get<int32_t>(val);
      return;
    }
    case SqlType::Int64: {
      os << std::get<int64_t>(val);
      return;
    }
    case SqlType::Float32: {
      os << std::get<float>(val);
      return;
    }
    case SqlType::Float64: {
      os << std::get<double>(val);
      return;
    }
    case SqlType::Date: {
      os << std::get<Date>(val).ToString();
      return;
    }
    default: {
      ASSERT(false, "Unsupported value type!");
    }
  }
}

Value ValueUtil::Add(const Value& val, int x, SqlType val_type) {
  switch (val_type) {
    case SqlType::Char: {
      auto v = int(std::get<uint8_t>(val));
      v += x;
      return uint8_t(v);
    }
    case SqlType::Int8: {
      auto v = int(std::get<int8_t>(val));
      v += x;
      return int8_t(v);
    }
    case SqlType::Int16: {
      auto v = int(std::get<int16_t>(val));
      v += x;
      return int16_t(v);
    }
    case SqlType::Int32: {
      auto v = std::get<int32_t>(val);
      v += x;
      return int32_t(v);
    }
    case SqlType::Int64: {
      auto v = std::get<int64_t>(val);
      v += x;
      return int64_t(v);
    }
    case SqlType::Float32: {
      auto v = std::get<float>(val);
      v += float(x);
      return float(v);
    }
    case SqlType::Float64: {
      auto v = std::get<double>(val);
      v += double(x);
      return double(v);
    }
    default: {
      ASSERT(false, "Unsupported add value type!");
    }
  }
}

}
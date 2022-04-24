#include "storage/dict_encoding.h"
#include "storage/table.h"
#include "common/info_store.h"
#include <sstream>
#include <iostream>

namespace smartid {

DictEncoding::DictEncoding(int64_t table_id, int64_t col_idx, InfoStore* info_store)
: table_id_(table_id)
, col_idx_(col_idx)
, info_store_(info_store) {
  try {
    auto & db = *info_store_->db;
    std::stringstream ss;
    ss
        << "CREATE TABLE IF NOT EXISTS dict_encodings ("
        << "table_id BIGINT,"
        << "col_idx BIGINT,"
        << "val TEXT,"
        << "code INT"
        << ");";
    auto q = ss.str();
    std::cout << q << std::endl;
    SQLite::Statement query(db, q);
    query.exec();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }

  RestoreFromDB();
}

void DictEncoding::RestoreFromDB() {
  // Read db block infos.
  try {
    auto& db = *info_store_->db;
    {
      SQLite::Statement q(db, "SELECT val, code FROM dict_encodings WHERE table_id=? AND col_idx=?");
      q.bind(1, table_id_);
      q.bind(2, col_idx_);
      std::cout << q.getExpandedSQL() << std::endl;
      while (q.executeStep()) {
        const char* val = q.getColumn(0);
        int code = q.getColumn(1);
        value_codes_.emplace(val, code);
        code_values_.emplace(code, val);
      }
    }
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

int DictEncoding::Insert(const std::string &val) {
  value_codes_[val] = 0;
  auto temp_code = curr_code_++;
  temp_codes_[temp_code] = val;
  return temp_code;
}

void DictEncoding::Finalize(Table *table) {
  // Compute codes.
  std::map<std::string, int> codes;
  int next_code = 0;
  for (const auto& [k, _]: value_codes_) {
    codes[k] = next_code;
    code_values_[next_code] = k;
    next_code++;
  }
  value_codes_ = codes;

  // Deal with tables.
  auto block_size = Settings::Instance()->BlockSize();
  std::vector<uint64_t> active_rows_bitmap(Bitmap::ComputeNumWords(block_size), 0);
  for (const auto& block_id: table->BlockIDS()) {
    auto block_info = table->BM()->Pin(block_id);
    auto raw_block = TableBlock::FromBlockInfo(block_info);
    auto col_data = raw_block->MutableColDataAs<int>(col_idx_);
    auto presence = raw_block->PresenceBitmap();
    auto null_bitmap = raw_block->BitmapData(col_idx_);
    Bitmap::Intersect(presence, null_bitmap, block_size, active_rows_bitmap.data());
    Bitmap::Map(active_rows_bitmap.data(), block_size, [&](sel_t row_idx) {
      int temp_code = col_data[row_idx];
      int code = value_codes_[temp_codes_[temp_code]];
      col_data[row_idx] = code;
    });
    table->BM()->Unpin(block_info, true);
  }
  temp_codes_.clear();

  // Store in db
  try {
    auto& db = *info_store_->db;
    {
      SQLite::Statement q(db, "INSERT INTO dict_encodings (table_id, col_idx, val, code) VALUES (?, ?, ?, ?)");
      for (const auto& [val, code]: value_codes_) {
        q.bind(1, table_id_);
        q.bind(2, col_idx_);
        q.bind(3, val);
        q.bind(4, code);
        std::cout << q.getExpandedSQL() << std::endl;
        q.exec();
        q.reset();
      }
    }
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }

}

std::string DictEncoding::GetVal(int code) {
  return code_values_.at(code);
}

int DictEncoding::GetCode(const std::string &val) {
  return value_codes_.at(val);
}

void DictEncoding::ToString(std::ostream &os) {
  os << "DictEncoding(num_codes=" << value_codes_.size() << ")" << std::endl;
  for (const auto& [val, code]: value_codes_) {
    os << val << " = " << code << std::endl;
  }
}
}
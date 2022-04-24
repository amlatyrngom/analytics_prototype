#pragma once
#include <string>
#include <map>
#include <unordered_map>

namespace smartid {
class InfoStore;
class Table;

class DictEncoding {
 public:
  DictEncoding(int64_t table_id, int64_t col_idx, InfoStore* info_store);

  int Insert(const std::string& val);

  void Finalize(Table* table);

  std::string GetVal(int code);

  int GetCode(const std::string& val);

  void ToString(std::ostream& os);

 private:
  void RestoreFromDB();

  int64_t table_id_;
  int64_t col_idx_;
  InfoStore* info_store_;
  std::map<std::string, int> value_codes_;
  std::map<int, std::string> code_values_;
  // Inefficient way to avoid ever putting varchars in table.
  std::unordered_map<int, std::string> temp_codes_;
  int curr_code_{0};
};

}

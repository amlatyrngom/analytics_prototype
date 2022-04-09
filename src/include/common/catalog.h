#pragma once
#include <unordered_map>
#include <memory>
#include <string>
#include <mutex>


namespace smartid {

class InfoStore;
class BufferManager;
class Table;
class Schema;
class WorkloadInfo;

/**
 * Catalog of tables and indices
 */
class Catalog {
 public:

  // Constructor
  Catalog(const std::string& workload_file);

  /**
   * Create a new table in the catalog.
   */
  Table *CreateTable(const std::string &name, const Schema *schema);

  /**
   * Return the table with the given name.
   */
  Table *GetTable(const std::string &name);

  InfoStore* GetInfoStore();

  static std::string SampleName(const std::string& table_name);

//  const TableStats* GetTableStats(const Table* table) const {
//    if (tables_stats_.find(table) == tables_stats_.end()) {
//      return nullptr;
//    }
//    return tables_stats_.at(table).get();
//  }
//
//  const TableStats* MakeTableStats(const Table* table, const std::vector<uint64_t>& wanted_cols, const std::vector<HistogramType>& wanted_hist_types) {
//    if (const auto& iter = tables_stats_.find(table); iter != tables_stats_.end()) {
//      if (iter->second->HasAll(wanted_cols, wanted_hist_types)) return iter->second.get();
//    }
//    auto table_stats = std::make_unique<TableStats>(table, wanted_cols, wanted_hist_types);
//    auto ret = table_stats.get();
//    tables_stats_.emplace(table, std::move(table_stats));
//    return ret;
//  }

  BufferManager* BM();

  ~Catalog();
 private:


  static std::string GetDBFilename();
  void RestoreFromDB();


  // Table Map
  // Persisted.
  int64_t curr_table_id_{0};
  std::unordered_map<int64_t, std::string> table_id_name_map_;
  std::unordered_map<std::string, int64_t> table_name_id_map_;
  std::unordered_map<int64_t, std::unique_ptr<Table>> tables_;

  // Table Stats
//  std::unordered_map<const Table*, std::unique_ptr<TableStats>> tables_stats_;

  std::unique_ptr<InfoStore> info_store_;
  std::unique_ptr<BufferManager> buffer_manager_;
  std::unique_ptr<WorkloadInfo> workload_info_;
  std::mutex catalog_m;
};
}
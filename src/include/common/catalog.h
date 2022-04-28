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
class RowIDIndex;

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
  Table *CreateTable(const std::string &name, const Schema *schema, bool add_row_id=true);

  // Remove all table blocks if table exists.
  Table *CreateOrClearTable(const std::string &name, const Schema *schema);

  void DeleteTable(const std::string& name);
  /**
   * Return the table with the given name.
   */
  Table *GetTable(const std::string &name);

  InfoStore* GetInfoStore();

  static std::string SampleName(const std::string& table_name);

  void AddIndex(const std::string& table_name, std::unique_ptr<RowIDIndex>&& index);
  RowIDIndex* GetIndex(const std::string& table_name);

  BufferManager* BM();
  WorkloadInfo* Workload();

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

  std::unordered_map<std::string, std::unique_ptr<RowIDIndex>> table_name_indexes_;
  std::unique_ptr<InfoStore> info_store_;
  std::unique_ptr<BufferManager> buffer_manager_;
  std::unique_ptr<WorkloadInfo> workload_info_;
  std::mutex catalog_m;
};
}
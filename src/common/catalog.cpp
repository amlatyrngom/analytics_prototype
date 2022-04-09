#include "common/catalog.h"
#include "common/info_store.h"
#include "storage/buffer_manager.h"
#include "storage/table.h"
#include "optimizer/workload_metadata.h"
#include "optimizer/workload_reader.h"

namespace smartid {


Table *Catalog::CreateTable(const std::string &name, const Schema *schema) {
  auto g = std::lock_guard(catalog_m);
  Schema copy_schema = *schema;
  auto table_id = curr_table_id_++;
  auto table = std::make_unique<Table>(table_id, name, std::move(copy_schema), buffer_manager_.get(), info_store_.get());
  tables_.emplace(table_id, std::move(table));
  table_id_name_map_.emplace(table_id, name);
  table_name_id_map_.emplace(name, table_id);
  return tables_.at(table_id).get();
}

Table *Catalog::GetTable(const std::string &name) {
  auto g = std::lock_guard(catalog_m);
  if (table_name_id_map_.find(name) == table_name_id_map_.end()) {
    return nullptr;
  }
  return tables_.at(table_name_id_map_.at(name)).get();
}

std::string Catalog::SampleName(const std::string &name) {
  return "__sample_" + name;
}

InfoStore *Catalog::GetInfoStore() {
  // No lock.
  return info_store_.get();
}

BufferManager *Catalog::BM() {
  // No lock.
  return buffer_manager_.get();
}

Catalog::Catalog(const std::string& workload_file) {
  auto settings = Settings::Instance();
  workload_info_ = WorkloadReader::ReadWorkloadInfo(workload_file);
  settings->SetDataFolder(workload_info_->data_folder);
  settings->SetBufferMem(1 << workload_info_->log_mem_space);
  settings->SetBufferDisk(1 << workload_info_->log_disk_space);
  settings->SetLogVecSize(workload_info_->log_vec_size);
  settings->SetLogBlockSize(workload_info_->log_block_size);


  bool reload = workload_info_->reload || !std::filesystem::exists(workload_info_->data_folder);
  if (reload) {
    std::filesystem::remove_all(workload_info_->data_folder);
    std::filesystem::create_directory(workload_info_->data_folder);
  }
  // Init Info Store and Buffer Manager.
  info_store_ = std::make_unique<InfoStore>();
  info_store_->db = std::make_unique<SQLite::Database>(GetDBFilename(), SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
  buffer_manager_ = std::make_unique<BufferManager>(info_store_.get());

  // Create table information.
  try {
    auto & db = *info_store_->db;
    {
      // Tables list
      std::stringstream ss;
      ss
          << "CREATE TABLE IF NOT EXISTS tables ("
          << "table_id BIGINT PRIMARY KEY,"
          << "table_name TEXT"
          << ");";
      auto q = ss.str();
      std::cout << q << std::endl;
      SQLite::Statement query(db, q);
      query.exec();
    }
    {
      // Schemas list
      std::stringstream ss;
      ss
          << "CREATE TABLE IF NOT EXISTS table_schemas ("
          << "table_id BIGINT,"
          << "col_name TEXT,"
          << "col_idx INT,"
          << "col_type TEXT,"
          << "FOREIGN KEY(table_id) REFERENCES tables(table_id)"
          << ");";
      auto q = ss.str();
      std::cout << q << std::endl;
      SQLite::Statement query(db, q);
      query.exec();
    }
    {
      std::stringstream ss;
      ss
          << "CREATE TABLE IF NOT EXISTS table_blocks ("
          << "table_id BIGINT,"
          << "block_id BIGINT,"
          << "FOREIGN KEY(table_id) REFERENCES tables(table_id)"
          << ");";
      auto q = ss.str();
      std::cout << q << std::endl;
      SQLite::Statement query(db, q);
      query.exec();
    }
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
  // RestoreFromDB.
  RestoreFromDB();
}

std::string Catalog::GetDBFilename() {
  auto settings = Settings::Instance();
  std::stringstream ss;
  ss << settings->DataFolder() << "/buffer_info.db" << settings->NodeID();
  return ss.str();
}

Catalog::~Catalog() {
  // Make sure tables are freed before the buffer manager.
  tables_.clear();
}

void Catalog::RestoreFromDB() {
  try {
    auto& db = *info_store_->db;
    SQLite::Statement q(db, "SELECT table_id, table_name FROM tables;");
    while (q.executeStep()) {
      int64_t table_id = q.getColumn(0);
      const char* name = q.getColumn(1);
      // Empty schema means table will be restored from db.
      auto table = std::make_unique<Table>(table_id, name, Schema({}), buffer_manager_.get(), info_store_.get());
      tables_.emplace(table_id, std::move(table));
      table_name_id_map_[name] = table_id;
      table_id_name_map_[table_id] = name;
      if (table_id > curr_table_id_) {
        curr_table_id_ = table_id + 1;
      }
    }
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

}
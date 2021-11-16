#pragma once
#include <unordered_map>
#include <memory>
#include "storage/table.h"
#include "stats/table_stats.h"

namespace smartid {
/**
 * Catalog of tables and indices
 */
class Catalog {
 public:
  /**
   * TODO(Amadou): Redesign if singleton becomes a problem.
   * @return Singleton Instance.
   */
  static Catalog *Instance() {
    static Catalog instance;
    return &instance;
  }

  /**
   * Create a new table in the catalog.
   */
  Table *CreateTable(const std::string &name, Schema &&schema) {
    auto table = std::make_unique<Table>(name, std::move(schema));
    auto ret = table.get();
    tables_.emplace(name, std::move(table));
    return ret;
  }

  /**
   * Return the table with the given name.
   */
  Table *GetTable(const std::string &name) {
    if (tables_.find(name) == tables_.end()) {
      return nullptr;
    }
    return tables_.at(name).get();
  }

  const TableStats* GetTableStats(const Table* table) const {
    if (tables_stats_.find(table) == tables_stats_.end()) {
      return nullptr;
    }
    return tables_stats_.at(table).get();
  }

  const TableStats* MakeTableStats(const Table* table, const std::vector<uint64_t>& wanted_cols, const std::vector<HistogramType>& wanted_hist_types) {
    if (const auto& iter = tables_stats_.find(table); iter != tables_stats_.end()) {
      if (iter->second->HasAll(wanted_cols, wanted_hist_types)) return iter->second.get();
    }
    auto table_stats = std::make_unique<TableStats>(table, wanted_cols, wanted_hist_types);
    auto ret = table_stats.get();
    tables_stats_.emplace(table, std::move(table_stats));
    return ret;
  }

  void SetParallelismLevel(uint64_t level) {
    parallelism_level_ = level;
  }

  uint64_t  GetParallelismLevel() const {
    return parallelism_level_;
  }

 private:
  // Private Constructor prevents initialization.
  Catalog() {}

  // Table Map
  std::unordered_map<std::string, std::unique_ptr<Table>> tables_;

  // Table Stats
  std::unordered_map<const Table*, std::unique_ptr<TableStats>> tables_stats_;

  // Parallelism Level
  uint64_t parallelism_level_{1};
};
}
#pragma once
#include <unordered_map>
#include "common/types.h"


namespace smartid {
class Catalog;
class TableInfo;
class WorkloadInfo;
class Vector;
class Bitmap;
class EquiDepthHistogram;

struct ColumnStats {

  // Allows pre-declaration.
  ~ColumnStats();
  void ToString(std::ostream& os) const;

  // Persisted somehow.
  int64_t table_id;
  int64_t col_idx;
  SqlType col_type;
  int64_t count_;
  int64_t count_distinct_;
  int64_t max_freq_;
  double top_freq_;
  std::unique_ptr<EquiDepthHistogram> hist_;
  std::vector<Value> value_samples_; // For randomized execution.
  // Not persisted. Maybe read from hist_.
  Value min_;
  Value max_;
};

struct TableStatistics {
  ~TableStatistics();

  // Persisted.
  int64_t table_id;
  double num_tuples;
  double sample_size;
  std::vector<std::unique_ptr<ColumnStats>> column_stats;

  static void GenerateAllStats(Catalog* catalog, WorkloadInfo* workload);
  static void ComputeTableStats(Catalog* catalog, TableInfo* table_info);
  static void RestoreTableStats(Catalog* catalog, TableInfo* table_info);
  static void GenerateMatViewStats(Catalog* catalog);

  void ToString(std::ostream& os) const;
};

}
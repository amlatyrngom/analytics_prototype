#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include "common/types.h"

namespace smartid {

struct TableInfo {
  // Constructor.
  TableInfo(std::string name, Schema&& schema, std::string datafile, std::unordered_set<std::string>&& pks, bool to_many)
      : name(std::move(name))
      , schema(std::move(schema))
      , datafile(std::move(datafile))
      , pks(pks)
      , to_many(to_many) {}

  std::string name;
  Schema schema;
  std::string datafile;
  std::unordered_set<std::string> pks;
  bool to_many;

  // Computed during statistics collection.
  double sample_rate{0};
  double base_size{0};
};

struct LogicalFilter {
  std::string col_name;
  std::string op;
  bool lo_strict{false};
  bool hi_strict{false};
  std::vector<std::string> vals; // should contain a single '?' when vals should be sampled.

  void ToString(std::ostream& os) const;
};

struct LogicalScanNode {
  TableInfo* table_info{nullptr};
  std::vector<LogicalFilter> filters;
  std::vector<std::string> projections;

  void ToString(std::ostream& os) const;

//  double output_size{std::numeric_limits<double>::max()};
//  double cost{std::numeric_limits<double>::max()};
//  double base_size{std::numeric_limits<double>::max()};
};

struct LogicalJoinNode {
  // Only one of left_join and left_scan is allowed to be non-null.
  LogicalJoinNode* left_join{nullptr};
  LogicalScanNode* left_scan{nullptr};
  // Right table is always a table scan or an index.
  LogicalScanNode* right_scan{nullptr};
  std::vector<TableInfo*> scanned_tables;
//  double output_size{std::numeric_limits<double>::max()};
//  double cost{std::numeric_limits<double>::max()};
};

struct QueryInfo {
  std::string name;
  bool count{false};
  std::vector<LogicalScanNode*> scans;
  std::vector<LogicalJoinNode*> join_orders; // empty when scans.size() == 1;

  // Print to stream.
  void ToString(std::ostream& os) const;

  // Copy a scan using given allocator.
  LogicalScanNode* CopyScan(std::vector<std::unique_ptr<LogicalScanNode>>& scan_allocator);

  // Copy join order using given allocator.
  std::vector<LogicalJoinNode*> CopyJoinOrders(std::vector<std::unique_ptr<LogicalScanNode>>& scan_allocator, std::vector<std::unique_ptr<LogicalJoinNode>>& join_allocator);

 private:
  // Copy join order using given allocator.
  static LogicalJoinNode* RecursiveCopy(LogicalJoinNode* src, std::vector<std::unique_ptr<LogicalScanNode>>& scan_allocator, std::vector<std::unique_ptr<LogicalJoinNode>>& join_allocator);

 public:
  std::vector<std::unique_ptr<LogicalScanNode>> allocated_scans;
  std::vector<std::unique_ptr<LogicalJoinNode>> allocated_joins;
};

struct WorkloadInfo {
  std::string workload_name;
  std::string central_table_name;
  std::string input_folder;
  std::string data_folder;
  std::string workload_file;
  uint64_t log_vec_size;
  uint64_t log_block_size;
  uint64_t log_mem_space;
  uint64_t log_disk_space;
  bool reload{false};
  std::unordered_map<std::string, std::unique_ptr<TableInfo>> table_infos;
  std::unordered_map<std::string, std::unique_ptr<QueryInfo>> query_infos;
};
}
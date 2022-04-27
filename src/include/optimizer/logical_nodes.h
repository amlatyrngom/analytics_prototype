#pragma once

#include "common/types.h"
#include "execution/execution_types.h"
#include <set>

namespace smartid {
class TableInfo;
class ExprNode;
class ExecutionFactory;


// Contains all variables to avoid inheritence pattern. Too tedious to write.
struct LogicalFilter {
  // Shared by all
  ExprType expr_type;
  std::string expr_name;
  std::string col_name;
  uint64_t col_idx;
  SqlType col_type;
  std::string param_name; // param_name0, param_name1, etc.
  std::vector<Value> vals;
  bool should_sample; // When sample_params is false, then vals must correctly initialized values.
  ExprNode* expr_node; // Only execute after parameters are bound.

  // By between
  bool left_closed{false};
  bool right_closed{false};
  int range{-1};

  // By regular binary comp.
  OpType op_type{OpType::EQ};

  // Computed during estimation. Needed for scan estimation and filter ordering.
  double estimated_sel{-1};

  // Helper.
  void ToString(std::ostream& os) const;
};





struct LogicalScanNode {
  TableInfo* table_info{nullptr};
  std::unordered_map<uint64_t, uint64_t> col_idx_read_idxs; // Used to map col idx to read idx.
  std::unordered_map<uint64_t, uint64_t> col_idx_proj_idxs; // Used to map col idx to proj idx.
  std::vector<uint64_t> cols_to_read;
  std::vector<ExprNode*> projections;
  std::vector<LogicalFilter> filters;

  // Needed by upper nodes.
  std::unordered_map<uint64_t, std::string> proj_idx_full_name; // Used map proj idx to full col name.
  std::unordered_map<uint64_t, uint64_t> join_key_proj_idx; // Map from join key to proj idx.
  std::unordered_map<uint64_t, uint64_t> proj_idx_join_keys; // Map from proj idx to join key.
  std::unordered_map<uint64_t, bool> proj_idx_should_output; // boolean indicates whether column is part of join output.

  // Computed during estimation. Need for join ordering.
  double estimated_output_size{-1.0};
  double base_table_size{-1.0};

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
  std::set<std::string> scanned_tables_set;

  // Info for physical joins.
  uint64_t build_key_idx;
  uint64_t probe_key_idx;
  std::vector<std::pair<uint64_t, uint64_t>> projections;
  // Need by upper nodes.
  std::unordered_map<uint64_t, std::string> proj_idx_full_name; // Used map proj idx to full col name.
  std::unordered_map<uint64_t, uint64_t> join_key_proj_idx; // Map from join key to proj idx.
  std::unordered_map<uint64_t, uint64_t> proj_idx_join_keys; // Map from proj idx to join key.
  std::unordered_map<uint64_t, bool> proj_idx_should_output; // boolean indicates whether column is part of join output.

  // Estimated by
  double estimated_output_size{-1};
  double estimated_freq{-1};
  double total_materialized_size{-1};
  double estimated_denorm_size{-1};

  void ToString(std::ostream& os) const;
};

struct QueryInfo {
  std::string name;
  bool count{false};
  bool is_mat_view{false};
  bool for_training{true};
  bool for_running{true};
  std::vector<LogicalScanNode*> scans;
  std::vector<LogicalJoinNode*> join_orders; // empty when scans.size() == 1;
  LogicalJoinNode* best_join_order{nullptr};

  // Print to stream.
  void ToString(std::ostream& os) const;

  bool IsRegularJoin() const {
    return scans.size() > 1 && !is_mat_view;
  }

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
  ExecutionFactory* execution_factory;
};

}
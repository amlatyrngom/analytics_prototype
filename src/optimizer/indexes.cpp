#include "optimizer/indexes.h"
#include "optimizer/default.h"
#include "optimizer/table_statistics.h"
#include "common/catalog.h"
#include "common/settings.h"
#include "storage/table.h"
#include "storage/index.h"
#include "storage/vector_projection.h"
#include "storage/vector.h"
#include "optimizer/workload_metadata.h"
#include "optimizer/to_physical.h"
#include <set>
#include <map>
#include <string>
#include <regex>
#include <fstream>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "execution/nodes/plan_node.h"
#include "execution/nodes/expr_node.h"
#include "execution/nodes/scan_node.h"
#include "execution/nodes/hash_join_node.h"
#include "execution/execution_factory.h"



namespace smartid {

void Indexes::GenerateCostsForOptimization(Catalog *catalog) {
  auto workload = catalog->Workload();
  if (!workload->gen_costs) return;
  {
    std::ofstream os(workload->data_folder + "/index_join_cost.csv");
    Indexes::GenerateRowIDIndexCosts(catalog, os);
  }
}


std::string ParseIdxName(const std::string& idx_name) {
  std::string prefix("index_");
  std::string suffix = idx_name.substr(prefix.size(), idx_name.size() - prefix.size());
  return suffix;
}

void InsertByType(const VectorProjection* vp, RowIDIndex* index, SqlType key_type) {
  if (key_type == SqlType::Int32) {
    auto& index_table = *index->GetIndex32();
    auto key_data = vp->VectorAt(0)->DataAs<int32_t>();
    auto rowid_data = vp->VectorAt(1)->DataAs<int64_t>();
    vp->GetFilter()->Map([&](sel_t i) {
      index_table[key_data[i] & RowIDIndex::KEY_MASK32].emplace_back(rowid_data[i]);
    });
  }
  if (key_type == SqlType::Int64) {
    auto& index_table = *index->GetIndex64();
    auto key_data = vp->VectorAt(0)->DataAs<int64_t>();
    auto rowid_data = vp->VectorAt(1)->DataAs<int64_t>();
    vp->GetFilter()->Map([&](sel_t i) {
      index_table[key_data[i] & RowIDIndex::KEY_MASK64].emplace_back(rowid_data[i]);
    });
  }
}

double BuildIndex(Catalog* catalog, const std::string& table_name) {
//  if (table_name != "title") return 0.0;
  auto table = catalog->GetTable(table_name);
  ExecutionFactory factory(catalog);
  fmt::print("Build idx for {}\n", table_name);
  std::string row_id_col_name("row_id");
  auto row_id_col_idx = table->GetSchema().ColIdx(row_id_col_name);
  uint64_t key_col_idx{0};
  SqlType key_col_type;
  auto& central_table_name = catalog->Workload()->central_table_name;
  if (table_name == central_table_name) {
    for (const auto& col: table->GetSchema().Cols()) {
      if (col.IsPK()) {
        key_col_type = col.Type();
        break;
      }
      key_col_idx++;
    }
  } else {
    for (const auto& col: table->GetSchema().Cols()) {
      if (col.IsFK()) {
        key_col_type = col.Type();
        break;
      }
      key_col_idx++;
    }
  }
  ScanNode* table_scan;
  {
    // Indexes on fk.
    std::vector<uint64_t> cols_to_read{key_col_idx, row_id_col_idx};
    auto key_col = factory.MakeCol(0);
    auto row_id_col = factory.MakeCol(1);
    std::vector<ExprNode*> projs;
    projs.emplace_back(key_col);
    projs.emplace_back(row_id_col);
    std::vector<ExprNode*> filters;
    table_scan = factory.MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  auto scan_executor = ExecutionFactory::MakePlanExecutor(table_scan);
  auto row_id_idx = std::make_unique<RowIDIndex>();
  const VectorProjection* vp;
  auto start = std::chrono::high_resolution_clock::now();
  while ((vp = scan_executor->Next()) != nullptr) {
    InsertByType(vp, row_id_idx.get(), key_col_type);
  }
  catalog->AddIndex(table_name, std::move(row_id_idx));
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
  // Index Content.
  fmt::print("Index {} content for debugging: \n", table_name);
  if (key_col_type == SqlType::Int32) {
    catalog->GetIndex(table_name)->PrinTop32(10);
  } else {
    catalog->GetIndex(table_name)->PrinTop64(10);
  }
  return double(duration) / double(1e9);
}

void Indexes::BuildAllKeyIndexes(Catalog* catalog) {
  // Read materialized views.
  auto workload = catalog->Workload();
  if (workload->gen_costs || workload->reload) return;
  std::set<std::string> idxs_to_build;
  bool prev_built = !workload->available_idxs.empty();
  workload->available_idxs.clear();
  uint64_t budget = workload->budget * (1 << 26);

  std::string index_names_file(fmt::format("{}/opts/index_{}.csv", workload->data_folder, budget));
  std::ifstream is(index_names_file);
  std::vector<std::string> index_names;
  std::string index_name;
  while ((is >> index_name)) {
    auto table_name = ParseIdxName(index_name);
    workload->available_idxs.emplace(table_name);
  }

  if (prev_built) return; // Do not rebuild indexes.
  std::string index_build_times(fmt::format("{}/index_build_times.csv", workload->data_folder));
  std::ofstream os(index_build_times);
  for (const auto& [table_name, _]: workload->table_infos) {
    double duration = BuildIndex(catalog, table_name);
    os << fmt::format("{},{}\n", table_name, duration);
  }
}

double RecursiveIndexSavings(const std::string& table_name, LogicalJoinNode* logical_join_node) {
  auto scan_discount = Settings::Instance()->ScanDiscount();
  auto idx_overhead = Settings::Instance()->IdxLookupOverhead();
  auto right_scan = logical_join_node->right_scan;
  if (table_name == right_scan->table_info->name) {
    double hj_cost = Default::RecursiveEstimateDefault(logical_join_node);
    double output_size = logical_join_node->estimated_output_size;
    double left_size;
    double left_cost;
    if (logical_join_node->left_scan != nullptr) {
      left_size = logical_join_node->left_scan->estimated_output_size;
      left_cost = scan_discount * logical_join_node->left_scan->base_table_size;
    } else {
      left_size = logical_join_node->left_join->estimated_output_size;
      left_cost = Default::RecursiveEstimateDefault(logical_join_node->left_join);
    }
    double idx_join_cost = left_cost + idx_overhead * left_size * std::max(double(1.0), output_size / left_size);
    return std::max(hj_cost - idx_join_cost, double(0.0));
  }
  auto left_scan = logical_join_node->left_scan;
  if (left_scan != nullptr && left_scan->table_info->name == table_name) {
    double hj_cost = Default::RecursiveEstimateDefault(logical_join_node);
    double output_size = logical_join_node->estimated_output_size;
    double right_size = right_scan->estimated_output_size;
    double right_cost = scan_discount * right_scan->base_table_size;
    double idx_join_cost = right_cost + idx_overhead * right_size * std::max(double(1.0), output_size / right_size);
    return std::max(hj_cost - idx_join_cost, double(0.0));
  }

  if (left_scan == nullptr) {
    return RecursiveIndexSavings(table_name, logical_join_node->left_join);
  }

  // This index does not affect this query.
  return 0.0;
}

void Indexes::GenerateRowIDIndexCosts(Catalog *catalog, std::ostream &os) {
  std::map<std::string, double> index_sizes;
  auto workload = catalog->Workload();
  auto central_table_name = workload->central_table_name;
  auto central_table = catalog->GetTable(central_table_name);
  auto central_table_stats = central_table->GetStatistics();
  for (const auto& [table_name, table_info]: workload->table_infos) {
    auto table = catalog->GetTable(table_name);
    auto table_stats = table->GetStatistics();
    // Worst-case estimated index size: 64 bits key sizes and val sizes.
    double idx_size = double(sizeof(uint64_t)) * (table_stats->num_tuples + central_table_stats->num_tuples);
    index_sizes[table_name] = idx_size;
  }


  for (const auto& [idx_name, idx_size]: index_sizes) {
    for (const auto&[q_name, query_info]: workload->query_infos) {
      if (!query_info->IsRegularJoin()) continue;
      if (!query_info->for_training) continue;
      double cost_savings = RecursiveIndexSavings(idx_name, query_info->best_join_order);
      auto str = fmt::format("index_{},{},{},{}\n", idx_name, idx_size, q_name, cost_savings);
      os << str;
    }
  }
}


std::pair<std::unordered_set<std::string>, double> RecursiveFindIndexesToUse(LogicalJoinNode* logical_join, const std::unordered_set<std::string> available_idxs) {
  auto scan_discount = Settings::Instance()->ScanDiscount();
  auto idx_overhead = Settings::Instance()->IdxLookupOverhead();
  auto output_size = logical_join->estimated_output_size;
  auto right_scan = logical_join->right_scan;
  auto right_output_size = right_scan->estimated_output_size;
  auto right_base_size = right_scan->base_table_size;
  if (logical_join->left_join != nullptr) {
    auto [to_use, left_cost] = RecursiveFindIndexesToUse(logical_join->left_join, available_idxs);
    auto left_output_size = logical_join->left_join->estimated_output_size;
    double hj_cost = output_size + left_cost + scan_discount * right_base_size;
    double idx_cost = left_cost + idx_overhead * left_output_size * std::max(double(1.0), output_size / left_output_size);
    double best_cost;
    if (idx_cost <= hj_cost && available_idxs.contains(right_scan->table_info->name)) {
      best_cost = idx_cost;
      to_use.emplace(right_scan->table_info->name);
      fmt::print("Using index on {}\n", right_scan->table_info->name);
    } else {
      best_cost = hj_cost;
      fmt::print("Not using index on {}\n", right_scan->table_info->name);
      fmt::print("hj_cost={}, idx_cost={}, left_cost={}, left_output_size={}, output_size={}\n", hj_cost, idx_cost, left_cost, left_output_size, output_size);
    }
    return {to_use, best_cost};
  }
  // If both sides are table scan, check which is best used has index.
  std::unordered_set<std::string> to_use;
  auto left_scan = logical_join->left_scan;
  auto left_output_size = left_scan->estimated_output_size;
  auto left_base_size = left_scan->base_table_size;
  double hj_cost = output_size + scan_discount * left_base_size + scan_discount * right_base_size;
  double left_idx_cost{std::numeric_limits<double>::max()};
  double right_idx_cost{std::numeric_limits<double>::max()};
  if (available_idxs.contains(left_scan->table_info->name)) {
    left_idx_cost = scan_discount*right_base_size + idx_overhead * right_output_size * std::max(double(1.0), output_size / right_output_size);
  }
  if (available_idxs.contains(right_scan->table_info->name)) {
    right_idx_cost = scan_discount*left_base_size + idx_overhead * left_output_size * std::max(double(1.0), output_size / left_output_size);
  }
  if (left_idx_cost <= hj_cost && left_idx_cost <= right_idx_cost) {
    to_use.emplace(left_scan->table_info->name);
    fmt::print("Using index on {}\n", left_scan->table_info->name);
    return {to_use, left_idx_cost};
  }
  if (right_idx_cost <= hj_cost && right_idx_cost <= left_idx_cost) {
    to_use.emplace(right_scan->table_info->name);
    fmt::print("Using index on {}\n", right_scan->table_info->name);
    return {to_use, right_idx_cost};
  }
  return {to_use, hj_cost};
}

PlanNode* Indexes::GenerateBestPlanWithIndexes(Catalog *catalog,
                                          QueryInfo *query_info,
                                          ExecutionFactory *factory,
                                          ExecutionContext *exec_ctx) {
  if (!query_info->IsRegularJoin()) return nullptr;
  auto workload = catalog->Workload();
  LogicalJoinNode* best_logical_plan = query_info->best_join_order;
  auto [to_use, cost] = RecursiveFindIndexesToUse(best_logical_plan, workload->available_idxs);
  return ToPhysical::MakePhysicalPlanWithIndexes(catalog, best_logical_plan, to_use, factory, exec_ctx);
}


}
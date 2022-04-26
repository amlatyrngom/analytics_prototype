#include "optimizer/materialized_views.h"
#include "optimizer/default.h"
#include "common/catalog.h"
#include "common/settings.h"
#include "storage/table.h"
#include "optimizer/logical_nodes.h"
#include "optimizer/workload_metadata.h"
#include "optimizer/table_statistics.h"
#include "optimizer/to_physical.h"
#include "execution/execution_factory.h"
#include "optimizer/workload_reader.h"
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <toml++/toml.h>

namespace smartid {
void RecursiveDematList(std::map<DematTablesSet, std::pair<double, double>>& res, LogicalJoinNode* logical_join) {
  DematTablesSet table_set;
  double total_tuple_size{0.0};
  for (const auto& table_info: logical_join->scanned_tables) {
    table_set.emplace(table_info->name);
    double tuple_size{0.0};
    for (const auto& col_idx: table_info->used_cols) {
      tuple_size += double(TypeUtil::TypeSize(table_info->schema.GetColumn(col_idx).Type()));
    }
//    fmt::print("Tuple Size {}: {}\n", table_info->name, tuple_size);
    total_tuple_size += tuple_size;
  }
  logical_join->scanned_tables_set = table_set;
  res[table_set] = {logical_join->estimated_denorm_size, logical_join->estimated_denorm_size * total_tuple_size};
  if (logical_join->left_join != nullptr) {
    RecursiveDematList(res, logical_join->left_join);
  }
}


double RecursiveEstimateDemat(const DematTablesSet& tables_set, const LogicalJoinNode* logical_join) {
  auto scan_discount = Settings::Instance()->ScanDiscount();
  if (logical_join->scanned_tables_set == tables_set) {
    return scan_discount * logical_join->estimated_denorm_size;
  }
  // Right scan
  double right_cost = scan_discount * logical_join->right_scan->base_table_size;
  double left_cost;
  if (logical_join->left_scan != nullptr) {
    left_cost = scan_discount * logical_join->left_scan->base_table_size;
  } else {
    left_cost = RecursiveEstimateDemat(tables_set, logical_join->left_join);
  }
  return right_cost + left_cost + logical_join->estimated_output_size;
}



std::pair<bool, double> EstimateCostWithMatView(LogicalJoinNode* logical_join, const std::string& mat_view) {
  if (logical_join->scanned_tables_set.empty()) {
    for (const auto& t: logical_join->scanned_tables) logical_join->scanned_tables_set.emplace(t->name);
  }
  auto table_set_name = fmt::format("mat_view_{}", fmt::join(logical_join->scanned_tables_set, "___"));
  auto scan_discount = Settings::Instance()->ScanDiscount();
  if (table_set_name == mat_view) {
    fmt::print("Cost with mat view {}: {}; Size is {}\n", table_set_name, scan_discount * logical_join->estimated_denorm_size, logical_join->estimated_denorm_size);
    return {true, scan_discount * logical_join->estimated_denorm_size};
  }
  double right_cost = scan_discount * logical_join->right_scan->base_table_size;
  double left_cost;
  bool uses_mat{false};
  if (logical_join->left_scan != nullptr) {
    left_cost = scan_discount * logical_join->left_scan->base_table_size;
    uses_mat = false;
  } else {
    const auto [x1, x2] = EstimateCostWithMatView(logical_join->left_join, mat_view);
    uses_mat = x1;
    left_cost = x2;
  }
  return {uses_mat, right_cost + left_cost + logical_join->estimated_output_size};
}

PlanNode* MaterializedViews::GenBestPlanWithMaterialization(Catalog* catalog, QueryInfo* query_info, ExecutionFactory* factory, ExecutionContext* exec_ctx) {
  auto workload = catalog->Workload();
  if (!query_info->IsRegularJoin()) return nullptr;
  LogicalJoinNode* best_logical_plan = query_info->best_join_order;
  double best_cost{Default::RecursiveEstimateDefault(best_logical_plan)};
  std::string best_demat{};
  for (const auto& mat_view: workload->available_mat_views) {
    for (auto& logical_join: query_info->join_orders) {
      const auto& [used_mat, cost] = EstimateCostWithMatView(logical_join, mat_view);
      fmt::print("cost={}, best_cost={}.\n", cost, best_cost);
      if (cost < best_cost) {
        best_demat = mat_view;
        best_cost = cost;
        best_logical_plan = logical_join;
      }
    }
  }
  return ToPhysical::MakePhysicalPlanWithMat(catalog, best_logical_plan, best_demat, factory, exec_ctx);
}


void MaterializedViews::GenerateMateralizationCosts(Catalog *catalog, std::ostream& os, std::ostream& toml_os) {
  std::map<DematTablesSet, std::pair<double, double>> res;
  auto workload = catalog->Workload();
  for (const auto& [q_name, query_info]: workload->query_infos) {
    if (!query_info->IsRegularJoin()) continue;
    for (const auto& logical_join: query_info->join_orders) {
      RecursiveDematList(res, logical_join);
    }
  }
//  fmt::print("DematLists: {}\n", res);

  for (const auto& [tables_set, total_demat_size]: res) {
    auto demat_num_rows = total_demat_size.first;
    auto demat_bytes = total_demat_size.second;
    for (const auto& [q_name, query_info]: workload->query_infos) {
      if (!query_info->IsRegularJoin()) continue;
      double best_cost{std::numeric_limits<double>::max()};
      for (const auto& logical_join: query_info->join_orders) {
        double cost = RecursiveEstimateDemat(tables_set, logical_join);
        if (cost < best_cost) {
          best_cost = cost;
        }
      }
      auto str = fmt::format("mat_view_{},{},{},{},{}\n", fmt::join(tables_set, "___"), demat_num_rows, demat_bytes, q_name, best_cost);
      os << str;
    }
  }

  // Generate mat views toml files.
  auto res_toml = toml::table{};
  auto queries_toml = toml::table{};
  for (const auto& [table_set, _]: res) {
    auto query_tbl = toml::table{};
    auto query_name = fmt::format("mat_view_{}", fmt::join(table_set, "___"));
    query_tbl.emplace<std::string, std::string>("type", "join");
    query_tbl.insert("is_mat_view", true);
    for (const auto& table_name: table_set) {
      auto table_info = workload->table_infos.at(table_name).get();
      auto scan_tbl = toml::table{};
      scan_tbl.emplace<std::string, std::string>("type", "scan");
      scan_tbl.emplace<std::string, std::string>("table", table_name);
      auto inputs_arr = toml::array{};
      auto projections_arr = toml::array{};
      for (const auto& col_idx: table_info->used_cols) {
        const auto& col_name = table_info->schema.GetColumn(col_idx).Name();
        inputs_arr.emplace_back<std::string>(col_name);
        projections_arr.emplace_back<std::string>(col_name);
      }
      scan_tbl.insert("inputs", inputs_arr);
      scan_tbl.insert("projections", projections_arr);
      query_tbl.insert(fmt::format("scan_{}", table_name), scan_tbl);
    }
    queries_toml.insert(query_name, query_tbl);
  }
  res_toml.insert("queries", queries_toml);
  toml_os << res_toml;
}






void MaterializedViews::GenerateCostsForOptimization(Catalog *catalog) {
  auto workload = catalog->Workload();
  if (!workload->gen_costs) return;
  {
    std::ofstream os(workload->data_folder + "/mat_cost.csv");
    std::ofstream toml_os(workload->data_folder + "/mat_views.toml");
    MaterializedViews::GenerateMateralizationCosts(catalog, os, toml_os);
  }
//  std::string script_file(fmt::format("python ./{}/gen_best_opt.py", workload->input_folder));
//  system(script_file.c_str());
}

void MaterializedViews::BuildAllValuableMatViews(Catalog *catalog) {
  // Read materialized views.
  auto workload = catalog->Workload();
  std::set<std::string> mat_views_to_build;
  workload->available_mat_views.clear();
  std::vector<uint64_t> possible_budgets{2, 4, 6, 8, 10, 16, 32, 48, 64, 80, 96, 112, 128};
  for (const auto& i: possible_budgets) {
    uint64_t budget = i * (1ull << 26);
    std::string mat_view_names_file(fmt::format("{}/opts/mat_view_{}.csv", workload->data_folder, budget));
    std::ifstream is(mat_view_names_file);
    std::vector<std::string> mat_view_names;
    std::string mat_view_name;
    while ((is >> mat_view_name)) {
      mat_views_to_build.emplace(mat_view_name);
      if (i == workload->budget) {
        workload->available_mat_views.emplace(mat_view_name);
      }
    }
  }
  if (!workload->rebuild) return;
  std::string mat_views_toml_file(fmt::format("{}/mat_views.toml", workload->data_folder));
  ExecutionFactory factory(catalog);
  QueryReader::ReadWorkloadQueries(catalog, workload, &factory, {mat_views_toml_file});
  std::string mat_view_build_times(fmt::format("{}/mat_views_build_times.csv", workload->data_folder));
  std::ofstream os(mat_view_build_times);
  for (auto& [query_name, query_info]: workload->query_infos) {
    if (mat_views_to_build.contains(query_name)) {
      ToPhysical::EstimateScanSelectivities(catalog, query_info.get());
      ToPhysical::ResolveJoins(catalog, workload, query_info.get(), &factory);
      ToPhysical::FindBestJoinOrder(query_info.get());
      std::cout << "Found: " << query_info->name << std::endl;
      query_info->best_join_order->ToString(std::cout);

      double duration = ToPhysical::MakeMaterializedView(catalog, query_info.get());
      os << fmt::format("{},{}\n", query_name, duration);
    }
  }
}

}
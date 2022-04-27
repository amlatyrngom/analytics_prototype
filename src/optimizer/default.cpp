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

double Default::RecursiveEstimateDefault(const LogicalJoinNode* logical_join) {
  auto scan_discount = Settings::Instance()->ScanDiscount();
  // Right scan
  double right_cost = scan_discount * logical_join->right_scan->base_table_size;
  double left_cost;
  if (logical_join->left_scan != nullptr) {
    left_cost = scan_discount * logical_join->left_scan->base_table_size;
  } else {
    left_cost = RecursiveEstimateDefault(logical_join->left_join);
  }
  return right_cost + left_cost + logical_join->estimated_output_size;
}

void Default::GenerateDefaultCosts(Catalog *catalog, std::ostream& os) {
  auto workload = catalog->Workload();
  for (auto& [q_name, query_info]: workload->query_infos) {
    if (!query_info->IsRegularJoin()) continue;
    double best_cost{std::numeric_limits<double>::max()};
    LogicalJoinNode* best_order{nullptr};
    for (const auto& logical_join: query_info->join_orders) {
      double cost = Default::RecursiveEstimateDefault(logical_join);
      if (cost < best_cost) {
        best_cost = cost;
        best_order = logical_join;
      }
    }
    query_info->best_join_order = best_order;
    std::cout << "Best Order" << std::endl;
    best_order->ToString(std::cout);
    auto str = fmt::format("{},{},{},{}\n", "default", 0, q_name, best_cost);
    os << str;
  }
}

void Default::GenerateCostsForOptimization(Catalog *catalog) {
  auto workload = catalog->Workload();
//  if (!workload->gen_costs) return;
  {
    std::ofstream os(workload->data_folder + "/default_cost.csv");
    Default::GenerateDefaultCosts(catalog, os);
  }
}

}
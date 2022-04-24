#pragma once

#include <set>
#include <string>
#include <vector>
#include <map>


namespace smartid {
class LogicalJoinNode;
class QueryInfo;
class Catalog;
class ExecutionFactory;
class ExecutionContext;
class PlanNode;


using DematTablesSet = std::set<std::string>;

struct MaterializedViews {
  static void GenerateMateralizationCosts(Catalog* catalog, std::ostream& os, std::ostream& toml_os);
  static void GenerateCostsForOptimization(Catalog* catalog);
  static void BuildAllValuableMatViews(Catalog* catalog);
  static PlanNode* GenBestPlanWithMaterialization(Catalog* catalog, QueryInfo* query_info, ExecutionFactory* factory, ExecutionContext* exec_ctx);
};
}
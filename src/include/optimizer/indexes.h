#pragma once
#include <string>

namespace smartid {
class Catalog;
class QueryInfo;
class ExecutionFactory;
class ExecutionContext;
class PlanNode;

struct Indexes {
  static void BuildAllKeyIndexes(Catalog* catalog);
  static void GenerateRowIDIndexCosts(Catalog* catalog, std::ostream& os);
  static void GenerateCostsForOptimization(Catalog* catalog);
  static PlanNode* GenerateBestPlanWithIndexes(Catalog* catalog, QueryInfo* query_info, ExecutionFactory* factory, ExecutionContext* exec_ctx);
};

}
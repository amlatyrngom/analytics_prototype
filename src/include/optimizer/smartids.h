#pragma once
#include <iostream>

namespace smartid {

class Catalog;
class QueryInfo;
class PlanNode;
class ExecutionFactory;
class ExecutionContext;



struct SmartIDOptimizer {
  static void GatherFilters(Catalog* catalog, std::ostream& table_os, std::ostream& filter_os);
  static void GenerateCostsForOptimization(Catalog* catalog);
  static void BuildSmartIDs(Catalog* catalog);
  static PlanNode* GenerateBestPlanWithSmartIDs(Catalog* catalog, QueryInfo* query_info, ExecutionFactory* factory, ExecutionContext* exec_ctx);

};
}
#pragma once
#include "common/types.h"
#include <unordered_set>

namespace smartid {
class Table;
class LogicalFilter;
class LogicalJoinNode;
class ExecutionContext;
class ExecutionFactory;
class ScanNode;
class PlanNode;
class QueryInfo;
class Catalog;
class WorkloadInfo;

enum class FreqType {
  MAX, TOP, AVG
};

struct ToPhysical {
  static void BindParams(Table* table, LogicalFilter* filter, ExecutionContext* exec_ctx);
  static void EstimateFilterSelectivity(Table* table, LogicalFilter* filter);
  static void EstimateScanSelectivities(Catalog* catalog, QueryInfo* query_info);
  static void ResolveJoin(Catalog* catalog, WorkloadInfo* workload, LogicalJoinNode* logical_join, ExecutionFactory* factory);
  static void ResolveJoins(Catalog* catalog, WorkloadInfo* workload, QueryInfo* query_info, ExecutionFactory* factory);
  static PlanNode* MakePhysicalJoin(Catalog* catalog, WorkloadInfo* workload, LogicalJoinNode* logical_join, ExecutionContext* exec_ctx, ExecutionFactory *factory, bool with_sip=false);
  static void EstimateJoinCardinalities(Catalog* catalog, WorkloadInfo* workload_info, QueryInfo* query_info, FreqType freq_type);
  static void FindBestJoinOrder(QueryInfo* query_info);
  static void FindBestJoinOrders(Catalog* catalog, ExecutionFactory* execution_factory, FreqType freq_type);
  static void MakeMaterializedView(Catalog* catalog, QueryInfo* query_info);
  static PlanNode* MakePhysicalPlanWithMat(Catalog* catalog, LogicalJoinNode* logical_join, const std::string& mat_view, ExecutionFactory* factory, ExecutionContext* exec_ctx);
  static PlanNode* MakePhysicalPlanWithIndexes(Catalog* catalog, LogicalJoinNode* logical_join, const std::unordered_set<std::string>& indexes_to_use, ExecutionFactory* factory, ExecutionContext* exec_ctx);
  static PlanNode* MakePhysicalPlanWithSmartIDs(Catalog* catalog, LogicalJoinNode* logical_join, const std::unordered_map<std::string, std::pair<uint64_t, int64_t>>& table_embeddings, ExecutionFactory* factory, ExecutionContext* exec_ctx);
};

}
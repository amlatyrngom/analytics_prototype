#pragma once
#include <memory>

#include "execution/executors/plan_executor.h"
#include "execution/executors/hash_aggr_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/executors/materializer_executor.h"
#include "execution/executors/projection_executor.h"
#include "execution/executors/scan_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/executors/static_aggr_executor.h"
#include "execution/executors/build_rowid_index_executor.h"
#include "execution/execution_context.h"


namespace smartid {
class ExecutionFactory {
 public:
  ExecutionFactory() = default;

  // Noop
  NoopOutputNode* MakeNoop(PlanNode* child);

  // Print
  PrintNode* MakePrint(PlanNode* child);

  // Hash aggregation
  HashAggregationNode* MakeHashAggregation(PlanNode *child,
                                        std::vector<uint64_t> &&group_by_keys,
                                        std::vector<std::pair<uint64_t, AggType>> &&aggs);

  // Hash join
  HashJoinNode* MakeHashJoin(PlanNode *build,
                                 PlanNode *probe,
                                 std::vector<uint64_t> &&build_key_cols,
                                 std::vector<uint64_t> &&probe_key_cols,
                                 std::vector<std::pair<uint64_t, uint64_t>> &&projections,
                                 JoinType join_type);

  // Materializer
  MaterializerNode* MakeMaterializer(PlanNode* child, Table* table);

  // Projection
  ProjectionNode* MakeProjection(PlanNode* child, std::vector<ExprNode*>&& projections);

  // Scan
  ScanNode* MakeScan(Table* table,
                     std::vector<ExprNode *> &&projections,
                     std::vector<ExprNode *> &&filters, bool record_rows=false);

  ScanNode* MakeScan(const Table* table,
                     std::vector<ExprNode *> &&projections,
                     std::vector<ExprNode *> &&filters, bool record_rows=false) {
    return MakeScan(Catalog::Instance()->GetTable(table->Name()), std::move(projections), std::move(filters), record_rows);
  }


  // Sort (orderby)
  SortNode* MakeSort(PlanNode *child, std::vector<std::pair<uint64_t, SortType>> &&sort_keys);

  // Static aggregation
  StaticAggregateNode* MakeStaticAggregation(PlanNode *child, std::vector<std::pair<uint64_t, AggType>> &&aggs);

  // Constant expr
  template<typename T>
  ConstantNode* MakeConst(T val, SqlType sql_type) {
    return MakeConst(Value(val), sql_type);
  }

  // Constant expr
  ConstantNode* MakeConst(Value && val, SqlType sql_type);

  // Column expr
  ColumnNode* MakeCol(uint64_t col_idx);

  // Column expr
  ColumnNode* MakeCol(const Table* table, const std::string& col_name);

  // Param expr
  ParamNode* MakeParam(const std::string& param_name);

  // Binary comparison
  BinaryCompNode* MakeBinaryComp(ExprNode *left, ExprNode *right, OpType op);

  // Binary arithmetic
  BinaryArithNode* MakeBinaryArith(ExprNode *left, ExprNode *right, OpType op, SqlType res_type);

  // Embedding
  EmbeddingCheckNode* MakeEmbeddingCheck(ExprNode* child, int16_t mask);

  // Create Exec Context
  static std::unique_ptr<ExecutionContext> MakeExecContext() {
    return std::make_unique<ExecutionContext>();
  }

  ExecutionContext* MakeStoredExecContext() {
    auto exec_ctx = MakeExecContext();
    auto ret = exec_ctx.get();
    execution_contexts_.emplace_back(std::move(exec_ctx));
    return ret;
  }

  static std::unique_ptr<PlanExecutor> MakePlanExecutor(PlanNode * node, ExecutionContext* ctx=nullptr);

  PlanExecutor* MakeStoredPlanExecutor(PlanNode* node, ExecutionContext* ctx=nullptr) {
    auto exec = MakePlanExecutor(node, ctx);
    auto ret = exec.get();
    plan_executors_.emplace_back(std::move(exec));
    return ret;
  }
  static std::vector<std::unique_ptr<PlanExecutor>> MakePlanExecutors(const std::vector<PlanNode *>& nodes, ExecutionContext* ctx);

  static std::unique_ptr<ExprExecutor> MakeExprExecutor(ExprNode * node, ExecutionContext* ctx);
  static std::vector<std::unique_ptr<ExprExecutor>> MakeExprExecutors(const std::vector<ExprNode *>& nodes, ExecutionContext* ctx);

 private:
  std::vector<std::unique_ptr<PlanNode>> plans_{};
  std::vector<std::unique_ptr<ExprNode>> exprs_{};
  std::vector<std::unique_ptr<PlanExecutor>> plan_executors_{};
  std::vector<std::unique_ptr<ExecutionContext>> execution_contexts_{};
};
}


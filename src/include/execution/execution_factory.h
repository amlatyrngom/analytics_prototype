#pragma once
#include <memory>
#include <vector>
#include "common/types.h"
#include "execution/nodes/expr_node.h"
#include "execution/nodes/plan_node.h"
#include "execution/executors/plan_executor.h"
#include "execution/execution_context.h"


namespace smartid {
class Catalog;
class Table;
class RowIDIndex;
class ExprNode;
class ConstantNode;
class ColumnNode;
class BinaryCompNode;
class ParamNode;
class BetweenNode;
class EmbeddingCheckNode;
class InNode;
class PlanNode;
class NoopOutputNode;
class PrintNode;
class ScanNode;
class StaticAggregateNode;
class HashAggregationNode;
class HashJoinNode;
class SortNode;
class ProjectionNode;
class RowIDIndexJoinNode;
class PlanExecutor;
class ExprExecutor;
class ExecutionContext;

class ExecutionFactory {
 public:
  ExecutionFactory(Catalog* catalog): catalog_(catalog) {}

  // Index Join
  RowIDIndexJoinNode *MakeIndexJoin(PlanNode *key_side, RowIDIndex * index, uint64_t key_idx, ScanNode* lookup_side, std::vector<std::pair<uint64_t, uint64_t>> &&projections);

  // Noop
  NoopOutputNode* MakeNoop(PlanNode* child);

  // Print
  PrintNode* MakePrint(PlanNode* child, std::vector<std::string>&& col_names);

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
//
//  // Materializer
//  MaterializerNode* MakeMaterializer(PlanNode* child, Table* table);
//
  // Projection
  ProjectionNode* MakeProjection(PlanNode* child, std::vector<ExprNode*>&& projections, std::vector<ExprNode*>&& filters);

  // Scan
  ScanNode* MakeScan(Table* table,
                     std::vector<uint64_t> && cols_to_read,
                     std::vector<ExprNode *> &&projections,
                     std::vector<ExprNode *> &&filters);

  ScanNode* MakeScan(const Table* table,
                     std::vector<uint64_t> && cols_to_read,
                     std::vector<ExprNode *> &&projections,
                     std::vector<ExprNode *> &&filters);


  // Sort (orderby)
  SortNode* MakeSort(PlanNode *child, std::vector<std::pair<uint64_t, SortType>> &&sort_keys, int64_t limit=-1);
//
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

  // Between
  BetweenNode* MakeBetween(ExprNode *left, ExprNode* middle, ExprNode *right, bool left_closed, bool right_closed);

  // In
  InNode* MakeIn(ExprNode* input, std::vector<ExprNode*>&& vals);

  NonNullNode* MakeNonNull(ExprNode* input);

//
//  // Binary arithmetic
//  BinaryArithNode* MakeBinaryArith(ExprNode *left, ExprNode *right, OpType op, SqlType res_type);
//
  // Embedding
  EmbeddingCheckNode* MakeEmbeddingCheck(ExprNode* child, int64_t mask);
//
  // Create Exec Context
  static std::unique_ptr<ExecutionContext> MakeExecContext();

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
  Catalog* catalog_;
  std::vector<std::unique_ptr<PlanNode>> plans_{};
  std::vector<std::unique_ptr<ExprNode>> exprs_{};
  std::vector<std::unique_ptr<PlanExecutor>> plan_executors_{};
  std::vector<std::unique_ptr<ExecutionContext>> execution_contexts_{};
};
}


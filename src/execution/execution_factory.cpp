#include "execution/execution_factory.h"

namespace smartid {
NoopOutputNode* ExecutionFactory::MakeNoop(PlanNode* child) {
  auto node = std::make_unique<NoopOutputNode>(child);
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}

PrintNode* ExecutionFactory::MakePrint(PlanNode* child) {
  auto node = std::make_unique<PrintNode>(child);
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}

HashAggregationNode* ExecutionFactory::MakeHashAggregation(PlanNode *child,
                                         std::vector<uint64_t> &&group_by_keys,
                                         std::vector<std::pair<uint64_t, AggType>> &&aggs) {
  auto node = std::make_unique<HashAggregationNode>(child, std::move(group_by_keys), std::move(aggs));
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}

HashJoinNode* ExecutionFactory::MakeHashJoin(PlanNode *build,
                           PlanNode *probe,
                           std::vector<uint64_t> &&build_key_cols,
                           std::vector<uint64_t> &&probe_key_cols,
                           std::vector<std::pair<uint64_t, uint64_t>> &&projections, JoinType join_type) {
  auto node = std::make_unique<HashJoinNode>(build, probe, std::move(build_key_cols), std::move(probe_key_cols), std::move(projections), join_type);
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}

MaterializerNode* ExecutionFactory::MakeMaterializer(PlanNode* child, Table* table) {
  auto node = std::make_unique<MaterializerNode>(child, table);
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}

ProjectionNode* ExecutionFactory::MakeProjection(PlanNode* child, std::vector<ExprNode*>&& projections) {
  auto node = std::make_unique<ProjectionNode>(child, std::move(projections));
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}


ScanNode* ExecutionFactory::MakeScan(Table* table,
                   std::vector<ExprNode *> &&projections,
                   std::vector<ExprNode *> &&filters, bool record_rows) {
  auto node = std::make_unique<ScanNode>(table, std::move(projections), std::move(filters), record_rows);
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}


SortNode* ExecutionFactory::MakeSort(PlanNode *child, std::vector<std::pair<uint64_t, SortType>> &&sort_keys) {
  auto node = std::make_unique<SortNode>(child, std::move(sort_keys));
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}


StaticAggregateNode* ExecutionFactory::MakeStaticAggregation(PlanNode *child, std::vector<std::pair<uint64_t, AggType>> &&aggs) {
  auto node = std::make_unique<StaticAggregateNode>(child, std::move(aggs));
  auto ret = node.get();
  plans_.emplace_back(std::move(node));
  return ret;
}

ConstantNode* ExecutionFactory::MakeConst(Value && val, SqlType sql_type) {
  auto expr = std::make_unique<ConstantNode>(std::move(val), sql_type);
  auto ret = expr.get();
  exprs_.emplace_back(std::move(expr));
  return ret;
}

ColumnNode* ExecutionFactory::MakeCol(uint64_t col_idx) {
  auto expr = std::make_unique<ColumnNode>(col_idx);
  auto ret = expr.get();
  exprs_.emplace_back(std::move(expr));
  return ret;
}

ParamNode* ExecutionFactory::MakeParam(const std::string& param_name) {
  auto expr = std::make_unique<ParamNode>(param_name);
  auto ret = expr.get();
  exprs_.emplace_back(std::move(expr));
  return ret;
}

ColumnNode* ExecutionFactory::MakeCol(const Table* table, const std::string& col_name) {
  return MakeCol(table->GetSchema().ColIdx(col_name));
}

BinaryCompNode* ExecutionFactory::MakeBinaryComp(ExprNode *left, ExprNode *right, OpType op) {
  auto expr = std::make_unique<BinaryCompNode>(left, right, op);
  auto ret = expr.get();
  exprs_.emplace_back(std::move(expr));
  return ret;
}

BinaryArithNode* ExecutionFactory::MakeBinaryArith(ExprNode *left, ExprNode *right, OpType op, SqlType res_type) {
  auto expr = std::make_unique<BinaryArithNode>(left, right, op, res_type);
  auto ret = expr.get();
  exprs_.emplace_back(std::move(expr));
  return ret;
}

EmbeddingCheckNode * ExecutionFactory::MakeEmbeddingCheck(ExprNode* child, int16_t mask) {
  auto expr = std::make_unique<EmbeddingCheckNode>(child, mask);
  auto ret = expr.get();
  exprs_.emplace_back(std::move(expr));
  return ret;
}


std::unique_ptr<PlanExecutor> ExecutionFactory::MakePlanExecutor(PlanNode *node, ExecutionContext* ctx) {
  std::vector<std::unique_ptr<PlanExecutor>> children;
  switch (node->GetPlanType()) {
    case PlanType::Noop: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<NoopOutputNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<NoopOutputExecutor>(std::move(children));
    }
    case PlanType::Print: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<PrintNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<PrintExecutor>(std::move(children));
    }
    case PlanType::HashAgg: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<HashAggregationNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<HashAggregationExecutor>(n, std::move(children));
    }
    case PlanType::HashJoin: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      children.emplace_back(MakePlanExecutor(node->Child(1), ctx));
      auto n = dynamic_cast<HashJoinNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<HashJoinExecutor>(n, std::move(children));
    }
    case PlanType::Materialize: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<MaterializerNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<MaterializerExecutor>(n, std::move(children));
    }
    case PlanType::Projection: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<ProjectionNode*>(node);
      auto projections = MakeExprExecutors(n->GetProjections(), ctx);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<ProjectionExecutor>(n, std::move(projections), std::move(children));
    }
    case PlanType::Scan: {
      auto n = dynamic_cast<ScanNode*>(node);
      auto filters = MakeExprExecutors(n->GetFilters(), ctx);
      auto embedding_filters = MakeExprExecutors(n->GetEmbeddingFilters(), ctx);
      auto projections = MakeExprExecutors(n->GetProjections(), ctx);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<ScanExecutor>(n, std::move(filters), std::move(embedding_filters), std::move(projections));
    }
    case PlanType::Sort: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<SortNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<SortExecutor>(n, std::move(children));
    }
    case PlanType::StaticAgg: {
      children.emplace_back(MakePlanExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<StaticAggregateNode*>(node);
      ASSERT(n != nullptr, "Wrong plan node type!!!");
      return std::make_unique<StaticAggregateExecutor>(n, std::move(children));
    }
  }
  return nullptr;
}

std::vector<std::unique_ptr<PlanExecutor>> ExecutionFactory::MakePlanExecutors(const std::vector<PlanNode *>& nodes, ExecutionContext* ctx) {
  std::vector<std::unique_ptr<PlanExecutor>> res;
  for (const auto& n: nodes) {
    res.emplace_back(MakePlanExecutor(n, ctx));
  }
  return std::move(res);
}

std::unique_ptr<ExprExecutor> ExecutionFactory::MakeExprExecutor(ExprNode *node, ExecutionContext* ctx) {
  std::vector<std::unique_ptr<ExprExecutor>> children;
  switch (node->GetExprType()) {
    case ExprType::Constant: {
      auto n = dynamic_cast<ConstantNode*>(node);
      ASSERT(n != nullptr, "Wrong expr node type!!!");
      return std::make_unique<ConstantExprExecutor>(n);
    }
    case ExprType::Column: {
      auto n = dynamic_cast<ColumnNode*>(node);
      ASSERT(n != nullptr, "Wrong expr node type!!!");
      return std::make_unique<ColumnExprExecutor>(n);
    }
    case ExprType::EmbeddingCheck: {
      children.emplace_back(MakeExprExecutor(node->Child(0), ctx));
      auto n = dynamic_cast<EmbeddingCheckNode*>(node);
      ASSERT(n != nullptr, "Wrong expr node type!!!");
      return std::make_unique<EmbeddingCheckExecutor>(n, std::move(children));
    }
    case ExprType::BinaryComp: {
      children.emplace_back(MakeExprExecutor(node->Child(0), ctx));
      children.emplace_back(MakeExprExecutor(node->Child(1), ctx));
      auto n = dynamic_cast<BinaryCompNode*>(node);
      ASSERT(n != nullptr, "Wrong expr node type!!!");
      return std::make_unique<BinaryCompExecutor>(n, std::move(children));
    }
    case ExprType::BinaryArith: {
      children.emplace_back(MakeExprExecutor(node->Child(0), ctx));
      children.emplace_back(MakeExprExecutor(node->Child(1), ctx));
      auto n = dynamic_cast<BinaryArithNode*>(node);
      ASSERT(n != nullptr, "Wrong expr node type!!!");
      return std::make_unique<BinaryArithExecutor>(n, std::move(children));
    }
    case ExprType::Param: {
      auto n = dynamic_cast<ParamNode*>(node);
      ASSERT(n != nullptr, "Wrong expr node type!!!");
      auto param = ctx->GetParam(n->ParamName());
      return std::make_unique<ParamExecutor>(n, param.first, param.second);
    }
  }
  return {};
}

std::vector<std::unique_ptr<ExprExecutor>> ExecutionFactory::MakeExprExecutors(const std::vector<ExprNode *>& nodes, ExecutionContext* ctx) {
  std::vector<std::unique_ptr<ExprExecutor>> res;
  for (const auto& n: nodes) {
    res.emplace_back(MakeExprExecutor(n, ctx));
  }
  return std::move(res);
}

}
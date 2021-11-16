#pragma once

#include "execution/nodes/expr_node.h"

namespace smartid {
class ExprExecutor {
 public:
  ExprExecutor(std::vector<std::unique_ptr<ExprExecutor>>&& children) : children_(std::move(children)) {}

  virtual const Vector *Evaluate(const VectorProjection *vp, Filter *filter) = 0;

  virtual ~ExprExecutor() = default;

  ExprExecutor* Child(uint64_t idx) {
    return children_[idx].get();
  }

  // Default implementation does nothing.
  virtual void ReportStats() {}

  void CollectStats() {
    for (auto& child: children_) {
      child->CollectStats();
    }
    ReportStats();
  }

 protected:
  std::unique_ptr<Vector> result_{nullptr};
  std::vector<std::unique_ptr<ExprExecutor>> children_;
};

class ConstantExprExecutor : public ExprExecutor {
 public:
  explicit ConstantExprExecutor(ConstantNode* node);

  const Vector * Evaluate(const VectorProjection *vp, Filter *filter) override;
 private:
  ConstantNode* node_;
  Filter filter_one_;
};

class ColumnExprExecutor : public ExprExecutor {
 public:
  explicit ColumnExprExecutor(ColumnNode* node) : ExprExecutor({}), node_(node) {}

  const Vector * Evaluate(const VectorProjection *vp, Filter *filter) override {
    return vp->VectorAt(node_->ColIdx());
  }
 private:
  ColumnNode* node_;
};

class BinaryCompExecutor : public ExprExecutor {
 public:
  BinaryCompExecutor(BinaryCompNode* node, std::vector<std::unique_ptr<ExprExecutor>>&& children)
  : ExprExecutor(std::move(children)), node_(node) {}

  const Vector * Evaluate(const VectorProjection *vp, Filter *filter) override;

  void ReportStats() override {
    node_->ReportStats(input_size_, output_size_);
  }
 private:
  BinaryCompNode* node_;
  uint64_t input_size_{0};
  uint64_t output_size_{0};
};

class BinaryArithExecutor : public ExprExecutor {
 public:
  BinaryArithExecutor(BinaryArithNode* node, std::vector<std::unique_ptr<ExprExecutor>>&& children)
      : ExprExecutor(std::move(children)), node_(node) {
    result_ = std::make_unique<Vector>(node_->ResType());
  }

  const Vector * Evaluate(const VectorProjection *vp, Filter *filter) override;

 private:
  BinaryArithNode* node_;
};

class EmbeddingCheckExecutor: public ExprExecutor {
 public:
  EmbeddingCheckExecutor(EmbeddingCheckNode* node, std::vector<std::unique_ptr<ExprExecutor>>&& children)
  : ExprExecutor(std::move(children)), node_(node) {}


  const Vector * Evaluate(const VectorProjection *vp, Filter *filter) override;
 private:
  EmbeddingCheckNode* node_;
};

class ParamExecutor: public ExprExecutor {
 public:
  ParamExecutor(ParamNode* node, const Value& val, SqlType val_type);

  const Vector * Evaluate(const VectorProjection *vp, Filter *filter) override;

  void ReportStats() override {
    node_->ReportStats(val_, val_type_);
  }
 private:
  ParamNode* node_;
  Filter filter_one_;
  Value val_;
  SqlType val_type_;
};

}
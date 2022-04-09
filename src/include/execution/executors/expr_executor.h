#pragma once

#include "execution/nodes/expr_node.h"

namespace smartid {
class Bitmap;
class VectorProjection;
class Vector;

class ExprExecutor {
 public:
  explicit ExprExecutor(std::vector<std::unique_ptr<ExprExecutor>>&& children);

  virtual const Vector *Evaluate(const VectorProjection *vp, Bitmap *filter) = 0;

  virtual ~ExprExecutor();

  ExprExecutor* Child(uint64_t idx) {
    return children_[idx].get();
  }

 protected:
  std::unique_ptr<Vector> result_{nullptr};
  std::vector<std::unique_ptr<ExprExecutor>> children_;
};

class ConstantExprExecutor : public ExprExecutor {
 public:
  explicit ConstantExprExecutor(ConstantNode* node);

  const Vector * Evaluate(const VectorProjection *vp, Bitmap *filter) override;
 private:
  ConstantNode* node_;
  std::unique_ptr<Bitmap> filter_one_;
};

class ColumnExprExecutor : public ExprExecutor {
 public:
  explicit ColumnExprExecutor(ColumnNode* node) : ExprExecutor({}), node_(node) {}

  const Vector * Evaluate(const VectorProjection *vp, Bitmap *filter) override;
 private:
  ColumnNode* node_;
};

class BinaryCompExecutor : public ExprExecutor {
 public:
  BinaryCompExecutor(BinaryCompNode* node, std::vector<std::unique_ptr<ExprExecutor>>&& children)
  : ExprExecutor(std::move(children)), node_(node) {}

  const Vector * Evaluate(const VectorProjection *vp, Bitmap *filter) override;

 private:
  BinaryCompNode* node_;
};

//class BinaryArithExecutor : public ExprExecutor {
// public:
//  BinaryArithExecutor(BinaryArithNode* node, std::vector<std::unique_ptr<ExprExecutor>>&& children)
//      : ExprExecutor(std::move(children)), node_(node) {
//    result_ = std::make_unique<Vector>(node_->ResType());
//  }
//
//  const Vector * Evaluate(const VectorProjection *vp, Bitmap *filter) override;
//
// private:
//  BinaryArithNode* node_;
//};
//
//class EmbeddingCheckExecutor: public ExprExecutor {
// public:
//  EmbeddingCheckExecutor(EmbeddingCheckNode* node, std::vector<std::unique_ptr<ExprExecutor>>&& children)
//  : ExprExecutor(std::move(children)), node_(node) {}
//
//
//  const Vector * Evaluate(const VectorProjection *vp, Bitmap *filter) override;
// private:
//  EmbeddingCheckNode* node_;
//};
//
//class ParamExecutor: public ExprExecutor {
// public:
//  ParamExecutor(ParamNode* node, const Value& val, SqlType val_type);
//
//  const Vector * Evaluate(const VectorProjection *vp, Bitmap *filter) override;
//
//  void ReportStats() override {
//    node_->ReportStats(val_, val_type_);
//  }
// private:
//  ParamNode* node_;
//  Bitmap filter_one_;
//  Value val_;
//  SqlType val_type_;
//};

}
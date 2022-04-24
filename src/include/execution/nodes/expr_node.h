#pragma once

#include "common/types.h"
#include "execution/execution_types.h"


namespace smartid {


class ExprNode {
 public:
  ExprNode(ExprType expr_type, std::vector<ExprNode*> && children) : expr_type_(expr_type), children_(std::move(children)) {}

  virtual ~ExprNode() = default;

  [[nodiscard]] ExprType GetExprType() const {
    return expr_type_;
  }

  ExprNode* Child(uint64_t idx) {
    return children_[idx];
  }

  [[nodiscard]] const ExprNode* Child(uint64_t idx) const {
    return children_[idx];
  }

  [[nodiscard]] uint64_t NumChildren() const {
    return children_.size();
  }

 protected:
  ExprType expr_type_;
  std::vector<ExprNode*> children_;
};

class ConstantNode : public ExprNode {
 public:
  explicit ConstantNode(Value && val, SqlType val_type) : ExprNode(ExprType::Constant, {}), val_{std::move(val)}, val_type_(val_type) {}

  [[nodiscard]] const Value& Val() const {
    return val_;
  }

  [[nodiscard]] SqlType ValType() const {
    return val_type_;
  }
 private:
  Value val_;
  SqlType val_type_;
};

class ColumnNode : public ExprNode {
 public:
  explicit ColumnNode(uint64_t col_idx) : ExprNode(ExprType::Column, {}), col_idx_{col_idx} {}

  [[nodiscard]] uint64_t ColIdx() const {
    return col_idx_;
  }

 private:
  uint64_t col_idx_;
};

class NonNullNode : public ExprNode {
 public:
  explicit NonNullNode(ExprNode* child) : ExprNode(ExprType::NonNull, {child}) {}

 private:
};


class BinaryCompNode : public ExprNode {
 public:
  BinaryCompNode(ExprNode *left, ExprNode *right, OpType op) : ExprNode(ExprType::BinaryComp, {left, right}), op_(op) {}

  [[nodiscard]] OpType GetOpType() const {
    return op_;
  }

 private:
  OpType op_;
};

class BetweenNode : public ExprNode {
 public:
  BetweenNode(ExprNode* left, ExprNode* middle, ExprNode* right, bool left_closed, bool right_closed)
  : ExprNode(ExprType::Between, {left, middle, right})
  , left_closed_(left_closed)
  , right_closed_(right_closed) {}

  [[nodiscard]] bool LeftClosed() const {
    return left_closed_;
  }

  [[nodiscard]] bool RightClosed() const {
    return right_closed_;
  }

 private:
  bool left_closed_;
  bool right_closed_;
};


class InNode: public ExprNode {
 public:
  InNode(ExprNode* input, const std::vector<ExprNode*>& vals)
  : ExprNode(ExprType::InList, {input}) {
    for (auto& val: vals) {
      children_.emplace_back(val);
    }
  }
};



//
//class BinaryArithNode : public ExprNode {
// public:
//  BinaryArithNode(ExprNode *left, ExprNode *right, OpType op, SqlType res_type) : ExprNode(ExprType::BinaryArith, {left, right}), op_(op), res_type_(res_type) {}
//
//  [[nodiscard]] OpType GetOpType() const {
//    return op_;
//  }
//
//  [[nodiscard]] SqlType ResType() const {
//    return res_type_;
//  }
// private:
//  OpType op_;
//  SqlType res_type_;
//};
//
class EmbeddingCheckNode : public ExprNode {
 public:
  EmbeddingCheckNode(ExprNode *child, int64_t mask): ExprNode(ExprType::EmbeddingCheck, {child}), mask_(mask) {}

  [[nodiscard]] int64_t Mask() const {
    return mask_;
  }

 private:
  int64_t mask_;
};
//
class ParamNode: public ExprNode {
 public:
  explicit ParamNode(std::string param_name): ExprNode(ExprType::Param, {}), param_name_(std::move(param_name)) {}

  const auto& ParamName() const {
    return param_name_;
  }

 private:
  std::string param_name_;
};
}
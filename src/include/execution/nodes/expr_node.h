#pragma once
#include "execution/vector_projection.h"
#include "execution/execution_common.h"

namespace smartid {
enum class ExprType {
  Constant,
  Column,
  BinaryComp,
  BinaryArith,
  EmbeddingCheck,
  Param,
};

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

 private:
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


class BinaryCompNode : public ExprNode {
 public:
  struct Stats {
    // pairs of (input_size, output_size);
    std::vector<std::pair<uint64_t, uint64_t>> sels_;
  };
 public:
  BinaryCompNode(ExprNode *left, ExprNode *right, OpType op) : ExprNode(ExprType::BinaryComp, {left, right}), op_(op) {}

  [[nodiscard]] OpType GetOpType() const {
    return op_;
  }

  void ReportStats(uint64_t in_size, uint64_t out_size) {
    stats_.sels_.emplace_back(in_size, out_size);
  }

  [[nodiscard]] const Stats& GetStats() const {
    return stats_;
  }
 private:
  OpType op_;
  Stats stats_;
};

class BinaryArithNode : public ExprNode {
 public:
  BinaryArithNode(ExprNode *left, ExprNode *right, OpType op, SqlType res_type) : ExprNode(ExprType::BinaryArith, {left, right}), op_(op), res_type_(res_type) {}

  [[nodiscard]] OpType GetOpType() const {
    return op_;
  }

  [[nodiscard]] SqlType ResType() const {
    return res_type_;
  }
 private:
  OpType op_;
  SqlType res_type_;
};

class EmbeddingCheckNode : public ExprNode {
 public:
  EmbeddingCheckNode(ExprNode *child, int16_t mask): ExprNode(ExprType::EmbeddingCheck, {child}), mask_(mask) {}

  [[nodiscard]] int16_t Mask() const {
    return mask_;
  }

 private:
  int16_t mask_;
};

class ParamNode: public ExprNode {
 public:
  struct Stats {
    std::vector<Value> vals;
    SqlType val_type{-1};
  };
 public:
  explicit ParamNode(std::string param_name): ExprNode(ExprType::Param, {}), param_name_(std::move(param_name)) {}

  void ReportStats(const Value& val, SqlType val_type) {
    stats_.vals.emplace_back(val);
    ASSERT(static_cast<int>(stats_.val_type) == -1 || stats_.val_type==val_type, "Changing param type!!!");
    stats_.val_type = val_type;
  }

  [[nodiscard]] const Stats& GetStats() const {
    return stats_;
  }

  const auto& ParamName() const {
    return param_name_;
  }

 private:
  std::string param_name_;
  Stats stats_;
};
}
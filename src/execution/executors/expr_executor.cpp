#include "execution/executors/expr_executor.h"
#include "execution/vector_ops.h"

namespace smartid {
ConstantExprExecutor::ConstantExprExecutor(ConstantNode *node) : ExprExecutor({}), node_(node), filter_one_(FilterMode::SelVecFull) {
  filter_one_.Reset(1, FilterMode::SelVecFull);
  result_ = std::make_unique<Vector>(node_->ValType(), 1);
  VectorOps::InitVector(&filter_one_, node_->Val(), node_->ValType(), result_.get());
}

const Vector * ConstantExprExecutor::Evaluate(const VectorProjection *vp, Filter *filter) {
  return result_.get();
}

const Vector *BinaryCompExecutor::Evaluate(const VectorProjection *vp, Filter *filter) {
  input_size_ += filter->ActiveSize();
  auto l_res = Child(0)->Evaluate(vp, filter);
  auto r_res = Child(1)->Evaluate(vp, filter);
  VectorOps::BinaryCompVector(l_res, r_res, filter, node_->GetOpType());
  output_size_ += filter->ActiveSize();
  return nullptr;
}

const Vector *BinaryArithExecutor::Evaluate(const VectorProjection *vp, Filter *filter) {
  auto l_res = Child(0)->Evaluate(vp, filter);
  auto r_res = Child(1)->Evaluate(vp, filter);
  VectorOps::BinaryArithVector(l_res, r_res, filter, node_->GetOpType(), result_.get());
  return result_.get();
}

const Vector * EmbeddingCheckExecutor::Evaluate(const VectorProjection *vp, Filter *filter) {
  auto child_res = Child(0)->Evaluate(vp, filter);
  auto child_data = child_res->DataAs<int16_t>();
  auto mask = node_->Mask();
  filter->SafeUpdate([&](sel_t i) {
    return (child_data[i] & mask) != 0;
  });
  return nullptr;
}

ParamExecutor::ParamExecutor(ParamNode *node, const Value &val, SqlType val_type)
    : ExprExecutor({})
    , node_(node)
    , val_(val)
    , val_type_(val_type) {
  filter_one_.Reset(1, FilterMode::SelVecFull);
  result_ = std::make_unique<Vector>(val_type, 1);
  VectorOps::InitVector(&filter_one_, val, val_type, result_.get());
}

const Vector * ParamExecutor::Evaluate(const VectorProjection *vp, Filter *filter) {
  return result_.get();
}

}
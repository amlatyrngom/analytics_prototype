#include "execution/executors/expr_executor.h"
#include "execution/vector_ops.h"
#include "storage/vector_projection.h"
#include "storage/vector.h"

namespace smartid {

ExprExecutor::ExprExecutor(std::vector<std::unique_ptr<ExprExecutor>> &&children) : children_(std::move(children)) {}

ExprExecutor::~ExprExecutor() = default;

const Vector *ColumnExprExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  return vp->VectorAt(node_->ColIdx());
}

ConstantExprExecutor::ConstantExprExecutor(ConstantNode *node) : ExprExecutor({}), node_(node) {
  filter_one_ = std::make_unique<Bitmap>();
  filter_one_->Reset(1);
  result_ = std::make_unique<Vector>(node_->ValType());
  result_->Resize(1);
  VectorOps::InitVector(filter_one_.get(), node_->Val(), node_->ValType(), result_.get());
}

const Vector * ConstantExprExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  return result_.get();
}

const Vector *NonNullExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  auto child_res = Child(0)->Evaluate(vp, filter);
  Bitmap::Intersect(filter->Words(), child_res->NullBitmap()->Words(), filter->TotalSize(), filter->MutableWords());
  return nullptr;
}

const Vector *BinaryCompExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  auto l_res = Child(0)->Evaluate(vp, filter);
  auto r_res = Child(1)->Evaluate(vp, filter);
  VectorOps::BinaryCompVector(l_res, r_res, filter, node_->GetOpType());
  return nullptr;
}

const Vector *BetweenExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  auto l_res = Child(0)->Evaluate(vp, nullptr); // Must be constant
  auto middle_res = Child(1)->Evaluate(vp, filter);
  auto r_res = Child(2)->Evaluate(vp, nullptr); // Must be constant
  OpType left_op = node_->LeftClosed() ? OpType::GE : OpType::GT; // middle >= left
  OpType right_op = node_->RightClosed() ? OpType::LE : OpType::LT; // middle <= right
  VectorOps::BinaryCompVector(middle_res, l_res, filter, left_op);
  VectorOps::BinaryCompVector(middle_res, r_res, filter, right_op);
  return nullptr;
}

InExecutor::InExecutor(InNode *node, std::vector<std::unique_ptr<ExprExecutor>> &&children)
    : ExprExecutor(std::move(children)), node_(node) {
  tmp_filter_ = std::make_unique<Bitmap>();
  final_filter_ = std::make_unique<Bitmap>();
}

const Vector *InExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  auto in_res = Child(0)->Evaluate(vp, filter);
  // All to zero.
  final_filter_->Reset(filter->TotalSize());
  std::memset(final_filter_->MutableWords(), 0, final_filter_->NumWords() * sizeof(uint64_t));
  for (uint64_t i = 1; i < children_.size(); i++) {
    // Get value. filter is null because val must be constant.
    auto val_res = Child(i)->Evaluate(vp, nullptr);
    // Run equality check
    tmp_filter_->SetFrom(filter);
    VectorOps::BinaryCompVector(in_res, val_res, tmp_filter_.get(), OpType::EQ);
    // Union with final result.
    final_filter_->Union(tmp_filter_.get());
  }
  filter->SetFrom(final_filter_.get());
  return nullptr;
}


//
//const Vector *BinaryArithExecutor::Evaluate(const VectorProjection *vp, Filter *filter) {
//  auto l_res = Child(0)->Evaluate(vp, filter);
//  auto r_res = Child(1)->Evaluate(vp, filter);
//  VectorOps::BinaryArithVector(l_res, r_res, filter, node_->GetOpType(), result_.get());
//  return result_.get();
//}
//

const Vector * EmbeddingCheckExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  auto child_res = Child(0)->Evaluate(vp, filter);
  auto child_data = child_res->DataAs<int64_t>();
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
  filter_one_ = std::make_unique<Bitmap>();
  filter_one_->Reset(1);
  result_ = std::make_unique<Vector>(val_type);
  result_->Resize(1);
  VectorOps::InitVector(filter_one_.get(), val, val_type, result_.get());
}

const Vector * ParamExecutor::Evaluate(const VectorProjection *vp, Bitmap *filter) {
  return result_.get();
}

}
#include "execution/vector_ops.h"

namespace smartid {
///////////////////////////////////////
//// Comparison with 2 vectors
//////////////////////////////////////
template <typename T, typename FN>
void DoUpdate(FN fn, Filter* filter) {
  if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, Date>) {
    filter->SafeUpdate(fn);
  } else {
    filter->Update(fn);
  }
}

template <typename Op, typename cpp_type>
__attribute__((noinline))
void TemplatedBinaryCompSame(const cpp_type& val1, const cpp_type* data2, Filter* filter) {
  // Left hand side is a single value.
  auto fn = [&](sel_t i) {
    return Op::Apply(val1, data2[i]);
  };
  DoUpdate<cpp_type>(fn, filter);
}

template <typename Op, typename cpp_type>
__attribute__((noinline))
void TemplatedBinaryCompSame(const cpp_type* data1, const cpp_type& val2, Filter* filter) {
  // Left hand side is a single value.
  auto fn = [&](sel_t i) {
    return Op::Apply(data1[i], val2);
  };
  DoUpdate<cpp_type>(fn, filter);
}

template <typename Op, typename cpp_type>
__attribute__((noinline))
void TemplatedBinaryCompSame(const cpp_type* data1, const cpp_type* data2, Filter* filter) {
  // Left hand side is a single value.
  auto fn = [&](sel_t i) {
    return Op::Apply(data1[i], data2[i]);
  };
  DoUpdate<cpp_type>(fn, filter);
}



template <typename Op, typename cpp_type>
__attribute__((noinline))
void TemplatedBinaryCompSame(const Vector* in1, const Vector* in2, Filter* filter) {
  auto data1 = in1->DataAs<cpp_type>();
  auto data2 = in2->DataAs<cpp_type>();
  if (in1->NumElems() == 1) {
    // Left hand side is a single value.
    TemplatedBinaryCompSame<Op, cpp_type>(data1[0], data2, filter);
  } else if (in2->NumElems() == 1) {
    // RHS is a single value.
    TemplatedBinaryCompSame<Op, cpp_type>(data1, data2[0], filter);
  } else {
    // Both sides are vectors.
    TemplatedBinaryCompSame<Op>(data1, data2, filter);
  }
}


#define TEMPLATED_BINARY_COMP_SAME(sql_type, cpp_type, func) \
  case SqlType::sql_type: \
    if constexpr (std::is_same_v<cpp_type, Date>) {          \
      TemplatedBinaryCompSame<func<Date::NativeType>, Date::NativeType>(in1, in2, filter);                                                        \
    } else {                                                 \
      TemplatedBinaryCompSame<func<cpp_type>, cpp_type>(in1, in2, filter);                                                       \
    }                                                        \
    break;

#define TEMPLATED_BINARY_COMP_SAME_BY_TYPE(op, func) \
  case OpType::op:                                   \
    switch (in1->ElemType()) {                       \
      SQL_TYPE(TEMPLATED_BINARY_COMP_SAME, TEMPLATED_BINARY_COMP_SAME, func)                                                 \
    }                                                \
    return;


void BinaryCompSameType(const Vector *in1, const Vector *in2, Filter *filter, OpType op_type) {
  switch (op_type) {
    OP_TYPE(TEMPLATED_BINARY_COMP_SAME_BY_TYPE, NOOP)
    default: {
      ASSERT(false, "Not a comparison operation!");
    }
  }
}

void VectorOps::BinaryCompVector(const Vector *in1, const Vector *in2, Filter *filter, OpType op_type) {
  ASSERT(in1->ElemType() == in2->ElemType(), "Binary comparison on distinct types!");
  BinaryCompSameType(in1, in2, filter, op_type);
}
}

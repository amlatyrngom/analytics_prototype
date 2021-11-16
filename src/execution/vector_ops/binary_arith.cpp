#include "execution/vector_ops.h"

namespace smartid {
////////////////////////////////////
//// Arithmetic with 2 vectors
//////////////////////////////////////
template<typename result_type, typename l_cpp_type, typename r_cpp_type, typename Op>
__attribute__ ((noinline))
void TemplatedBinaryArith(const Filter *filter, const Vector *l_in, const Vector *r_in, Vector *out) {
  out->Resize(std::max(l_in->NumElems(), r_in->NumElems()));
  auto l_data = l_in->DataAs<l_cpp_type>();
  auto r_data = r_in->DataAs<r_cpp_type>();
  auto raw_result = out->MutableDataAs<result_type>();
  if (l_in->NumElems() == 1) {
    // Left hand side is a single value.
    auto fn = [&](sel_t i) {
      raw_result[i] = Op::Apply(static_cast<result_type>(l_data[0]), static_cast<result_type>(r_data[i]));
    };
    if constexpr (std::is_arithmetic_v<l_cpp_type>) {
      filter->SafeMap(fn);
    } else {
      filter->Map(fn);
    }
  } else if (r_in->NumElems() == 1) {
    // Right hand side is a single value.
    auto fn = [&](sel_t i) {
      raw_result[i] = Op::Apply(static_cast<result_type>(l_data[i]), static_cast<result_type>(r_data[0]));
    };
    if constexpr (std::is_arithmetic_v<l_cpp_type>) {
      filter->SafeMap(fn);
    } else {
      filter->Map(fn);
    }
  } else {
    // Both LHS and RHS are vectors.
    auto fn = [&](sel_t i) {
      raw_result[i] = Op::Apply(static_cast<result_type>(l_data[i]), static_cast<result_type>(r_data[i]));
    };
    if constexpr (std::is_arithmetic_v<l_cpp_type>) {
      filter->SafeMap(fn);
    } else {
      filter->Map(fn);
    }
  }

}

#define BinaryArithRightTypeCase(sql_type, r_cpp_type, ...) \
        case SqlType::sql_type: {                                      \
            TemplatedBinaryArith<result_type, l_cpp_type, r_cpp_type, Op>(filter, in1, in2, out); \
            break;\
        }

template<typename result_type, typename l_cpp_type, typename Op>
void BinaryArithRight(const Vector *in1, const Vector *in2, const Filter *filter, Vector *out) {
  switch (in2->ElemType()) {
    SQL_TYPE(BinaryArithRightTypeCase, NOOP);
    default:std::cerr << "INVALID ARITHMETIC TYPE!!!!!!!" << std::endl;
      return;
  }
}

#define BinaryArithLeftTypeCase(sql_type, l_cpp_type, ...) \
        case SqlType::sql_type: {                                      \
            BinaryArithRight<result_type, l_cpp_type, Op>(in1, in2, filter, out); \
            break;\
        }

template<typename result_type, typename Op>
void BinaryArithLeft(const Vector *in1, const Vector *in2, const Filter *filter, Vector *out) {
  switch (in1->ElemType()) {
    SQL_TYPE(BinaryArithLeftTypeCase, NOOP);
    default:std::cerr << "INVALID ARITHMETIC TYPE!!!!!!!" << std::endl;
      return;
  }
}

#define ResultTypeCase(res_sql_type, res_cpp_type, func) \
        case SqlType::res_sql_type: {               \
            BinaryArithLeft<res_cpp_type, func<res_cpp_type>>(in1, in2, filter, out); \
            break;                                       \
        }

#define BinaryArithOpCase(op_type, func) \
        case OpType::op_type: {           \
             switch (out->ElemType()) {                            \
                 SQL_TYPE(ResultTypeCase, NOOP, func) \
             default:                        \
                 std::cout << "Invalid Arithmetic Type!!!!!!!!!" << std::endl; \
                 return; \
             }                           \
             return; \
        }                                \


void VectorOps::BinaryArithVector(const Vector *in1, const Vector *in2, const Filter *filter, OpType op_type,
                                  Vector *out) {
  switch (op_type) {
    OP_TYPE(NOOP, BinaryArithOpCase)
    default:
      // TODO(Amadou): Throw some kind of error.
      std::cerr << "INVALID FILTER PASSED IN!!!!!!!!" << std::endl;
      return;
  }
}
}

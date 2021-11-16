#include "execution/vector_ops.h"

namespace smartid {
////////////////////////////////////
//// Comparison with constant
//////////////////////////////////////

template<typename in_cpp_type, typename Op>
void TemplatedConstantCompVector(const Vector *in, const Value &val, Filter *filter) {
  auto data = in->DataAs<in_cpp_type>();
  auto cpp_val = std::get<in_cpp_type>(val);
  auto fn = [&](sel_t i) {
    return Op::Apply(data[i], cpp_val);
  };
  // Full compute only on arithmetic types.
  if constexpr (std::is_arithmetic_v<in_cpp_type>) {
    filter->SafeUpdate(fn);
  } else {
    filter->Update(fn);
  }
}

#define ConstCompTypeCase(sql_type, cpp_type, func) \
        case SqlType::sql_type: {                     \
            TemplatedConstantCompVector<cpp_type, func<cpp_type>>(in, val, filter); \
            break; \
       }

#define ConstCompOpCase(comp_type, func) \
        case OpType::comp_type: {           \
             switch (in_type) {                            \
                SQL_TYPE(ConstCompTypeCase, ConstCompTypeCase, func)\
             }                           \
             break; \
        }

/**
 * Compares values in the input vector to the value.
 */
void VectorOps::ConstantCompVector(const Vector *in, const Value &val, OpType op_type, Filter *filter) {
  auto in_type = in->ElemType();
  switch (op_type) {
    OP_TYPE(ConstCompOpCase, NOOP)
    default:
      // TODO(Amadou): Throw some kind of error.
      std::cerr << "INVALID FILTER PASSED IN!!!!!!!!" << std::endl;
      break;
  }
}

////////////////////////////////////
//// Arithmetic with constant
//////////////////////////////////////
template<typename cpp_type, typename Op>
__attribute__ ((noinline))
void TemplatedConstantArith(const Vector *in, const Value &val, const Filter *filter, Vector *out) {
  out->Resize(in->NumElems());
  auto data = in->DataAs<cpp_type>();
  auto raw_result = out->MutableDataAs<cpp_type>();
  auto cpp_val = std::get<cpp_type>(val);
  auto fn = [&](sel_t i) { raw_result[i] = Op::Apply(data[i], cpp_val); };
  // Full compute on arithmetic types.
  if constexpr (std::is_arithmetic_v<cpp_type>) {
    filter->SafeMap(fn);
  } else {
    filter->Map(fn);
  }
}

#define ConstantArithTypeCase(sql_type, cpp_type, func) \
        case SqlType::sql_type: {                                      \
             TemplatedConstantArith<cpp_type, func<cpp_type>>(in, val, filter, out); \
             break;\
       }

#define ConstantArithOpCase(op_type, func) \
        case OpType::op_type: {           \
             switch (input_type) {                            \
                 SQL_TYPE(ConstantArithTypeCase, NOOP, func) \
             default:                        \
                 std::cout << "Invalid Arithmetic Type!!!!!!!!!" << std::endl; \
                 return; \
             }                           \
             return; \
        }                                \


void VectorOps::ConstantArithVector(const Vector *in, const Value &val, const Filter *filter, OpType op_type,
                                    Vector *out) {
  auto input_type = in->ElemType();
  switch (op_type) {
    OP_TYPE(NOOP, ConstantArithOpCase)
    default:std::cerr << "INVALID FILTER PASSED IN!!!!!!!!" << std::endl;
      break;
  }
}
}
#pragma once

namespace smartid {

// Op Types.
#define OP_TYPE(COMP, ARITH) \
COMP(LT, ops::Lt)\
COMP(LE, ops::Le)\
COMP(NE, ops::Ne)\
COMP(EQ, ops::Eq)\
COMP(GT, ops::Gt)\
COMP(GE, ops::Ge)\
ARITH(ADD, ops::Add)\
ARITH(MUL, ops::Mul)\
ARITH(SUB, ops::Sub)\
ARITH(DIV, ops::Div)

#define ENUM_DEFINER(entry, ...) entry,
enum class OpType {
  OP_TYPE(ENUM_DEFINER, ENUM_DEFINER)
};
#undef ENUM_DEFINER


enum class PlanType {
  Noop,
  Print,
  Scan,
  Projection,
  HashJoin,
  HashAgg,
  StaticAgg,
  Sort,
  Materialize,
  BuildRowIDIndex,
  RowIDIndexJoin,
};


enum class ExprType {
  Constant,
  Column,
  BinaryComp,
  Between,
  InList,
  Param,
  NonNull,
//  BinaryArith,
  EmbeddingCheck,
};

// Types of aggregation.
enum class AggType {
  COUNT,
  SUM,
  MAX,
  MIN,
};



enum class JoinType {
  INNER,
  RIGHT_SEMI,
  LEFT_SEMI,
};


enum class SortType {
  ASC,
  DESC,
};

struct HTEntry {
  HTEntry *next;
  char payload[0];
};

// Keys are already hashed. No need to repeat.
struct ExecHasher {
  std::size_t operator()(const uint64_t& x) const noexcept {
    return x;
  }
};
}
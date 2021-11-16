#include "execution/vector_ops.h"

namespace smartid {

/**
 * Count Aggregation.
 */
template<typename T>
struct Count {
  static int64_t Apply(T input, int64_t curr) {
    return curr + 1;
  }

  static int64_t Init(T input) {
    return 1;
  }
};

/**
 * Sum Aggregation.
 */
template<typename T>
struct Sum {
  static double Apply(T input, double curr_out) {
    return curr_out + static_cast<double>(input);
  }

  static double Init(T input) {
    return static_cast<double>(input);
  }
};

/**
 * Min Aggregation.
 */
template<typename T>
struct Min {
  static T Apply(T input, T curr_out) {
    return std::min<T>(input, curr_out);
  }

  static T Init(T input) {
    return input;
  }
};

/**
 * Max Aggregation.
 */
template<typename T>
struct Max {
  static T Apply(T input, T curr_out) {
    return std::max<T>(input, curr_out);
  }

  static T Init(T input) {
    return input;
  }
};

template<typename Op, typename in_cpp_type, typename out_cpp_type>
void TemplatedReduce(const Filter *filter, const Vector *in, Vector *out) {
  auto in_data = in->DataAs<in_cpp_type>();
  auto out_val = out->DataAs<out_cpp_type>()[0];
  auto fn = [&](sel_t i) {
    out_val = Op::Apply(in_data[i], out_val);
  };
  filter->Map(fn);
  out->MutableDataAs<out_cpp_type>()[0] = out_val;
}

template<typename in_cpp_type>
void TemplatedReduceByIn(const Filter *filter, const Vector *in, Vector *out, AggType agg_type) {
  switch (agg_type) {
    case AggType::COUNT:TemplatedReduce<Count<in_cpp_type>, in_cpp_type, int64_t>(filter, in, out);
      return;
    case AggType::MAX:TemplatedReduce<Max<in_cpp_type>, in_cpp_type, in_cpp_type>(filter, in, out);
      return;
    case AggType::MIN:TemplatedReduce<Min<in_cpp_type>, in_cpp_type, in_cpp_type>(filter, in, out);
      return;
    case AggType::SUM:
      if constexpr (std::is_arithmetic_v<in_cpp_type>) {
        TemplatedReduce<Sum<in_cpp_type>, in_cpp_type, double>(filter, in, out);
      } else {
        std::cerr << "Sum can only be called on arithmetic types!!!" << std::endl;
      }
      return;
  }
}

#define TEMPLATED_REDUCE_BY_IN(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type:                            \
          TemplatedReduceByIn<in_cpp_type>(filter, in, out, agg_type); \
          return;\


void VectorOps::ReduceVector(const Filter *filter, const Vector *in, Vector *out, AggType agg_type) {
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_REDUCE_BY_IN, TEMPLATED_REDUCE_BY_IN)
  }
}

/////////////////////////////////////////
//// Scatter Reduce
////////////////////////////////////////
template<typename Op, typename in_cpp_type, typename out_cpp_type>
void TemplatedScatterReduce(const Filter *filter, const Vector *in, Vector *out, uint64_t agg_offset) {
  out->Resize(in->NumElems());
  auto in_data = in->DataAs<in_cpp_type>();
  auto out_data = out->MutableDataAs<char *>();
  auto scatter_fn = [&](sel_t i) {
    auto curr_val = reinterpret_cast<out_cpp_type *>(out_data[i] + agg_offset);
    if constexpr (std::is_same_v<float, out_cpp_type>) std::cout << "CurrVal: " << *curr_val << ", " << in_data[i] << std::endl;
    *curr_val = Op::Apply(in_data[i], *curr_val);
    if constexpr (std::is_same_v<float, out_cpp_type>) std::cout << "CurrVal: " << *curr_val << std::endl;
  };
  filter->Map(scatter_fn);
}

template<typename in_cpp_type>
void TemplatedScatterReduceByIn(const Filter *filter,
                                const Vector *in,
                                Vector *out,
                                uint64_t agg_offset,
                                AggType agg_type) {
  switch (agg_type) {
    case AggType::COUNT:TemplatedScatterReduce<Count<in_cpp_type>, in_cpp_type, int64_t>(filter, in, out, agg_offset);
      return;
    case AggType::MAX:TemplatedScatterReduce<Max<in_cpp_type>, in_cpp_type, in_cpp_type>(filter, in, out, agg_offset);
      return;
    case AggType::MIN:TemplatedScatterReduce<Min<in_cpp_type>, in_cpp_type, in_cpp_type>(filter, in, out, agg_offset);
      return;
    case AggType::SUM:
      if constexpr (std::is_arithmetic_v<in_cpp_type>) {
        TemplatedScatterReduce<Sum<in_cpp_type>, in_cpp_type, double>(filter, in, out, agg_offset);
      } else {
        std::cerr << "Sum can only be called on arithmetic types!!!" << std::endl;
      }
      return;
  }
}

#define TEMPLATED_SCATTER_REDUCE_BY_IN(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type:                            \
          TemplatedScatterReduceByIn<in_cpp_type>(filter, in, out, agg_offset, agg_type); \
          return;\


void VectorOps::ScatterReduceVector(const Filter *filter,
                                    const Vector *in,
                                    Vector *out,
                                    uint64_t agg_offset,
                                    AggType agg_type) {
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_SCATTER_REDUCE_BY_IN, TEMPLATED_SCATTER_REDUCE_BY_IN)
  }
}

//////////////////////////////////
//// Init Reduce
//////////////////////////////////
template<typename Op, typename in_cpp_type, typename out_cpp_type>
void TemplatedScatterInitReduce(const Vector *in, sel_t i, char *out, uint64_t agg_offset) {
  auto in_data = in->DataAs<in_cpp_type>();
  auto curr_val = reinterpret_cast<out_cpp_type *>(out + agg_offset);
  *curr_val = Op::Init(in_data[i]);
}

template<typename in_cpp_type>
void TemplatedScatterInitReduceByIn(const Vector *in, sel_t i, char *out, uint64_t agg_offset, AggType agg_type) {
  switch (agg_type) {
    case AggType::COUNT:TemplatedScatterInitReduce<Count<in_cpp_type>, in_cpp_type, int64_t>(in, i, out, agg_offset);
      return;
    case AggType::MAX:TemplatedScatterInitReduce<Max<in_cpp_type>, in_cpp_type, in_cpp_type>(in, i, out, agg_offset);
      return;
    case AggType::MIN:TemplatedScatterInitReduce<Min<in_cpp_type>, in_cpp_type, in_cpp_type>(in, i, out, agg_offset);
      return;
    case AggType::SUM:
      if constexpr (std::is_arithmetic_v<in_cpp_type>) {
        TemplatedScatterInitReduce<Sum<in_cpp_type>, in_cpp_type, double>(in, i, out, agg_offset);
      } else {
        std::cerr << "Sum can only be called on arithmetic types!!!" << std::endl;
      }
      return;
  }
}

#define TEMPLATED_SCATTER_INIT_REDUCE_BY_IN(in_sql_type, in_cpp_type, ...) \
        case SqlType::in_sql_type:                            \
          TemplatedScatterInitReduceByIn<in_cpp_type>(in, i, out, agg_offset, agg_type); \
          return;\


void VectorOps::ScatterInitReduceScalar(const Vector *in, sel_t i, char *out, uint64_t agg_offset, AggType agg_type) {
  switch (in->ElemType()) {
    SQL_TYPE(TEMPLATED_SCATTER_INIT_REDUCE_BY_IN, TEMPLATED_SCATTER_INIT_REDUCE_BY_IN)
  }
}

}

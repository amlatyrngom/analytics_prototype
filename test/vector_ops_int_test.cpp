#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/vector_ops.h"
#include "smhasher/src/MurmurHash2.h"
using namespace smartid;

template<typename T, typename Op>
void GenVecData(Op gen_fn, Vector *out) {
  auto data = out->MutableDataAs<T>();
  for (int i = 0; i < VEC_SIZE; i++) {
    data[i] = gen_fn(i);
  }
}

TEST_CASE("Constant Arithmetic") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    int modulo = 10;
    int offset = 0;
    auto gen_fn = [&](int i) {
      return i % modulo + offset;
    };
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    Vector vec1(SqlType::Int64);
    GenVecData<int64_t>(gen_fn, &vec1);
    Vector vec2(SqlType::Int64);
    Vector vec3(SqlType::Int64);
    Vector vec4(SqlType::Int64);
    Vector vec5(SqlType::Int64);
    VectorOps::ConstantArithVector(&vec1, Value(int64_t(1)), &filter, OpType::ADD, &vec2);
    VectorOps::ConstantArithVector(&vec1, Value(int64_t(1)), &filter, OpType::SUB, &vec3);
    VectorOps::ConstantArithVector(&vec1, Value(int64_t(2)), &filter, OpType::MUL, &vec4);
    VectorOps::ConstantArithVector(&vec1, Value(int64_t(2)), &filter, OpType::DIV, &vec5);

    auto data1 = vec1.DataAs<int64_t>();
    auto data2 = vec2.DataAs<int64_t>();
    auto data3 = vec3.DataAs<int64_t>();
    auto data4 = vec4.DataAs<int64_t>();
    auto data5 = vec5.DataAs<int64_t>();
    filter.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn(i));
      REQUIRE(data2[i] == (gen_fn(i) + 1));
      REQUIRE(data3[i] == (gen_fn(i) - 1));
      REQUIRE(data4[i] == (gen_fn(i) * 2));
      REQUIRE(data5[i] == (gen_fn(i) / 2));
    });
  }
}

TEST_CASE("Binary Arithmetic") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int64);
    auto gen_fn1 = [&](int i) {
      return i % 10 + 3;
    };
    auto gen_fn2 = [&](int i) {
      return i % 5 + 1;
    };
    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int64_t>(gen_fn2, &vec2);

    Vector res1(SqlType::Int64);
    VectorOps::BinaryArithVector(&vec1, &vec2, &filter, OpType::ADD, &res1);
    Vector res2(SqlType::Int64);
    VectorOps::BinaryArithVector(&vec1, &vec2, &filter, OpType::SUB, &res2);
    Vector res3(SqlType::Int64);
    VectorOps::BinaryArithVector(&vec1, &vec2, &filter, OpType::MUL, &res3);
    Vector res4(SqlType::Int64);
    VectorOps::BinaryArithVector(&vec1, &vec2, &filter, OpType::DIV, &res4);

    auto res1_data = res1.DataAs<int64_t>();
    auto res2_data = res2.DataAs<int64_t>();
    auto res3_data = res3.DataAs<int64_t>();
    auto res4_data = res4.DataAs<int64_t>();

    filter.Map([&](sel_t i) {
      REQUIRE(res1_data[i] == (gen_fn1(i) + gen_fn2(i)));
      REQUIRE(res2_data[i] == (gen_fn1(i) - gen_fn2(i)));
      REQUIRE(res3_data[i] == (gen_fn1(i) * gen_fn2(i)));
      REQUIRE(res4_data[i] == (gen_fn1(i) / gen_fn2(i)));
    });
  }
}

TEST_CASE("Constant Comparison") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Vector vec(SqlType::Int64);
    auto gen_fn = [&](int i) {
      return i % 10;
    };
    GenVecData<int64_t>(gen_fn, &vec);
    Filter filter1;
    Filter filter2;
    Filter filter3;
    Filter filter4;
    Filter filter5;
    Filter filter6;
    filter1.Reset(VEC_SIZE, filter_mode);
    filter2.Reset(VEC_SIZE, filter_mode);
    filter3.Reset(VEC_SIZE, filter_mode);
    filter4.Reset(VEC_SIZE, filter_mode);
    filter5.Reset(VEC_SIZE, filter_mode);
    filter6.Reset(VEC_SIZE, filter_mode);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::LT, &filter1);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::LE, &filter2);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::GT, &filter3);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::GE, &filter4);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::EQ, &filter5);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::NE, &filter6);

    auto data = vec.DataAs<int64_t>();
    filter1.Map([&](sel_t i) {
      REQUIRE(data[i] == gen_fn(i));
      REQUIRE(data[i] < 5);
    });
    filter2.Map([&](sel_t i) {
      REQUIRE(data[i] == gen_fn(i));
      REQUIRE(data[i] <= 5);
    });
    filter3.Map([&](sel_t i) {
      REQUIRE(data[i] == gen_fn(i));
      REQUIRE(data[i] > 5);
    });
    filter4.Map([&](sel_t i) {
      REQUIRE(data[i] == gen_fn(i));
      REQUIRE(data[i] >= 5);
    });
    filter5.Map([&](sel_t i) {
      REQUIRE(data[i] == gen_fn(i));
      REQUIRE(data[i] == 5);
    });
    filter6.Map([&](sel_t i) {
      REQUIRE(data[i] == gen_fn(i));
      REQUIRE(data[i] != 5);
    });
  }
}

TEST_CASE("Binary Comparison") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int64);
    auto gen_fn1 = [&](int i) {
      return i % 7;
    };
    auto gen_fn2 = [&](int i) {
      return i % 13 - 5;
    };
    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int64_t>(gen_fn2, &vec2);
    Filter filter1;
    Filter filter2;
    Filter filter3;
    Filter filter4;
    Filter filter5;
    Filter filter6;
    filter1.Reset(VEC_SIZE, filter_mode);
    filter2.Reset(VEC_SIZE, filter_mode);
    filter3.Reset(VEC_SIZE, filter_mode);
    filter4.Reset(VEC_SIZE, filter_mode);
    filter5.Reset(VEC_SIZE, filter_mode);
    filter6.Reset(VEC_SIZE, filter_mode);
    VectorOps::BinaryCompVector(&vec1, &vec2, &filter1, OpType::LT);
    VectorOps::BinaryCompVector(&vec1, &vec2, &filter2, OpType::LE);
    VectorOps::BinaryCompVector(&vec1, &vec2, &filter3, OpType::GT);
    VectorOps::BinaryCompVector(&vec1, &vec2, &filter4, OpType::GE);
    VectorOps::BinaryCompVector(&vec1, &vec2, &filter5, OpType::EQ);
    VectorOps::BinaryCompVector(&vec1, &vec2, &filter6, OpType::NE);

    auto data1 = vec1.DataAs<int64_t>();
    auto data2 = vec2.DataAs<int64_t>();
    filter1.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(data1[i] < data2[i]);
    });
    filter2.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(data1[i] <= data2[i]);
    });
    filter3.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(data1[i] > data2[i]);
    });
    filter4.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(data1[i] >= data2[i]);
    });
    filter5.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(data1[i] == data2[i]);
    });
    filter6.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(data1[i] != data2[i]);
    });
  }
}

TEST_CASE("Mix arithmetic and comparison operations") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    Vector vec(SqlType::Int64);
    auto gen_fn = [&](int i) {
      return i % 10;
    };
    GenVecData<int64_t>(gen_fn, &vec);
    VectorOps::ConstantCompVector(&vec, Value(int64_t(5)), OpType::LT, &filter);

    Vector res1(SqlType::Int64);
    VectorOps::ConstantArithVector(&vec, Value(int64_t(3)), &filter, OpType::ADD, &res1);
    VectorOps::ConstantCompVector(&res1, Value(int64_t(5)), OpType::LT, &filter);

    auto data = res1.DataAs<int64_t>();
    filter.Map([&](sel_t i) {
      REQUIRE(data[i] == (gen_fn(i) + 3));
      REQUIRE(data[i] < 5);
    });
  }
}

TEST_CASE("Single Column Hashing") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    Vector vec(SqlType::Int64);
    Vector hash_vec(SqlType::Int64);
    auto gen_fn = [&](int i) {
      return i % 10;
    };
    auto hash_gen_fn = [&](int i) {
      int64_t val = gen_fn(i);
      auto hash = ArithHashMurmur2(val, 0);
      return hash;
    };

    GenVecData<int64_t>(gen_fn, &vec);
    VectorOps::HashVector(&vec, &filter, &hash_vec);

    auto hash_data = hash_vec.DataAs<uint64_t>();
    filter.Map([&](sel_t i) {
      REQUIRE(hash_data[i] == hash_gen_fn(i));
    });
  }
}

TEST_CASE("Multiple Columns Hashing") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int32);
    Vector hash_vec(SqlType::Int64);
    Vector hash_vec_tmp(SqlType::Int64);
    auto gen_fn1 = [&](int i) {
      return i % 10;
    };
    auto gen_fn2 = [&](int i) {
      return i % 13 + 5;
    };
    auto hash_gen_fn1 = [&](int i) {
      int64_t val = gen_fn1(i);
      return ArithHashMurmur2(val, 0);
    };
    auto hash_gen_fn2 = [&](int i) {
      int32_t val = gen_fn2(i);
      auto seed = hash_gen_fn1(i);
      return ArithHashMurmur2(val, seed);
    };

    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int32_t>(gen_fn2, &vec2);
    VectorOps::HashVector(&vec1, &filter, &hash_vec);
    VectorOps::HashVector(&vec1, &filter, &hash_vec_tmp);
    VectorOps::HashCombineVector(&vec2, &filter, &hash_vec);

    auto hash_data = hash_vec.DataAs<uint64_t>();
    auto hash_data_tmp = hash_vec_tmp.DataAs<uint64_t>();
    auto data1 = vec1.DataAs<int64_t>();
    auto data2 = vec2.DataAs<int32_t>();
    filter.Map([&](sel_t i) {
      REQUIRE(data1[i] == gen_fn1(i));
      REQUIRE(data2[i] == gen_fn2(i));
      REQUIRE(hash_data_tmp[i] == hash_gen_fn1(i));
      REQUIRE(hash_data[i] == hash_gen_fn2(i));
    });
  }
}
namespace {
struct TestEntry {
  int64_t val1{388};
  int32_t val2{323};
  int8_t val3{37}; // sanity check;
};
}

TEST_CASE("Scatter Test") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    auto offset1 = offsetof(TestEntry, val1);
    auto offset2 = offsetof(TestEntry, val2);
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int32);
    Vector vec3(SqlType::Pointer);
    auto gen_fn1 = [&](int i) {
      return i % 7;
    };
    auto gen_fn2 = [&](int i) {
      return i % 13 - 5;
    };
    auto gen_fn3 = [&](int i) {
      return new TestEntry;
    };
    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int32_t>(gen_fn2, &vec2);
    GenVecData<TestEntry *>(gen_fn3, &vec3);
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    VectorOps::ScatterVector(&vec1, &filter, SqlType::Int64, offset1, &vec3);
    VectorOps::ScatterVector(&vec2, &filter, SqlType::Int32, offset2, &vec3);
    auto data1 = vec1.DataAs<int64_t>();
    auto data2 = vec2.DataAs<int32_t>();
    auto data3 = vec3.DataAs<const TestEntry *>();

    filter.Map([&](sel_t i) {
      REQUIRE(data3[i]->val1 == data1[i]);
      REQUIRE(data3[i]->val2 == data2[i]);
      REQUIRE(data3[i]->val3 == 37);
      delete data3[i];
    });
  }
}

TEST_CASE("Scatter Scalar Test") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    auto offset1 = offsetof(TestEntry, val1);
    auto offset2 = offsetof(TestEntry, val2);
    TestEntry entry;
    entry.val3 = 37;
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int32);
    auto gen_fn1 = [&](int i) {
      return i % 7;
    };
    auto gen_fn2 = [&](int i) {
      return i % 13 - 5;
    };
    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int32_t>(gen_fn2, &vec2);
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    filter.Map([&](sel_t i) {
      VectorOps::ScatterScalar(&vec1, i, reinterpret_cast<char *>(&entry), offset1);
      VectorOps::ScatterScalar(&vec2, i, reinterpret_cast<char *>(&entry), offset2);
      REQUIRE(entry.val1 == gen_fn1(i));
      REQUIRE(entry.val2 == gen_fn2(i));
      REQUIRE(entry.val3 == 37);
    });
  }
}

struct AggTestEntry {
  int64_t count;
  double sum;
  int8_t canary{37};
};

TEST_CASE("Scatter Init Reduce Scalar Test") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    auto count_offset = offsetof(AggTestEntry, count);
    auto sum_offset = offsetof(AggTestEntry, sum);
    AggTestEntry entry;
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int32);
    auto gen_fn1 = [&](int i) {
      return i % 7;
    };
    auto gen_fn2 = [&](int i) {
      return i % 13 - 5;
    };
    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int32_t>(gen_fn2, &vec2);
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    filter.Map([&](sel_t i) {
      VectorOps::ScatterInitReduceScalar(&vec1, i, reinterpret_cast<char *>(&entry), count_offset, AggType::COUNT);
      VectorOps::ScatterInitReduceScalar(&vec2, i, reinterpret_cast<char *>(&entry), sum_offset, AggType::SUM);
      REQUIRE(entry.count == 1);
      REQUIRE(entry.sum == Approx(gen_fn2(i)));
      REQUIRE(entry.canary == 37);
    });
  }
}

TEST_CASE("Gather Test") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    auto offset1 = offsetof(TestEntry, val1);
    auto offset2 = offsetof(TestEntry, val2);
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int32);
    Vector vec3(SqlType::Pointer);
    auto gen_fn1 = [&](int i) {
      return i % 7;
    };
    auto gen_fn2 = [&](int i) {
      return i % 13 - 5;
    };
    auto gen_fn3 = [&](int i) {
      return new TestEntry{gen_fn1(i), gen_fn2(i), 37};
    };
    GenVecData<TestEntry *>(gen_fn3, &vec3);
    Filter filter;
    filter.Reset(VEC_SIZE, filter_mode);
    VectorOps::GatherVector(&vec3, &filter, SqlType::Int64, offset1, &vec1);
    VectorOps::GatherVector(&vec3, &filter, SqlType::Int32, offset2, &vec2);
    auto data1 = vec1.DataAs<int64_t>();
    auto data2 = vec2.DataAs<int32_t>();
    auto data3 = vec3.DataAs<const TestEntry *>();

    filter.Map([&](sel_t i) {
      REQUIRE(data3[i]->val1 == data1[i]);
      REQUIRE(data3[i]->val2 == data2[i]);
      REQUIRE(data3[i]->val3 == 37);
      delete data3[i];
    });
  }
}

TEST_CASE("Gather Compare") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    auto offset1 = offsetof(TestEntry, val1);
    auto offset2 = offsetof(TestEntry, val2);
    Vector vec1(SqlType::Int64);
    Vector vec2(SqlType::Int32);
    Vector vec3(SqlType::Pointer);
    auto gen_fn1 = [&](int i) {
      return i % 10;
    };
    auto gen_fn2 = [&](int i) {
      return i % 12;
    };
    auto gen_fn3 = [&](int i) {
      return new TestEntry{i % 5, i % 6, 37};
    };
    GenVecData<int64_t>(gen_fn1, &vec1);
    GenVecData<int32_t>(gen_fn2, &vec2);
    GenVecData<TestEntry *>(gen_fn3, &vec3);
    Filter filter1;
    filter1.Reset(VEC_SIZE, filter_mode);
    Filter filter2;
    filter2.Reset(VEC_SIZE, filter_mode);
    VectorOps::GatherCompareVector(&vec3, &vec1, &filter1, SqlType::Int64, offset1);
    VectorOps::GatherCompareVector(&vec3, &vec2, &filter2, SqlType::Int32, offset2);
    auto data1 = vec1.DataAs<int64_t>();
    auto data2 = vec2.DataAs<int32_t>();
    auto data3 = vec3.DataAs<const TestEntry *>();
    filter1.Map([&](sel_t i) {
      REQUIRE(data3[i]->val1 == data1[i]);
    });
    filter2.Map([&](sel_t i) {
      REQUIRE(data3[i]->val2 == data2[i]);
    });
  }
}

TEST_CASE("Select Test") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    Vector vec1(SqlType::Int64);
    auto gen_fn1 = [&](int i) {
      return i % 20;
    };
    auto match_gen_fn = [&](int i) {
      return (i % 37 + 37) % VEC_SIZE;
    };

    GenVecData<int64_t>(gen_fn1, &vec1);
    int num_matches = BLOCK_SIZE;
    std::vector<uint64_t> indices(BLOCK_SIZE);
    for (int i = 0; i < num_matches; i++) {
      indices[i] = (match_gen_fn(i));
    }

    Filter filter;
    filter.Reset(num_matches, filter_mode);
    Vector match_vec(SqlType::Int64);
    match_vec.Resize(num_matches);
    VectorOps::SelectVector(&vec1, indices, &match_vec);

    auto data = vec1.DataAs<int64_t>();
    auto match_data = match_vec.DataAs<int64_t>();

    REQUIRE(filter.ActiveSize() == num_matches);
    REQUIRE(match_vec.NumElems() == num_matches);
    filter.Map([&](sel_t i) {
      REQUIRE(match_data[i] == data[indices[i]]);
    });
  }
}

TEST_CASE("Reduce Test") {
  for (const auto &filter_mode: {FilterMode::BitmapSelective, FilterMode::BitmapFull, FilterMode::SelVecSelective,
                                 FilterMode::SelVecFull}) {
    int vec_size = 100;
    Filter filter;
    filter.Reset(vec_size, filter_mode);
    Vector vec(SqlType::Int64);
    auto gen_fn = [&](int i) {
      return i;
    };
    GenVecData<int64_t>(gen_fn, &vec);
    // Filter out a few elements.
    int upper = vec_size / 2;
    VectorOps::ConstantCompVector(&vec, Value(int64_t(vec_size / 2)), OpType::LT, &filter);

    Vector res1(SqlType::Float64, vec_size);
    Vector res2(SqlType::Int64, vec_size);
    Vector res3(SqlType::Int64, vec_size);
    Vector res4(SqlType::Int64, vec_size);
    auto res_data1 = res1.DataAs<double>();
    auto res_data2 = res2.DataAs<int64_t>();
    auto res_data3 = res3.DataAs<int64_t>();
    auto res_data4 = res4.DataAs<int64_t>();
    VectorOps::ReduceVector(&filter, &vec, &res1, AggType::SUM);
    VectorOps::ReduceVector(&filter, &vec, &res2, AggType::COUNT);
    VectorOps::ReduceVector(&filter, &vec, &res3, AggType::MAX);
    VectorOps::ReduceVector(&filter, &vec, &res4, AggType::MIN);
    REQUIRE(res_data1[0] == Approx(upper * (upper - 1) / 2.0));
    REQUIRE(res_data2[0] == upper);
    REQUIRE(res_data3[0] == (upper - 1));
    REQUIRE(res_data4[0] == 0);
  }
}


#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include <chrono>
#include "execution/vector_ops.h"
using namespace smartid;

template<typename T, typename Op>
void GenVecData(Op gen_fn, Vector *out) {
  auto data = out->MutableDataAs<T>();
  for (int i = 0; i < VEC_SIZE; i++) {
    data[i] = gen_fn(i);
  }
}

TEST_CASE("Simple Selvec Subset Difference") {
  int vec_size = 2048;
  auto gen_fn = [&](int i) {
    return i % 16;
  };

  double total_runtime = 0;
  int num_runs = 100000;
  Vector vec(SqlType::Int64);
  GenVecData<int64_t>(gen_fn, &vec);
  Filter filter1;
  Filter filter2;
  Filter filter3;
  filter1.Reset(vec_size, FilterMode::SelVecSelective);
  filter2.Reset(vec_size, FilterMode::SelVecSelective);
  VectorOps::ConstantCompVector(&vec, Value(int64_t(8)), OpType::LT, &filter2);
  for (int i = 0; i < num_runs; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    filter1.SubsetDifference(&filter2, &filter3);
    DoNotOptimize(filter1);
    DoNotOptimize(filter2);
    DoNotOptimize(filter3);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start);
    total_runtime += duration.count();
  }
  std::cout << "SelVec Took: " << (total_runtime / num_runs) << "ns" << std::endl;
}

TEST_CASE("Simple Selvec Subset Difference with Same Selvec") {
  int vec_size = 32;
  auto gen_fn = [&](int i) {
    return i % 16;
  };
  Vector vec(SqlType::Int64);
  GenVecData<int64_t>(gen_fn, &vec);
  Filter filter1;
  Filter filter2;
  filter1.Reset(vec_size, FilterMode::SelVecSelective);
  filter2.Reset(vec_size, FilterMode::SelVecSelective);
  VectorOps::ConstantCompVector(&vec, Value(int64_t(4)), OpType::LT, &filter2);
  filter1.SubsetDifference(&filter2, &filter1);
  filter1.Map([&](sel_t i) {
    std::cout << "Filter1: " << i << std::endl;
  });
  filter2.Map([&](sel_t i) {
    std::cout << "Filter2: " << i << std::endl;
  });
}


TEST_CASE("Simple Bitmap Subset Difference") {
  int vec_size = 2048;
  auto gen_fn = [&](int i) {
    return i % 16;
  };

  double total_runtime = 0;
  int num_runs = 100000;
  Vector vec(SqlType::Int64);
  GenVecData<int64_t>(gen_fn, &vec);
  Filter filter1;
  Filter filter2;
  Filter filter3;
  filter1.Reset(vec_size, FilterMode::BitmapFull);
  filter2.Reset(vec_size, FilterMode::BitmapFull);
  VectorOps::ConstantCompVector(&vec, Value(int64_t(8)), OpType::LT, &filter2);
  for (int i = 0; i < num_runs; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    filter1.SubsetDifference(&filter2, &filter3);
    DoNotOptimize(filter1);
    DoNotOptimize(filter2);
    DoNotOptimize(filter3);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start);
    total_runtime += duration.count();
  }
  std::cout << "Bitmap Took: " << (total_runtime / num_runs) << "ns" << std::endl;
}

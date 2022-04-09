#include "execution/executors/plan_executor.h"
#include "storage/vector_projection.h"
#include "storage/vector.h"

namespace smartid {

PlanExecutor::PlanExecutor(std::vector<std::unique_ptr<PlanExecutor>> &&children) : children_(std::move(children)){}
PlanExecutor::~PlanExecutor() = default;


////////////////////////////
/// No Op.
/////////////////////////////////
const VectorProjection * NoopOutputExecutor::Next() {
  const VectorProjection *vp;
  auto child = Child(0);
  while ((vp = child->Next())) {
    auto num_cols = vp->NumCols();
    auto filter = vp->GetFilter();
    for (uint64_t i = 0; i < num_cols; i++) {
      auto data = vp->VectorAt(i)->Data();
      auto elem_size = vp->VectorAt(i)->ElemSize();
      filter->Map([&](sel_t i) {
        DoNotOptimize(data[i * elem_size]);
      });
    }
  }
  return nullptr;
}

///////////////////////////////////////////
/// Print
///////////////////////////////////////////
namespace {
template<typename T>
static void TemplatedPrintElem(const Vector *vec, uint64_t row) {
  T elem = vec->DataAs<T>()[row];
  if constexpr (std::is_same_v<T, Varlen>) {
    std::string attr(elem.Data(), elem.Info().NormalSize());
    std::cout << attr;
  } else if constexpr (std::is_same_v<T, Date>) {
    std::cout << elem.ToString();
  } else {
    std::cout << elem;
  }
}

#define TEMPLATED_PRINT_ELEM(sql_type, cpp_type, ...) \
        case SqlType::sql_type: \
          TemplatedPrintElem<cpp_type>(vec, row); \
          break;

void PrintVectorElem(const Vector *vec, uint64_t row) {
  switch (vec->ElemType()) {
    SQL_TYPE(TEMPLATED_PRINT_ELEM, TEMPLATED_PRINT_ELEM);
  }
}
}

const VectorProjection *PrintExecutor::Next() {
  auto child = Child(0);
  const VectorProjection *vp;
  while ((vp = child->Next())) {
    auto num_cols = vp->NumCols();
    auto filter = vp->GetFilter();
    filter->Map([&](sel_t i) {
      for (uint64_t col = 0; col < num_cols; col++) {
        PrintVectorElem(vp->VectorAt(col), i);
        if (col == num_cols - 1) {
          std::cout << std::endl;
        } else {
          std::cout << ", ";
        }
      }
    });
  }
  return nullptr;
}
}
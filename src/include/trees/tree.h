#pragma once

#include "common/util.h"
#include "execution/execution_common.h"
#include "execution/vector_projection.h"
#include "json/json.h"
#include "duckdb/duckdb.hpp"
#include "training.h"
#include <fstream>

namespace smartid {


class TreeLeaf {
 public:
  explicit TreeLeaf(uint64_t idx): idx_(0) {}

  [[nodiscard]] uint64_t Mask() const {
    return 1ull << idx_;
  }

  void SetMasks(const Filter* filter, Vector* out) const {
    auto out_data = out->MutableDataAs<uint64_t>();
    filter->Map([&](sel_t i) {
      out_data[i] = Mask();
    });
  }

  [[nodiscard]] uint64_t Idx() const {
    return idx_;
  }
 private:
  uint64_t idx_{0};
};

enum class TreeCompType {
  LESS, EQ, GREATER, BETWEEN
};

struct TreeComp {
  TreeComp(TreeCompType comp_type) : comp_type(comp_type) {}

  bool IsSingleVar() const {
    return comp_type == TreeCompType::LESS || comp_type == TreeCompType::EQ || comp_type == TreeCompType::GREATER;
  }

  bool IsDoubleVar() const {

  }

  TreeCompType comp_type;
};

struct SingleVarComp {
  Value val;
};

class Tree {
 public:
  Tree()
};



}
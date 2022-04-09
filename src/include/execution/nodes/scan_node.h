#pragma once
#include "execution/nodes/plan_node.h"

namespace smartid {
class Table;
class ExprNode;

class ScanNode : public PlanNode {
 public:
  ScanNode(Table* table,
                     std::vector<uint64_t>&& cols_to_read,
                     std::vector<ExprNode *> &&projections,
                     std::vector<ExprNode *> &&filters);


  [[nodiscard]] const auto& GetColsToRead() const{
    return cols_to_read_;
  }

  [[nodiscard]] const auto& GetFilters() const{
    return filters_;
  }

  [[nodiscard]] const auto& GetProjections() const{
    return projections_;
  }

  [[nodiscard]] const auto* GetTable() const {
    return table_;
  }

  [[nodiscard]] Table* GetTable() {
    return table_;
  }

 private:
  Table* table_;
  std::vector<uint64_t> cols_to_read_;
  std::vector<ExprNode *> projections_;
  std::vector<ExprNode *> filters_;
};

}
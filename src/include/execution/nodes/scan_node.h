#pragma once
#include "execution/execution_common.h"
#include "common/catalog.h"
#include "expr_node.h"
#include "execution/nodes/plan_node.h"

namespace smartid {
class ScanNode : public PlanNode {
 public:
  ScanNode(Table* table,
                     std::vector<ExprNode *> &&projections,
                     std::vector<ExprNode *> &&filters, bool record_rows)
      : PlanNode(PlanType::Scan, {})
      , table_(table)
      , projections_(std::move(projections))
      , filters_(std::move(filters))
      , record_rows_(record_rows) {}


  void ResetEmbeddingFilter() {
    embedding_filters_.clear();
  }

  void AddEmbeddingFilters(std::vector<ExprNode*>&& embedding_filters) {
    embedding_filters_.insert(embedding_filters_.end(), embedding_filters.begin(), embedding_filters.end());
  }

  [[nodiscard]] const std::vector<ExprNode*>& GetFilters() const{
    return filters_;
  }

  [[nodiscard]] const std::vector<ExprNode*>& GetEmbeddingFilters() const{
    return embedding_filters_;
  }

  [[nodiscard]] const std::vector<ExprNode*>& GetProjections() const{
    return projections_;
  }

  [[nodiscard]] const Table* GetTable() const {
    return table_;
  }

  [[nodiscard]] Table* GetTable() {
    return table_;
  }

  [[nodiscard]] bool RecordRows() const {
    return record_rows_;
  }


  void ReportStats(double scan_time) {
    scan_times_.emplace_back(scan_time);
  }

  const auto& GetStats() {
    return scan_times_;
  }

 private:
  Table* table_;
  std::vector<ExprNode *> projections_;
  std::vector<ExprNode *> filters_;
  bool record_rows_;
  std::vector<ExprNode *> embedding_filters_;
  std::vector<double> scan_times_;
};

}
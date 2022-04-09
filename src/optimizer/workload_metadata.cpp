#include "optimizer/workload_metadata.h"

namespace smartid {
void LogicalFilter::ToString(std::ostream &os) const {
  os << "Filter("
     << "col_name=" << col_name << ", "
     << "op=" << op << ", ";
  os << "[";
  uint64_t i = 0;
  for(const auto& v: vals) {
    os << v;
    if (i < vals.size() - 1) {
      std::cout << ",";
    }
    i++;
  }
  os << "])";
}

void LogicalScanNode::ToString(std::ostream &os) const {
  os << "Scan(name=" << table_info->name << ", "
     << "projections=(";
  uint64_t i = 0;
  for (const auto& p: projections) {
    os << p;
    if (i < projections.size() - 1) {
      std::cout << ",";
    }
    i++;
  }
  os << "), filters=(";
  i=0;
  for (const auto& f: filters) {
    std::cout << i << ": ";
    f.ToString(os);
    if (i < filters.size() - 1) {
      std::cout << ", ";
    }
    i++;
  }
  os << ")";
}

void QueryInfo::ToString(std::ostream &os) const {
  os << "Showing Query: " << name << std::endl;
  for (const auto& s: scans) {
    s->ToString(os);
    os << std::endl;
  }
}

LogicalScanNode *QueryInfo::CopyScan(std::vector<std::unique_ptr<LogicalScanNode>> &scan_allocator) {
  scan_allocator.emplace_back(std::make_unique<LogicalScanNode>());
  auto right_scan = scan_allocator.back().get();
  *right_scan = *scans[0];
  return right_scan;
}

std::vector<LogicalJoinNode *> QueryInfo::CopyJoinOrders(std::vector<std::unique_ptr<LogicalScanNode>> &scan_allocator,
                                                         std::vector<std::unique_ptr<LogicalJoinNode>> &join_allocator) {
  std::vector<LogicalJoinNode*> res;
  for (const auto& order: join_orders) {
    res.emplace_back(RecursiveCopy(order, scan_allocator, join_allocator));
  }
  return res;
}

LogicalJoinNode *QueryInfo::RecursiveCopy(LogicalJoinNode *src,
                                          std::vector<std::unique_ptr<LogicalScanNode>> &scan_allocator,
                                          std::vector<std::unique_ptr<LogicalJoinNode>> &join_allocator) {
  join_allocator.emplace_back(std::make_unique<LogicalJoinNode>());
  auto curr_node = join_allocator.back().get();
  scan_allocator.emplace_back(std::make_unique<LogicalScanNode>());
  auto right_scan = scan_allocator.back().get();
  *right_scan = *src->right_scan;
  curr_node->scanned_tables = src->scanned_tables;
  curr_node->right_scan = right_scan;
  if (src->left_scan != nullptr) {
    scan_allocator.emplace_back(std::make_unique<LogicalScanNode>());
    auto left_scan = scan_allocator.back().get();
    *left_scan = *src->left_scan;
    curr_node->left_scan = left_scan;
  } else {
    curr_node->left_join = RecursiveCopy(src->left_join, scan_allocator, join_allocator);
  }
  return curr_node;
}

}
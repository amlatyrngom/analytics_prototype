#include "execution/nodes/scan_node.h"

namespace smartid {

ScanNode::ScanNode(Table *table,
                   std::vector<uint64_t>&& cols_to_read,
                   std::vector<ExprNode *> &&projections,
                   std::vector<ExprNode *> &&filters)
    : PlanNode(PlanType::Scan, {})
    , table_(table)
    , cols_to_read_(std::move(cols_to_read))
    , projections_(std::move(projections))
    , filters_(std::move(filters)) {}

}
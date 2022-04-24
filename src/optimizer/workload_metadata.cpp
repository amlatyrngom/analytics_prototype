#include "optimizer/workload_metadata.h"
#include <fmt/core.h>

namespace smartid {

void SmartIDInfo::ToString(std::ostream &os) const {
  os << fmt::format("SmartIDInfo(from=({}, {}, {}), to=({}, {}, {}), key={}, bit_offset={}, num_bits={})\n", from_table_name, from_col_name, from_col_idx, to_table_name, to_col_name, to_col_idx, from_key_col_name, bit_offset, num_bits);
//  if (smartid_query->scans.size() == 1) {
//    smartid_query->scans[0]->ToString(os);
//  } else {
//    smartid_query->best_join_order->ToString(os);
//  }
}

}
#pragma once
#include <unordered_map>

namespace smartid {
class Catalog;
class WorkloadInfo;
class Table;
class LogicalScanNode;
class TableInfo;
class InfoStore;

class TableStatistics {

  TableStatistics(TableInfo* table_info, InfoStore* info_store);
  void EstimateSelectivity(LogicalScanNode* scan_node);

  double num_tuples_;
  std::unordered_map<uint64_t, double> max_freq_;
};

struct Statistics {
  void GenValueDistribution(Catalog* catalog, WorkloadInfo* workload);

  void SampleTables(Catalog* catalog, WorkloadInfo* workload);
};

}
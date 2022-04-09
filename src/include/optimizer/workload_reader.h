#pragma once
#include <memory>


namespace smartid {
class Catalog;
class WorkloadInfo;

struct WorkloadReader {
  static std::unique_ptr<WorkloadInfo> ReadWorkloadInfo(const std::string &toml_file);
  void ReadQueriesAndTables(Catalog* catalog, WorkloadInfo *workload);
};

}
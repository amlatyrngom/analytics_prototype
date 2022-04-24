#pragma once
#include <memory>
#include <vector>


namespace smartid {
class Catalog;
class WorkloadInfo;
class ExecutionFactory;
class LogicalJoin;

struct WorkloadReader {
  static std::unique_ptr<WorkloadInfo> ReadWorkloadInfo(const std::string &toml_file);
  static void ReadWorkloadTables(Catalog* catalog, WorkloadInfo *workload);
};

struct QueryReader {
  static void ReadWorkloadQueries(Catalog* catalog, WorkloadInfo *workload, ExecutionFactory* execution_factory, const std::vector<std::string>& query_files);
};

}
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include "common/types.h"
#include "logical_nodes.h"

namespace smartid {

struct TableInfo {
  // Constructor.
  TableInfo(std::string name, Schema&& schema, std::string datafile, std::unordered_set<std::string>&& pks, bool to_many, bool is_central)
      : name(std::move(name))
      , schema(std::move(schema))
      , datafile(std::move(datafile))
      , pks(pks)
      , to_many(to_many)
      , is_central(is_central){}

  std::string name;
  Schema schema;
  std::string datafile;
  std::unordered_set<std::string> pks;
  bool to_many;
  bool is_central;

  // Computed when reading queries.
  std::unordered_set<uint64_t> used_cols{};

  // Computed during statistics collection.
  double sample_size{0};
  double base_size{0};
};

struct SmartIDInfo {
  std::string from_table_name;
  TableInfo* from_table_info;
  std::string from_col_name;
  uint64_t from_col_idx;
  std::string from_key_col_name;
  std::string to_table_name;
  TableInfo* to_table_info;
  std::string to_col_name;
  uint64_t to_col_idx;
  SqlType to_col_type;
  uint64_t bit_offset;
  uint64_t num_bits;
  QueryInfo* smartid_query{nullptr};

  void ToString(std::ostream& os) const;
};


struct WorkloadInfo {
  std::string workload_name;
  std::string central_table_name;
  std::string input_folder;
  std::string data_folder;
  std::string workload_file;
  uint64_t log_vec_size;
  uint64_t log_block_size;
  uint64_t log_mem_space;
  uint64_t log_disk_space;
  bool reload{false};
  bool gen_costs{false};
  bool rebuild{false};
  int budget{32};
  int embedding_size{16};
  std::unordered_map<std::string, std::unique_ptr<TableInfo>> table_infos;
  std::unordered_map<std::string, std::unique_ptr<QueryInfo>> query_infos;
  std::unordered_map<std::string, std::unique_ptr<QueryInfo>> mat_view_queries;

  std::unordered_set<std::string> available_mat_views;
  std::unordered_set<std::string> available_idxs;
  std::vector<std::unique_ptr<SmartIDInfo>> smartid_infos;
};
}
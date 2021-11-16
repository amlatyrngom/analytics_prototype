#pragma once

#include "common/util.h"
#include "execution/execution_common.h"
#include "execution/vector_projection.h"
#include "json/json.h"
#include <fstream>

namespace smartid {
struct TreeInput {
 public:
  static std::string DenormFormat(const std::string& table_name, const std::string& col_name) {
    return (table_name + "___") + col_name;
  }

  explicit TreeInput(const std::string& filename) {
    std::stringstream ss;
    std::ifstream in(filename);
    ss << in.rdbuf();
    auto json = nlohmann::json::parse(ss.str());
    std::cout << "Got Json: " << json << std::endl;
    auto denorm_name = json["denorm_name"].get<std::string>();
    std::cout << "Get Denorm Name: " << denorm_name << std::endl;
    auto tables = json["tables"];
    for (const auto& [table_name, table_info]: tables.items()) {
      std::cout << table_name << ", " << table_info << std::endl;
      for (const auto& pk_col: table_info["pks"]) {
        std::cout << "PK: " << pk_col << std::endl;
        table_pks[table_name].emplace_back(pk_col.get<std::string>());
      }
      for (const auto& training_col: table_info["induced"]) {
        std::cout << "Induced: " << training_col << std::endl;
        table_training_cols[table_name].emplace_back(training_col.get<std::string>());
      }
    }
    // Tables with a non-empty set of induced filters.
    for (const auto&p: table_training_cols) {
      std::stringstream query_ss;
      query_ss << "SELECT ";
      for (const auto& pk_col: table_pks[p.first]) {
        query_ss << DenormFormat(p.first, pk_col) << ", ";
      }
      for (uint64_t i = 0; i < p.second.size(); i++) {
        query_ss << p.second[i];
        if (i != p.second.size() - 1) {
          query_ss << ", ";
        }
      }
      query_ss << " FROM " << denorm_name;
      training_queries[p.first] = query_ss.str();
    }
  }

 public:
  std::string denorm_table_name;
  std::unordered_map<std::string, std::vector<std::string>> table_pks;
  std::unordered_map<std::string, std::vector<std::string>> table_training_cols;
  std::unordered_map<std::string, std::string> training_queries;
};

struct TrainingData {
  // Does not take ownership.
  TrainingData(const Block* block, const std::vector<SqlType>& sql_types, const std::vector<std::string>& pk_cols, const std::vector<std::string>& induced) {
    uint64_t idx = 0;
    for (const auto& pk: pk_cols) {
      col_idxs[pk] = idx;
      col_types[pk] = sql_types[idx];
      idx++;
    }
    for (const auto& filter_col: induced) {
      col_idxs[filter_col] = idx;
      col_types[filter_col] = sql_types[idx];
      idx++;
    }
    for (uint64_t i = 0; i < block->NumCols(); i++) {
      auto vec = std::make_unique<Vector>(sql_types[i]);
      vec->ShallowReset(block->NumElems(), block->ColumnData(i));
      vp.AddVector(vec.get());
      vecs.emplace_back(std::move(vec));
    }
  }

  std::vector<std::unique_ptr<Vector>> vecs;
  VectorProjection vp;
  std::unordered_map<std::string, uint64_t> col_idxs;
  std::unordered_map<std::string, SqlType> col_types;
};

}
#pragma once

#include "common/util.h"
#include "execution/execution_common.h"
#include "execution/vector_projection.h"
#include "json/json.h"
#include "duckdb/duckdb.hpp"
#include <fstream>

namespace smartid {

class TreeSet {
 public:
  TreeSet(duckdb::Connection* conn, const std::string& filename, double sample_rate=1): tree_input_(filename), conn_(conn) {
    std::cout << "TREESET" << std::endl;
    for (const auto& p: tree_input_.table_training_cols) {
      const auto& table_name = p.first;
      const auto& training_cols =  p.second;
      const auto& pk_cols = tree_input_.table_pks[table_name];
      const auto& training_q = tree_input_.training_queries[table_name];
      BuildTree(table_name, pk_cols, training_cols, training_q);
    }
  }

  void BuildTree(const std::string& table_name, const std::vector<std::string>& pk_cols, const std::vector<std::string>& training_cols, const std::string& training_query) {
    auto result = conn_->Query(training_query);

    // Set column types.
    std::vector<SqlType> col_types;
    for (const auto& logical_type: result->types) {
      std::cout << logical_type.ToString() << std::endl;
      switch (logical_type.id()) {
        case duckdb::LogicalTypeId::INTEGER:
          col_types.emplace_back(SqlType::Int32);
          break;
        case duckdb::LogicalTypeId::BIGINT:
          col_types.emplace_back(SqlType::Int64);
          break;
        case duckdb::LogicalTypeId::DATE:
          col_types.emplace_back(SqlType::Int32); // Underlying duckdb representation.
          break;
        case duckdb::LogicalTypeId::FLOAT:
          col_types.emplace_back(SqlType::Float32);
          break;
        default:
          ASSERT(false, ("Usupported duckdb type " + logical_type.ToString()));
      }
    }
    // Create training data
    uint64_t total_rows = result->collection.Count();
    uint64_t num_cols = col_types.size();
    std::vector<std::vector<char>> cols{num_cols};
    // Resize
    for (uint64_t i = 0; i < num_cols; i++) {
      cols[i].resize(TypeUtil::TypeSize(col_types[i]) * total_rows);
    }
    // Write data
    uint64_t curr_offset = 0;
    for (auto chunk = result->Fetch(); chunk != nullptr; chunk = result->Fetch()) {
      for (uint64_t i = 0; i < num_cols; i++) {
        auto write_offset = curr_offset * TypeUtil::TypeSize(col_types[i]);
        auto write_amount = chunk->size() * TypeUtil::TypeSize(col_types[i]);
        // now get an orrified vector
        duckdb::VectorData vdata;
        chunk->data[i].Orrify(chunk->size(), vdata);
        auto val = *reinterpret_cast<const int32_t*>(vdata.data);
        std::memcpy(cols[i].data() + write_offset, vdata.data, write_amount);
      }
      curr_offset += chunk->size();
    }
    // Sanity Check
    for (uint64_t r = 0; r < 10; r++) {
      for (uint64_t i = 0; i < num_cols; i++) {
        auto read_offset = r * TypeUtil::TypeSize(col_types[i]);
        auto val = *reinterpret_cast<const int32_t*>(cols[i].data() + read_offset);
        std::cout << val << ", ";
      }
      std::cout << std::endl;
    }
    std::cout << sizeof(duckdb::Date) << std::endl;
    std::cout << "Result Size: " << result->collection.Count() << ", " << curr_offset << std::endl;
  }

 private:
  TreeInput tree_input_;
  duckdb::Connection* conn_;
  std::unordered_map<std::string, std::unique_ptr<Tree>> trees_;
};
}
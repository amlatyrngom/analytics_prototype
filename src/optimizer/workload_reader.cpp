#include "optimizer/workload_reader.h"
#include "storage/vector.h"
#include "common/catalog.h"
#include "storage/vector_projection.h"
#include "optimizer/workload_metadata.h"
#include "storage/dict_encoding.h"
#include "csv/csv.h"
#include <memory>
#include <random>
#include <unordered_map>
#include <toml++/toml.h>


namespace smartid {

void ReadScan(toml::table& tml_scan, WorkloadInfo* workload, QueryInfo* res) {
  // Read table name.
  auto table_name = tml_scan["table"].value<std::string>().value();
  auto table_info = workload->table_infos[table_name].get();
  res->allocated_scans.emplace_back(std::make_unique<LogicalScanNode>());
  auto scan = res->allocated_scans.back().get();
  scan->table_info = table_info;
  // Read projections.
  auto& projections_tml = *tml_scan["projections"].as_array();
  for (const auto& col_name: projections_tml) {
    scan->projections.emplace_back(col_name.value<std::string>().value());
  }
  // Read filters.
  auto& filters_tml = *tml_scan["filters"].as_array();
  for (const auto& filter_tml: filters_tml) {
    LogicalFilter filter;
    auto filter_tml_table = *filter_tml.as_table();
    filter.col_name = filter_tml_table["col"].value<std::string>().value();
    filter.op = filter_tml_table["op"].value<std::string>().value();
    filter.lo_strict = filter_tml_table["lo_strict"].value_or<bool>(false);
    filter.hi_strict = filter_tml_table["hi_strict"].value_or<bool>(false);
    auto& vals_tml = *filter_tml_table["vals"].as_array();
    for (const auto& val_tml: vals_tml) {
      filter.vals.emplace_back(val_tml.value<std::string>().value());
    }
    scan->filters.emplace_back(std::move(filter));
  }
  std::cout << "ReadScan: ";
  scan->ToString(std::cout);
  std::cout << std::endl;
  res->scans.emplace_back(scan);
}

void ReadQueries(toml::table& tml, WorkloadInfo* workload) {
  auto& queries_tml = *tml["queries"].as_table();
  std::cout << queries_tml << std::endl;
  std::unordered_map<std::string, std::unique_ptr<QueryInfo>> query_infos;
  for (const auto& [q_name, q]: queries_tml) {
    auto res = std::make_unique<QueryInfo>();
    res->name = std::string(q_name);
    auto q_tml = *q.as_table();
    auto type = q_tml["type"].value<std::string>().value();
    res->count = q_tml["count"].value_or<bool>(false);
    if (type == "scan") {
      ReadScan(q_tml, workload, res.get());
    }
    if (type == "join") {
      for (const auto& [table_name, scan_tml]: q_tml) {
        if (scan_tml.is_table()) {
          ReadScan(*scan_tml.as_table(), workload, res.get());
        }
      }
    }
    res->ToString(std::cout);
    query_infos[res->name] = std::move(res);
  }
  workload->query_infos = std::move(query_infos);
}



void EnumerateAllJoinOrders(WorkloadInfo* workload) {
  for (auto& [query_name, query_info]: workload->query_infos) {
    if (query_info->scans.size() == 1) continue; // Query is not a join.
    auto& allocated_scans = query_info->allocated_scans;
    auto& allocated_joins = query_info->allocated_joins;
    std::vector<TableInfo*> table_infos;
    std::vector<uint64_t> table_idxs;
    uint64_t table_idx = 0;
    int64_t central_table_idx{-1};
    for (const auto& scan: query_info->scans) {
      table_infos.emplace_back(scan->table_info);
      table_idxs.emplace_back(table_idx);
      if (scan->table_info->name == workload->central_table_name) {
        central_table_idx = static_cast<int64_t>(table_idx);
      }
      table_idx++;
    }
    ASSERT(central_table_idx >= 0, "Joins should always contain the central table!");

    std::vector<LogicalJoinNode*> possible_orders;

    do {
      // Check if the join is feasible.
      if (table_idxs[0] != central_table_idx && table_idxs[1] != central_table_idx) continue;
      // Left most scan
      allocated_scans.emplace_back(std::make_unique<LogicalScanNode>());
      auto left_most_scan = allocated_scans.back().get();
      left_most_scan->table_info = table_infos[table_idxs[0]];
      allocated_scans.emplace_back(std::make_unique<LogicalScanNode>());
      // Next scan
      auto curr_scan = allocated_scans.back().get();
      curr_scan->table_info = table_infos[table_idxs[1]];
      allocated_joins.emplace_back(std::make_unique<LogicalJoinNode>());
      // First join
      auto curr_join = allocated_joins.back().get();
      curr_join->left_scan = left_most_scan;
      curr_join->right_scan = curr_scan;
      curr_join->scanned_tables.emplace_back(left_most_scan->table_info);
      curr_join->scanned_tables.emplace_back(curr_scan->table_info);
      // Add subsequent joins.
      for (uint64_t i = 2; i < table_idxs.size(); i++) {
        auto prev_join = curr_join;
        // First make right scan
        allocated_scans.emplace_back(std::make_unique<LogicalScanNode>());
        curr_scan = allocated_scans.back().get();
        curr_scan->table_info = table_infos[table_idxs[i]];
        // Now join right scan with previous join.
        allocated_joins.emplace_back(std::make_unique<LogicalJoinNode>());
        curr_join = allocated_joins.back().get();
        curr_join->left_join = prev_join;
        curr_join->right_scan = curr_scan;
        // Scan tables is prev tables + right table.
        curr_join->scanned_tables = prev_join->scanned_tables;
        curr_join->scanned_tables.emplace_back(curr_scan->table_info);
      }
      possible_orders.emplace_back(curr_join);
      std::cout <<  "Got Join order: ";
      for (const auto& table_info: curr_join->scanned_tables) {
        std::cout << table_info->name << ", ";
      }
      std::cout << std::endl;
    } while(std::next_permutation(table_idxs.begin(), table_idxs.end()));

    query_info->join_orders = possible_orders;
  }
}




void WriteCol(char *col_data, uint64_t* bitmap, csv::CSVField &field, uint64_t row_idx, SqlType type) {
  auto field_size = field.get<std::string_view>().size();

  // Check for null
  if (field_size == 0 && type != SqlType::Varchar) {
    // Null. No need to do anything.
    return;
  } else if (field_size == 1 && type == SqlType::Varchar) {
    auto val = field.get<std::string_view>();
    if (val == "\\N") return;
  }
  // Non null
  Bitmap::SetBit(bitmap, row_idx);
  auto write_location = col_data + TypeUtil::TypeSize(type) * row_idx;
//  std::cout << "FIELD: " << field << std::endl;
//  std::cout << "Write Loc: " << (write_location - col_data) << std::endl;
  switch (type) {
    case SqlType::Char: {
      auto val = field.get<std::string_view>();
      std::memcpy(write_location, &val[0], sizeof(char));
      break;
    }
    case SqlType::Int8: {
      auto val = static_cast<int8_t>(field.get<int>());
      std::memcpy(write_location, &val, sizeof(int8_t));
      break;
    }
    case SqlType::Int16: {
      auto val = field.get<int16_t>();
      std::memcpy(write_location, &val, sizeof(int16_t));
      break;
    }
    case SqlType::Int32: {
      auto val = field.get<int32_t>();
      std::memcpy(write_location, &val, sizeof(int32_t));
      break;
    }
    case SqlType::Int64: {
      auto val = field.get<int64_t>();
      std::memcpy(write_location, &val, sizeof(int64_t));
      break;
    }
    case SqlType::Float32: {
      auto val = field.get<float>();
      std::memcpy(write_location, &val, sizeof(float));
      break;
    }
    case SqlType::Float64: {
      auto val = field.get<double>();
      std::memcpy(write_location, &val, sizeof(double));
      break;
    }
    case SqlType::Date: {
      auto val = field.get<std::string>();
      auto date_val = Date::FromString(val);
      std::memcpy(write_location, &date_val, sizeof(Date));
      break;
    }
    case SqlType::Varchar: {
      auto val = field.get<std::string_view>();
      auto str_size = val.size();
      auto varlen = Varlen::MakeNormal(str_size, val.data());
      std::memcpy(write_location, &varlen, sizeof(Varlen));
      break;
    }
    case SqlType::Pointer: {
      std::cerr << "Unsupported Types!" << std::endl;
      break;
    }
  }
}

void WriteEncodedCol(char* col_data, uint64_t* bitmap, csv::CSVField &field, uint64_t row_idx, DictEncoding* dict_encoding) {
  // Type of col must by int32 with encoding=true;
  auto field_size = field.get<std::string_view>().size();
  // Check for null
  if (field_size == 0) {
    // Null. No need to do anything.
    return;
  }
  // Non null
  Bitmap::SetBit(bitmap, row_idx);
  auto write_location = col_data + TypeUtil::TypeSize(SqlType::Int32) * row_idx;
  auto val = field.get<std::string_view>();
  int temp_code = dict_encoding->Insert(std::string(val.data(), val.size()));
  std::memcpy(write_location, &temp_code, sizeof(int));
}

void LoadTable(Table *table, const std::string &filename) {
  auto settings = Settings::Instance();
  auto schema = table->GetSchema();
  auto num_cols = schema.NumCols();
  // CSV Setup.
  csv::CSVFormat format;
  format.delimiter(',').no_header();
  std::vector<std::string> col_names;
  std::vector<SqlType> col_types;
  std::vector<bool> encode;
  bool contains_varchar = false;
  col_names.reserve(num_cols);
  for (uint64_t i = 0; i < num_cols; i++) {
    contains_varchar |= schema.GetColumn(i).Type() == SqlType::Varchar;
    col_names.emplace_back(schema.GetColumn(i).Name());
    col_types.emplace_back(schema.GetColumn(i).Type());
    encode.emplace_back(schema.GetColumn(i).DictEncode());
  }

  format.column_names(col_names);
  csv::CSVReader reader(filename, format);

  // Read data in batches.
  uint64_t curr_batch = 0;
  std::unique_ptr<TableBlock> table_block = nullptr;

  for (csv::CSVRow &row : reader) {
    // Reset current batch.
    if (curr_batch % settings->BlockSize() == 0) {
      // Insert current batch in table.
      if (curr_batch > 0) table->InsertTableBlock(std::move(table_block));
      // Reset batch.
      auto col_types_copy = col_types;
      table_block = std::make_unique<TableBlock>(std::move(col_types_copy));
      if (contains_varchar) table_block->SetHot();
      curr_batch = 0;
    }
    // Read a single row.
    uint64_t curr_col = 0;
    auto presence = table_block->RawBlock()->MutablePresenceBitmap();
    Bitmap::SetBit(presence, curr_batch);
    for (csv::CSVField &field: row) {
      // Set row index
      const auto &col = schema.GetColumn(curr_col);
      auto col_type = col.Type();
      char *col_data = table_block->RawBlock()->MutableColData(curr_col);
      uint64_t* bitmap = table_block->RawBlock()->MutableBitmapData(curr_col);
      bool should_encode = encode[curr_col];
      if (!should_encode) {
        WriteCol(col_data, bitmap, field, curr_batch, col_type);
      } else {
        auto dict_encoding = table->GetDictEncoding(curr_col);
        WriteEncodedCol(col_data, bitmap, field, curr_batch, dict_encoding);
      }
      curr_col++;
    }
    curr_batch++;
  }
  // Deal with last batch.
  if (curr_batch != 0) {
    table->InsertTableBlock(std::move(table_block));
  }
  // Finalize dict encoding.
  uint64_t col_idx = 0;
  for (const auto& col: table->GetSchema().Cols()) {
    if (col.DictEncode()) {
      table->FinalizeEncodings();
    }
    col_idx++;
  }
}


void LoadData(Catalog* catalog, WorkloadInfo* workload) {
  for (const auto& [table_name, table_info]: workload->table_infos) {
    auto table = catalog->CreateTable(table_name, &table_info->schema);
    LoadTable(table, workload->input_folder + "/" + table_info->datafile);
  }
}

double CountNumTuples(Table* table) {
  TableIterator table_iterator(table, table->BM(), {0});
  VectorProjection vp(&table_iterator);
  double count = 0;
  while (table_iterator.Advance()) {
    count += double(vp.GetFilter()->NumOnes());
  }
  return count;
}

void SampleTable(Table* table, Table* sample_table, double sample_rate) {
  std::vector<SqlType> col_types;
  bool contains_varchar = false;
  std::vector<uint64_t> col_idxs;
  uint64_t col_idx = 0;
  for (const auto& col: sample_table->GetSchema().Cols()) {
    contains_varchar |= col.Type() == SqlType::Varchar;
    col_types.emplace_back(col.Type());
    col_idxs.emplace_back(col_idx);
    col_idx++;
  }

  // RNG for sampling.
  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution(0.0,1.0);

  // Do iteration.
  auto vec_size = Settings::Instance()->VecSize();
  auto block_size = Settings::Instance()->BlockSize();
  uint64_t curr_batch = 0;
  std::unique_ptr<TableBlock> table_block = nullptr;

  TableIterator table_iterator(table, table->BM(), col_idxs);
  VectorProjection vp(&table_iterator);
  while (table_iterator.Advance()) {
    std::vector<double> rand_values;
    for (uint64_t i = 0; i < vec_size; i++) rand_values.emplace_back(distribution(generator));
    vp.GetFilter()->Map([&](sel_t row_idx) {
      // Randomness check.
      if (rand_values[row_idx] > sample_rate) return;
      if (curr_batch % block_size == 0) {
        // Insert current batch in table.
        if (curr_batch > 0) sample_table->InsertTableBlock(std::move(table_block));
        // Reset batch.
        auto col_types_copy = col_types;
        table_block = std::make_unique<TableBlock>(std::move(col_types_copy));
        if (contains_varchar) table_block->SetHot();
        curr_batch = 0;
      }
      uint64_t curr_col = 0;
      auto presence = table_block->RawBlock()->MutablePresenceBitmap();
      Bitmap::SetBit(presence, curr_batch);
      for (const auto& col: sample_table->GetSchema().Cols()) {
        auto col_type = col.Type();
        auto col_size = TypeUtil::TypeSize(col_type);
        // Write data. Read from row_idx but write to curr_batch.
        char *write_location = table_block->RawBlock()->MutableColData(curr_col) + col_size * curr_batch;
        const char *read_location = vp.VectorAt(curr_col)->Data() + col_size * row_idx;
        if (col_type == SqlType::Varchar) {
          auto write_location_varlen = reinterpret_cast<Varlen*>(write_location);
          auto read_location_varlen = reinterpret_cast<const Varlen*>(read_location);
          ASSERT(!read_location_varlen->Info().IsCompact(), "VP varlen must be normal!");
          *write_location_varlen = Varlen::MakeNormal(read_location_varlen->Info().NormalSize(), read_location_varlen->Data());
        } else {
          std::memcpy(write_location, read_location, col_size);
        }
        // Set null bitmap using the same indexing scheme.
        uint64_t* write_bitmap = table_block->RawBlock()->MutableBitmapData(curr_col);
        auto read_bitmap = vp.VectorAt(curr_col)->NullBitmap();
        if (Bitmap::IsSet(read_bitmap->Words(), row_idx)) {
          Bitmap::SetBit(write_bitmap, curr_batch);
        }
        curr_col++;
      }
      curr_batch++;
    });
  }
  // Deal with last batch.
  if (curr_batch != 0) {
    sample_table->InsertTableBlock(std::move(table_block));
  }
}

void SampleTables(Catalog* catalog, WorkloadInfo* workload) {
  for (auto& [table_name, table_info]: workload->table_infos) {
    auto table = catalog->GetTable(table_name);
    auto count = CountNumTuples(table);
    std::cout << table_name << " Table Count: " << count << std::endl;
    auto sample_rate{0.0};
    double min_desired_size = 1000;
    if (count < min_desired_size) {
      sample_rate = 1.0;
    } else {
      sample_rate = std::max(0.01, min_desired_size / count);
    }
    table_info->base_size = count;
    table_info->sample_rate = sample_rate;
    auto sample_name = Catalog::SampleName(table_name);
    std::vector<Column> sample_cols; // Should not include 'row_id'
    for (const auto& col: table->GetSchema().Cols()) {
      if (col.Name() != "row_id") {
        sample_cols.emplace_back(col);
      }
    }
    Schema sample_schemas(std::move(sample_cols));
    auto sample_table = catalog->CreateTable(sample_name, &sample_schemas);
    SampleTable(table, sample_table, sample_rate);
  }
}


std::unique_ptr<WorkloadInfo> WorkloadReader::ReadWorkloadInfo(const std::string &toml_file) {
  toml::table tml;
  try {
    tml = toml::parse_file(toml_file);
    std::cout << tml << std::endl;
  } catch (const toml::parse_error& err) {
    std::cerr << "Parsing failed:\n" << err << "\n";
    throw err;
  }

  auto workload_info = std::make_unique<WorkloadInfo>();
  workload_info->workload_file = toml_file;
  workload_info->workload_name = tml["workload_title"].value<std::string>().value();
  workload_info->input_folder = tml["input_folder"].value<std::string>().value();
  workload_info->data_folder = tml["data_folder"].value<std::string>().value();
  workload_info->central_table_name = tml["central_table"].value<std::string>().value();
  workload_info->log_vec_size = tml["log_vec_size"].value<int>().value();
  workload_info->log_block_size = tml["log_block_size"].value<int>().value();
  workload_info->log_mem_space = tml["log_mem_space"].value<int>().value();
  workload_info->log_disk_space = tml["log_disk_space"].value<int>().value();
  workload_info->reload = tml["reload"].value_or(false);

  auto tables = tml["tables"].as_table();

  bool found_central = false;
  std::unordered_map<std::string, std::unique_ptr<TableInfo>> table_infos;
  for (const auto& [table_name, table_info]: *tables) {
    bool is_central = table_name == workload_info->central_table_name;
    found_central = found_central || is_central;
    std::cout << table_name << std::endl;
    const auto& table_info_vals = *table_info.as_table();
    // Read datafile
    auto datafile = table_info_vals["datafile"].value<std::string>().value();
    std::cout << "Datafile: " << datafile << std::endl;
    // Read primary keys
    const auto& primary_keys_arr = *table_info_vals["primary"].as_array();
    std::unordered_set<std::string> pks;
    std::cout << "Pks: ";
    for (const auto& pk: primary_keys_arr) {
      auto pk_val = pk.value<std::string>().value();
      std::cout << pk_val << ", ";
      pks.emplace(pk_val);
    }
    std::cout << std::endl;
    // Read to_many
    auto to_many = table_info_vals["to_many"].value_or<bool>(false);
    std::cout << "to_many: " << to_many << std::endl;
    // Read columns.
    std::vector<Column> cols;
    auto& cols_tml = *table_info_vals["col"].as_array();
    for (const auto& col_info_tml: cols_tml) {
      auto &col_info_table = *col_info_tml.as_table();
      auto col_name = col_info_table["name"].value<std::string>().value();
      bool is_pk = pks.contains(std::string(col_name));
      const auto& col_info = *col_info_table.as_table();
      auto col_type =  TypeUtil::NameToType(col_info["type"].value<std::string>().value());
      auto fkname = col_info["foreign"].value_or<std::string>("");
      bool is_fk = !fkname.empty();
      auto dict_encode = col_info["dict_encode"].value_or<bool>(false);
      ASSERT(!dict_encode || col_type==SqlType::Int32, "Dict encoding type must be int32.");
      cols.emplace_back(Column(std::string(col_name), col_type, is_pk, is_fk, fkname, dict_encode));
    }
    Schema schema(std::move(cols));
    table_infos[std::string(table_name)] = std::make_unique<TableInfo>(std::string(table_name), std::move(schema), std::string(datafile), std::move(pks), to_many);
  }
  ASSERT(found_central, "Central table not found");

  // Check foreign keys.
  auto central_table_info = table_infos[workload_info->central_table_name].get();
  std::unordered_set<std::string> central_col_names;
  for (const auto& central_col: central_table_info->schema.Cols()) {
    central_col_names.emplace(central_col.Name());
  }
  for (const auto& [table_name, table_info]: table_infos) {
    if (workload_info->central_table_name != table_name) {
      for (const auto& col: table_info->schema.Cols()) {
        if (col.IsFK()) {
          ASSERT(central_col_names.contains(col.FKName()), "FK is not in central table!");
        }
      }
    }
  }

  workload_info->table_infos = std::move(table_infos);
  return workload_info;
}

void WorkloadReader::ReadQueriesAndTables(Catalog* catalog, WorkloadInfo *workload) {
  // Open toml file.
  toml::table tml;
  try {
    tml = toml::parse_file(workload->workload_file);
    std::cout << tml << std::endl;
  } catch (const toml::parse_error& err) {
    std::cerr << "Parsing failed:\n" << err << "\n";
    throw err;
  }

  ReadQueries(tml, workload);
  EnumerateAllJoinOrders(workload);
  if (!workload->reload) {
    return;
  }
  LoadData(catalog, workload);
  SampleTables(catalog, workload);
}


}
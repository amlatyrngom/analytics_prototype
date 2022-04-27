#include "optimizer/workload_reader.h"
#include "storage/vector.h"
#include "common/catalog.h"
#include "storage/vector_projection.h"
#include "optimizer/workload_metadata.h"
#include "storage/dict_encoding.h"
#include "execution/execution_factory.h"
#include "csv/csv.h"
#include <memory>
#include <random>
#include <unordered_map>
#include <toml++/toml.h>
#include <fmt/core.h>
#include <fmt/ranges.h>



namespace smartid {

void ReadFilter(const toml::table& tml_filter, WorkloadInfo* workload, QueryInfo* res, LogicalScanNode* scan) {
  LogicalFilter filter;
  filter.col_name = tml_filter["col"].value<std::string>().value();
  filter.col_idx = scan->table_info->schema.ColIdx(filter.col_name);
  scan->table_info->used_cols.emplace(filter.col_idx);
  auto col = scan->table_info->schema.GetColumn(filter.col_idx);
  filter.col_type = col.Type();
  auto read_idx = scan->col_idx_read_idxs.at(filter.col_idx);
  filter.param_name = scan->table_info->name + "___" + col.Name();
  filter.expr_name = tml_filter["expr"].value<std::string>().value();

  if (filter.expr_name == "between") {
    filter.expr_type = ExprType::Between;
    filter.left_closed = tml_filter["left_closed"].value_or<bool>(true);
    filter.right_closed = tml_filter["right_closed"].value_or<bool>(true);
    filter.range = tml_filter["range"].value_or<int>(-1);
    filter.should_sample = (filter.range > 0);

    if (filter.should_sample) {
      filter.vals.resize(2); // Will be initialized later.
    } else {
      auto& vals_tml = *tml_filter["vals"].as_array();
      for (const auto& val_tml: vals_tml) {
        filter.vals.emplace_back(ValueUtil::ReadVal(val_tml.value<std::string>().value(), filter.col_type));
      }
    }

    auto column_node = res->execution_factory->MakeCol(read_idx);
    auto param_node1 = res->execution_factory->MakeParam(filter.param_name + "0");
    auto param_node2 = res->execution_factory->MakeParam(filter.param_name + "1");
    filter.expr_node = res->execution_factory->MakeBetween(param_node1, column_node, param_node2, filter.left_closed, filter.right_closed);
  } else if (filter.expr_name == "in") {
    filter.expr_type = ExprType::InList;
    auto num_ins = tml_filter["num_ins"].value_or<int>(-1);
    filter.should_sample = (num_ins > 0);
    if (filter.should_sample) {
      filter.vals.resize(num_ins); // Will be initialized later.
    } else {
      auto& vals_tml = *tml_filter["vals"].as_array();
      for (const auto& val_tml: vals_tml) {
        filter.vals.emplace_back(ValueUtil::ReadVal(val_tml.value<std::string>().value(), filter.col_type));
      }
    }
    auto column_node = res->execution_factory->MakeCol(read_idx);
    std::vector<ExprNode*> in_vals;
    for (int i = 0; i < filter.vals.size(); i++) {
      auto param_node = res->execution_factory->MakeParam(filter.param_name + std::to_string(i));
      in_vals.emplace_back(param_node);
    }
    filter.expr_node = res->execution_factory->MakeIn(column_node, std::move(in_vals));
  } else {
    filter.expr_type = ExprType::BinaryComp;
    filter.op_type = OpType::EQ;
    if (filter.expr_name == "lt") filter.op_type = OpType::LT;
    if (filter.expr_name == "le") filter.op_type = OpType::LE;
    if (filter.expr_name == "gt") filter.op_type = OpType::GT;
    if (filter.expr_name == "ge") filter.op_type = OpType::GE;
    if (filter.expr_name == "eq") filter.op_type = OpType::EQ;
    if (filter.expr_name == "ne") filter.op_type = OpType::NE;
    auto val = tml_filter["val"].value<std::string>().value();
    filter.should_sample = (val == "?");
    if (filter.should_sample) {
      filter.vals.resize(1); // Will be initialized later.
    } else {
      filter.vals.emplace_back(ValueUtil::ReadVal(val, filter.col_type));
    }
    auto column_node = res->execution_factory->MakeCol(read_idx);
    auto param_node = res->execution_factory->MakeParam(filter.param_name + "0");
    filter.expr_node = res->execution_factory->MakeBinaryComp(column_node, param_node, filter.op_type);
  }
  scan->filters.emplace_back(std::move(filter));
}

void ReadScan(toml::table& tml_scan, WorkloadInfo* workload, QueryInfo* res, bool for_join=false) {
  // Read table name.
  auto table_name = tml_scan["table"].value<std::string>().value();
  auto table_info = workload->table_infos[table_name].get();
  res->allocated_scans.emplace_back(std::make_unique<LogicalScanNode>());
  auto scan = res->allocated_scans.back().get();
  scan->table_info = table_info;

  // Cols to read
  uint64_t read_idx = 0;
  auto& input_cols = *tml_scan["inputs"].as_array();
  for (const auto& col_name: input_cols) {
    auto col_idx = table_info->schema.ColIdx(col_name.value<std::string>().value());
    auto col = table_info->schema.GetColumn(col_idx);
    scan->cols_to_read.emplace_back(col_idx);
    scan->col_idx_read_idxs[col_idx] = read_idx;
    scan->table_info->used_cols.emplace(col_idx);
    read_idx++;
  }

  // Read projections.
  auto& projections_tml = *tml_scan["projections"].as_array();
  uint64_t proj_idx = 0;
  for (const auto& col_name_tml: projections_tml) {
    auto col_name = col_name_tml.value<std::string>().value();
    auto col_idx = table_info->schema.ColIdx(col_name);
    auto col_read_idx = scan->col_idx_read_idxs.at(col_idx);
    scan->projections.emplace_back(res->execution_factory->MakeCol(col_read_idx));
    scan->col_idx_proj_idxs[col_idx] = proj_idx;
    scan->proj_idx_full_name[proj_idx] = table_info->name + "." + col_name;
    scan->table_info->used_cols.emplace(col_idx);
    proj_idx++;
  }


  // Read filters.
  if (tml_scan.contains("filters")) {
    auto& filters_tml = *tml_scan["filters"].as_array();
    for (const auto& filter_tml: filters_tml) {
      auto& filter_tml_table = *filter_tml.as_table();
      ReadFilter(filter_tml_table, workload, res, scan);
    }
  }
  std::cout << "ReadScan: ";
  scan->ToString(std::cout);
  std::cout << std::endl;
  res->scans.emplace_back(scan);
}

void ReadQueries(toml::table& tml, Catalog* catalog, WorkloadInfo* workload, ExecutionFactory* execution_factory) {
  auto& queries_tml = *tml["queries"].as_table();
//  std::cout << queries_tml << std::endl;
  for (const auto& [q_name, q]: queries_tml) {
    auto res = std::make_unique<QueryInfo>();
    res->execution_factory = execution_factory;
    res->name = std::string(q_name);
    auto q_tml = *q.as_table();
    auto type = q_tml["type"].value<std::string>().value();
    res->count = q_tml["count"].value_or<bool>(false);
    res->is_mat_view = q_tml["is_mat_view"].value_or<bool>(false);
    res->for_training = q_tml["for_training"].value_or<bool>(true);
    res->for_running = q_tml["for_running"].value_or<bool>(true);
    if (type == "scan") {
      ReadScan(q_tml, workload, res.get());
    }
    if (type == "join") {
      for (const auto& [_, scan_tml]: q_tml) {
        if (scan_tml.is_table()) {
          ReadScan(*scan_tml.as_table(), workload, res.get());
        }
      }
      bool central_table_found = false;
      for (const auto& scan: res->scans) {
        if (scan->table_info->name == workload->central_table_name) {
          central_table_found = true;
          break;
        }
      }
      ASSERT(central_table_found, "Join must contain central table!");
    }
    res->ToString(std::cout);
    workload->query_infos[res->name] = std::move(res);
  }
}



void EnumerateAllJoinOrders(WorkloadInfo* workload) {
  for (auto& [query_name, query_info]: workload->query_infos) {
    if (query_info->scans.size() == 1) continue; // Query is not a join.
    if (!query_info->join_orders.empty()) continue; // Join order already generated.
    auto& allocated_scans = query_info->allocated_scans;
    auto& allocated_joins = query_info->allocated_joins;
    std::vector<uint64_t> table_idxs;
    uint64_t table_idx = 0;
    int64_t central_table_idx{-1};
    for (const auto& scan: query_info->scans) {
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
      if (table_idxs[0] != central_table_idx) continue;
      // Check if all pk-fk joins precede fk-fk joins.
      bool found_fkfk{false};
      bool valid{true};
      for (const auto& idx: table_idxs) {
        if (found_fkfk && !query_info->scans[idx]->table_info->to_many) {
          valid = false;
          break;
        }
        if (query_info->scans[idx]->table_info->to_many) {
          found_fkfk = true;
        }
      }
      if (!valid) continue;
      // Left most scan
      auto left_most_scan = query_info->scans[table_idxs[0]];
      // Next scan
      auto curr_scan = query_info->scans[table_idxs[1]];
      // First join
      allocated_joins.emplace_back(std::make_unique<LogicalJoinNode>());
      auto curr_join = allocated_joins.back().get();
      curr_join->left_scan = left_most_scan;
      curr_join->right_scan = curr_scan;
      curr_join->scanned_tables.emplace_back(left_most_scan->table_info);
      curr_join->scanned_tables.emplace_back(curr_scan->table_info);
      // Add subsequent joins.
      for (uint64_t i = 2; i < table_idxs.size(); i++) {
        auto prev_join = curr_join;
        // First make right scan
        curr_scan = query_info->scans[table_idxs[i]];
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
//      std::cout <<  "Got Join order: ";
//      for (const auto& table_info: curr_join->scanned_tables) {
//        std::cout << table_info->name << ", ";
//      }
//      std::cout << std::endl;
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
      break;
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

uint64_t SampleTable(Table* table, Table* sample_table, double sample_rate) {
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
  uint64_t final_sample_size = 0;
  while (table_iterator.Advance()) {
    std::vector<double> rand_values;
    for (uint64_t i = 0; i < vec_size; i++) rand_values.emplace_back(distribution(generator));
    vp.GetFilter()->Map([&](sel_t row_idx) {
      // Randomness check.
      if (rand_values[row_idx] > sample_rate) return;
      final_sample_size++;
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

//  std::cout << table->Name() << " final sample size: " << final_sample_size << std::endl;
  return final_sample_size;
}

void SampleTables(Catalog* catalog, WorkloadInfo* workload) {
  for (auto& [table_name, table_info]: workload->table_infos) {
    auto table = catalog->GetTable(table_name);
    auto count = CountNumTuples(table);
//    std::cout << table_name << " Table Count: " << count << std::endl;
    auto sample_rate{0.0};
    double min_desired_size = 1000;
    if (count < min_desired_size) {
      sample_rate = 1.0;
    } else {
      sample_rate = std::max(0.01, min_desired_size / count);
    }
    table_info->base_size = count;
    std::cout << table_name << " Sample Rate: " << sample_rate << std::endl;
    auto sample_name = Catalog::SampleName(table_name);
    std::vector<Column> sample_cols; // Should not include 'row_id'
    for (const auto& col: table->GetSchema().Cols()) {
      if (col.Name() != "row_id") {
        sample_cols.emplace_back(col);
      }
    }
    Schema sample_schemas(std::move(sample_cols));
    auto sample_table = catalog->CreateTable(sample_name, &sample_schemas);
    table_info->sample_size = double(SampleTable(table, sample_table, sample_rate));
  }
}


std::unique_ptr<WorkloadInfo> WorkloadReader::ReadWorkloadInfo(const std::string &toml_file) {
  toml::table tml;
  try {
    tml = toml::parse_file(toml_file);
//    std::cout << tml << std::endl;
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
  workload_info->just_load = tml["just_load"].value_or(false);
  if (workload_info->reload) {
    workload_info->gen_costs = false;
  } else {
    workload_info->gen_costs = tml["gen_costs"].value_or(false);
  }
  if (workload_info->gen_costs) {
    workload_info->rebuild = false;
  } else {
    workload_info->rebuild = tml["rebuild"].value_or(false);
  }
  workload_info->budget = tml["budget"].value_or<int>(256);
  workload_info->embedding_size = tml["embedding_size"].value_or<int>(16);

  auto tables = tml["tables"].as_table();

  bool found_central = false;
  std::unordered_map<std::string, std::unique_ptr<TableInfo>> table_infos;
  for (const auto& [table_name, table_info]: *tables) {
    bool is_central = table_name == workload_info->central_table_name;
    found_central = found_central || is_central;
//    std::cout << table_name << std::endl;
    const auto& table_info_vals = *table_info.as_table();
    // Read datafile
    auto datafile = table_info_vals["datafile"].value<std::string>().value();
//    std::cout << "Datafile: " << datafile << std::endl;
    // Read primary keys
    const auto& primary_keys_arr = *table_info_vals["primary"].as_array();
    std::unordered_set<std::string> pks;
//    std::cout << "Pks: ";
    for (const auto& pk: primary_keys_arr) {
      auto pk_val = pk.value<std::string>().value();
//      std::cout << pk_val << ", ";
      pks.emplace(pk_val);
    }
//    std::cout << std::endl;
    // Read to_many
    auto to_many = table_info_vals["to_many"].value_or<bool>(false);
//    std::cout << "to_many: " << to_many << std::endl;
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
    table_infos[std::string(table_name)] = std::make_unique<TableInfo>(std::string(table_name), std::move(schema), std::string(datafile), std::move(pks), to_many, is_central);

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

void WorkloadReader::ReadWorkloadTables(Catalog *catalog, WorkloadInfo *workload) {
  if (!workload->reload) {
    return;
  }
  // Open toml file.
  toml::table tml;
  try {
    tml = toml::parse_file(workload->workload_file);
//    std::cout << tml << std::endl;
  } catch (const toml::parse_error& err) {
    std::cerr << "Parsing failed:\n" << err << "\n";
    throw err;
  }
  LoadData(catalog, workload);
  if (workload->just_load) return;
  SampleTables(catalog, workload);
}

void QueryReader::ReadWorkloadQueries(Catalog* catalog, WorkloadInfo *workload, ExecutionFactory* execution_factory, const std::vector<std::string>& query_files) {
  // Open toml file.
  for (const auto& query_file: query_files) {
    toml::table tml;
    try {
      tml = toml::parse_file(query_file);
//    std::cout << tml << std::endl;
    } catch (const toml::parse_error& err) {
      std::cerr << "Parsing failed:\n" << err << "\n";
      throw err;
    }
    ReadQueries(tml, catalog, workload, execution_factory);
  }
  EnumerateAllJoinOrders(workload);
}

void LogicalFilter::ToString(std::ostream &os) const {
  os << "Filter("
     << "col_name=" << col_name << ", "
     << "expr_name=" << expr_name << ", "
     << "estimated_sel=" << estimated_sel << ", ";
  os << "[";
  uint64_t i = 0;
  for(const auto& v: vals) {
    if (should_sample) {
      std::cout << '?';
    } else {
      ValueUtil::WriteVal(v, std::cout, col_type);
    }
    if (i < vals.size() - 1) {
      std::cout << ",";
    }
    i++;
  }
  os << "])";
}

void LogicalScanNode::ToString(std::ostream &os) const {
  os << "Scan(name=" << table_info->name << ", ";
  os << "estimated_output=" << estimated_output_size << ", ";
  os << fmt::format("cols_to_read={},", cols_to_read);
  os << fmt::format("col_idx_read_idxs={},", col_idx_read_idxs);
  os << fmt::format("col_idx_proj_idxs={},", col_idx_proj_idxs);
  os << fmt::format("proj_fullnames={},", proj_idx_full_name);
  os << "filters=(";
  uint64_t i=0;
  for (const auto& f: filters) {
    std::cout << i << ": ";
    f.ToString(os);
    if (i < filters.size() - 1) {
      std::cout << ", ";
    }
    i++;
  }
  os << "))\n";
}

void LogicalJoinNode::ToString(std::ostream &os) const {
  std::vector<std::string> table_names;
  for (const auto& table_info: scanned_tables) table_names.emplace_back(table_info->name);
  os << fmt::format("Join(tables={}, proj_names={})\n",
                    table_names, proj_idx_full_name);
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
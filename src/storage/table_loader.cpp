#include "storage/table_loader.h"

namespace smartid {

namespace {
/**
 * Create test 1 table (int64, int32).
 * Return storage table + csv filename.
 */
std::pair<Table *, std::string> CreateTest1(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("col1", SqlType::Int64),
      Column::ScalarColumn("col2", SqlType::Int32),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create test 2 table (int64, int32, varchar).
 */
std::pair<Table *, std::string> CreateTest2(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("col1", SqlType::Int64),
      Column::ScalarColumn("col2", SqlType::Int32),
      Column::VarcharColumn("col3", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create a build table for testing joins (int64 pk, int32 pk, varchar pk).
 * All values in a row are the same, just with different types. This is to test different scenarios of joins.
 */
std::pair<Table *, std::string> CreateBuildTable(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("col1", SqlType::Int64),
      Column::ScalarColumn("col2", SqlType::Int32),
      Column::VarcharColumn("col3", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create a build table for testing embeddings (int32 pk).
 */
std::pair<Table *, std::string> CreateEmbeddingBuildTable(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("pk", SqlType::Int32),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}


/**
 * Create a probe table for testing joins (int64 pk, int64 fk, int32 fk, varchar fk)
 */
std::pair<Table *, std::string> CreateProbeTable(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("col1", SqlType::Int64),
      Column::ScalarColumn("col2", SqlType::Int64),
      Column::ScalarColumn("col3", SqlType::Int32),
      Column::VarcharColumn("col4", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create a probe table for testing embeddings (int32 pk, int32 fk, int32 filter_col)
 */
std::pair<Table *, std::string> CreateEmbeddingProbeTable(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("pk", SqlType::Int32),
      Column::ScalarColumnWithConstraint("fk", SqlType::Int32, false, FKConstraint{"embedding_build", "pk"}),
      Column::ScalarColumn("filter_col", SqlType::Int32),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create a interm table for testing embeddings (int32 origin_key, int32 target_key)
 */
std::pair<Table *, std::string> CreateEmbeddingIntermTable(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("col1", SqlType::Int32),
      Column::ScalarColumn("col2", SqlType::Int32),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}


/**
 * Create test 3 table (int64, int32, varchar, float64, float32, date).
 */
std::pair<Table *, std::string> CreateTest3(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumn("col1", SqlType::Int64),
      Column::ScalarColumn("col2", SqlType::Int32),
      Column::VarcharColumn("col3", SqlType::Varchar, true),
      Column::ScalarColumn("col4", SqlType::Float64),
      Column::ScalarColumn("col5", SqlType::Float32),
      Column::ScalarColumn("col6", SqlType::Date),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create region table.
 */
std::pair<Table *, std::string> CreateRegion(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("regionkey", SqlType::Int32, true, {}),
      Column::VarcharColumn("name", SqlType::Varchar, true),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create nation table.
 */
std::pair<Table *, std::string> CreateNation(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("nationkey", SqlType::Int32, true, {}),
      Column::VarcharColumn("name", SqlType::Varchar, true),
      Column::ScalarColumnWithConstraint("regionkey", SqlType::Int32, false, FKConstraint{"region", "regionkey"}),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create part table.
 */
std::pair<Table *, std::string> CreatePart(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("partkey", SqlType::Int32, true, {}),
      Column::VarcharColumn("name", SqlType::Varchar, true),
      Column::VarcharColumn("mfgr", SqlType::Varchar, true),
      Column::VarcharColumn("brand", SqlType::Varchar, true),
      Column::VarcharColumn("type", SqlType::Varchar, true),
      Column::ScalarColumn("size", SqlType::Int32),
      Column::VarcharColumn("container", SqlType::Varchar, true),
      Column::ScalarColumn("retailprice", SqlType::Float32),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create supplier table.
 */
std::pair<Table *, std::string> CreateSupplier(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("suppkey", SqlType::Int32, true, {}),
      Column::VarcharColumn("name", SqlType::Varchar, true),
      Column::VarcharColumn("address", SqlType::Varchar, true),
      Column::ScalarColumnWithConstraint("nationkey", SqlType::Int32, false, FKConstraint{"nation", "nationkey"}),
      Column::VarcharColumn("phone", SqlType::Varchar, true),
      Column::ScalarColumn("acctbal", SqlType::Float32),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}


/**
 * Create partsupp table.
 */
std::pair<Table *, std::string> CreatePartsupp(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("partkey", SqlType::Int32, true, FKConstraint{"part", "partkey"}),
      Column::ScalarColumnWithConstraint("suppkey", SqlType::Int32, true, FKConstraint{"supplier", "suppkey"}),
      Column::ScalarColumn("availqty", SqlType::Int32),
      Column::ScalarColumn("supplycost", SqlType::Float32),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}


/**
 * Create customer table.
 */
std::pair<Table *, std::string> CreateCustomer(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("custkey", SqlType::Int32, true, {}),
      Column::VarcharColumn("name", SqlType::Varchar, true),
      Column::VarcharColumn("address", SqlType::Varchar, true),
      Column::ScalarColumnWithConstraint("nationkey", SqlType::Int32, false, FKConstraint{"nation", "nationkey"}),
      Column::VarcharColumn("phone", SqlType::Varchar, true),
      Column::ScalarColumn("acctbal", SqlType::Float32),
      Column::VarcharColumn("mktsegment", SqlType::Varchar, true),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

/**
 * Create orders table.
 */
std::pair<Table *, std::string> CreateOrders(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("orderkey", SqlType::Int32, true, {}),
      Column::ScalarColumnWithConstraint("custkey", SqlType::Int32, false, FKConstraint{"customer", "custkey"}),
      Column::ScalarColumn("orderstatus", SqlType::Char),
      Column::ScalarColumn("totalprice", SqlType::Float32),
      Column::ScalarColumn("orderdate", SqlType::Date),
      Column::VarcharColumn("orderpriority", SqlType::Varchar, true),
      Column::VarcharColumn("clerk", SqlType::Varchar, true),
      Column::ScalarColumn("shippriority", SqlType::Int32),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

std::pair<Table *, std::string> CreateLineitem(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".tbl";
  std::vector<std::pair<std::string, std::string>> partkey_fks = {
      {"partsupp", "partkey"},
      {"part", "partkey"}
  };
  std::vector<std::pair<std::string, std::string>> suppkey_fks = {
      {"partsupp", "suppkey"},
      {"supplier", "suppkey"}
  };

  std::vector<Column> cols = {
      Column::ScalarColumnWithConstraint("orderkey", SqlType::Int32, true, FKConstraint{"orders", "orderkey"}),
      Column::ScalarColumnWithConstraint("partkey", SqlType::Int32, false, FKConstraint{partkey_fks}),
      Column::ScalarColumnWithConstraint("suppkey", SqlType::Int32, false, FKConstraint{suppkey_fks}),
      Column::ScalarColumn("linenumber", SqlType::Int32),
      Column::ScalarColumn("quantity", SqlType::Float32),
      Column::ScalarColumn("extendedprice", SqlType::Float32),
      Column::ScalarColumn("discount", SqlType::Float32),
      Column::ScalarColumn("tax", SqlType::Float32),
      Column::ScalarColumn("returnflag", SqlType::Char),
      Column::ScalarColumn("linestatus", SqlType::Char),
      Column::ScalarColumn("shipdate", SqlType::Date),
      Column::ScalarColumn("commitdate", SqlType::Date),
      Column::ScalarColumn("receiptdate", SqlType::Date),
      Column::VarcharColumn("shipinstruct", SqlType::Varchar, true),
      Column::VarcharColumn("shipmode", SqlType::Varchar, true),
      Column::VarcharColumn("comment", SqlType::Varchar, true),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}



std::pair<Table *, std::string> CreateTitle(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".csv";
  std::vector<Column> cols = {
      Column::ScalarColumn("id", SqlType::Int32),
      Column::ScalarColumn("kind_id", SqlType::Int32),
      Column::ScalarColumn("production_year", SqlType::Int32),
      Column::ScalarColumn("embedding", SqlType::Int16),
      Column::ScalarColumn("ideal_embedding", SqlType::Int16),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}

std::pair<Table *, std::string> CreateCastInfo(const std::string &directory, const std::string &table_name) {
  auto catalog = Catalog::Instance();
  std::string filename = directory + "/" + table_name + ".csv";
  std::vector<Column> cols = {
      Column::ScalarColumn("id", SqlType::Int32),
      Column::ScalarColumn("movie_id", SqlType::Int32),
      Column::ScalarColumn("role_id", SqlType::Int32),
      Column::ScalarColumn("embedding", SqlType::Int16),
      Column::ScalarColumn("ideal_embedding", SqlType::Int16),
  };
  auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
  return {table, filename};
}


}

#define CREATE_TABLE(name, CreateFn) \
        if (catalog->GetTable(name) == nullptr) { \
          std::cout << "Loading " << name << std::endl;                        \
          auto table_info = CreateFn(directory, name); \
          LoadTable(table_info.first, table_info.second); \
        }

void TableLoader::LoadTPCH(const std::string &directory) {
  auto catalog = Catalog::Instance();
  CREATE_TABLE("region", CreateRegion)
  CREATE_TABLE("nation", CreateNation)
  CREATE_TABLE("part", CreatePart)
  CREATE_TABLE("supplier", CreateSupplier)
  CREATE_TABLE("partsupp", CreatePartsupp)
  CREATE_TABLE("customer", CreateCustomer)
  CREATE_TABLE("orders", CreateOrders)
  CREATE_TABLE("lineitem", CreateLineitem)
}

void TableLoader::LoadJOBLight(const std::string &directory) {
  auto catalog = Catalog::Instance();
  CREATE_TABLE("title", CreateTitle);
  CREATE_TABLE("title_mini", CreateTitle);
  CREATE_TABLE("cast_info", CreateCastInfo);
  CREATE_TABLE("cast_info_mini", CreateCastInfo);
}

void TableLoader::LoadTestTables(const std::string &directory) {
  auto catalog = Catalog::Instance();
  CREATE_TABLE("test1", CreateTest1);
//  CREATE_TABLE("test2", CreateTest2);
//  CREATE_TABLE("test3", CreateTest3);
}

void TableLoader::LoadJoinTables(const std::string &directory) {
  auto catalog = Catalog::Instance();
  CREATE_TABLE("small_build_table", CreateBuildTable);
  CREATE_TABLE("embedding_build", CreateEmbeddingBuildTable);
  //CREATE_TABLE("large_build_table", CreateBuildTable);
  CREATE_TABLE("small_probe_table", CreateProbeTable);
  CREATE_TABLE("embedding_probe", CreateEmbeddingProbeTable);
  //CREATE_TABLE("large_probe_table", CreateProbeTable);
  CREATE_TABLE("embedding_interm", CreateEmbeddingIntermTable);
}

void TableLoader::LoadTable(Table *table, const std::string &filename) {
  auto settings = Settings::Instance();
  auto schema = table->GetSchema();
  auto num_cols = schema.NumCols();
  // CSV Setup.
  csv::CSVFormat format;
  format.delimiter(',').no_header();
  std::vector<std::string> col_names;
  std::vector<SqlType> col_types;
  bool contains_varchar = false;
  col_names.reserve(num_cols);
  for (uint64_t i = 0; i < num_cols; i++) {
    contains_varchar |= schema.GetColumn(i).Type() == SqlType::Varchar;
    col_names.emplace_back(schema.GetColumn(i).Name());
    col_types.emplace_back(schema.GetColumn(i).Type());
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
    }
    // Read a single row.
    uint64_t curr_col = 0;
    table_block->PrintBlock();
    auto block_data_base = table_block->RawBlock()->BlockDataStart(); // For debugging.
    auto presence = table_block->RawBlock()->MutablePresenceBitmap();
//    std::cout << "PresenceOffset: " << (reinterpret_cast<char*>(presence)-block_data_base) << std::endl;
    Bitmap::SetBit(presence, curr_batch);
    for (csv::CSVField &field: row) {
      // Set row index
      const auto &col = schema.GetColumn(curr_col);
      auto col_type = col.Type();
      char *col_data = table_block->RawBlock()->MutableColData(curr_col);
//      std::cout << "ColOffset " << curr_col << ": " << (col_data-block_data_base) << std::endl;
      uint64_t* bitmap = table_block->RawBlock()->MutableBitmapData(curr_col);
//      std::cout << "BitmapOffset " << curr_col << ": " << (reinterpret_cast<char*>(bitmap)-block_data_base) << std::endl;
      WriteCol(col_data, bitmap, field, curr_batch, col_type);
      curr_col++;
    }
    curr_batch++;
  }
  // Deal with last batch.
  if (curr_batch != 0) {
    table->InsertTableBlock(std::move(table_block));
  }
}

void TableLoader::WriteCol(char *col_data, uint64_t* bitmap, csv::CSVField &field, uint64_t row_idx, SqlType type) {
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

}
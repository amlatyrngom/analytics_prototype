#include "storage/table_loader.h"

namespace smartid {

struct TpchCreates {

#define CREATE_TABLE(name, CreateFn) \
        if (catalog->GetTable(name) == nullptr) { \
          auto table_info = CreateFn(directory, name); \
          TableLoader::LoadTable(table_info.first, table_info.second); \
        }

  static void LoadTPCH(const std::string &directory) {
    auto catalog = Catalog::Instance();
    CREATE_TABLE("region_s", CreateRegion)
    CREATE_TABLE("region_c", CreateRegion)
    CREATE_TABLE("nation_s", CreateNation)
    CREATE_TABLE("nation_c", CreateNation)
    CREATE_TABLE("part", CreatePart)
    CREATE_TABLE("supplier", CreateSupplier)
    CREATE_TABLE("partsupp", CreatePartsupp)
    CREATE_TABLE("customer", CreateCustomer)
    CREATE_TABLE("orders", CreateOrders)
    CREATE_TABLE("lineitem", CreateLineitem)
  }

#undef CREATE_TABLE

  /**
 * Create region table.
 */
  static std::pair<Table *, std::string> CreateRegion(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("r_regionkey", SqlType::Int32, true, {}),
        Column::ScalarColumn("r_name", SqlType::Int32),
        Column::VarcharColumn("r_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }

/**
 * Create nation table.
 */
  static std::pair<Table *, std::string> CreateNation(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("n_nationkey", SqlType::Int32, true, {}),
        Column::ScalarColumn("n_name", SqlType::Int32),
        Column::ScalarColumnWithConstraint("n_regionkey", SqlType::Int32, false, FKConstraint{"region", "regionkey"}),
        Column::VarcharColumn("n_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }

/**
 * Create part table.
 */
  static std::pair<Table *, std::string> CreatePart(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("p_partkey", SqlType::Int32, true, {}),
        Column::VarcharColumn("p_name", SqlType::Varchar, true),
        Column::VarcharColumn("p_mfgr", SqlType::Varchar, true),
        Column::ScalarColumn("p_brand", SqlType::Int32),
        Column::ScalarColumn("p_type", SqlType::Int32),
        Column::ScalarColumn("p_size", SqlType::Int32),
        Column::ScalarColumn("p_container", SqlType::Int32),
        Column::ScalarColumn("p_retailprice", SqlType::Float32),
        Column::VarcharColumn("p_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }

/**
 * Create supplier table.
 */
  static std::pair<Table *, std::string> CreateSupplier(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("s_suppkey", SqlType::Int32, true, {}),
        Column::VarcharColumn("s_name", SqlType::Varchar, true),
        Column::VarcharColumn("s_address", SqlType::Varchar, true),
        Column::ScalarColumnWithConstraint("s_nationkey", SqlType::Int32, false, FKConstraint{"nation", "n_nationkey"}),
        Column::VarcharColumn("s_phone", SqlType::Varchar, true),
        Column::ScalarColumn("s_acctbal", SqlType::Float32),
        Column::VarcharColumn("s_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }


/**
 * Create partsupp table.
 */
  static std::pair<Table *, std::string> CreatePartsupp(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("ps_partkey", SqlType::Int32, true, FKConstraint{"part", "partkey"}),
        Column::ScalarColumnWithConstraint("ps_suppkey", SqlType::Int32, true, FKConstraint{"supplier", "suppkey"}),
        Column::ScalarColumn("ps_availqty", SqlType::Int32),
        Column::ScalarColumn("ps_supplycost", SqlType::Float32),
        Column::VarcharColumn("ps_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }


/**
 * Create customer table.
 */
  static std::pair<Table *, std::string> CreateCustomer(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("c_custkey", SqlType::Int32, true, {}),
        Column::VarcharColumn("c_name", SqlType::Varchar, true),
        Column::VarcharColumn("c_address", SqlType::Varchar, true),
        Column::ScalarColumnWithConstraint("c_nationkey", SqlType::Int32, false, FKConstraint{"nation", "nationkey"}),
        Column::VarcharColumn("c_phone", SqlType::Varchar, true),
        Column::ScalarColumn("c_acctbal", SqlType::Float32),
        Column::ScalarColumn("c_mktsegment", SqlType::Int32),
        Column::VarcharColumn("c_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }

/**
 * Create orders table.
 */
  static std::pair<Table *, std::string> CreateOrders(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("o_orderkey", SqlType::Int32, true, {}),
        Column::ScalarColumnWithConstraint("o_custkey", SqlType::Int32, false, FKConstraint{"customer", "custkey"}),
        Column::ScalarColumn("o_orderstatus", SqlType::Char),
        Column::ScalarColumn("o_totalprice", SqlType::Float32),
        Column::ScalarColumn("o_orderdate", SqlType::Date),
        Column::VarcharColumn("o_orderpriority", SqlType::Varchar, true),
        Column::VarcharColumn("o_clerk", SqlType::Varchar, true),
        Column::ScalarColumn("o_shippriority", SqlType::Int32),
        Column::VarcharColumn("o_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }

  static std::pair<Table *, std::string> CreateLineitem(const std::string &directory, const std::string &table_name) {
    auto catalog = Catalog::Instance();
    std::string filename = directory + "/" + table_name + ".csv";
    std::vector<std::pair<std::string, std::string>> partkey_fks = {
        {"partsupp", "partkey"},
        {"part", "partkey"}
    };
    std::vector<std::pair<std::string, std::string>> suppkey_fks = {
        {"partsupp", "suppkey"},
        {"supplier", "suppkey"}
    };

    std::vector<Column> cols = {
        Column::ScalarColumnWithConstraint("l_orderkey", SqlType::Int32, true, FKConstraint{"orders", "orderkey"}),
        Column::ScalarColumnWithConstraint("l_partkey", SqlType::Int32, false, FKConstraint{partkey_fks}),
        Column::ScalarColumnWithConstraint("l_suppkey", SqlType::Int32, false, FKConstraint{suppkey_fks}),
        Column::ScalarColumn("l_linenumber", SqlType::Int32),
        Column::ScalarColumn("l_quantity", SqlType::Float32),
        Column::ScalarColumn("l_extendedprice", SqlType::Float32),
        Column::ScalarColumn("l_discount", SqlType::Float32),
        Column::ScalarColumn("l_tax", SqlType::Float32),
        Column::ScalarColumn("l_returnflag", SqlType::Int32),
        Column::ScalarColumn("l_linestatus", SqlType::Char),
        Column::ScalarColumn("l_shipdate", SqlType::Date),
        Column::ScalarColumn("l_commitdate", SqlType::Date),
        Column::ScalarColumn("l_receiptdate", SqlType::Date),
        Column::ScalarColumn("l_shipinstruct", SqlType::Int32),
        Column::ScalarColumn("l_shipmode", SqlType::Int32),
        Column::VarcharColumn("l_comment", SqlType::Varchar, true),
        Column::ScalarColumn("embedding", SqlType::Int64),
    };
    auto table = catalog->CreateTable(table_name, Schema{std::move(cols)});
    return {table, filename};
  }

};

}
/*
 * Make sure working directory is project top level where the tpch_data folder is.
 * Also run the gen_test.py file before hand.
 */
#include "storage/table.h"
#include "storage/table_loader.h"

#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

using namespace smartid;

TEST_CASE("Reading integer test table") {
  // Table loading.
  TableLoader::LoadTestTables("tpch_data");
  auto catalog = Catalog::Instance();
  // Read Table 1.
  auto test1 = catalog->GetTable("test1");
  TableIterator ti1(test1);
  int64_t curr_row = 0;
  while (ti1.Advance()) {
    auto col1 = ti1.VectorAt(0);
    auto col2 = ti1.VectorAt(1);
    auto col1_data = col1->DataAs<int64_t>();
    auto col2_data = col2->DataAs<int32_t>();
    for (uint64_t i = 0; i < col1->NumElems(); i++) {
      REQUIRE(col1_data[i] == curr_row);
      REQUIRE(col2_data[i] == (curr_row % 10));
      curr_row++;
    }
  }
}

TEST_CASE("Reading string test table") {
  // Table loading.
  TableLoader::LoadTestTables("tpch_data");
  auto catalog = Catalog::Instance();
  // Read Table 2.
  auto test2 = catalog->GetTable("test2");
  std::vector<std::string> values{"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj"};
  TableIterator ti2(test2);
  int64_t curr_row = 0;
  while (ti2.Advance()) {
    auto col1 = ti2.VectorAt(0);
    auto col2 = ti2.VectorAt(1);
    auto col3 = ti2.VectorAt(2);
    auto col1_data = col1->DataAs<int64_t>();
    auto col2_data = col2->DataAs<int32_t>();
    auto col3_data = col3->DataAs<Varlen>();
    for (uint64_t i = 0; i < col1->NumElems(); i++) {
      std::string attr3(col3_data[i].Data(), col3_data[i].Size());
      REQUIRE(col1_data[i] == curr_row);
      REQUIRE(col2_data[i] < 10);
      REQUIRE(col3_data[i].Size() == 3);
      REQUIRE(attr3 == values[col2_data[i]]);
      curr_row++;
    }
  }
}

TEST_CASE("Reading float and date table") {
  // Table loading.
  TableLoader::LoadTestTables("tpch_data");
  auto catalog = Catalog::Instance();
  // Read Table 2.
  auto test2 = catalog->GetTable("test3");
  std::vector<std::string> str_values{"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj"};
  std::vector<double> float_values{0.37, 0.73, 3.7, 7.3, 0.037, 0.073, 0.0037, 0.0073, 0.00037, 0.00073};
  std::vector<std::string> date_values{"1996-01-02", "1996-12-01", "1993-10-14", "1995-10-11", "1994-07-30",
                                       "1992-02-21", "1996-01-10", "1995-07-16", "1993-10-27", "1998-07-21"};

  TableIterator ti2(test2);
  int64_t curr_row = 0;
  while (ti2.Advance()) {
    auto col1 = ti2.VectorAt(0);
    auto col2 = ti2.VectorAt(1);
    auto col3 = ti2.VectorAt(2);
    auto col4 = ti2.VectorAt(3);
    auto col5 = ti2.VectorAt(4);
    auto col6 = ti2.VectorAt(5);
    auto col1_data = col1->DataAs<int64_t>();
    auto col2_data = col2->DataAs<int32_t>();
    auto col3_data = col3->DataAs<Varlen>();
    auto col4_data = col4->DataAs<double>();
    auto col5_data = col5->DataAs<float>();
    auto col6_data = col6->DataAs<Date>();

    for (uint64_t i = 0; i < col1->NumElems(); i++) {
      std::string attr3(col3_data[i].Data(), col3_data[i].Size());
      REQUIRE(col1_data[i] == curr_row);
      REQUIRE(col2_data[i] < 10);
      REQUIRE(col3_data[i].Size() == 3);
      REQUIRE(attr3 == str_values[col2_data[i]]);
      REQUIRE(col4_data[i] == Approx(float_values[col2_data[i]]));
      REQUIRE(col5_data[i] == Approx(float_values[col2_data[i]] + 1.0));
      REQUIRE(col6_data[i].ToString() == date_values[col2_data[i]]);
      curr_row++;
    }
  }
}



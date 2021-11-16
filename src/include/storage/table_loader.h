#pragma once
#include "csv/csv.h"
#include "common/types.h"
#include "common/catalog.h"
#include "storage/table.h"
#include <vector>
#include <string>

namespace smartid {

struct TableLoader {
  /**
   * Load tpch table from the given directory.
   * @param directory The directory with the data files.
   */
  static void LoadTPCH(const std::string &directory);

  static void LoadJOBLight(const std::string &directory);

  /**
   * Load test files.
   * @param directory The directory with the data files.
   */
  static void LoadTestTables(const std::string &directory);

  /**
   * Load test files.
   * @param directory The directory with the data files.
   */
  static void LoadJoinTables(const std::string &directory);

  /**
   * Load a table from CSV Files.
   * @param table storage table.
   * @param filename name of the csv file.
   */
  static void LoadTable(Table *table, const std::string &filename);

 private:

  /**
   * Write a column into the byte array.
   * @param col_data byte array.
   * @param field csv field.
   * @param type column type.
   */
  static void WriteCol(char *col_data, csv::CSVField &field, SqlType type);
};

}

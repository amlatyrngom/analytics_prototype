#include <iostream>
#include "storage/table.h"
#include "storage/table_loader.h"
#include "common/catalog.h"
#include "common/settings.h"
#include "storage/buffer_manager.h"
#include <filesystem>

using namespace smartid;

// Print some TPCH tables to stdout for sanity check.
int main() {
  auto settings = Settings::Instance();
  std::filesystem::remove_all(settings->DataFolder());
  std::filesystem::create_directory(settings->DataFolder());
  settings->SetVecSize(16);
  settings->SetLogBlockSize(10);
  TableLoader::LoadTestTables("tpch_data");
  return 0;
}

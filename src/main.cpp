#include <iostream>
#include "storage/table.h"
#include "storage/table_loader.h"
#include "common/catalog.h"
#include "common/settings.h"
#include "storage/buffer_manager.h"
#include "execution/nodes/scan_node.h"
#include "execution/executors/scan_executor.h"
#include "execution/execution_context.h"
#include "execution/execution_factory.h"
#include "storage/vector.h"
#include <filesystem>

using namespace smartid;

// Print some TPCH tables to stdout for sanity check.
int main() {
  Catalog catalog("sample_workload/workload.toml");
  ExecutionFactory execution_factory(&catalog);

  {
    auto table = catalog.GetTable("central_table");
    const auto& schema = table->GetSchema();
    std::vector<uint64_t> cols_to_read{0, 1, 2, 3};
    std::vector<ExprNode*> filters;
    std::vector<ExprNode*> projs;
    projs.emplace_back(execution_factory.MakeCol(0));
    projs.emplace_back(execution_factory.MakeCol(1));
    projs.emplace_back(execution_factory.MakeCol(2));
    projs.emplace_back(execution_factory.MakeCol(3));
    auto scan = execution_factory.MakeScan(table, std::move(cols_to_read), std::move(filters), std::move(projs));
    auto printer = execution_factory.MakePrint(scan);
    auto executor = execution_factory.MakeStoredPlanExecutor(printer);
    executor->Next();
  }

//  TableLoader::LoadTestTables("tpch_data"); // Comment out to prevent reset.

//  auto table = catalog->GetTable("test2");
//  TableIterator table_iterator(table, {0, 1, 2, 3});
//  VectorProjection vp(&table_iterator);
//  while (table_iterator.Advance()) {
//    auto filter = vp.GetFilter();
//    DoNotOptimize(filter);
//    auto col_data1 = vp.VectorAt(0)->DataAs<int64_t>();
//    auto col_data2 = vp.VectorAt(1)->DataAs<int32_t>();
//    auto col_data3 = vp.VectorAt(2)->DataAs<Varlen>();
//    auto rowid_data = vp.VectorAt(3)->DataAs<int64_t>();
//    filter->Map([&](sel_t i) {
//      ASSERT(!col_data3[i].Info().IsCompact(), "Varlen must be normal");
//      std::cout << col_data1[i] << ", " << col_data2[i] << ", " << std::string(col_data3[i].Data(), col_data3[i].Info().NormalSize()) << ", " << rowid_data[i] << std::endl;
//    });
//  }

  return 0;
}

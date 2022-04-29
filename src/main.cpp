#include <iostream>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "storage/table.h"
#include "storage/table_loader.h"
#include "common/catalog.h"
#include "common/settings.h"
#include "storage/buffer_manager.h"
#include "execution/executors/scan_executor.h"
#include "execution/executors/static_aggr_executor.h"
#include "execution/executors/hash_aggr_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/execution_context.h"
#include "execution/execution_factory.h"
#include "storage/vector.h"
#include "optimizer/workload_reader.h"
#include "optimizer/workload_metadata.h"
#include "optimizer/table_statistics.h"
#include "storage/dict_encoding.h"
#include "optimizer/to_physical.h"
#include "optimizer/default.h"
#include "optimizer/materialized_views.h"
#include <chrono>
#include "optimizer/indexes.h"
#include "optimizer/smartids.h"


using namespace smartid;

// Print some TPCH tables to stdout for sanity check.

void InitMatView(Catalog* catalog, int budget) {
  catalog->Workload()->budget = budget;
  MaterializedViews::GenerateCostsForOptimization(catalog);
  MaterializedViews::BuildAllValuableMatViews(catalog);
}

void InitIndexes(Catalog* catalog, int budget) {
  catalog->Workload()->budget = budget;
  Indexes::GenerateCostsForOptimization(catalog);
  Indexes::BuildAllKeyIndexes(catalog);
}

void InitSmartIDs(Catalog* catalog) {
  SmartIDOptimizer::GenerateCostsForOptimization(catalog);
  SmartIDOptimizer::BuildSmartIDs(catalog);
}

double RunBestDefault(Catalog* catalog, const auto& query_name, std::ostream& os, bool for_benchmark, bool with_sip) {
  ExecutionFactory execution_factory(catalog);
  auto workload = catalog->Workload();
  ExecutionContext exec_ctx;
  auto query = workload->query_infos.at(query_name).get();
  if (!query->for_running) return 0.0;
  auto logical_join = query->best_join_order;
  auto physical_plan = ToPhysical::MakePhysicalJoin(catalog, workload, logical_join, &exec_ctx, &execution_factory, with_sip);
  if (query->count) {
    physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
        {0, AggType::COUNT},
    });
  }
  auto printer = execution_factory.MakePrint(physical_plan, {});
  {
    std::cout << "Warming execution" << std::endl;
    auto warming_start = std::chrono::high_resolution_clock::now();
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    executor->Next();
    auto warming_stop = std::chrono::high_resolution_clock::now();
    auto warming_duration = duration_cast<std::chrono::nanoseconds>(warming_stop - warming_start).count();
    auto warming_duration_sec = double(warming_duration) / double(1e9);
    if (warming_duration_sec > 50) {
      os << fmt::format("default,{},{}\n", query_name, warming_duration_sec);
      return warming_duration_sec; // Special case for query 60. It's too slow.
    }
  }
  double first_duration_sec;
  {
    std::cout << "Warm execution" << std::endl;
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto first_start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto first_stop = std::chrono::high_resolution_clock::now();
    auto first_duration = duration_cast<std::chrono::nanoseconds>(first_stop - first_start).count();
    first_duration_sec = double(first_duration) / double(1e9);
  }
  std::cout << "First Duration: " << first_duration_sec << "s" << std::endl;
  if (!for_benchmark) return first_duration_sec;
  uint64_t total_num_exec = 0;
  if (first_duration_sec < 1) {
    total_num_exec = std::min(uint64_t(10 / first_duration_sec) + 1, uint64_t(5));
  } else if (first_duration_sec < 10) {
    total_num_exec = std::min(uint64_t(30 / first_duration_sec) + 1, uint64_t(3));
  } else {
    total_num_exec = 1;
  }
  uint64_t curr_execs = 1; // Includes first warm execution.
  double duration_sum = first_duration_sec;
  for (; curr_execs < total_num_exec; curr_execs++) {
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
    duration_sum += double(duration) / double(1e9);
  }
  os << fmt::format("default,{},{}\n", query_name, duration_sum / double(total_num_exec));
  return duration_sum / double(total_num_exec);
}

double RunBestMatView(Catalog* catalog, const auto& query_name, std::ostream& os, bool for_benchmark) {
  ExecutionFactory execution_factory(catalog);
  auto workload = catalog->Workload();
  ExecutionContext exec_ctx;
  auto query = workload->query_infos.at(query_name).get();
  if (!query->for_running) return 0.0;
  // Force mat view selection.
  Settings::Instance()->SetScanDiscount(0.1);
  auto physical_plan = MaterializedViews::GenBestPlanWithMaterialization(catalog, query, &execution_factory, &exec_ctx);
  if (query->count) {
    physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
        {0, AggType::COUNT},
    });
  }
  auto printer = execution_factory.MakePrint(physical_plan, {});
  {
    std::cout << "Warming execution" << std::endl;
    auto warming_start = std::chrono::high_resolution_clock::now();
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    executor->Next();
    auto warming_stop = std::chrono::high_resolution_clock::now();
    auto warming_duration = duration_cast<std::chrono::nanoseconds>(warming_stop - warming_start).count();
    auto warming_duration_sec = double(warming_duration) / double(1e9);
    DoNotOptimize(warming_duration_sec);
  }
  double first_duration_sec;
  {
    std::cout << "Warm execution" << std::endl;
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto first_start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto first_stop = std::chrono::high_resolution_clock::now();
    auto first_duration = duration_cast<std::chrono::nanoseconds>(first_stop - first_start).count();
    first_duration_sec = double(first_duration) / double(1e9);
    if (first_duration_sec > 50) {
      os << fmt::format("mat_view,{},{},{}\n", query_name, workload->budget, first_duration_sec);
      return first_duration_sec; // Special case for query 60. It's too slow. For mat view, warming is needed.
    }
  }
  std::cout << "First Duration: " << first_duration_sec << "s" << std::endl;
  if (!for_benchmark) return first_duration_sec;
  uint64_t total_num_exec = 0;
  if (first_duration_sec < 1) {
    total_num_exec = std::min(uint64_t(10 / first_duration_sec) + 1, uint64_t(5));
  } else if (first_duration_sec < 10) {
    total_num_exec = std::min(uint64_t(30 / first_duration_sec) + 1, uint64_t(3));
  } else {
    total_num_exec = 1;
  }
  uint64_t curr_execs = 1; // Includes first warm execution.
  double duration_sum = first_duration_sec;
  for (; curr_execs < total_num_exec; curr_execs++) {
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
    duration_sum += double(duration) / double(1e9);
  }
  os << fmt::format("mat_view,{},{},{}\n", query_name, workload->budget, duration_sum / double(total_num_exec));
  return duration_sum / double(total_num_exec);
}

double RunBestIndexes(Catalog* catalog, const auto& query_name, std::ostream& os, bool for_benchmark) {
  ExecutionFactory execution_factory(catalog);
  auto workload = catalog->Workload();
  ExecutionContext exec_ctx;
  auto query = workload->query_infos.at(query_name).get();
  if (!query->for_running) return 0.0;
  // Prevent scans from being too cheap compared to idx lookup.
  Settings::Instance()->SetScanDiscount(0.1);
  Settings::Instance()->SetIdxLookupOverhead(2);
  auto physical_plan = Indexes::GenerateBestPlanWithIndexes(catalog, query, &execution_factory, &exec_ctx);
  if (query->count) {
    physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
        {0, AggType::COUNT},
    });
  }
  auto printer = execution_factory.MakePrint(physical_plan, {});
  {
    std::cout << "Warming execution" << std::endl;
    auto warming_start = std::chrono::high_resolution_clock::now();
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    executor->Next();
    auto warming_stop = std::chrono::high_resolution_clock::now();
    auto warming_duration = duration_cast<std::chrono::nanoseconds>(warming_stop - warming_start).count();
    auto warming_duration_sec = double(warming_duration) / double(1e9);
    if (warming_duration_sec > 50) {
      os << fmt::format("index,{},{},{}\n", query_name, workload->budget, warming_duration_sec);
      return warming_duration_sec; // Special case for query 60. It's too slow.
    }
  }
  double first_duration_sec;
  {
    std::cout << "Warm execution" << std::endl;
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto first_start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto first_stop = std::chrono::high_resolution_clock::now();
    auto first_duration = duration_cast<std::chrono::nanoseconds>(first_stop - first_start).count();
    first_duration_sec = double(first_duration) / double(1e9);
  }
  std::cout << "First Duration: " << first_duration_sec << "s" << std::endl;
  if (!for_benchmark) return first_duration_sec;
  uint64_t total_num_exec = 0;
  if (first_duration_sec < 1) {
    total_num_exec = std::min(uint64_t(10 / first_duration_sec) + 1, uint64_t(5));
  } else if (first_duration_sec < 10) {
    total_num_exec = std::min(uint64_t(30 / first_duration_sec) + 1, uint64_t(3));
  } else {
    total_num_exec = 1;
  }
  uint64_t curr_execs = 1; // Includes first warm execution.
  double duration_sum = first_duration_sec;
  for (; curr_execs < total_num_exec; curr_execs++) {
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
    duration_sum += double(duration) / double(1e9);
  }
  os << fmt::format("index,{},{},{}\n", query_name, workload->budget, duration_sum / double(total_num_exec));
  return duration_sum / double(total_num_exec);
}

double RunBestSmartID(Catalog* catalog, const auto& query_name, std::ostream& os, bool for_benchmark) {
  ExecutionFactory execution_factory(catalog);
  auto workload = catalog->Workload();
  ExecutionContext exec_ctx;
  auto query = workload->query_infos.at(query_name).get();
  if (!query->for_running) return 0.0;
  // Prevent scans from being too cheap compared to idx lookup.
  auto physical_plan = SmartIDOptimizer::GenerateBestPlanWithSmartIDs(catalog, query, &execution_factory, &exec_ctx);
  if (query->count) {
    physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
        {0, AggType::COUNT},
    });
  }
  auto printer = execution_factory.MakePrint(physical_plan, {});
  {
    std::cout << "Warming execution" << std::endl;
    auto warming_start = std::chrono::high_resolution_clock::now();
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    executor->Next();
    auto warming_stop = std::chrono::high_resolution_clock::now();
    auto warming_duration = duration_cast<std::chrono::nanoseconds>(warming_stop - warming_start).count();
    auto warming_duration_sec = double(warming_duration) / double(1e9);
    if (warming_duration_sec > 50) {
      os << fmt::format("smartid,{},{}\n", query_name, warming_duration_sec);
      return warming_duration_sec; // Special case for query 60. It's too slow.
    }
  }
  double first_duration_sec;
  {
    std::cout << "Warm execution" << std::endl;
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto first_start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto first_stop = std::chrono::high_resolution_clock::now();
    auto first_duration = duration_cast<std::chrono::nanoseconds>(first_stop - first_start).count();
    first_duration_sec = double(first_duration) / double(1e9);
  }
  std::cout << "First Duration: " << first_duration_sec << "s" << std::endl;
  if (!for_benchmark) return first_duration_sec;
  uint64_t total_num_exec = 0;
  if (first_duration_sec < 1) {
    total_num_exec = std::min(uint64_t(10 / first_duration_sec) + 1, uint64_t(5));
  } else if (first_duration_sec < 10) {
    total_num_exec = std::min(uint64_t(30 / first_duration_sec) + 1, uint64_t(3));
  } else {
    total_num_exec = 1;
  }
  uint64_t curr_execs = 1; // Includes first warm execution.
  double duration_sum = first_duration_sec;
  for (; curr_execs < total_num_exec; curr_execs++) {
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
    duration_sum += double(duration) / double(1e9);
  }
  os << fmt::format("smartid,{},{}\n", query_name, duration_sum / double(total_num_exec));
  return duration_sum / double(total_num_exec);
}


std::pair<std::unique_ptr<Catalog>, std::unique_ptr<ExecutionFactory>> InitCommon() {
  auto catalog = std::make_unique<Catalog>("job_light_workload/workload.toml");
//  auto catalog = std::make_unique<Catalog>("job_light_workload32/workload.toml");
//  auto catalog = std::make_unique<Catalog>("job_light_full_workload32/workload.toml");
//  auto catalog = std::make_unique<Catalog>("job_light_full_workload64/workload.toml");
//  auto catalog = std::make_unique<Catalog>("sample_workload/workload.toml");
//  auto catalog = std::make_unique<Catalog>("synthetic_workload/workload.toml");
//  auto catalog = std::make_unique<Catalog>("motivation_workload/workload.toml");
  auto workload = catalog->Workload();
  WorkloadReader::ReadWorkloadTables(catalog.get(), workload);
//  if (workload->just_load) {
//    return {nullptr, nullptr};
//  }
  TableStatistics::GenerateAllStats(catalog.get(), workload);

  // Deal with queries.
  std::vector<std::string> query_files{
      "job_light_workload/full.toml",
//      "job_light_workload/training_queries.toml",
//      "job_light_workload/testing_queries.toml",
//      "job_light_workload/join1.toml",
//      "job_light_workload32/full.toml",
//      "job_light_workload32/training_queries.toml",
//      "job_light_workload32/testing_queries.toml",
//      "job_light_workload32/join1.toml",
//      "sample_workload/test_joins1.toml",
//      "sample_workload/test_joins2.toml",
//      "sample_workload/test_joins3.toml",
//      "synthetic_workload/join1.toml",
//      "motivation_workload/join1.toml",
  };
  auto execution_factory = std::make_unique<ExecutionFactory>(catalog.get());
  QueryReader::ReadWorkloadQueries(catalog.get(), workload, execution_factory.get(), query_files);
  ToPhysical::FindBestJoinOrders(catalog.get(), execution_factory.get(), FreqType::TOP);

  Default::GenerateCostsForOptimization(catalog.get());
  return {std::move(catalog), std::move(execution_factory)};
}

void RunDefaultExpt(Catalog* catalog, bool with_sip) {
  auto result_file = fmt::format("{}/default_results{}.csv", catalog->Workload()->data_folder, with_sip ? "_with_sip" : "");
  std::ofstream result_os(result_file);
  for (int i = 18; i <= 18; i += 1) {
//    if (i == 60) continue;
    auto query_name = fmt::format("query{}", i);
    fmt::print("Running query {}\n", query_name);
    RunBestDefault(catalog, query_name, result_os, true, with_sip);
  }
}

void RunMatViewExpt(Catalog* catalog, int budget) {
  auto result_file = fmt::format("{}/mat_view_results_{}.csv", catalog->Workload()->data_folder, budget);
  std::ofstream result_os(result_file);
  InitMatView(catalog, budget);
  catalog->Workload()->rebuild = false;
  fmt::print("Available mat views for budget={}: {}\n", budget, catalog->Workload()->available_mat_views);
  for (const auto& mat_view_name: catalog->Workload()->available_mat_views) {
    if (catalog->GetTable(mat_view_name) == nullptr) {
      std::cerr << "Mat view not found: " << mat_view_name << std::endl;
      std::terminate();
    }
  }
  for (int i = 18; i <= 18; i += 1) {
//    if (i == 60) continue;
    auto query_name = fmt::format("query{}", i);
    fmt::print("Running query {}\n", query_name);
    RunBestMatView(catalog, query_name, result_os, true);
  }
}


void RunIndexExpt(Catalog* catalog, int budget) {
  auto result_file = fmt::format("{}/index_results_{}.csv", catalog->Workload()->data_folder, budget);
  std::ofstream result_os(result_file);
  InitIndexes(catalog, budget);
  fmt::print("Available indexes for budget={}: {}\n", budget, catalog->Workload()->available_idxs);
  catalog->Workload()->rebuild = false;
  for (int i = 1; i <= 70; i += 1) {
//    if (i == 60) continue;
    auto query_name = fmt::format("query{}", i);
    fmt::print("Running query {}\n", query_name);
    RunBestIndexes(catalog, query_name, result_os, true);
  }
}

void RunSmartIDsExpt(Catalog* catalog) {
  auto result_file = fmt::format("{}/smartids_results.csv", catalog->Workload()->data_folder);
  std::ofstream result_os(result_file);
  InitSmartIDs(catalog);
  catalog->Workload()->rebuild = false;
  for (int i = 18; i <= 18; i += 1) {
//    if (i == 60) continue;
    auto query_name = fmt::format("query{}", i);
    fmt::print("Running query {}\n", query_name);
    RunBestSmartID(catalog, query_name, result_os, true);
  }
}

int main() {
  // Do not ever let global_factory be freed until the end.
  auto [catalog, global_factory] = InitCommon();
  if (catalog == nullptr) {
    std::cout << "Done Just Loading!" << std::endl;
    return 0;
  }
  auto workload = catalog->Workload();

  // Motivation.
//  {
//    InitSmartIDs(catalog.get());
//    if (!(workload->reload || workload->rebuild || workload->gen_costs)) {
//      SmartIDOptimizer::DoMotivationExpts(catalog.get());
//    }
//  }

  // Synthetic
//  {
//    InitSmartIDs(catalog.get());
//    if (!(workload->reload || workload->rebuild || workload->gen_costs)) {
//      SmartIDOptimizer::DoUpdateExpts(catalog.get());
//    }
//  }

  // Default.
//  {
//    if (!(workload->reload || workload->rebuild || workload->gen_costs)) {
//      RunDefaultExpt(catalog.get(), false);
//      RunDefaultExpt(catalog.get(), true);
//    }
//  }

//   Mat views.
//  {
//    if (workload->rebuild || workload->gen_costs) {
//      InitMatView(catalog.get(), 16);
//    }
//    if (!(workload->reload || workload->rebuild || workload->gen_costs)) {
//      for (int budget: {2, 4, 6, 8, 10, 16, 32, 48, 64, 80, 96, 112, 128}) {
//        RunMatViewExpt(catalog.get(), budget);
//      }
//    }
//  }
//
  // Indexes
  {
    InitIndexes(catalog.get(), 4);
    for (int budget = 2; budget <= 10; budget += 2) {
      if (!(workload->reload || workload->rebuild || workload->gen_costs)) {
        RunIndexExpt(catalog.get(), budget);
      }
    }
  }
//////
////   SmartIDs
//  {
//    InitSmartIDs(catalog.get());
//    if (!(workload->reload || workload->rebuild || workload->gen_costs)) {
//      RunSmartIDsExpt(catalog.get());
//    }
//  }

  // Indexes
//  {
//    InitIndexes(catalog.get(), 10);
//    fmt::print("Available indexes: {}\n", workload->available_idxs);
//    RunBestDefault(catalog.get(), "query34", true);
//    RunBestIndexes(catalog.get(), "query34", true);
//  }

  // SmartIDs
//  {
//    InitSmartIDs(catalog.get());
//    RunBestDefault(catalog.get(), "query21", true);
//    RunBestSmartID(catalog.get(), "query21", true);
//  }


//
////   Try running join
//  {
//    ExecutionContext exec_ctx;
//    auto query = workload->query_infos["query34"].get();
//    auto logical_join = query->best_join_order;
//
//    auto physical_plan = ToPhysical::MakePhysicalJoin(&catalog, workload, logical_join, &exec_ctx, &execution_factory);
//    if (query->count) {
//      physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
//          {0, AggType::COUNT},
//      });
//    }
//    std::cout << "FirstPrint" << std::endl;
//    auto printer = execution_factory.MakePrint(physical_plan, {});
//    {
//      auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
//      executor->Next();
//    }
//    query->best_join_order->ToString(std::cout);
//    std::cout << "SecondPrint" << std::endl;
//    {
//      auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
//      auto start = std::chrono::high_resolution_clock::now();
//      executor->Next();
//      auto stop = std::chrono::high_resolution_clock::now();
//      auto duration = duration_cast<std::chrono::milliseconds>(stop - start);
//      std::cout << "Duration: " << duration.count() << "ms" << std::endl;
//    }
//  }
//  {
//    ExecutionContext exec_ctx;
//    auto query = workload->query_infos["query34"].get();
//    auto physical_plan = MatViewEstimator::GenBestPlanWithMaterialization(&catalog, query, &execution_factory, &exec_ctx);
//    if (query->count) {
//      physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
//          {0, AggType::COUNT},
//      });
//    }
//    std::cout << "FirstPrint" << std::endl;
//    auto printer = execution_factory.MakePrint(physical_plan, {});
//    {
//      auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
//      executor->Next();
//    }
//    std::cout << "SecondPrint" << std::endl;
//    {
//      auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
//      auto start = std::chrono::high_resolution_clock::now();
//      executor->Next();
//      auto stop = std::chrono::high_resolution_clock::now();
//      auto duration = duration_cast<std::chrono::milliseconds>(stop - start);
//      std::cout << "Duration: " << duration.count() << "ms" << std::endl;
//    }
//  }

//  {
//    auto mat_table = catalog.GetTable("mat_view_movie_info_idx___title");
//    std::vector<uint64_t> cols_to_read{0, 4};
//    std::vector<ExprNode*> projs;
//    auto info_id_col = execution_factory.MakeCol(0);
//    auto title_id_col = execution_factory.MakeCol(1);
//    projs.emplace_back(info_id_col);
//    projs.emplace_back(title_id_col);
//    std::vector<ExprNode*> filters;
//    auto const_val = execution_factory.MakeConst(int(112), SqlType::Int32);
//    auto comp = execution_factory.MakeBinaryComp(info_id_col, const_val, OpType::EQ);
//    filters.emplace_back(comp);
//    auto scan = execution_factory.MakeScan(mat_table, std::move(cols_to_read), std::move(projs), std::move(filters));
//    auto counter = execution_factory.MakeStaticAggregation(scan, {
//          {0, AggType::COUNT},
//    });
//
//    auto printer = execution_factory.MakePrint(counter, {});
//    auto executor = ExecutionFactory::MakePlanExecutor(printer);
//    executor->Next();
//  }


  // Scan title
//  {
//    ExecutionContext exec_ctx;
//    auto query = workload->query_infos["title_scan"].get();
//    auto logical_scan = query->scans[0];
//    auto table = catalog.GetTable(logical_scan->table_info->name);
//    auto cols_to_read = logical_scan->cols_to_read;
//    auto projections = logical_scan->projections;
//    std::vector<ExprNode*> filters;
//    for (auto& logical_filter: logical_scan->filters) {
//      ToPhysical::BindParams(table, &logical_filter, &exec_ctx);
//      filters.emplace_back(logical_filter.expr_node);
//    }
//    PlanNode* physical_plan = execution_factory.MakeScan(table, std::move(cols_to_read), std::move(projections), std::move(filters));
//    if (query->count) {
//      physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
//          {0, AggType::COUNT},
//      });
//    }
//    auto printer = execution_factory.MakePrint(physical_plan, {});
//    {
//      auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
//      executor->Next();
//    }
//    {
//      auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
//      executor->Next();
//    }
//
//  }


  // Make catalog and tables.
//  Catalog catalog("sample_workload/workload.toml");
//  auto workload = catalog.Workload();
//  WorkloadReader::ReadWorkloadTables(&catalog, workload);
//  TableStatistics::GenerateAllStats(&catalog, workload);
//
//  // Deal with queries.
//  std::vector<std::string> query_files{
////      "sample_workload/test_scans.toml",
//      "sample_workload/test_joins1.toml",
//      "sample_workload/test_joins2.toml",
////      "sample_workload/scan_queries.toml",
////      "sample_workload/join_queries.toml"
//  };
//  ExecutionFactory execution_factory(&catalog);
//  QueryReader::ReadWorkloadQueries(&catalog, workload, &execution_factory, query_files);
//  ToPhysical::FindBestJoinOrders(&catalog, &execution_factory);
//
//  Settings::Instance()->SetScanDiscount(0.05);
//  Settings::Instance()->SetIdxLookupOverhead(2);
//  MatViewEstimator::GenerateDefaultCosts(&catalog, std::cout);
//  MatViewEstimator::GenerateMateralizationCosts(&catalog, std::cout);
//  MatViewEstimator::GenerateRowIDIndexCosts(&catalog, std::cout);
//  {
//    ExecutionContext exec_ctx;
//    auto query = workload->query_infos["join1"].get();
//    ToPhysical::EstimateScanSelectivities(&catalog, query);
//    ToPhysical::ResolveJoins(&catalog, workload, query, &execution_factory);
//    ToPhysical::EstimateJoinCardinalities(&catalog, workload, query);
//    ToPhysical::FindBestJoinOrder(query);
//    query->best_join_order->ToString(std::cout);
//    // Estimation issues
//    fmt::print("denorm_size={}; estimated_output={}; estimated_mat={}\n",
//               query->best_join_order->estimated_denorm_size, query->best_join_order->estimated_output_size, query->best_join_order->total_materialized_size);
//  }

//  ExecutionFactory execution_factory(&catalog);
//
//  ScanNode* central_scan;
//  {
//    auto table = catalog.GetTable("central_table");
//    const auto& schema = table->GetSchema();
//    std::vector<uint64_t> cols_to_read{0, 1, 2, 3};
//    auto id_col = execution_factory.MakeCol(0);
//    auto id1_col = execution_factory.MakeCol(1);
//    auto id2_col = execution_factory.MakeCol(2);
//    auto int64_col = execution_factory.MakeCol(3);
//    auto const_val1 = execution_factory.MakeConst(int64_t(10001), SqlType::Int64);
//    auto const_val2 = execution_factory.MakeConst(int64_t(10004), SqlType::Int64);
//    std::vector<ExprNode*> filters;
//    filters.emplace_back(execution_factory.MakeBinaryComp(id1_col, const_val1, OpType::GE));
//    filters.emplace_back(execution_factory.MakeBinaryComp(id1_col, const_val2, OpType::LE));
//    std::vector<ExprNode*> projs;
//    projs.emplace_back(id_col);
//    projs.emplace_back(id1_col);
//    projs.emplace_back(id2_col);
//    projs.emplace_back(int64_col);
//    auto scan = execution_factory.MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
//    central_scan = scan;
////    std::vector<uint64_t> build_keys{1};
////    std::vector<std::pair<uint64_t, AggType>> aggs{
////        {0, AggType::COUNT}, {0, AggType::MAX}, {0, AggType::MIN}, {0, AggType::SUM},
////    };
////    auto hash_agg = execution_factory.MakeHashAggregation(scan, std::move(build_keys), std::move(aggs));
////    std::vector<std::pair<uint64_t, SortType>> sort_keys {
////        {2, SortType::DESC},
////    };
////    auto sort_node = execution_factory.MakeSort(hash_agg, std::move(sort_keys), 1);
////    auto printer = execution_factory.MakePrint(sort_node);
////    auto executor = execution_factory.MakeStoredPlanExecutor(printer);
////    std::cout << "Executing Print" << std::endl;
////    executor->Next();
//  }
//
//  ScanNode* dim2_scan;
//  {
//    auto table = catalog.GetTable("dimension_table2");
//    const auto& schema = table->GetSchema();
//    std::vector<uint64_t> cols_to_read{0, 1, 2, 3};
//    auto id_col = execution_factory.MakeCol(0);
//    auto fk_col = execution_factory.MakeCol(1);
//    auto text_col = execution_factory.MakeCol(2);
//    auto date_col = execution_factory.MakeCol(3);
////    auto const_val = execution_factory.MakeConst(table->GetDictEncoding(2)->GetCode("ccc"), SqlType::Int32);
//    std::vector<ExprNode*> filters;
////    filters.emplace_back(execution_factory.MakeBinaryComp(text_col, const_val, OpType::LE));
//    std::vector<ExprNode*> projs;
//    projs.emplace_back(id_col);
//    projs.emplace_back(fk_col);
//    projs.emplace_back(text_col);
//    projs.emplace_back(date_col);
//    auto scan = execution_factory.MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
////    dim2_scan = scan;
//    auto printer = execution_factory.MakePrint(scan, {});
//    auto executor = execution_factory.MakeStoredPlanExecutor(printer);
//    std::cout << "Executing Print" << std::endl;
//    executor->Next();
//  }
//
//  {
//    // Do join
//    std::vector<uint64_t> build_keys{1}; // dim2.fk
//    std::vector<uint64_t> probe_keys{2}; // central.id2
//    std::vector<std::pair<uint64_t, uint64_t>> join_projs{
//        {0, 0}, {0, 1}, {0, 2}, {0, 3},
//        {1, 0}, {1, 1}, {1, 2}, {1, 3},
//    };
//    auto join_node = execution_factory.MakeHashJoin(dim2_scan, central_scan, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
//    auto printer = execution_factory.MakePrint(join_node);
//    auto executor = execution_factory.MakeStoredPlanExecutor(printer);
//    std::cout << "Executing Print" << std::endl;
//    executor->Next();
//  }

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

#include "optimizer/smartids.h"
#include "optimizer/indexes.h"
#include "optimizer/logical_nodes.h"
#include "optimizer/workload_metadata.h"
#include "optimizer/workload_reader.h"
#include "optimizer/to_physical.h"
#include "optimizer/table_statistics.h"
#include "optimizer/histogram.h"
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <fstream>
#include "common/catalog.h"
#include "execution/execution_factory.h"
#include "execution/execution_context.h"
#include "execution/executors/scan_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/executors/plan_executor.h"
#include <toml++/toml.h>
#include "storage/table.h"
#include "storage/index.h"
#include "storage/vector_projection.h"
#include "storage/vector.h"
#include "storage/buffer_manager.h"

namespace smartid {


void RecursiveGatherFilters(const std::string& q_name, const LogicalJoinNode* logical_join, std::ostream& table_os, std::ostream& filter_os) {
  {
    // Right scan
    auto right_scan = logical_join->right_scan;
    table_os << fmt::format("{},{},{},{}\n", q_name, right_scan->table_info->name, right_scan->estimated_output_size, right_scan->base_table_size);
    for (const auto& f: right_scan->filters) {
      filter_os << fmt::format("{},{},{},{},{}\n", q_name, right_scan->table_info->name, f.col_name, f.estimated_sel, right_scan->base_table_size);
    }
  }
  if (logical_join->left_scan != nullptr) {
    auto left_scan = logical_join->left_scan;
    table_os << fmt::format("{},{},{},{}\n", q_name, left_scan->table_info->name, left_scan->estimated_output_size, left_scan->base_table_size);
    for (const auto& f: left_scan->filters) {
      filter_os << fmt::format("{},{},{},{},{}\n", q_name, left_scan->table_info->name, f.col_name, f.estimated_sel, left_scan->base_table_size);
    }
  } else {
    RecursiveGatherFilters(q_name, logical_join->left_join, table_os, filter_os);
  }
}


void GenSmartIDJoins(Catalog* catalog, std::ostream& toml_os) {
  auto workload = catalog->Workload();
  auto res_toml = toml::table{};
  auto queries_toml = toml::table{};
  for (const auto& [from_table_name, from_table_info]: workload->table_infos) {
    for (const auto& [to_table_name, to_table_info]: workload->table_infos) {
      if (from_table_name == to_table_name) continue;
      std::vector<std::string> table_set;
      table_set.emplace_back(from_table_name);
      table_set.emplace_back(to_table_name);
      if (workload->central_table_name != from_table_name && workload->central_table_name != to_table_name) {
        table_set.emplace_back(workload->central_table_name);
      }
      std::string key_col_name;
      for (const auto& col: from_table_info->schema.Cols()) {
        if (from_table_name != workload->central_table_name && col.IsFK()) {
          key_col_name = col.Name();
          break;
        } else if (from_table_name == workload->central_table_name && col.IsPK()) {
          key_col_name = col.Name();
          break;
        }
      }
      for (const auto& from_col_idx: from_table_info->used_cols) {
        const auto& from_col_name = from_table_info->schema.GetColumn(from_col_idx).Name();
        auto query_tbl = toml::table{};
        auto query_name = fmt::format("smart_id_{}___{}___{}", from_table_name, from_col_name, to_table_name);
        query_tbl.insert("is_mat_view", true); // Mark as mat view.
        if (table_set.size() == 2) { // from_table, to_table.
          // Just scan from table.
          query_tbl.emplace<std::string, std::string>("type", "scan");
          query_tbl.emplace<std::string, std::string>("table", from_table_name);
          auto inputs_arr = toml::array{};
          auto projections_arr = toml::array{};
          inputs_arr.emplace_back<std::string>(from_col_name);
          inputs_arr.emplace_back<std::string>(key_col_name);
          projections_arr.emplace_back<std::string>(from_col_name);
          projections_arr.emplace_back<std::string>(key_col_name);
          query_tbl.insert("inputs", inputs_arr);
          query_tbl.insert("projections", projections_arr);
          queries_toml.insert(query_name, query_tbl);
        } else { // from_table, central_table, to_table.
          query_tbl.emplace<std::string, std::string>("type", "join");
          // Join from table and central_table.
          for (const auto& table_name: table_set) {
            if (table_name == to_table_name) continue; // No need to scan to_table. Will just use fk index.
            auto scan_tbl = toml::table{};
            scan_tbl.emplace<std::string, std::string>("type", "scan");
            scan_tbl.emplace<std::string, std::string>("table", table_name);
            auto inputs_arr = toml::array{};
            auto projections_arr = toml::array{};
            if (table_name == from_table_name) {
              inputs_arr.emplace_back<std::string>(from_col_name);
              inputs_arr.emplace_back<std::string>(key_col_name);
              projections_arr.emplace_back<std::string>(from_col_name);
              projections_arr.emplace_back<std::string>(key_col_name);
            }
            scan_tbl.insert("inputs", inputs_arr);
            scan_tbl.insert("projections", projections_arr);
            query_tbl.insert(fmt::format("scan_{}", table_name), scan_tbl);
          }
          queries_toml.insert(query_name, query_tbl);
        }

      }
    }
  }
  res_toml.insert("queries", queries_toml);
  toml_os << res_toml;
}

void SmartIDOptimizer::GatherFilters(Catalog* catalog, std::ostream& table_os, std::ostream& filter_os) {
  auto workload = catalog->Workload();
  for (const auto& [query_name, query_info]: workload->query_infos) {
    if (!query_info->IsRegularJoin()) continue; // Not a join.
    RecursiveGatherFilters(query_name, query_info->best_join_order, table_os, filter_os);
  }
}

void SmartIDOptimizer::GenerateCostsForOptimization(Catalog *catalog) {
  auto workload = catalog->Workload();
  if (!workload->gen_costs) return;
  {
    std::ofstream table_os(workload->data_folder + "/table_outputs.csv");
    std::ofstream filter_os(workload->data_folder + "/filter_sels.csv");
    std::ofstream joins_os(workload->data_folder + "/smartid_queries.toml");
    SmartIDOptimizer::GatherFilters(catalog, table_os, filter_os);
    GenSmartIDJoins(catalog, joins_os);
  }
}


void ResetSmartID(Catalog* catalog, SmartIDInfo* smartid_info) {
//  if (smartid_info->to_table_name != "movie_info_idx" && smartid_info->to_table_name != "title") return;
  auto to_table = catalog->GetTable(smartid_info->to_table_name);
  auto block_size = Settings::Instance()->BlockSize();
  for (const auto& block_id: to_table->BlockIDS()) {
    auto block_info = to_table->BM()->Pin(block_id);
    auto raw_table_block = TableBlock::FromBlockInfo(block_info);
    auto to_col_data = raw_table_block->MutableColDataAs<uint64_t>(smartid_info->to_col_idx);
    for (uint64_t row_idx = 0; row_idx < block_size; row_idx++) {
      to_col_data[row_idx] &= 0xFFFFFFFFFFFFull; // 48 ones.
    }
    to_table->BM()->Unpin(block_info, true);
  }
}


void BuildSmartID(Catalog* catalog, SmartIDInfo* smartid_info, PlanNode* plan_node, uint64_t from_col_proj_idx, uint64_t key_col_proj_idx) {
//  if (smartid_info->to_table_name != "movie_info_idx" && smartid_info->to_table_name != "title") return;
  std::cout << "Building SmartID: " << std::endl;
  smartid_info->ToString(std::cout);
  uint64_t log_block_size = Settings::Instance()->LogBlockSize();
  uint64_t ones = ~(0ull);
  uint64_t row_id_mask = ~(ones << log_block_size);

  auto to_table = catalog->GetTable(smartid_info->to_table_name);
  auto from_table = catalog->GetTable(smartid_info->from_table_name);
  auto from_col_hist = from_table->GetStatistics()->column_stats.at(smartid_info->from_col_idx)->hist_.get();
  auto executor = ExecutionFactory::MakePlanExecutor(plan_node, nullptr);
  auto index = catalog->GetIndex(smartid_info->to_table_name)->GetIndex();
  const VectorProjection* vp;
  Vector offsets(SqlType::Int64);
//  uint64_t num_printed{0};
  std::unordered_map<int64_t, std::vector<std::pair<uint64_t, uint64_t>>> block_row_idx_offset; // Map from block_id to <row_idx, offset>
  while ((vp = executor->Next()) != nullptr) {
    auto key_col_data = vp->VectorAt(key_col_proj_idx)->DataAs<int64_t>(); // Assumes keys are int64s.
    auto from_vec = vp->VectorAt(from_col_proj_idx);
//    auto from_col_data = from_vec->DataAs<int32_t>();
    from_col_hist->BitIndices(vp->GetFilter(), from_vec, &offsets, smartid_info->bit_offset, smartid_info->num_bits);
    auto offsets_data = offsets.DataAs<int64_t>();
    vp->GetFilter()->Map([&](sel_t i) {
//      auto col_val = from_col_data[i];
      auto key = key_col_data[i] & 0xFFFFFFFFFFFF;
      auto offset = offsets_data[i];
      ASSERT(offset >= 48, "Writing at wrong offset");
//      bool print_vals{false};
//      if (num_printed < 10) {
//        num_printed++;
//        print_vals = true;
//        fmt::print("val={}; key={}; offset={}\n", col_val, key, offset);
//      }
      if (auto it =  index->find(key); it != index->end()) {
        for (const auto& row_id: it->second) {
          auto row_idx = row_id & row_id_mask;
          auto block_id = row_id >> log_block_size;
          block_row_idx_offset[block_id].emplace_back(row_idx, offset);
//          if (print_vals) {
//            fmt::print("(block_id, row_idx)=({}, {})\n", block_id, row_idx);
//          }
        }
      }
    });
  }

//  num_printed = 0;
  for (const auto& [block_id, row_idx_offsets]: block_row_idx_offset) {
    auto block_info = to_table->BM()->Pin(block_id);
    auto raw_table_block = TableBlock::FromBlockInfo(block_info);
    auto to_col_data = raw_table_block->MutableColDataAs<uint64_t>(smartid_info->to_col_idx);
    for (const auto& [row_idx, offset]: row_idx_offsets) {
//      if (num_printed < 10) {
//        num_printed++;
//        fmt::print("Replacing {:x} by {:x} in (block_id, row_idx)=({}, {})\n", to_col_data[row_idx], to_col_data[row_idx] | (1ull << offset), block_id, row_idx);
//      }
      to_col_data[row_idx] |= (1ull << offset);
    }
    to_table->BM()->Unpin(block_info, true);
  }
//  std::terminate();

}


void SmartIDOptimizer::BuildSmartIDs(Catalog *catalog) {
  auto workload = catalog->Workload();
  // Read embedding infos.
  auto opt_file = fmt::format("{}/opts/embeddings_opt_16.csv", catalog->Workload()->data_folder);
  std::ifstream is(opt_file);
  std::string from_table, from_col, to_table;
  uint64_t bit_offset, num_bits;
  while ((is >> from_table >> from_col >> to_table >> num_bits >> bit_offset)) {
    auto smartid_info = std::make_unique<SmartIDInfo>();
    smartid_info->from_table_name = from_table;
    smartid_info->from_table_info = workload->table_infos.at(from_table).get();
    smartid_info->from_col_name = from_col;
    smartid_info->from_col_idx = smartid_info->from_table_info->schema.ColIdx(from_col);
    smartid_info->to_table_name = to_table;
    smartid_info->to_table_info = workload->table_infos.at(to_table).get();
    smartid_info->bit_offset = bit_offset;
    smartid_info->num_bits = num_bits;
    // Compute key column name.
    for (const auto& col: smartid_info->from_table_info->schema.Cols()){
      if (smartid_info->from_table_name == workload->central_table_name && col.IsPK()) {
        smartid_info->from_key_col_name = col.Name();
        break;
      }
      if (smartid_info->from_table_name != workload->central_table_name && col.IsFK()) {
        smartid_info->from_key_col_name = col.Name();
        break;
      }
    }
    // Compute to_col_name.
    uint64_t col_idx = 0;
    for (const auto& col: smartid_info->to_table_info->schema.Cols()){
      if (smartid_info->to_table_name == workload->central_table_name && col.IsPK()) {
        smartid_info->to_col_name = col.Name();
        smartid_info->to_col_idx = col_idx;
        break;
      }
      if (smartid_info->to_table_name != workload->central_table_name && col.IsFK()) {
        smartid_info->to_col_name = col.Name();
        smartid_info->to_col_idx = col_idx;
        break;
      }
      col_idx++;
    }
    if (workload->rebuild) {
      ResetSmartID(catalog, smartid_info.get());
    }
    workload->smartid_infos.emplace_back(std::move(smartid_info));
  }

  if (!workload->rebuild) return;
  if (workload->available_idxs.empty()) {
    // Rebuild indexes to speedup updates.
    Indexes::BuildAllKeyIndexes(catalog);
  }

  // Read smart id query to see how to join any pair of tables.
  std::string mat_views_toml_file(fmt::format("{}/smartid_queries.toml", workload->data_folder));
  ExecutionFactory factory(catalog);
  QueryReader::ReadWorkloadQueries(catalog, workload, &factory, {mat_views_toml_file});
  for (auto& smartid_info: workload->smartid_infos) {
    ExecutionFactory execution_factory(catalog);
    ExecutionContext exec_ctx;
    std::string from_col_fullname = (smartid_info->from_table_name + "." + smartid_info->from_col_name);
    std::string key_col_fullname = (smartid_info->from_table_name + "." + smartid_info->from_key_col_name);

    auto query_name = fmt::format("smart_id_{}___{}___{}", smartid_info->from_table_name, smartid_info->from_col_name, smartid_info->to_table_name);
    smartid_info->smartid_query = workload->query_infos.at(query_name).get();
    ToPhysical::EstimateScanSelectivities(catalog, smartid_info->smartid_query);
    uint64_t from_col_proj_idx{std::numeric_limits<uint64_t>::max()};
    uint64_t key_col_proj_idx{std::numeric_limits<uint64_t>::max()};
    PlanNode* physical_query{nullptr};
    if (smartid_info->smartid_query->scans.size() == 1) {
      std::cout << "SmartID Query: " << smartid_info->smartid_query->name << std::endl;
      auto logical_scan = smartid_info->smartid_query->scans[0];
      logical_scan->ToString(std::cout);
      auto table = catalog->GetTable(logical_scan->table_info->name);
      auto cols_to_read = logical_scan->cols_to_read;
      auto projs = logical_scan->projections;
      std::vector<ExprNode*> filters;
      physical_query = factory.MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
      for (const auto& [proj_idx, fullname]:logical_scan->proj_idx_full_name) {
        if (fullname == from_col_fullname) {
          from_col_proj_idx = proj_idx;
        }
        if (fullname == key_col_fullname) {
          key_col_proj_idx = proj_idx;
        }
      }
    } else {
      ToPhysical::ResolveJoins(catalog, workload, smartid_info->smartid_query, &factory);
      ToPhysical::FindBestJoinOrder(smartid_info->smartid_query);
      std::cout << "SmartID Query: " << smartid_info->smartid_query->name << std::endl;
      smartid_info->smartid_query->best_join_order->ToString(std::cout);
      for (const auto& [proj_idx, fullname]:smartid_info->smartid_query->best_join_order->proj_idx_full_name) {
        if (fullname == from_col_fullname) {
          from_col_proj_idx = proj_idx;
        }
        if (fullname == key_col_fullname) {
          key_col_proj_idx = proj_idx;
        }
      }
      physical_query = ToPhysical::MakePhysicalJoin(catalog, workload, smartid_info->smartid_query->best_join_order, &exec_ctx, &execution_factory);
    }
    ASSERT(from_col_proj_idx != std::numeric_limits<uint64_t>::max(), "From col not found!");
    ASSERT(key_col_proj_idx != std::numeric_limits<uint64_t>::max(), "Key col not found!");
    std::cout << "From Col Idx: " << from_col_proj_idx << std::endl;
    std::cout << "Key Col Idx: " << key_col_proj_idx << std::endl;
    BuildSmartID(catalog, smartid_info.get(), physical_query, from_col_proj_idx, key_col_proj_idx);
  }
}

int64_t MakeEmbedding(Catalog* catalog, LogicalScanNode* logical_scan, SmartIDInfo* smartid_info) {
  auto from_hist = catalog->GetTable(smartid_info->from_table_name)->GetStatistics()->column_stats.at(smartid_info->from_col_idx)->hist_.get();
  uint64_t ones = ~(0ull);
  uint64_t lo_offset = smartid_info->bit_offset;
  uint64_t hi_offset = lo_offset + smartid_info->num_bits;
  uint64_t lo_mask = ~(ones << lo_offset);
  uint64_t hi_mask = hi_offset == 64 ? ones : ~(ones << hi_offset);
  uint64_t curr_embedding = (hi_mask ^ lo_mask);
  for (const auto& filter: logical_scan->filters) {
    Vector vec(filter.col_type);
    Vector offsets(SqlType::Int64);
    if (filter.expr_type == ExprType::Between) {
      // Assumes values already bound.
      vec.Resize(2);
      offsets.Resize(2);
      auto& lo_val = filter.vals[0];
      auto& hi_val = filter.vals[1];
      // TODO: add other types.
      if (filter.col_type == SqlType::Int32) {
        auto vec_data = vec.MutableDataAs<int32_t>();
        vec_data[0] = std::get<int32_t>(lo_val);
        vec_data[1] = std::get<int32_t>(hi_val);
      } else {
        ASSERT(false, "Type not yet added!!");
      }
      from_hist->BitIndices(vec.NullBitmap(), &vec, &offsets, smartid_info->bit_offset, smartid_info->num_bits);
      auto offset_data = offsets.DataAs<uint64_t>();
      lo_offset = offset_data[0];
      hi_offset = offset_data[1] + 1; // hi_offset is non-inclusive.
    } else if (filter.expr_type == ExprType::BinaryComp) {
      vec.Resize(1);
      offsets.Resize(1);
      auto val = filter.vals[0];
      // TODO: add other types.
      if (filter.col_type == SqlType::Int32) {
        auto vec_data = vec.MutableDataAs<int32_t>();
        vec_data[0] = std::get<int32_t>(val);
      } else {
        ASSERT(false, "Type not yet added!!");
      }
      from_hist->BitIndices(vec.NullBitmap(), &vec, &offsets, smartid_info->bit_offset, smartid_info->num_bits);
      auto offset_data = offsets.DataAs<uint64_t>();
      if (filter.op_type == OpType::EQ) {
        lo_offset = offset_data[0];
        hi_offset = lo_offset + 1;
      } else if (filter.op_type == OpType::LE || filter.op_type == OpType::LT) {
        lo_offset = smartid_info->bit_offset;
        hi_offset = offset_data[0] + 1;
      } else if (filter.op_type == OpType::GE || filter.op_type == OpType::GT) {
        lo_offset = offset_data[0];
        hi_offset = smartid_info->bit_offset + smartid_info->num_bits;
      } else {
        ASSERT(false, "NE filter not supported!!");
      }
    } else {
      ASSERT(false, "Comp type not yet supported!!");
    }
    lo_mask = ~(ones << lo_offset);
    hi_mask = hi_offset == 64 ? ones : ~(ones << hi_offset);
    curr_embedding &= (hi_mask ^ lo_mask);
  }
  return curr_embedding;
}

std::pair<int64_t, bool> RecursiveGatherEmbeddings(Catalog* catalog, LogicalJoinNode* logical_join, SmartIDInfo* smartid_info) {
//  if (smartid_info->to_table_name != "movie_info_idx" && smartid_info->to_table_name != "title") return {0, false};
  auto right_scan = logical_join->right_scan;
  if (right_scan->table_info->name == smartid_info->from_table_name) {
    if (right_scan->filters.empty()) {
      return {0, false};
    }
    return {MakeEmbedding(catalog, right_scan, smartid_info), true};
  }
  if (logical_join->left_scan != nullptr) {
    auto left_scan = logical_join->left_scan;
    ASSERT(left_scan->table_info->name == smartid_info->from_table_name, "From table must be scanned!!");
    if (left_scan->filters.empty()) {
      return {0, false};
    }
    return {MakeEmbedding(catalog, left_scan, smartid_info), true};
  }
  return RecursiveGatherEmbeddings(catalog, logical_join->left_join, smartid_info);
}

PlanNode* SmartIDOptimizer::GenerateBestPlanWithSmartIDs(Catalog* catalog, QueryInfo* query_info, ExecutionFactory* factory, ExecutionContext* exec_ctx) {
  auto workload = catalog->Workload();
  auto logical_join = query_info->best_join_order;
  std::unordered_set<std::string> scanned_tables;
  std::unordered_map<std::string, std::pair<uint64_t, int64_t>> table_embeddings;
  for (const auto& table_info: logical_join->scanned_tables) {
    scanned_tables.emplace(table_info->name);
  }
  for (auto& smartid_info: workload->smartid_infos) {
    if (scanned_tables.contains(smartid_info->from_table_name) && scanned_tables.contains(smartid_info->to_table_name)) {
      const auto& [embedding, has_filter] = RecursiveGatherEmbeddings(catalog, logical_join, smartid_info.get());
      if (has_filter) {
        std::cout << "Using SmartID: "; smartid_info->ToString(std::cout);
        if (table_embeddings.contains(smartid_info->to_table_name)) {
          table_embeddings[smartid_info->to_table_name].second |= embedding;
        } else {
          table_embeddings[smartid_info->to_table_name] = {smartid_info->to_col_idx, embedding};
        }
      }
    }
  }

  for (const auto& [table_name, embedding]: table_embeddings) {
    fmt::print("Embedding for {} is {:x}\n", table_name, embedding.second);
  }
  return ToPhysical::MakePhysicalPlanWithSmartIDs(catalog, logical_join, table_embeddings, factory, exec_ctx);
}


}
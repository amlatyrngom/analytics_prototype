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
#include "execution/executors/scalar_hash_join_executor.h"
#include "execution/executors/static_aggr_executor.h"
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
        if (from_col_name == key_col_name) continue;
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
    if (!query_info->for_training) continue;
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
    if (smartid_info->to_col_type == SqlType::Int64) {
      auto to_col_data = raw_table_block->MutableColDataAs<uint64_t>(smartid_info->to_col_idx);
      for (uint64_t row_idx = 0; row_idx < block_size; row_idx++) {
        to_col_data[row_idx] &= 0xFFFFFFFFFFFFull; // 48 ones.
      }
    }
    if (smartid_info->to_col_type == SqlType::Int32) {
      auto to_col_data = raw_table_block->MutableColDataAs<uint32_t>(smartid_info->to_col_idx);
      for (uint64_t row_idx = 0; row_idx < block_size; row_idx++) {
        to_col_data[row_idx] &= 0xFFFFFF; // 24 ones.
      }
    }
    to_table->BM()->Unpin(block_info, true);
  }
}

void LookupByType(const VectorProjection* vp, RowIDIndex* index, SqlType key_type, const char* keys, const int64_t* offsets, std::unordered_map<int64_t, std::vector<std::pair<uint64_t, uint64_t>>>& out) {
  uint64_t log_block_size = Settings::Instance()->LogBlockSize();
  uint64_t ones = ~(0ull);
  uint64_t row_id_mask = ~(ones << log_block_size);
  if (key_type == SqlType::Int32) {
    auto typed_keys = reinterpret_cast<const int32_t*>(keys);
    auto index_table = index->GetIndex32();
    vp->GetFilter()->Map([&](sel_t i) {
      auto key = typed_keys[i] & RowIDIndex::KEY_MASK32;
      auto offset = offsets[i];
      if (auto it =  index_table->find(key); it != index_table->end()) {
        for (const auto& row_id: it->second) {
          auto row_idx = row_id & row_id_mask;
          auto block_id = row_id >> log_block_size;
          out[block_id].emplace_back(row_idx, offset);
        }
      }
    });
  }
  if (key_type == SqlType::Int64) {
    auto typed_keys = reinterpret_cast<const int64_t*>(keys);
    auto index_table = index->GetIndex64();
    vp->GetFilter()->Map([&](sel_t i) {
      auto key = typed_keys[i] & RowIDIndex::KEY_MASK64;
      auto offset = offsets[i];
      if (auto it =  index_table->find(key); it != index_table->end()) {
        for (const auto& row_id: it->second) {
          auto row_idx = row_id & row_id_mask;
          auto block_id = row_id >> log_block_size;
          out[block_id].emplace_back(row_idx, offset);
        }
      }
    });
  }
}

void BuildSmartID(Catalog* catalog, SmartIDInfo* smartid_info, PlanNode* plan_node, uint64_t from_col_proj_idx, uint64_t key_col_proj_idx) {
//  if (smartid_info->to_table_name != "movie_info_idx" && smartid_info->to_table_name != "title") return;
  std::cout << "Building SmartID: " << std::endl;
  smartid_info->ToString(std::cout);

  auto to_table = catalog->GetTable(smartid_info->to_table_name);
  auto from_table = catalog->GetTable(smartid_info->from_table_name);
  auto from_col_hist = from_table->GetStatistics()->column_stats.at(smartid_info->from_col_idx)->hist_.get();
  auto executor = ExecutionFactory::MakePlanExecutor(plan_node, nullptr);
  const VectorProjection* vp;
  Vector offsets(SqlType::Int64);
//  uint64_t num_printed{0};
  std::unordered_map<int64_t, std::vector<std::pair<uint64_t, uint64_t>>> block_row_idx_offset; // Map from block_id to <row_idx, offset>
  while ((vp = executor->Next()) != nullptr) {
    auto from_vec = vp->VectorAt(from_col_proj_idx);
//    auto from_col_data = from_vec->DataAs<int32_t>();
    from_col_hist->BitIndices(vp->GetFilter(), from_vec, &offsets, smartid_info->bit_offset, smartid_info->num_bits);
    auto offsets_data = offsets.DataAs<int64_t>();
    auto index = catalog->GetIndex(smartid_info->to_table_name);
    LookupByType(vp, index, smartid_info->to_col_type, vp->VectorAt(key_col_proj_idx)->Data(), offsets_data, block_row_idx_offset);
  }

//  num_printed = 0;
  for (const auto& [block_id, row_idx_offsets]: block_row_idx_offset) {
    auto block_info = to_table->BM()->Pin(block_id);
    auto raw_table_block = TableBlock::FromBlockInfo(block_info);
    if (smartid_info->to_col_type == SqlType::Int32) {
      auto to_col_data = raw_table_block->MutableColDataAs<uint32_t>(smartid_info->to_col_idx);
      for (const auto& [row_idx, offset]: row_idx_offsets) {
        to_col_data[row_idx] |= (1ul << offset);
      }
    }
    if (smartid_info->to_col_type == SqlType::Int64) {
      auto to_col_data = raw_table_block->MutableColDataAs<uint64_t>(smartid_info->to_col_idx);
      for (const auto& [row_idx, offset]: row_idx_offsets) {
        to_col_data[row_idx] |= (1ull << offset);
      }
    }

    to_table->BM()->Unpin(block_info, true); // Set to true after debugging
  }
}


void SmartIDOptimizer::BuildSmartIDs(Catalog *catalog) {
  auto workload = catalog->Workload();
  // Read embedding infos.
  auto opt_file = fmt::format("{}/opts/embeddings_opt_{}.csv", workload->data_folder, workload->embedding_size);
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
        smartid_info->to_col_type = col.Type();
        break;
      }
      if (smartid_info->to_table_name != workload->central_table_name && col.IsFK()) {
        smartid_info->to_col_name = col.Name();
        smartid_info->to_col_idx = col_idx;
        smartid_info->to_col_type = col.Type();
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
  std::string smartids_build_times(fmt::format("{}/smartids_build_times.csv", workload->data_folder));
  std::ofstream os(smartids_build_times);

  auto start = std::chrono::high_resolution_clock::now();
  if (workload->available_idxs.empty()) {
    // Rebuild indexes to speedup updates.
    Indexes::BuildAllKeyIndexes(catalog);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
  auto duration_sec = double(duration) / double(1e9);
  os << fmt::format("{},{},{}\n", "smartids", "index", duration_sec);


  // Read smart id query to see how to join any pair of tables.
  std::string mat_views_toml_file(fmt::format("{}/smartid_queries.toml", workload->data_folder));
  ExecutionFactory factory(catalog);
  QueryReader::ReadWorkloadQueries(catalog, workload, &factory, {mat_views_toml_file});
  ToPhysical::FindBestJoinOrders(catalog, &factory, FreqType::TOP);
  start = std::chrono::high_resolution_clock::now();
  for (auto& smartid_info: workload->smartid_infos) {
    ExecutionFactory execution_factory(catalog);
    ExecutionContext exec_ctx;
    std::string from_col_fullname = (smartid_info->from_table_name + "." + smartid_info->from_col_name);
    std::string key_col_fullname = (smartid_info->from_table_name + "." + smartid_info->from_key_col_name);

    auto query_name = fmt::format("smart_id_{}___{}___{}", smartid_info->from_table_name, smartid_info->from_col_name, smartid_info->to_table_name);
    smartid_info->smartid_query = workload->query_infos.at(query_name).get();
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
  stop = std::chrono::high_resolution_clock::now();
  duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
  duration_sec = double(duration) / double(1e9);
  os << fmt::format("{},{},{}\n", "smartids", "embeddings", duration_sec);
}

int64_t MakeEmbedding(Catalog* catalog, LogicalScanNode* logical_scan, SmartIDInfo* smartid_info) {
  auto from_hist = catalog->GetTable(smartid_info->from_table_name)->GetStatistics()->column_stats.at(smartid_info->from_col_idx)->hist_.get();
  uint64_t ones = ~(0ull);
  uint64_t max_bits = smartid_info->to_col_type == SqlType::Int64 ? 64 : 32;
  uint64_t curr_embedding = 0;
  for (const auto& filter: logical_scan->filters) {
    Vector vec(filter.col_type);
    Vector offsets(SqlType::Int64);
    uint64_t lo_offset, hi_offset;
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

    uint64_t lo_mask = ~(ones << lo_offset);
    uint64_t hi_mask = hi_offset == max_bits ? ones : ~(ones << hi_offset);
    curr_embedding |= (hi_mask ^ lo_mask);
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


struct ScanStat {
  std::string table_name;
  uint64_t in;
  uint64_t out;
  double scan_time;

  void ToString(std::ostream& os) const {
    os << fmt::format("ScanStat(name={}, in={}, out={}, time={})\n", table_name, in, out, scan_time);
  }
};

struct JoinStat {
  uint64_t probe_in;
  uint64_t join_out;
  double build_time;
  double probe_time;

  void ToString(std::ostream& os) const {
    os << fmt::format("JoinStat(probe_in={}, out={}, build_time={}, probe_time={})\n", probe_in, join_out, build_time, probe_time);
  }
};

using ScanStats = std::map<std::string, ScanStat>;
using JoinStats = std::vector<JoinStat>;

void MakeStats(ScanStats& scan_stats, JoinStats& join_stats, PlanExecutor* executor) {
  auto scan_exec = dynamic_cast<ScanExecutor*>(executor);
  if (scan_exec != nullptr) {
    auto table_name = scan_exec->scan_node_->GetTable()->Name();
    ScanStat s{};
    s.table_name = table_name;
    s.in = scan_exec->scan_in;
    s.out = scan_exec->scan_out;
    s.scan_time = scan_exec->scan_time / double(1e9);
    scan_stats[table_name] = s;
    return;
  }
  auto join_exec = dynamic_cast<HashJoinExecutor*>(executor);
  if (join_exec != nullptr) {
    MakeStats(scan_stats, join_stats, executor->Child(0));
    MakeStats(scan_stats, join_stats, executor->Child(1));
    JoinStat s{};
    s.probe_in = join_exec->probe_in;
    s.join_out = join_exec->join_out;
    s.probe_time = join_exec->probe_time / double(1e9);
    s.build_time = join_exec->build_time / double(1e9);
    join_stats.emplace_back(s);
    return;
  }
  auto s_join_exec = dynamic_cast<ScalarHashJoinExecutor*>(executor);
  if (s_join_exec != nullptr) {
    MakeStats(scan_stats, join_stats, executor->Child(0));
    MakeStats(scan_stats, join_stats, executor->Child(1));
    JoinStat s{};
    s.probe_in = s_join_exec->probe_in;
    s.join_out = s_join_exec->join_out;
    s.probe_time = s_join_exec->probe_time / double(1e9);
    s.build_time = s_join_exec->build_time / double(1e9);
    join_stats.emplace_back(s);
    return;
  }

  MakeStats(scan_stats, join_stats, executor->Child(0));
}


std::tuple<JoinStats, ScanStats, double> RunWithStats(Catalog* catalog, const std::string& q_name, bool with_smartids, bool with_sip=false, int num_runs=10) {
  auto workload = catalog->Workload();
  auto query = workload->query_infos.at("join1").get();
  auto logical_join = query->best_join_order;
  ExecutionContext exec_ctx;
  ExecutionFactory execution_factory(catalog);
  PlanNode* physical_plan;
  if (with_smartids) {
    physical_plan = SmartIDOptimizer::GenerateBestPlanWithSmartIDs(catalog, query, &execution_factory, &exec_ctx);
  } else {
    physical_plan = ToPhysical::MakePhysicalJoin(catalog, workload, logical_join, &exec_ctx, &execution_factory, with_sip);
  }
  if (query->count) {
    physical_plan = execution_factory.MakeStaticAggregation(physical_plan, {
        {0, AggType::COUNT},
    });
  }
  auto printer = execution_factory.MakePrint(physical_plan, {});
  {
    std::cout << "Warming execution" << std::endl;
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    executor->Next();
  }
  JoinStats acc_join_stats;
  ScanStats acc_scan_stats;
  double sum_duration_sec{0};
  for (int i = 0; i < num_runs; i++) {
    std::cout << "Warm execution" << std::endl;
    auto executor = ExecutionFactory::MakePlanExecutor(printer, &exec_ctx);
    auto first_start = std::chrono::high_resolution_clock::now();
    executor->Next();
    auto first_stop = std::chrono::high_resolution_clock::now();
    auto first_duration = duration_cast<std::chrono::nanoseconds>(first_stop - first_start).count();
    sum_duration_sec += double(first_duration) / double(1e9);
    JoinStats join_stats;
    ScanStats scan_stats;
    MakeStats(scan_stats, join_stats, executor.get());
    if (acc_join_stats.empty()) {
      acc_join_stats = join_stats;
      acc_scan_stats = scan_stats;
    } else {
      for (const auto& [table_name, s]: scan_stats) {
//        s.ToString(std::cout);
        acc_scan_stats[table_name].scan_time += s.scan_time;
      }
      acc_join_stats[0].build_time += join_stats[0].build_time;
      acc_join_stats[0].probe_time += join_stats[0].probe_time;
    }
  }
  for (const auto& [table_name, _]: acc_scan_stats) {
    acc_scan_stats[table_name].scan_time /= num_runs;
  }
  acc_join_stats[0].build_time /= num_runs;
  acc_join_stats[0].probe_time /= num_runs;


  for (const auto& [_, s]: acc_scan_stats) {
    s.ToString(std::cout);
  }
  for (const auto& s: acc_join_stats) {
    s.ToString(std::cout);
  }

  return {acc_join_stats, acc_scan_stats, sum_duration_sec / num_runs};
}

void SmartIDOptimizer::DoMotivationExpts(Catalog* catalog) {
  std::vector<std::pair<bool, bool>> runs = {
       {true, false}, {false, false}, {false, true},
  };
  auto logical_join = catalog->Workload()->query_infos.at("join1").get()->best_join_order;
  auto left_size = logical_join->left_scan->estimated_output_size;
  auto right_size = logical_join->right_scan->estimated_output_size;
  std::string outfile = fmt::format("{}/motivation_results.csv", catalog->Workload()->data_folder);
  std::ofstream results_os(outfile);
  for (bool is_good: {false, true}) {
    if (is_good) {
      logical_join->left_scan->estimated_output_size = left_size;
      logical_join->right_scan->estimated_output_size = right_size;
    } else {
      logical_join->left_scan->estimated_output_size = right_size;
      logical_join->right_scan->estimated_output_size = left_size;
    }
    for (const auto& [with_smartids, with_sip]: runs) {
      std::string expt_name;
      if (with_smartids) {
        expt_name = "SmartIDs";
      } else if (with_sip) {
        expt_name = "SIP";
      } else {
        expt_name = "Vanilla";
      }
      std::string goodness = is_good ? "Good" : "Bad";
      auto [join_stats, scan_stats, total_rt] = RunWithStats(catalog, "join1", with_smartids, with_sip, 100);
      double remaining_time{total_rt};
      for (const auto& [_, s]: scan_stats) {
        results_os << fmt::format("{},{},{} Scan,{}\n", expt_name, goodness, s.table_name, s.scan_time);
        remaining_time -= s.scan_time;
      }
      results_os << fmt::format("{},{},Build,{}\n", expt_name, goodness, join_stats[0].build_time);
      remaining_time -= join_stats[0].build_time;
      results_os << fmt::format("{},{},Probe,{}\n", expt_name, goodness, join_stats[0].probe_time);
      remaining_time -= join_stats[0].probe_time;
//      results_os << fmt::format("{},{},Other,{}\n", expt_name, goodness, remaining_time);
    }
  }
  // Restore just in case.
  logical_join->left_scan->estimated_output_size = left_size;
  logical_join->right_scan->estimated_output_size = right_size;
}

void SmartIDOptimizer::DoUpdateExpts(Catalog *catalog) {
  // Run query with
  // Build Index on A and B.
  // Update A at ids [lo, hi] to set filter_col = 0.
  // Update corresponding rows in B by settings
  // Should be easy.
  auto workload = catalog->Workload();
  if (workload->available_idxs.empty()) {
    // Rebuild indexes to speedup updates.
    Indexes::BuildAllKeyIndexes(catalog);
  }
  std::ofstream update_os(workload->data_folder + "/update_results.csv");
  uint64_t log_block_size = Settings::Instance()->LogBlockSize();
  uint64_t ones = ~(0ull);
  uint64_t row_id_mask = ~(ones << log_block_size);
  auto A_table = catalog->GetTable("A");
  auto A_index = catalog->GetIndex("A");
  auto B_table = catalog->GetTable("B");
  auto B_index = catalog->GetIndex("B");
  uint64_t A_size = A_table->GetStatistics()->num_tuples;
  uint64_t updates_per_round = (1 << 15);
  double A_update_duration{0};
  double B_update_duration{0};
  uint64_t max_num_rounds = A_size / updates_per_round;
  uint64_t curr_num_rounds = 0;
  for (uint64_t i = 0; i < A_size; i += updates_per_round) {
    if (curr_num_rounds == max_num_rounds) return;
    curr_num_rounds++;
    auto start = std::chrono::high_resolution_clock::now();
    std::unordered_map<int64_t, std::vector<uint64_t>> A_rows;
    for (uint64_t id = i; id < i+updates_per_round; id++) {
      if (id%512 <= 31) continue;
      auto row_id = A_index->GetIndex64()->at(id & RowIDIndex::KEY_MASK64).at(0);
      auto block_id = row_id >> log_block_size;
      auto row_idx = row_id & row_id_mask;
      A_rows[block_id].emplace_back(row_idx);
    }
    for (const auto& [block_id, row_idxs]: A_rows) {
      auto block_info = A_table->BM()->Pin(block_id);
      auto raw_table_block = TableBlock::FromBlockInfo(block_info);
      auto filter_col_data = raw_table_block->MutableColDataAs<int32_t>(1);
      for (const auto& row_idx: row_idxs) {
        filter_col_data[row_idx] = 0;
      }
      A_table->BM()->Unpin(block_info, false); // Set dirty to false to be able to easily rerun expts.
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
    start = std::chrono::high_resolution_clock::now();
    A_update_duration += double(duration) / double(1e9);
    std::unordered_map<int64_t, std::vector<uint64_t>> B_rows;
    for (uint64_t id = i; id < i+updates_per_round; id++) {
      if (id%512 <= 31) continue;
      auto& row_ids = B_index->GetIndex64()->at(id & RowIDIndex::KEY_MASK64);
      for (const auto& row_id: row_ids) {
        auto block_id = row_id >> log_block_size;
        auto row_idx = row_id & row_id_mask;
        B_rows[block_id].emplace_back(row_idx);
      }
    }
    for (const auto& [block_id, row_idxs]: B_rows) {
      auto block_info = B_table->BM()->Pin(block_id);
      auto raw_table_block = TableBlock::FromBlockInfo(block_info);
      auto fk_col_data = raw_table_block->MutableColDataAs<int64_t>(1);
      for (const auto& row_idx: row_idxs) {
        fk_col_data[row_idx] = 1ull << 48;
      }
      B_table->BM()->Unpin(block_info, false); // Set dirty to false to be able to easily rerun expts.
    }
    stop = std::chrono::high_resolution_clock::now();
    duration = duration_cast<std::chrono::nanoseconds>(stop - start).count();
    B_update_duration += double(duration) / double(1e9);
    auto [default_join_stats, default_scan_stats, default_rt] = RunWithStats(catalog, "join1", false, false);
    auto [smartids_join_stats, smartids_scan_stats, smartids_rt] = RunWithStats(catalog, "join1", true, false);
    double fpr = 100.0 * ((double(smartids_scan_stats["B"].out) - smartids_join_stats[0].join_out) / double(smartids_scan_stats["B"].out));
    double prefilter_rate = 100.0 * ((double(smartids_scan_stats["B"].in) - smartids_scan_stats["B"].out) / double(smartids_scan_stats["B"].in));
    update_os << fmt::format("updates,{},{},{},{},{},{}\n", fpr, prefilter_rate, default_rt, smartids_rt, A_update_duration, B_update_duration);
  }
}

}
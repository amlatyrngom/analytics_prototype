#include "optimizer/to_physical.h"
#include "execution/execution_context.h"
#include "execution/execution_factory.h"
#include "execution/nodes/scan_node.h"
#include "execution/nodes/hash_join_node.h"
#include "execution/nodes/static_aggregation.h"
#include "execution/nodes/rowid_index_join_node.h"
#include "storage/table.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "optimizer/logical_nodes.h"
#include "optimizer/table_statistics.h"
#include "optimizer/histogram.h"
#include "common/catalog.h"
#include "optimizer/workload_metadata.h"
#include <algorithm>
#include <random>
#include <fmt/core.h>
#include <fmt/ranges.h>

namespace smartid {

std::vector<Value> SampleVals(ColumnStats* column_stats, int num_vals) {
  std::vector<Value> vals;
  uint64_t total_samples = column_stats->value_samples_.size();
  ASSERT(num_vals < total_samples, "Sampling more than available not yet implemented!");
  // Get random index.
  std::vector<uint64_t> rand_vec;
  for (uint64_t i = 0; i < total_samples; i++) rand_vec.emplace_back(i);
  std::shuffle(rand_vec.begin(), rand_vec.end(), std::default_random_engine{37});
  for (int i = 0; i < num_vals; i++) {
     vals.emplace_back(column_stats->value_samples_[rand_vec[i]]);
  }
  return vals;
}


void ToPhysical::BindParams(Table* table, LogicalFilter *filter, ExecutionContext *exec_ctx) {
  if (filter->should_sample) {
    auto table_stats = table->GetStatistics();
    auto col_stats = table_stats->column_stats[filter->col_idx].get();
    if (filter->expr_type == ExprType::BinaryComp) {
      // Sample single val.
      filter->vals = SampleVals(col_stats, 1);
    } else if (filter->expr_type == ExprType::Between) {
      // Sample single val and add range.
      filter->vals = SampleVals(col_stats, 1);
      filter->vals.emplace_back(ValueUtil::Add(filter->vals[0], filter->range, filter->col_type));
    } else {
      ASSERT(filter->expr_type == ExprType::InList, "Expr type not supported for sampling!");
      // Sample num_ins vals
      auto num_ins = filter->vals.size();
      filter->vals = SampleVals(col_stats, num_ins);
    }
    filter->should_sample = false;
  }
  for (uint64_t i = 0; i < filter->vals.size(); i++) {
    auto param_name = filter->param_name + std::to_string(i);
    exec_ctx->AddParam(param_name, filter->vals[i], filter->col_type);
  }
}

void ToPhysical::EstimateFilterSelectivity(Table *table,
                                     LogicalFilter *filter) {
  auto table_stats = table->GetStatistics();
  auto col_stats = table_stats->column_stats[filter->col_idx].get();
  auto hist = col_stats->hist_.get();
  std::vector<int64_t> part_idxs;
  for (const auto& val: filter->vals) {
    part_idxs.emplace_back(hist->PartIdx(val));
  }
  const auto& part_cards = hist->GetPartCards();
  double estimated_output = 0;
  if (filter->expr_type == ExprType::InList) {
    // Sum
    for (const auto& p: part_idxs) {
      if (p >= 0 && p < part_cards.size()) {
        estimated_output += part_cards[p];
      }
    }
  }
  if (filter->expr_type == ExprType::Between) {
    // Range sum.
    auto lo = part_idxs[0];
    auto hi = part_idxs[1];
    for (int64_t p = lo; p <= hi; p++) {
      if (p >= 0 && p < part_cards.size()) {
        estimated_output += part_cards[p];
      }
    }
  }
  if (filter->expr_type == ExprType::BinaryComp) {
    // Range sum depending on op
    int64_t lo, hi;
    if (filter->op_type == OpType::LT || filter->op_type == OpType::LE) {
      lo = 0;
      hi = part_idxs[0];
    } else if (filter->op_type == OpType::GT || filter->op_type == OpType::GE) {
      lo = part_idxs[0];
      hi = int64_t(part_cards.size()) - 1;
    } else if (filter->op_type == OpType::EQ) {
      lo = part_idxs[0];
      hi = part_idxs[0];
    } else {
      lo = 0;
      hi = int64_t(part_cards.size()) - 1;
      // Comment this out if this is not a bug.
      ASSERT(false, "NE comparison not supported for instagrams!!!");
    }
    for (auto p = lo; p <= hi; p++) {
      estimated_output += part_cards[p];
    }
  }
  auto sel = estimated_output / table_stats->num_tuples;
  filter->estimated_sel = sel;
}

void EstimateScanSel(Table* table, LogicalScanNode* logical_scan, ScanNode* sample_scan, ExecutionContext* exec_ctx) {
  auto table_stats = table->GetStatistics();
  uint64_t num_output = 0;
  auto executor = ExecutionFactory::MakePlanExecutor(sample_scan, exec_ctx);
  const VectorProjection* vp;
  while ((vp = executor->Next()) != nullptr) {
    num_output += vp->GetFilter()->NumOnes();
  }
  std::cout << "Found Num Outputs: " << num_output << std::endl;
  double sel;
  if (num_output > 0) {
    sel = double(num_output) / table_stats->sample_size;
  } else {
    // Assume independent filter when sampling does not help;
    sel = 1.0;
    for (const auto& f: logical_scan->filters) {
      sel *= f.estimated_sel;
    }
  }
  logical_scan->base_table_size = table_stats->num_tuples;
  logical_scan->estimated_output_size = sel * table_stats->num_tuples;
}

void ToPhysical::EstimateScanSelectivities(Catalog *catalog, QueryInfo *query_info) {
  ExecutionContext exec_ctx;
  ExecutionFactory execution_factory(catalog);
  for (auto& logical_scan: query_info->scans) {
    auto table = catalog->GetTable(logical_scan->table_info->name);
    auto sample_table = catalog->GetTable(Catalog::SampleName(logical_scan->table_info->name));
    auto cols_to_read = logical_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_scan->filters) {
      ToPhysical::BindParams(table, &logical_filter, &exec_ctx);
      ToPhysical::EstimateFilterSelectivity(table, &logical_filter);
      filters.emplace_back(logical_filter.expr_node);
    }
    auto sample_scan = execution_factory.MakeScan(sample_table, std::move(cols_to_read), std::move(projs), std::move(filters));
    EstimateScanSel(table, logical_scan, sample_scan, &exec_ctx);
  }
}

void ResolveScan(LogicalScanNode* scan_node, const std::vector<uint64_t>& join_keys, ExecutionFactory* factory) {
  // Mark existing projections as outputs.
  fmt::print("Scan: projections={}\n", scan_node->projections.size());
  for (uint64_t i = 0; i < scan_node->projections.size(); i++) {
    scan_node->proj_idx_should_output[i] = true;
  }
  auto table_name = scan_node->table_info->name;
  for (const auto& join_key: join_keys) {
    uint64_t read_idx, proj_idx;
    auto col_name = scan_node->table_info->schema.GetColumn(join_key).Name();
    if (scan_node->col_idx_read_idxs.contains(join_key)) {
      read_idx = scan_node->col_idx_read_idxs.at(join_key);
    } else {
      read_idx = scan_node->cols_to_read.size();
      scan_node->cols_to_read.emplace_back(join_key);
      scan_node->col_idx_read_idxs[join_key] = read_idx;
    }
    if (scan_node->col_idx_proj_idxs.contains(join_key)) {
      proj_idx = scan_node->col_idx_proj_idxs.at(join_key);
    } else {
      // Make projection
      proj_idx = scan_node->projections.size();
      scan_node->projections.emplace_back(factory->MakeCol(read_idx));
      scan_node->proj_idx_full_name[proj_idx] = (table_name + ".") + col_name;
      // Mark as join key that is not part of final output.
      scan_node->proj_idx_should_output[proj_idx] = false;
      scan_node->col_idx_proj_idxs[join_key] = proj_idx;
    }
    scan_node->join_key_proj_idx[join_key] = proj_idx;
    scan_node->proj_idx_join_keys[proj_idx] = join_key;
    scan_node->table_info->used_cols.emplace(join_key);
  }
}

void ResolveScans(WorkloadInfo* workload, LogicalJoinNode* join_node, const std::unordered_map<std::string, std::vector<uint64_t>>& join_keys_map, ExecutionFactory* factory) {
  // Resolve right.
  {
    auto right_scan = join_node->right_scan;
    auto right_table_info = right_scan->table_info;
    ResolveScan(right_scan, join_keys_map.at(right_table_info->name), factory);
  }

  // If left is scan, resolve left.
  if (join_node->left_scan != nullptr) {
    auto left_scan = join_node->left_scan;
    auto left_table_info = left_scan->table_info;
    ResolveScan(left_scan, join_keys_map.at(left_table_info->name), factory);
  }
}


// WARNING: Ugly code.
void RecursiveResolveJoin(WorkloadInfo* workload, LogicalJoinNode* join_node, const std::unordered_map<std::string, std::vector<uint64_t>>& join_keys_map, ExecutionFactory* factory) {
  // First resolve joins.
  ResolveScans(workload, join_node, join_keys_map, factory);
  // Central info.
  const auto& all_central_keys = join_keys_map.at(workload->central_table_name);
  std::unordered_set<uint64_t> all_central_keys_set(all_central_keys.begin(), all_central_keys.end());
  auto central_table_info = workload->table_infos.at(workload->central_table_name).get();
  uint64_t central_pk_idx = 0;
  for (uint64_t i = 0; i < central_table_info->schema.NumCols(); i++) {
    if (central_table_info->schema.GetColumn(i).IsPK()) {
      central_pk_idx = i;
      break;
    }
  }
  bool is_right_central = join_node->right_scan->table_info->name == workload->central_table_name;
  // Probe side info
  std::unordered_map<uint64_t, bool>* probe_proj_idx_should_output;
  std::unordered_map<uint64_t, uint64_t>* probe_proj_idx_join_keys;
  std::unordered_map<uint64_t, std::string>* probe_proj_idx_full_name;
  // Build side info
  std::unordered_map<uint64_t, bool>* build_proj_idx_should_output;
  std::unordered_map<uint64_t, uint64_t>* build_proj_idx_join_keys;
  std::unordered_map<uint64_t, std::string>* build_proj_idx_full_name;

  // Collect build info.
  uint64_t build_key_col_idx;
  if (is_right_central) {
    uint64_t probe_key_col_idx = join_keys_map.at(join_node->left_scan->table_info->name).at(0);// Get indirectly from left_scan;
    auto build_col_name = join_node->left_scan->table_info->schema.GetColumn(probe_key_col_idx).FKName(); // Get foreign name of left col.
    build_key_col_idx = join_node->right_scan->table_info->schema.ColIdx(build_col_name);
  } else {
    build_key_col_idx = join_keys_map.at(join_node->right_scan->table_info->name).at(0);// Get directly from right_scan;
  }
  join_node->build_key_idx = join_node->right_scan->join_key_proj_idx.at(build_key_col_idx);
  build_proj_idx_should_output = &join_node->right_scan->proj_idx_should_output;
  build_proj_idx_join_keys = &join_node->right_scan->proj_idx_join_keys;
  build_proj_idx_full_name = &join_node->right_scan->proj_idx_full_name;
  fmt::print("Build should output: names={}, should_output={}\n", *build_proj_idx_full_name, *build_proj_idx_should_output);

  // Collect probe side info.
  uint64_t probe_key_col_idx;
  if (join_node->left_scan != nullptr) {
    if (is_right_central) {
      probe_key_col_idx = join_keys_map.at(join_node->left_scan->table_info->name).at(0);// Get from left_scan;
    } else {
      auto probe_col_name = join_node->right_scan->table_info->schema.GetColumn(build_key_col_idx).FKName(); // Get foreign name of right col.
      probe_key_col_idx = central_table_info->schema.ColIdx(probe_col_name);
    }
    join_node->probe_key_idx = join_node->left_scan->join_key_proj_idx.at(probe_key_col_idx);
    probe_proj_idx_should_output = &join_node->left_scan->proj_idx_should_output;
    probe_proj_idx_join_keys = &join_node->left_scan->proj_idx_join_keys;
    probe_proj_idx_full_name = &join_node->left_scan->proj_idx_full_name;
  } else {
    ASSERT(!is_right_central, "Central table must already be joined");
    RecursiveResolveJoin(workload, join_node->left_join, join_keys_map, factory);
    auto probe_col_name = join_node->right_scan->table_info->schema.GetColumn(build_key_col_idx).FKName(); // Get foreign name of right col.
    probe_key_col_idx = central_table_info->schema.ColIdx(probe_col_name);
    join_node->probe_key_idx = join_node->left_join->join_key_proj_idx.at(probe_key_col_idx);
    probe_proj_idx_should_output = &join_node->left_join->proj_idx_should_output;
    probe_proj_idx_join_keys = &join_node->left_join->proj_idx_join_keys;
    probe_proj_idx_full_name = &join_node->left_join->proj_idx_full_name;
  }
  fmt::print("Probe should output: names={}, should_output={}\n", *probe_proj_idx_full_name, *probe_proj_idx_should_output);

  // Build side.
  for (const auto& [build_proj_idx, should_output]: *build_proj_idx_should_output) {
    auto is_later_key = (build_proj_idx != join_node->build_key_idx) && build_proj_idx_join_keys->contains(build_proj_idx);
    if (should_output || is_later_key) {
      auto next_join_proj_idx = join_node->projections.size();
      join_node->projections.emplace_back(0, build_proj_idx); // 0 for build side.
      fmt::print("Adding build: next_proj_idx={}, build_proj_idx={}\n", next_join_proj_idx, build_proj_idx);
      join_node->proj_idx_should_output[next_join_proj_idx] = should_output;
      join_node->proj_idx_full_name[next_join_proj_idx] = build_proj_idx_full_name->at(build_proj_idx);
      if (is_later_key) {
        auto col_idx = build_proj_idx_join_keys->at(build_proj_idx);
        join_node->proj_idx_join_keys[next_join_proj_idx] = col_idx;
        join_node->join_key_proj_idx[col_idx] = next_join_proj_idx;
      }
    }
  }

  for (const auto& [probe_proj_idx, should_output]: *probe_proj_idx_should_output) {
    auto is_later_key = (probe_proj_idx != join_node->probe_key_idx) && probe_proj_idx_join_keys->contains(probe_proj_idx);
    bool is_central_pk = probe_proj_idx_join_keys->contains(probe_proj_idx) && (central_pk_idx == probe_proj_idx_join_keys->at(probe_proj_idx));
    if (should_output || is_later_key || is_central_pk) {
      auto next_join_proj_idx = join_node->projections.size();
      join_node->projections.emplace_back(1, probe_proj_idx); // 1 for probe side.
      join_node->proj_idx_should_output[next_join_proj_idx] = should_output;
      join_node->proj_idx_full_name[next_join_proj_idx] = probe_proj_idx_full_name->at(probe_proj_idx);
      if (is_later_key || is_central_pk) {
        auto col_idx = probe_proj_idx_join_keys->at(probe_proj_idx);
        join_node->proj_idx_join_keys[next_join_proj_idx] = col_idx;
        join_node->join_key_proj_idx[col_idx] = next_join_proj_idx;
      }
    }
  }

}

void ToPhysical::ResolveJoin(Catalog* catalog, WorkloadInfo* workload, LogicalJoinNode* logical_join, ExecutionFactory* factory) {
  // Find all tables to read;
  auto central_table_name = workload->central_table_name;
  auto central_table_info = workload->table_infos.at(central_table_name).get();
  // For compute the join keys that should be read.
  std::unordered_map<std::string, std::vector<uint64_t>> join_keys_map;
  for (const auto& table_info: logical_join->scanned_tables) {
    if (table_info->name != central_table_name) {
      std::string fk_name;
      uint64_t col_idx{0};
      for (const auto& col: table_info->schema.Cols()) {
        if (col.IsFK()) {
          fk_name = col.FKName();
          break;
        }
        col_idx++;
      }
      ASSERT(!fk_name.empty(), "Foreign col not found");
      join_keys_map[central_table_name].emplace_back(central_table_info->schema.ColIdx(fk_name));
      join_keys_map[table_info->name].emplace_back(col_idx);
    }
  }
  RecursiveResolveJoin(workload, logical_join, join_keys_map, factory);
}

void ToPhysical::ResolveJoins(Catalog *catalog,
                              WorkloadInfo *workload,
                              QueryInfo *query_info,
                              ExecutionFactory *factory) {
  for (auto& logical_join: query_info->join_orders) {
    ResolveJoin(catalog, workload, logical_join, factory);
  }
}


PlanNode* ToPhysical::MakePhysicalJoin(Catalog* catalog, WorkloadInfo* workload, LogicalJoinNode* logical_join, ExecutionContext* exec_ctx, ExecutionFactory *factory, bool with_sip) {
  ScanNode* physical_right_scan;
  double right_size, left_size;
  {
    auto logical_right_scan = logical_join->right_scan;
    right_size = logical_right_scan->estimated_output_size;
    auto right_table = catalog->GetTable(logical_right_scan->table_info->name);
    auto cols_to_read = logical_right_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_right_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_right_scan->filters) {
      ToPhysical::BindParams(right_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    physical_right_scan = factory->MakeScan(right_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  PlanNode* left_node;
  if (logical_join->left_scan != nullptr) {
    auto logical_left_scan = logical_join->left_scan;
    left_size = logical_left_scan->estimated_output_size;
    auto left_table = catalog->GetTable(logical_left_scan->table_info->name);
    auto cols_to_read = logical_left_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_left_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_left_scan->filters) {
      ToPhysical::BindParams(left_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    left_node = factory->MakeScan(left_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  } else {
    left_node = MakePhysicalJoin(catalog, workload, logical_join->left_join, exec_ctx, factory);
    left_size = logical_join->left_join->estimated_output_size;
  }


  bool keep_sides = left_size >= right_size;
  auto build_key_idx = keep_sides ? logical_join->build_key_idx : logical_join->probe_key_idx;
  auto probe_key_idx = keep_sides ? logical_join->probe_key_idx : logical_join->build_key_idx;
  auto join_projs = logical_join->projections;
  if (!keep_sides) {
    fmt::print("Flipping sides: {}, {}\n", left_size, right_size);
    for (uint64_t i = 0; i < join_projs.size(); i++) {
      const auto& [side, side_idx] = join_projs[i];
      join_projs[i] = {1-side, side_idx};
    }
  }
  PlanNode* build_side = keep_sides ? physical_right_scan : left_node;
  PlanNode* probe_side = keep_sides ? left_node : physical_right_scan;


  std::vector<uint64_t> build_keys{build_key_idx};
  std::vector<uint64_t> probe_keys{probe_key_idx};
  auto hash_join = factory->MakeHashJoin(build_side, probe_side, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
  if (with_sip) {
    hash_join->UseBloomFilter(std::ceil(std::min(left_size, right_size)));
  }
  return hash_join;
}


void EstimateJoinCardinality(Catalog* catalog, WorkloadInfo* workload, LogicalJoinNode* logical_join_node, FreqType freq_type) {
  double right_freq;
  double right_output_size;
  double right_denorm_size;
  double left_freq;
  double left_output_size;
  double left_denorm_size;
  bool is_right_to_many{false};
  double fk_freq{-1}; // For pk, fk joins, how often key repeats.
  double central_table_size = catalog->GetTable(workload->central_table_name)->GetStatistics()->num_tuples;
  {
    auto right_scan = logical_join_node->right_scan;
    is_right_to_many = right_scan->table_info->to_many;
    auto right_table = catalog->GetTable(right_scan->table_info->name);
    auto right_table_stats = right_table->GetStatistics();
    right_output_size = right_scan->estimated_output_size;
    auto build_key_proj_idx = logical_join_node->build_key_idx;
    auto build_key_col_idx = right_scan->proj_idx_join_keys[build_key_proj_idx];
    if (freq_type == FreqType::AVG) {
      right_freq = double(right_table_stats->column_stats.at(build_key_col_idx)->count_) / double(right_table_stats->column_stats.at(build_key_col_idx)->count_distinct_);
    } else if (freq_type == FreqType::MAX) {
      right_freq = right_table_stats->column_stats.at(build_key_col_idx)->max_freq_;
    } else {
      right_freq = right_table_stats->column_stats.at(build_key_col_idx)->top_freq_;
    }
    right_denorm_size = right_scan->base_table_size;
    fmt::print("Right Scan {}: base_size={}, freq={}, output_size={}\n", right_table->Name(), right_denorm_size, right_freq, right_output_size);
    if (!is_right_to_many) {
      auto central_table = catalog->GetTable(workload->central_table_name);
      auto central_table_stats = central_table->GetStatistics();
      const auto& build_key_col = right_table->GetSchema().GetColumn(build_key_col_idx);
      const auto& fk_name = build_key_col.FKName();
      const auto& fk_col_idx = central_table->GetSchema().ColIdx(fk_name);
      if (freq_type == FreqType::AVG) {
        fk_freq = double(central_table_stats->column_stats.at(fk_col_idx)->count_) / double(central_table_stats->column_stats.at(fk_col_idx)->count_distinct_);
      } else if (freq_type == FreqType::MAX) {
        fk_freq = double(central_table_stats->column_stats.at(fk_col_idx)->max_freq_);
      } else {
        fk_freq = double(central_table_stats->column_stats.at(fk_col_idx)->top_freq_);
      }
    }
  }
  if (logical_join_node->left_scan != nullptr) {
    auto left_scan = logical_join_node->left_scan;
    auto left_table = catalog->GetTable(left_scan->table_info->name);
    auto left_table_stats = left_table->GetStatistics();
    left_output_size = left_scan->estimated_output_size;
    auto probe_key_proj_idx = logical_join_node->probe_key_idx;
    auto probe_key_col_idx = left_scan->proj_idx_join_keys[probe_key_proj_idx];
    if (freq_type == FreqType::AVG) {
      left_freq = double(left_table_stats->column_stats.at(probe_key_col_idx)->count_) / double(left_table_stats->column_stats.at(probe_key_col_idx)->count_distinct_);
    } else if (freq_type == FreqType::MAX) {
      left_freq = left_table_stats->column_stats.at(probe_key_col_idx)->max_freq_;
    } else {
      left_freq = left_table_stats->column_stats.at(probe_key_col_idx)->top_freq_;
    }
    left_denorm_size = left_scan->base_table_size;
    fmt::print("Left Scan {}: base_size={}, freq={}, output_size={}\n", left_table->Name(), left_scan->base_table_size, left_freq, left_output_size);
  } else {
    EstimateJoinCardinality(catalog, workload, logical_join_node->left_join, freq_type);
    left_freq = logical_join_node->left_join->estimated_freq;
    left_output_size = logical_join_node->left_join->estimated_output_size;
    left_denorm_size = logical_join_node->left_join->estimated_denorm_size;
    fmt::print("Left Join: base_size={}, freq={}, output_size={}\n", left_denorm_size, left_freq, left_output_size);
  }
  if (!is_right_to_many) {
    // Pk-fk join.
    // Only start making max_freq product with many-many joins.
    logical_join_node->estimated_freq = 1;
    logical_join_node->estimated_output_size = std::min(left_output_size, right_output_size * fk_freq);
    logical_join_node->estimated_denorm_size = left_denorm_size;
  } else {
    // Fk-fk join. Start accumulating max_frequencies.
    logical_join_node->estimated_freq = (left_freq * right_freq);
    logical_join_node->estimated_output_size = std::min(left_output_size / left_freq, right_output_size / right_freq) * logical_join_node->estimated_freq;
    logical_join_node->estimated_denorm_size = std::min(left_denorm_size / left_freq, right_denorm_size / right_freq) * logical_join_node->estimated_freq;
  }
  if (logical_join_node->left_scan != nullptr) {
    // Current build and output.
    logical_join_node->total_materialized_size = right_output_size + logical_join_node->estimated_output_size;
  } else {
    // Previous materializations, current build and output
    logical_join_node->total_materialized_size = logical_join_node->left_join->total_materialized_size + right_output_size + logical_join_node->estimated_output_size;
  }
  fmt::print("Join: base_size={}, freq={}, output_size={}, mat_size={}\n", logical_join_node->estimated_denorm_size, logical_join_node->estimated_freq, logical_join_node->estimated_output_size, logical_join_node->total_materialized_size);
}

void ToPhysical::EstimateJoinCardinalities(Catalog *catalog, WorkloadInfo* workload, QueryInfo *query_info, FreqType freq_type) {
  for (auto& logical_join: query_info->join_orders) {
    std::vector<std::string> scanned_table_names;
    for (const auto& s: logical_join->scanned_tables) scanned_table_names.emplace_back(s->name);
    EstimateJoinCardinality(catalog, workload, logical_join, freq_type);
  }
}

void ToPhysical::FindBestJoinOrder(QueryInfo *query_info) {
  LogicalJoinNode* best_join{nullptr};
  double curr_best_materializations{std::numeric_limits<double>::max()};
  for (auto& logical_join: query_info->join_orders) {
    if (logical_join->total_materialized_size < curr_best_materializations) {
      curr_best_materializations = logical_join->total_materialized_size;
      best_join = logical_join;
    }
  }
  query_info->best_join_order = best_join;
}

void ToPhysical::FindBestJoinOrders(Catalog *catalog, ExecutionFactory* execution_factory, FreqType freq_type) {
  auto workload = catalog->Workload();
  for (const auto& [query_name, query]: workload->query_infos) {
    if (query->scans.size() <= 1) continue; // Not a join.
    if (query->best_join_order != nullptr) continue; // Already found.
    ToPhysical::EstimateScanSelectivities(catalog, query.get());
    ToPhysical::ResolveJoins(catalog, workload, query.get(), execution_factory);
    ToPhysical::EstimateJoinCardinalities(catalog, workload, query.get(), freq_type);
    ToPhysical::FindBestJoinOrder(query.get());
    fmt::print("Query {} best join:\n", query_name);
    query->best_join_order->ToString(std::cout);
  }
}


void ToPhysical::MakeMaterializedView(Catalog *catalog, QueryInfo *query_info) {
  if (catalog->GetTable(query_info->name) != nullptr) {
    fmt::print("View {} already exists!\n", query_info->name);
//    return;
  }
  auto workload = catalog->Workload();
  ExecutionFactory factory(catalog);
  ExecutionContext exec_ctx;
  auto logical_join = query_info->best_join_order;
  auto physical_join = ToPhysical::MakePhysicalJoin(catalog, workload, logical_join, &exec_ctx, &factory);
  auto executor = ExecutionFactory::MakePlanExecutor(physical_join, &exec_ctx);
  std::vector<Column> mat_view_cols(logical_join->proj_idx_full_name.size());
  for (const auto& [proj_idx, full_name]: logical_join->proj_idx_full_name) {
    auto dot_pos = full_name.find('.');
    std::string table_name = full_name.substr(0, dot_pos);
    std::string col_name = full_name.substr(dot_pos + 1, full_name.size() - (dot_pos + 1));
    fmt::print("Proj: {}, {}\n", table_name, col_name);
    auto table_info = workload->table_infos.at(table_name).get();
    auto col_idx = table_info->schema.ColIdx(col_name);
    const auto& old_col = table_info->schema.GetColumn(col_idx);
    mat_view_cols[proj_idx] = Column(full_name, old_col.Type(), false, false, "", false);
  }
  Schema mat_view_schema(std::move(mat_view_cols));
  auto mat_view_table = catalog->CreateOrClearTable(query_info->name, &mat_view_schema);
  std::vector<SqlType> mat_view_col_types;
  bool contains_varchar = false;
  for (const auto& col: mat_view_table->GetSchema().Cols()) {
    mat_view_col_types.emplace_back(col.Type());
    fmt::print("Demat col={}; type={}\n", col.Name(), TypeUtil::TypeToName(col.Type()));
    if (col.Type() == SqlType::Varchar) contains_varchar = true;
  }
  uint64_t curr_batch = 0;
  uint64_t block_size = Settings::Instance()->BlockSize();
  std::unique_ptr<TableBlock> table_block = nullptr;
  const VectorProjection* vp;

  uint64_t total_write_size = 0;
  while ((vp = executor->Next())) {
    // Optimization to attempt batch write.
    uint64_t batch_write_size = std::min(block_size - curr_batch, vp->GetFilter()->TotalSize());
    batch_write_size = (batch_write_size / 64) * 64; // Closest multiple of 64.
    if (batch_write_size > 0) {
      // Should reset.
      if (curr_batch % block_size == 0) {
        // Insert current batch in table.
        if (curr_batch > 0) {
          // Assume all bits present and no nulls.
          Bitmap::ResetAll(curr_batch, table_block->RawBlock()->MutablePresenceBitmap());
          total_write_size += Bitmap::NumOnes(table_block->RawBlock()->MutablePresenceBitmap(), curr_batch);
          for (uint64_t col_idx = 0; col_idx < mat_view_schema.NumCols(); col_idx++) {
            Bitmap::ResetAll(curr_batch, table_block->RawBlock()->MutableBitmapData(col_idx));
          }
          mat_view_table->InsertTableBlock(std::move(table_block));
        }
        // Reset batch.
        auto col_types_copy = mat_view_col_types;
        table_block = std::make_unique<TableBlock>(std::move(col_types_copy));
        if (contains_varchar) table_block->SetHot();
        curr_batch = 0;
      }

      uint64_t curr_col = 0;
      for (const auto& col: mat_view_schema.Cols()) {
        auto col_type = col.Type();
        auto col_size = TypeUtil::TypeSize(col_type);
        char *write_location = table_block->RawBlock()->MutableColData(curr_col) + col_size * curr_batch;
        const char *read_location = vp->VectorAt(curr_col)->Data();

        // TODO: Add support for varchars. This does not work with them.
        std::memcpy(write_location, read_location, col_size*batch_write_size);
        curr_col++;
      }
      curr_batch += batch_write_size;
    }
    vp->GetFilter()->Map([&](sel_t row_idx) {
      // Should reset.
      if (row_idx < batch_write_size) return; // Only write unwritten rows.
      if (curr_batch % block_size == 0) {
        // Insert current batch in table.
        if (curr_batch > 0) {
          // Assume all bits present and no nulls.
          Bitmap::ResetAll(curr_batch, table_block->RawBlock()->MutablePresenceBitmap());
          total_write_size += Bitmap::NumOnes(table_block->RawBlock()->MutablePresenceBitmap(), curr_batch);
          for (uint64_t col_idx = 0; col_idx < mat_view_schema.NumCols(); col_idx++) {
            Bitmap::ResetAll(curr_batch, table_block->RawBlock()->MutableBitmapData(col_idx));
          }
          mat_view_table->InsertTableBlock(std::move(table_block));
        }
        // Reset batch.
        auto col_types_copy = mat_view_col_types;
        table_block = std::make_unique<TableBlock>(std::move(col_types_copy));
        if (contains_varchar) table_block->SetHot();
        curr_batch = 0;
      }
      uint64_t curr_col = 0;
      for (const auto& col: mat_view_schema.Cols()) {
        auto col_type = col.Type();
        auto col_size = TypeUtil::TypeSize(col_type);
        // Write data. Read from row_idx but write to curr_batch.
        char *write_location = table_block->RawBlock()->MutableColData(curr_col) + col_size * curr_batch;
        const char *read_location = vp->VectorAt(curr_col)->Data() + col_size * row_idx;
        if (col_type == SqlType::Varchar) {
          auto write_location_varlen = reinterpret_cast<Varlen*>(write_location);
          auto read_location_varlen = reinterpret_cast<const Varlen*>(read_location);
          ASSERT(!read_location_varlen->Info().IsCompact(), "VP varlen must be normal!");
          *write_location_varlen = Varlen::MakeNormal(read_location_varlen->Info().NormalSize(), read_location_varlen->Data());
        } else {
          std::memcpy(write_location, read_location, col_size);
        }
        curr_col++;
      }
      curr_batch++;
    });

  }
  // Deal with last batch.
  if (curr_batch != 0) {
    Bitmap::ResetAll(curr_batch, table_block->RawBlock()->MutablePresenceBitmap());
    total_write_size += Bitmap::NumOnes(table_block->RawBlock()->MutablePresenceBitmap(), curr_batch);
    for (uint64_t col_idx = 0; col_idx < mat_view_schema.NumCols(); col_idx++) {
      Bitmap::ResetAll(curr_batch, table_block->RawBlock()->MutableBitmapData(col_idx));
    }
    mat_view_table->InsertTableBlock(std::move(table_block));
  }


  {
    std::vector<uint64_t> cols_to_read{0, 1, 2, 3, 4};
    std::vector<ExprNode*> projections;
    projections.emplace_back(factory.MakeCol(0));
    projections.emplace_back(factory.MakeCol(1));
    projections.emplace_back(factory.MakeCol(2));
    projections.emplace_back(factory.MakeCol(3));
    projections.emplace_back(factory.MakeCol(4));
    std::vector<ExprNode*> filters;
    auto demat_table = catalog->GetTable(query_info->name);
    auto demat_scan = factory.MakeScan(demat_table, std::move(cols_to_read), std::move(projections), std::move(filters));
    auto counter = factory.MakeStaticAggregation(demat_scan, {{0, AggType::COUNT}});
    auto printer = factory.MakePrint(counter, {});
    auto executor = ExecutionFactory::MakePlanExecutor(printer);
    std::cout << "DEMAT SCAN COUNT:" << std::endl;
    executor->Next();
  }
  {
    auto counter = factory.MakeStaticAggregation(physical_join, {{0, AggType::COUNT}});
    auto printer = factory.MakePrint(counter, {});
    auto executor = ExecutionFactory::MakePlanExecutor(printer);
    std::cout << "DEMAT JOIN COUNT:" << std::endl;
    executor->Next();
  }
  {
    std::cout << "TOTAL WRITE SIZE: " << total_write_size << std::endl;
  }
}

struct MatScanNode {
  Table* table{nullptr};
  std::unordered_map<uint64_t, uint64_t> col_idx_read_idxs; // Used to map col idx to read idx.
  std::unordered_map<uint64_t, uint64_t> col_idx_proj_idxs; // Used to map col idx to proj idx.
  std::vector<uint64_t> cols_to_read;
  std::vector<ExprNode*> projections;
  std::vector<std::pair<ExprNode*, double>> filters;
};

void CollectScanForMat(Catalog* catalog, LogicalScanNode* logical_scan, MatScanNode* mat_scan_node, ExecutionFactory* factory, ExecutionContext* exec_ctx) {
  // Inputs
  for (const auto &col_idx: logical_scan->cols_to_read) {
    auto col = logical_scan->table_info->schema.GetColumn(col_idx);
    auto full_name = logical_scan->table_info->name + "." + col.Name();
    auto mat_col_idx = mat_scan_node->table->GetSchema().ColIdx(full_name);
    auto mat_read_idx = mat_scan_node->cols_to_read.size();
    mat_scan_node->cols_to_read.emplace_back(mat_col_idx);
    mat_scan_node->col_idx_read_idxs.emplace(mat_col_idx, mat_read_idx);
  }
  // Filters
  for (auto &filter: logical_scan->filters) {
    ToPhysical::BindParams(catalog->GetTable(logical_scan->table_info->name), &filter, exec_ctx);
    auto filter_col_idx = filter.col_idx;
    auto filter_col = logical_scan->table_info->schema.GetColumn(filter_col_idx);
    auto full_name = logical_scan->table_info->name + "." + filter_col.Name();
    auto mat_col_idx = mat_scan_node->table->GetSchema().ColIdx(full_name);
    auto mat_read_idx = mat_scan_node->col_idx_read_idxs.at(mat_col_idx);
    auto mat_col_expr = factory->MakeCol(mat_read_idx);
    double sel = filter.estimated_sel;

    ExprNode *mat_filter{nullptr};
    if (filter.expr_type == ExprType::BinaryComp) {
      auto filter_expr = dynamic_cast<const BinaryCompNode *>(filter.expr_node);
      auto rhs = dynamic_cast<const ParamNode *>(filter_expr->Child(1));
      auto mat_rhs = factory->MakeParam(rhs->ParamName());
      auto op_type = filter_expr->GetOpType();
      mat_filter = factory->MakeBinaryComp(mat_col_expr, mat_rhs, op_type);
    } else if (filter.expr_type == ExprType::Between) {
      auto filter_expr = dynamic_cast<const BetweenNode *>(filter.expr_node);
      auto left = dynamic_cast<const ParamNode *>(filter_expr->Child(0));
      auto right = dynamic_cast<const ParamNode *>(filter_expr->Child(2));
      auto mat_left = factory->MakeParam(left->ParamName());
      auto mat_right = factory->MakeParam(right->ParamName());
      auto left_closed = filter_expr->LeftClosed();
      auto right_closed = filter_expr->RightClosed();
      mat_filter = factory->MakeBetween(mat_left, mat_col_expr, mat_right, left_closed, right_closed);
    } else {
      ASSERT(false, "Other filter types not yet supported!!!!");
    }
    mat_scan_node->filters.emplace_back(mat_filter, sel);
  }
}

void RecursiveCollectScansForMat(Catalog* catalog, LogicalJoinNode* logical_join, MatScanNode* mat_scan_node, ExecutionFactory* factory, ExecutionContext* exec_ctx) {
  // Right scan
  CollectScanForMat(catalog, logical_join->right_scan, mat_scan_node, factory, exec_ctx);
  if (logical_join->left_scan != nullptr) {
    CollectScanForMat(catalog, logical_join->left_scan, mat_scan_node, factory, exec_ctx);
  } else {
    RecursiveCollectScansForMat(catalog, logical_join->left_join, mat_scan_node, factory, exec_ctx);
  }
}

PlanNode* ToPhysical::MakePhysicalPlanWithMat(Catalog* catalog, LogicalJoinNode* logical_join, const std::string& mat_view, ExecutionFactory* factory, ExecutionContext* exec_ctx) {
  auto workload = catalog->Workload();
  auto table_set_name = fmt::format("mat_view_{}", fmt::join(logical_join->scanned_tables_set, "___"));
  if (table_set_name == mat_view) {
    fmt::print("Use mat view: {}\n", mat_view);
    // Return a regular scan.
    MatScanNode mat_scan_node;
    auto mat_table = catalog->GetTable(mat_view);
    ASSERT(mat_table != nullptr, "Mat view not found!!");
    mat_scan_node.table = mat_table;
    RecursiveCollectScansForMat(catalog, logical_join, &mat_scan_node, factory, exec_ctx);
    mat_scan_node.projections.resize(logical_join->projections.size());
    // Add projections
    for (const auto& [proj_idx, fullname]: logical_join->proj_idx_full_name) {
      auto mat_col_idx = mat_table->GetSchema().ColIdx(fullname);
      auto mat_read_idx = mat_scan_node.col_idx_read_idxs.at(mat_col_idx);
      auto mat_col = factory->MakeCol(mat_read_idx);
      mat_scan_node.projections[proj_idx] = mat_col;
    }
    auto cols_to_read = mat_scan_node.cols_to_read;
    std::vector<ExprNode*> projs = mat_scan_node.projections;
    std::vector<ExprNode*> filters;
    for (auto& f_sel: mat_scan_node.filters) filters.emplace_back(f_sel.first);
    return factory->MakeScan(mat_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  ScanNode* physical_right_scan;
  double right_size, left_size;
  {
    auto logical_right_scan = logical_join->right_scan;
    right_size = logical_right_scan->estimated_output_size;
    auto right_table = catalog->GetTable(logical_right_scan->table_info->name);
    auto cols_to_read = logical_right_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_right_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_right_scan->filters) {
      ToPhysical::BindParams(right_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    physical_right_scan = factory->MakeScan(right_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  PlanNode* left_node;
  if (logical_join->left_scan != nullptr) {
    auto logical_left_scan = logical_join->left_scan;
    left_size = logical_left_scan->estimated_output_size;
    auto left_table = catalog->GetTable(logical_left_scan->table_info->name);
    auto cols_to_read = logical_left_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_left_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_left_scan->filters) {
      ToPhysical::BindParams(left_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    left_node = factory->MakeScan(left_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  } else {
    left_node = MakePhysicalPlanWithMat(catalog, logical_join->left_join, mat_view, factory, exec_ctx);
    left_size = logical_join->left_join->estimated_output_size;
  }


  bool keep_sides = left_size >= right_size;
  auto build_key_idx = keep_sides ? logical_join->build_key_idx : logical_join->probe_key_idx;
  auto probe_key_idx = keep_sides ? logical_join->probe_key_idx : logical_join->build_key_idx;
  auto join_projs = logical_join->projections;
  if (!keep_sides) {
    fmt::print("Flipping sides: {}, {}\n", left_size, right_size);
    for (uint64_t i = 0; i < join_projs.size(); i++) {
      const auto& [side, side_idx] = join_projs[i];
      join_projs[i] = {1-side, side_idx};
    }
  }
  PlanNode* build_side = keep_sides ? physical_right_scan : left_node;
  PlanNode* probe_side = keep_sides ? left_node : physical_right_scan;


  std::vector<uint64_t> build_keys{build_key_idx};
  std::vector<uint64_t> probe_keys{probe_key_idx};
  return factory->MakeHashJoin(build_side, probe_side, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
}

PlanNode *ToPhysical::MakePhysicalPlanWithIndexes(Catalog *catalog,
                                                  LogicalJoinNode *logical_join,
                                                  const std::unordered_set<std::string> &indexes_to_use,
                                                  ExecutionFactory *factory,
                                                  ExecutionContext *exec_ctx) {
  auto workload = catalog->Workload();
  auto logical_right_scan = logical_join->right_scan;
  auto right_table = catalog->GetTable(logical_right_scan->table_info->name);
  bool is_right_index = indexes_to_use.contains(right_table->Name());
  double right_size = logical_right_scan->estimated_output_size;
  ScanNode* physical_right_scan;
  {
    auto cols_to_read = logical_right_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_right_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_right_scan->filters) {
      ToPhysical::BindParams(right_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    physical_right_scan = factory->MakeScan(right_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  double left_size;
  PlanNode* left_node;
  bool is_left_index = logical_join->left_scan != nullptr && indexes_to_use.contains(logical_join->left_scan->table_info->name);
  if (logical_join->left_scan != nullptr) {
    auto logical_left_scan = logical_join->left_scan;
    left_size = logical_left_scan->estimated_output_size;
    auto left_table = catalog->GetTable(logical_left_scan->table_info->name);
    auto cols_to_read = logical_left_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_left_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_left_scan->filters) {
      ToPhysical::BindParams(left_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    left_node = factory->MakeScan(left_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  } else {
    left_node = MakePhysicalJoin(catalog, workload, logical_join->left_join, exec_ctx, factory);
    left_size = logical_join->left_join->estimated_output_size;
  }

  if (is_right_index) {
    auto key_side = left_node;
    uint64_t key_idx = logical_join->probe_key_idx;
    auto rowid_index = catalog->GetIndex(right_table->Name());
    auto lookup_side = physical_right_scan;
    auto join_projs = logical_join->projections;
    auto index_join_node = factory->MakeIndexJoin(key_side, rowid_index, key_idx, lookup_side, std::move(join_projs));
    return index_join_node;
  } else if (is_left_index) {
    auto key_side = physical_right_scan;
    uint64_t key_idx = logical_join->build_key_idx;
    auto rowid_index = catalog->GetIndex(logical_join->left_scan->table_info->name);
    auto lookup_side = dynamic_cast<ScanNode*>(left_node);
    ASSERT(lookup_side != nullptr, "Lookup side has to be scan!");
    auto join_projs = logical_join->projections;
    // Flip projections
    for (uint64_t i = 0; i < join_projs.size(); i++) {
      const auto& [side, side_idx] = join_projs[i];
      join_projs[i] = {1-side, side_idx};
    }
    auto index_join_node = factory->MakeIndexJoin(key_side, rowid_index, key_idx, lookup_side, std::move(join_projs));
    return index_join_node;
  }

  // Regular join.
  bool keep_sides = left_size >= right_size;
  auto build_key_idx = keep_sides ? logical_join->build_key_idx : logical_join->probe_key_idx;
  auto probe_key_idx = keep_sides ? logical_join->probe_key_idx : logical_join->build_key_idx;
  auto join_projs = logical_join->projections;
  if (!keep_sides) {
    fmt::print("Flipping sides: {}, {}\n", left_size, right_size);
    for (uint64_t i = 0; i < join_projs.size(); i++) {
      const auto& [side, side_idx] = join_projs[i];
      join_projs[i] = {1-side, side_idx};
    }
  }
  PlanNode* build_side = keep_sides ? physical_right_scan : left_node;
  PlanNode* probe_side = keep_sides ? left_node : physical_right_scan;


  std::vector<uint64_t> build_keys{build_key_idx};
  std::vector<uint64_t> probe_keys{probe_key_idx};
  return factory->MakeHashJoin(build_side, probe_side, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
}

PlanNode *ToPhysical::MakePhysicalPlanWithSmartIDs(Catalog *catalog,
                                                   LogicalJoinNode *logical_join,
                                                   const std::unordered_map<std::string, std::pair<uint64_t, int64_t>>& table_embeddings,
                                                   ExecutionFactory *factory,
                                                   ExecutionContext *exec_ctx) {
  auto workload = catalog->Workload();
  ScanNode* physical_right_scan;
  double right_size, left_size;
  {
    auto logical_right_scan = logical_join->right_scan;
    right_size = logical_right_scan->estimated_output_size;
    auto right_table = catalog->GetTable(logical_right_scan->table_info->name);
    auto cols_to_read = logical_right_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_right_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_right_scan->filters) {
      ToPhysical::BindParams(right_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    if (table_embeddings.contains(right_table->Name())) {
      auto [to_col_idx, embedding] = table_embeddings.at(right_table->Name());
      fmt::print("Embedding on {} to col {} with mask {:x}\n", right_table->Name(), to_col_idx, embedding);
      auto to_col = factory->MakeCol(logical_right_scan->col_idx_read_idxs.at(to_col_idx));
      filters.emplace_back(factory->MakeEmbeddingCheck(to_col, embedding));
    }
    physical_right_scan = factory->MakeScan(right_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  PlanNode* left_node;
  if (logical_join->left_scan != nullptr) {
    auto logical_left_scan = logical_join->left_scan;
    left_size = logical_left_scan->estimated_output_size;
    auto left_table = catalog->GetTable(logical_left_scan->table_info->name);
    auto cols_to_read = logical_left_scan->cols_to_read;
    std::vector<ExprNode*> projs = logical_left_scan->projections;
    std::vector<ExprNode*> filters;
    for (auto & logical_filter: logical_left_scan->filters) {
      ToPhysical::BindParams(left_table, &logical_filter, exec_ctx);
      filters.emplace_back(logical_filter.expr_node);
    }
    if (table_embeddings.contains(left_table->Name())) {
      auto [to_col_idx, embedding] = table_embeddings.at(left_table->Name());
      fmt::print("Embedding on {} to col {} with mask {:x}\n", left_table->Name(), to_col_idx, embedding);
      auto to_col = factory->MakeCol(logical_left_scan->col_idx_read_idxs.at(to_col_idx));
      filters.emplace_back(factory->MakeEmbeddingCheck(to_col, embedding));
    }
    left_node = factory->MakeScan(left_table, std::move(cols_to_read), std::move(projs), std::move(filters));
  } else {
    left_node = MakePhysicalPlanWithSmartIDs(catalog, logical_join->left_join, table_embeddings, factory, exec_ctx);
    left_size = logical_join->left_join->estimated_output_size;
  }


  bool keep_sides = left_size >= right_size;
  auto build_key_idx = keep_sides ? logical_join->build_key_idx : logical_join->probe_key_idx;
  auto probe_key_idx = keep_sides ? logical_join->probe_key_idx : logical_join->build_key_idx;
  auto join_projs = logical_join->projections;
  if (!keep_sides) {
    fmt::print("Flipping sides: {}, {}\n", left_size, right_size);
    for (uint64_t i = 0; i < join_projs.size(); i++) {
      const auto& [side, side_idx] = join_projs[i];
      join_projs[i] = {1-side, side_idx};
    }
  }
  PlanNode* build_side = keep_sides ? physical_right_scan : left_node;
  PlanNode* probe_side = keep_sides ? left_node : physical_right_scan;


  std::vector<uint64_t> build_keys{build_key_idx};
  std::vector<uint64_t> probe_keys{probe_key_idx};
  return factory->MakeHashJoin(build_side, probe_side, std::move(build_keys), std::move(probe_keys), std::move(join_projs), JoinType::INNER);
}

}
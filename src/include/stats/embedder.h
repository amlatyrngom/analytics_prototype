#pragma once

#include "stats/table_stats.h"
#include "execution/execution_common.h"
#include "execution/execution_factory.h"

namespace smartid {
template <typename T>
uint64_t TemplatedGetEmbedding(const Histogram* hist, const Value& val, SqlType sql_type, uint64_t offset, uint64_t num_bits) {
  // Search Input
  auto cpp_val = std::get<T>(val);
  Vector in_vec(sql_type, 1);
  in_vec.MutableDataAs<T>()[0] = cpp_val;
  Filter filter;
  filter.Reset(1, FilterMode::SelVecFull);
  // Search output
  Vector out_vec(SqlType::Int64, 1);
  hist->BitIndices(&filter, &in_vec, &out_vec, offset, num_bits);
  return out_vec.MutableDataAs<uint64_t>()[0];
}

#define TEMPLATED_GET_EMBEDDING(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                               \
    bit_idx =  TemplatedGetEmbedding<cpp_type>(hist, val, col_type, bit_offset, num_bits); \
    break;


static bool ExtractFilter(const ExecutionContext* ctx, const Schema& schema, const ExprNode* node, uint64_t* idx, SqlType* col_type, OpType* comp_type, Value* val) {
  auto comp = dynamic_cast<const BinaryCompNode*>(node);
  if (comp == nullptr) return false;
  // TODO(Amadou) Support flipping
  auto col = dynamic_cast<const ColumnNode*>(comp->Child(0));
  *idx = col->ColIdx();
  *col_type = schema.GetColumn(col->ColIdx()).Type();
  *comp_type = comp->GetOpType();
  if (auto const_node = dynamic_cast<const ConstantNode*>(comp->Child(1))) {
    *val = const_node->Val();
    return true;
  }
  if (auto param_node = dynamic_cast<const ParamNode*>(comp->Child(1))) {
    *val = ctx->GetParam(param_node->ParamName()).first;
    return true;
  }
  return true;
}

/**
 * Stores info about joins to perform before embedding.
 */
struct EmbeddingTableInfo {
  /**
   * Constructor.
   */
  EmbeddingTableInfo(const Table* table, std::vector<uint64_t> && build_keys, std::vector<uint64_t> && probe_keys)
  : table(table)
  , build_keys(std::move(build_keys))
  , probe_keys(std::move(probe_keys)) {}

  EmbeddingTableInfo() = default;

  const Table* table;
  std::vector<uint64_t> build_keys;
  std::vector<uint64_t> probe_keys;

  static void PrintKeys(const std::vector<uint64_t>& keys, const std::string& side) {
    std::cout << " (" << side << ": ";
    for (const auto&k : keys) {
      std::cout << k << ", ";
    }
    std::cout << ")";
  }

  void Print() const {
    std::cout << table->Name();
    PrintKeys(build_keys, "build");
    PrintKeys(probe_keys, "probe");
    std::cout << std::endl;
  }
};

/**
 * Stores info about an embedding for a specific column.
 */
struct EmbeddingColInfo {
  EmbeddingColInfo(uint64_t col_idx, HistogramType histogram_type, uint64_t num_bits, uint64_t bit_offset)
  : col_idx(col_idx), histogram_type(histogram_type), num_bits(num_bits), bit_offset(bit_offset) {}

  bool ComputeMask(const ExecutionContext* ctx, const Table* origin, const Table* target, const std::vector<ExprNode*>& filter, uint64_t* mask) const {
    auto origin_stats = Catalog::Instance()->GetTableStats(origin);
    auto hist = origin_stats->GetHistogram(col_idx, histogram_type);

    *mask = ~(0ull);
    bool found = false;
    for (const auto& f: filter) {
      uint64_t idx;
      SqlType col_type;
      OpType comp_type;
      Value val;
      if (ExtractFilter(ctx, origin->GetSchema(), f, &idx, &col_type, &comp_type, &val)) {
        if (idx == col_idx) {
          found = true;
          // Compute the bit index of the given value.
          uint64_t bit_idx = 0;
          switch (col_type) {
            SQL_TYPE(TEMPLATED_GET_EMBEDDING, NOOP);
            case SqlType::Date:
              bit_idx =  TemplatedGetEmbedding<Date>(hist, val, col_type, bit_offset, num_bits);
              break;
            default:
              ASSERT(false, "GetEmbedding on non arithmetic type not supported !!!!!");
          }
          // Subtract the offset for simpler computation. It will be readded later.
          bit_idx -= bit_offset;
          uint64_t compute_mask = 0;
          if (bit_idx < 0 || bit_idx > num_bits) return 0;
          // Compute the mask to return.
          // all_ones contains num_bits ones.
          uint64_t all_ones = num_bits == 64 ? ~0ull: (1ull << num_bits) - 1;
          switch (comp_type) {
            case OpType::LT:
            case OpType::LE: {
              // Want all bits in [0, bit_idx] to be ones.
              if (bit_idx == 63) {
                compute_mask = all_ones;
              } else {
                compute_mask = (1ull << (bit_idx + 1)) - 1;
              }
              break;
            }
            case OpType::GT:
            case OpType::GE: {
              // Want all bits in [bit_idx, num_bits) to be ones.
              compute_mask = all_ones ^ ((1ull << bit_idx) - 1);
              break;
            }
            case OpType::EQ: {
              // Want the bit at bit_idx to be set.
              compute_mask = 1ull << bit_idx;
              break;
            }
            case OpType::NE: {
              ASSERT(false, "GetEmbedding NE Not Implemented");
            }
            default:
              ASSERT(false, "Impossible (GetEmbedding)");
          }
          *mask &= (compute_mask << bit_offset);
        }
      }
    }
    return found;
  }

  uint64_t col_idx;
  uint64_t vp_idx{0xFFFFFFFF};
  HistogramType histogram_type;
  uint64_t num_bits;
  uint64_t bit_offset;
};

struct CompoundEmbeddingInfo {
  CompoundEmbeddingInfo(std::vector<uint64_t> && col_idxs, std::vector<SqlType> && col_types,
                        std::vector<OpType> && comp_types, std::vector<Value> && vals,
                        uint64_t bit_idx)
  : col_idxs(std::move(col_idxs))
  , comp_types(std::move(comp_types))
  , col_types(std::move(col_types))
  , vals(std::move(vals))
  , bit_idx(bit_idx) {}

  std::vector<uint64_t> col_idxs;
  std::vector<SqlType> col_types;
  std::vector<OpType> comp_types;
  std::vector<Value> vals;
  std::vector<uint64_t> vp_idxs{};
  uint64_t bit_idx;

  static bool Compare(SqlType col_type, const Value& lhs, const Value& rhs) {
    Filter one_elem_filter;
    one_elem_filter.Reset(1, FilterMode::SelVecFull);
    Vector lhs_vec(col_type, 1);
    Vector rhs_vec(col_type, 1);
    VectorOps::InitVector(&one_elem_filter, lhs, col_type, &lhs_vec);
    VectorOps::InitVector(&one_elem_filter, rhs, col_type, &rhs_vec);
    VectorOps::BinaryCompVector(&lhs_vec, &rhs_vec, &one_elem_filter, OpType::EQ);
    return one_elem_filter.ActiveSize() > 0;
  }
  
  void GetEmbedding(const ExecutionContext* ctx, const Table* origin, const Table* target, const std::vector<ExprNode*>& filter, uint64_t* match_mask) const {
    // Extract filters that are simple binary comparison.
    std::vector<uint64_t> input_col_idxs;
    std::vector<SqlType> input_col_types;
    std::vector<OpType> input_comp_types;
    std::vector<Value> input_vals;
    for (const auto& f: filter) {
      uint64_t idx;
      SqlType col_type;
      OpType comp_type;
      Value val;
      if (ExtractFilter(ctx, origin->GetSchema(), f, &idx, &col_type, &comp_type, &val)) {
        input_col_idxs.emplace_back(idx);
        input_col_types.emplace_back(col_type);
        input_comp_types.emplace_back(comp_type);
        input_vals.emplace_back(val);
      }
    }
    if (input_col_idxs != col_idxs || input_col_types != input_col_types || input_comp_types != comp_types) {
      // This compound does not correspond to the input filter.
      return;
    }
    // From this point onwards, we know the input filter corresponds to this compound.
    // We just have to update the mask if the match is exact.
    for (uint64_t i = 0; i < col_idxs.size(); i++) {
      if (!Compare(col_types[i], input_vals[i], vals[i])) {
        // Not match.
        return;
      }
    }
    // Update mask.
    *match_mask |= 1ull << bit_idx;
  }
  
  void Print() const {
    std::cout << "Compound: BitIdx " << bit_idx << ", Columns (";
    for (const auto& i: col_idxs) {
      std::cout << i << ", ";
    }
    std::cout << ")" << std::endl;
  }
};

struct EmbeddingInfo {
  using IdxVector = std::vector<uint64_t>;
  using IdxSet = std::unordered_set<uint64_t>;

  uint64_t build_id_vp_idx;
  uint64_t probe_id_vp_idx;
  std::vector<EmbeddingColInfo> probe_col_infos;
  std::vector<EmbeddingColInfo> build_col_infos;
  std::vector<CompoundEmbeddingInfo> compound_probe_infos;
  std::vector<CompoundEmbeddingInfo> compound_build_infos;
  std::vector<EmbeddingTableInfo> interm_tables;
  PlanNode* join_node;


  const Table* GetBuildTable() const {
    return interm_tables.front().table;
  }

  const Table* GetProbeTable() const {
    return interm_tables.back().table;
  }


  uint64_t GetCompoundMask(const ExecutionContext* ctx, const std::vector<ExprNode*>& filters, bool direction) const {
    const Table* origin;
    const Table* target;
    const std::vector<CompoundEmbeddingInfo>* compounds;
    if (direction) {
      origin = GetProbeTable();
      target = GetBuildTable();
      compounds = &compound_probe_infos;
    } else {
      origin = GetBuildTable();
      target = GetProbeTable();
      compounds = &compound_build_infos;
    }
    uint64_t match_mask = 0;
    for (const auto& compound: *compounds) {
      compound.GetEmbedding(ctx, origin, target, filters, &match_mask);
    }
    return match_mask;
  }

  std::vector<uint64_t> GetSingleMasks(const ExecutionContext* ctx, const std::vector<ExprNode*>& filters, bool direction) const {
    const Table* origin;
    const Table* target;
    const std::vector<EmbeddingColInfo>* col_infos;
    if (direction) {
      origin = GetProbeTable();
      target = GetBuildTable();
      col_infos = &probe_col_infos;
    } else {
      origin = GetBuildTable();
      target = GetProbeTable();
      col_infos = &build_col_infos;
    }
    std::vector<uint64_t> ret;
    for (const auto& col_info: *col_infos) {
      uint64_t mask;
      if (col_info.ComputeMask(ctx, origin, target, filters, &mask)) {
        ret.emplace_back(mask);
      }
    }
    return ret;
  }

  PlanNode* MakeJoinSteps(ExecutionFactory* factory) {
    auto print_out = [&](PlanNode* node) {
      auto print_node = factory->MakePrint(node);
      auto executor = ExecutionFactory::MakePlanExecutor(print_node, nullptr);
      executor->Next();
    };

    auto [node, build_keys, build_out] = ConstructInitialBuild(factory);
    for (uint64_t step = 1; step < interm_tables.size() - 1; step++) {
      auto triplet = MakeJoinStep(factory, step, node, std::move(build_keys), std::move(build_out));
      node = std::get<0>(triplet);
      build_keys = std::move(std::get<1>(triplet));
      build_out = std::move(std::get<2>(triplet));
    }
    auto final_node = ConstructFinalProbe(factory, node, std::move(build_keys), std::move(build_out));
    join_node = final_node;
    return final_node;
  }

  PlanNode* ConstructFinalProbe(ExecutionFactory* factory, PlanNode* build_node, IdxVector&& build_keys, IdxVector&& build_out) {
    const auto& probe_info = interm_tables.back();
    auto probe_table = probe_info.table;
    // First read all unique columns: id, probe_keys..., embedding_cols..., compound_cols, ...
    std::unordered_set<uint64_t> cols; // unique columns.
    // Add ID column.
    cols.emplace(probe_table->IdIdx());
    // Add probe keys
    for (const auto& key_idx: probe_info.probe_keys) cols.emplace(key_idx);
    // Add embedding cols
    for (const auto& e: probe_col_infos) cols.emplace(e.col_idx);
    // Add compound cols
    for (const auto& c: compound_probe_infos) cols.insert(c.col_idxs.begin(), c.col_idxs.end());
    // Convert to vector.
    // This map is used to track the projection index of each unique column.
    std::unordered_map<uint64_t, uint64_t> proj_idx_map;
    std::vector<uint64_t> probe_projs;
    for (const auto& idx: cols) {
      proj_idx_map[idx] = probe_projs.size();
      probe_projs.emplace_back(idx);
    }
    // Index of probe keys within the projection.
    std::vector<uint64_t> join_probe_keys;
    for (const auto& key_idx: probe_info.probe_keys) join_probe_keys.emplace_back(proj_idx_map[key_idx]);
    // Now output the id, embedding_cols..., compound_cols...
    // using the indexes within the projections.
    std::unordered_set<uint64_t> output_cols; // unique output cols
    // Id
    output_cols.emplace(proj_idx_map[probe_table->IdIdx()]);
    // Add embedding cols
    for (auto& e: probe_col_infos) { output_cols.emplace(proj_idx_map[e.col_idx]); }
    // Add compound cols
    for (const auto& c: compound_probe_infos) {
      for (const auto& idx: c.col_idxs) {
        output_cols.emplace(proj_idx_map[idx]);
      }
    }
    std::unordered_map<uint64_t, uint64_t> join_idx_map;
    std::vector<uint64_t> join_probe_projs;
    for (const auto& idx: output_cols) {
      join_idx_map[idx] = join_probe_projs.size();
      join_probe_projs.emplace_back(idx);
    }
    // All probe indices start after the build output
    // Set the index of the id
    probe_id_vp_idx = build_out.size() + join_idx_map[proj_idx_map[probe_table->IdIdx()]];
    // Set the indexes in the embeddings.
    for (auto& e: probe_col_infos) {
      e.vp_idx = build_out.size() + join_idx_map[proj_idx_map[e.col_idx]];
    }
    // Set the indexes of the compounds.
    for (auto& c: compound_probe_infos) {
      for (const auto& idx: c.col_idxs) {
        c.vp_idxs.emplace_back(build_out.size() + join_idx_map[proj_idx_map[idx]]);
      }
    }

    // Scan
    std::vector<ExprNode*> scan_proj;
    for (const auto& i: probe_projs) scan_proj.emplace_back(factory->MakeCol(i));
    auto scan_node = factory->MakeScan(probe_table, std::move(scan_proj), {});
    // Join
    std::vector<std::pair<uint64_t, uint64_t>> join_projections;
    for (const auto& b: build_out) join_projections.emplace_back(0, b);
    for (const auto& p: join_probe_projs) join_projections.emplace_back(1, p);
    auto join_node = factory->MakeHashJoin(build_node, scan_node,
                                           std::move(build_keys), std::move(join_probe_keys),
                                           std::move(join_projections), JoinType::INNER);
    return join_node;
  }

  std::tuple<PlanNode*, IdxVector, IdxVector> MakeJoinStep(ExecutionFactory* factory, uint64_t step, PlanNode* build_node, IdxVector&& build_keys, IdxVector&& build_out) {
    const auto& probe_info = interm_tables[step];
    auto probe_table = probe_info.table;
    // Step 1: The probe scan.
    std::vector<ExprNode*> scan_proj;
    for (const auto& k: probe_info.probe_keys) scan_proj.emplace_back(factory->MakeCol(k));
    for (const auto& k: probe_info.build_keys) {
      scan_proj.emplace_back(factory->MakeCol(k));
    }
    auto scan_node = factory->MakeScan(probe_table, std::move(scan_proj), {});
    // Step 2: The join node
    // Probe keys are the first keys output by the scan
    std::vector<uint64_t> join_probe_keys;
    for (uint64_t i = 0; i < probe_info.probe_keys.size(); i++) join_probe_keys.emplace_back(i);
    // Join projections will first contain the columns output by the previous node
    // Then the keys to be used by the next probe. These keys are right after the probe keys.
    std::vector<std::pair<uint64_t, uint64_t>> join_projections;
    std::vector<uint64_t> next_build_out;
    for (uint64_t i = 0; i < build_out.size(); i++) {
      next_build_out.emplace_back(i);
      join_projections.emplace_back(0, build_out[i]);
    }
    IdxVector next_build_keys;
    for (uint64_t i = 0; i < probe_info.build_keys.size(); i++) {
      next_build_keys.emplace_back(join_projections.size());
      join_projections.emplace_back(1, probe_info.probe_keys.size() + i);
    }
    auto join_node = factory->MakeHashJoin(build_node, scan_node,
                                           std::move(build_keys), std::move(join_probe_keys),
                                           std::move(join_projections), JoinType::INNER);
    return {join_node, std::move(next_build_keys), std::move(next_build_out)};
  }


  std::tuple<PlanNode*, IdxVector, IdxVector> ConstructInitialBuild(ExecutionFactory* factory) {
    const auto& build_info = interm_tables[0];
    auto build_table = build_info.table;
    // First read all unique columns: id, build_keys..., embedding_cols..., compound_cols, ...
    std::unordered_set<uint64_t> cols; // unique columns.
    // Add ID column.
    cols.emplace(build_table->IdIdx());
    // Add build keys
    for (const auto& key_idx: build_info.build_keys) cols.emplace(key_idx);
    // Add embedding cols
    for (const auto& e: build_col_infos) cols.emplace(e.col_idx);
    // Add compound cols
    for (const auto& c: compound_build_infos) cols.insert(c.col_idxs.begin(), c.col_idxs.end());
    // Convert to vector.
    // This map is used to track the projection index of each unique column.
    std::unordered_map<uint64_t, uint64_t> proj_idx_map;
    std::vector<uint64_t> build_projs;
    for (const auto& idx: cols) {
      proj_idx_map[idx] = build_projs.size();
      build_projs.emplace_back(idx);
    }
    // Index of build keys within the projection.
    std::vector<uint64_t> join_build_keys;
    for (const auto& key_idx: build_info.build_keys) join_build_keys.emplace_back(proj_idx_map[key_idx]);
    // Now output the id, embedding_cols..., compound_cols...
    // using the indexes within the projections.
    std::unordered_set<uint64_t> output_cols; // unique output cols
    // Id
    output_cols.emplace(proj_idx_map[build_table->IdIdx()]);
    // Add embedding cols
    for (auto& e: build_col_infos) { output_cols.emplace(proj_idx_map[e.col_idx]); }
    // Add compound cols
    for (const auto& c: compound_build_infos) {
      for (const auto& idx: c.col_idxs) {
        output_cols.emplace(proj_idx_map[idx]);
      }
    }
    std::unordered_map<uint64_t, uint64_t> join_idx_map;
    std::vector<uint64_t> join_build_projs;
    for (const auto& idx: output_cols) {
      join_idx_map[idx] = join_build_projs.size();
      join_build_projs.emplace_back(idx);
    }
    // Set the index of the id
    build_id_vp_idx = join_idx_map[proj_idx_map[build_table->IdIdx()]];
    // Set the indexes in the embeddings.
    for (auto& e: build_col_infos) {
      e.vp_idx = join_idx_map[proj_idx_map[e.col_idx]];
    }
    // Set the indexes of the compounds.
    for (auto& c: compound_build_infos) {
      for (const auto& idx: c.col_idxs) {
        c.vp_idxs.emplace_back(join_idx_map[proj_idx_map[idx]]);
      }
    }
    std::vector<ExprNode*> scan_projs;
    for (const auto& i: build_projs) scan_projs.emplace_back(factory->MakeCol(i));
    auto scan_node = factory->MakeScan(build_table, std::move(scan_projs), {}, false);
    return {scan_node, std::move(join_build_keys), std::move(join_build_projs)};
  }

  static void PrintColInfos(const std::vector<EmbeddingColInfo>& col_infos, const std::string& side) {
    std::cout << "(" << side << ": ";
    for (const auto&col_info : col_infos) {
      std::cout << col_info.col_idx << ", ";
    }
    std::cout << ")";
    std::cout << std::endl;
  }

  static void PrintCompoundInfos(const std::vector<CompoundEmbeddingInfo>& compound_infos, const std::string & side) {
    std::cout << side << std::endl;
    for (const auto& c: compound_infos) {
      c.Print();
    }
  }

  void Print() const {
    std::cout << "================== Embedding =======================" << std::endl;
    PrintColInfos(probe_col_infos, "probe_cols");
    PrintColInfos(build_col_infos, "build_cols");
    PrintCompoundInfos(compound_probe_infos, "probe_compounds");
    PrintCompoundInfos(compound_build_infos, "build_compounds");
    for (const auto& interm: interm_tables) {
      interm.Print();
    }
  }
};


/**
 * Helper class to perform embedding.
 */
class Embedder {
 public:
  /**
   * Constructor
   */
  explicit Embedder(std::vector<EmbeddingInfo> && embedding_infos) : embedding_infos_(std::move(embedding_infos)) {
    for (auto& embedding_info : embedding_infos_) {
      BuildEmbedding(embedding_info);
    }
  }

  EmbeddingInfo& GetEmbeddingInfo(uint64_t idx) {
    return embedding_infos_[idx];
  }

  [[nodiscard]] const std::vector<EmbeddingInfo>& GetEmbeddingInfos() const {
    return embedding_infos_;
  }

  void UpdateScan(ExecutionFactory* factory, const ExecutionContext* ctx, const ScanNode* origin_scan, ScanNode* target_scan);


 private:
  void BuildEmbedding(EmbeddingInfo& embedding_info);

  std::vector<EmbeddingInfo> embedding_infos_;
  ExecutionFactory factory_;
};

}
#pragma once

#include "storage/table.h"
#include "execution/execution_common.h"
#include "execution/execution_factory.h"
#include "stats/embedder.h"
#include <unordered_map>


namespace smartid {

/**
 * Stores all info that can be used to compute bit allocation
 */
struct FilterInfo {
  // Filter column.
  uint64_t col_idx;
  // Join info.
  JoinStats join_stats;
  // Comparison info
  OpType comp_type;
  // When the comparison is with a constant, the vector contains one elements.
  // When the comparison is with a parameter, the vector contains multiple elements.
  SqlType val_type;
  std::vector<Value> comp_vals;
  // The output/input selectivity of the filter.
  double selectivity;
};


struct CompoundFilterInfo {
  explicit CompoundFilterInfo(const std::vector<FilterInfo>& filter_infos): estimated_reduction(0) {
    ASSERT(filter_infos.size() > 0, "Empty Filter List");
    for (const auto& info: filter_infos) {
      if (info.comp_vals.size() == 1) {
        std::cout << "Coumpound Sel: " << info.selectivity;
        estimated_reduction += (1-estimated_reduction) * (1-info.selectivity);
        compound_filters.emplace_back(info);
      }
    }
  }
  
  void SetJoinStats(const JoinStats& js) {
    join_stats = js;
  }

  std::vector<FilterInfo> compound_filters;
  double estimated_reduction;
  double rank{0};
  const Table* table;
  JoinStats join_stats;
  bool set_join_stats{true};
};


class SchemaGraph {
 public:
  // Graph to store origin_table -> foreign_table -> <origin_col, foreign_col> list.
  using FKList = std::vector<std::pair<uint64_t, uint64_t>>;
  using TableFK = std::unordered_map<const Table*, FKList>;
  using TableGraph = std::unordered_map<const Table*, TableFK>;

  // Graph to store table1 --> table2 --> <filter_info>
  // Filters are on table2.
  // For probe to build embeddings: table1 is build, table2 is probe.
  // For build to probe embeddings: table1 is probe, table2 is build.
  using FilterList = std::vector<FilterInfo>;
  using TableFilter = std::unordered_map<const Table*, FilterList>;
  using FilterGraph = std::unordered_map<const Table*, TableFilter>;

  // As above, but for compound filters.
  using CompoundFilterList = std::vector<CompoundFilterInfo>;
  using TableCompoundFilter = std::unordered_map<const Table*, CompoundFilterList>;
  using CompoundFilterGraph = std::unordered_map<const Table*, TableCompoundFilter>;

  // Graph to store table1 --> table2 --> <col_idx, rank>
  // col_idx is on table2.
  // For probe to build embeddings: table1 is build, table2 is probe.
  // For build to probe embeddings: table1 is probe, table2 is build.
  using RankList = std::unordered_map<uint64_t, double>;
  using TableRank = std::unordered_map<const Table*, RankList>;
  using RankGraph = std::unordered_map<const Table*, TableRank>;

  using CompoundRankGraph = std::unordered_map<const Table*, std::vector<CompoundFilterInfo>>;

  /**
   * Construct a TableGraph from the input list of tables.
   */
  explicit SchemaGraph(std::vector<std::string> &&table_names);

  /**
   * Given plan nodes (with stats), generate the list of embeddings.
   */
  Embedder* BuildEmbeddings(ExecutionFactory* factory, const std::vector<const PlanNode*>& nodes) {
    MakeFilterGraphs(nodes);
    MakeRanks();
    MakeEmbeddingInfo();
    std::vector<PlanNode*> plan_nodes;
    for (auto& info: embedding_infos_) {
      info.MakeJoinSteps(factory);
    }
    embedder_ = std::make_unique<Embedder>(std::move(embedding_infos_));
    return embedder_.get();
  }


  void PrintFilterGraphs() const;
  void PrintRanks() const;

  [[nodiscard]] const TableGraph& GetTableGraph() const {
    return table_graph_;
  }


  [[nodiscard]] const FilterGraph& GetProbeFilterGraph() const {
    return probe_filter_graph_;
  }

  [[nodiscard]] const FilterGraph& GetBuildFilterGraph() const {
    return build_filter_graph_;
  }

  [[nodiscard]] const RankGraph& GetProbeRankGraph() const {
    return probe_rank_graph_;
  }

  [[nodiscard]] const RankGraph& GetBuildRankGraph() const {
    return build_rank_graph_;
  }

  void ResetEmbeddings();

  void MakeFilterGraphs(const std::vector<const PlanNode*>& nodes) {
    for (const auto&n: nodes) FilterGraphs(n);
  }

  void MakeJoinPath(const Table* build_table, const Table* probe_table, EmbeddingInfo* origin_info);

  void MakeRanks();

  void MakeEmbeddingInfo();

  void MakeTableRank(const FilterGraph* filter_graph, bool direction);
  void MakeCompoundRanks(CompoundFilterGraph* filter_graph, bool direction);

 private:

  void FilterGraphs(const PlanNode* node);



  void RecursiveMakeFilterGraph(const PlanNode *node,
                                FilterGraph * filter_graph, CompoundFilterGraph * compound_filter_graph,
                                TableFilter *curr_filters, TableCompoundFilter * curr_compound_filters,
                                const JoinStats* curr_join_stats, FilterInfo* curr_filter_info,
                                bool direction);

  void ExtractFilters(const ExprNode* expr, std::vector<FilterInfo>* infos, FilterInfo* curr_filter_info);

  void MakeColInfos(const RankList& rank_list, EmbeddingInfo* origin_info, uint64_t* curr_offset, bool direction);
  void MakeCompoundInfos(const CompoundFilterInfo& compound, EmbeddingInfo* origin_info, uint64_t* curr_offset, bool direction);

  std::vector<const Table*> RecursiveFindJoinPath(const Table* build_table, const Table* curr_table, EmbeddingInfo* origin_info);

  TableGraph table_graph_;
  FilterGraph probe_filter_graph_;
  CompoundFilterGraph probe_compound_filter_graph_;
  FilterGraph build_filter_graph_;
  CompoundFilterGraph build_compound_filter_graph_;
  RankGraph probe_rank_graph_;
  CompoundRankGraph compound_probe_rank_graph_;
  RankGraph build_rank_graph_;
  CompoundRankGraph compound_build_rank_graph_;
  std::vector<EmbeddingInfo> embedding_infos_;
  std::vector<Table*> tables_;
  std::vector<const ScanNode*> all_scans_;
  std::unique_ptr<Embedder> embedder_{nullptr};
};
}
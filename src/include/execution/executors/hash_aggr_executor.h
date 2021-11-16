#include "execution/nodes/hash_aggr_node.h"
#include "execution/executors/plan_executor.h"

namespace smartid {
class HashAggregationExecutor: public PlanExecutor {
 public:
  HashAggregationExecutor(HashAggregationNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children))
  , node_(node)
  , build_entries_(SqlType::Pointer) {
    result_ = std::make_unique<VectorProjection>(&result_filter_);
  }

  const VectorProjection * Next() override;

 private:
  /**
   * Compute HT entry offsets.
   */
  void Prepare(const VectorProjection *vp);

  /**
   * Compute the hash of the given columns.
   */
  void VectorHash(const VectorProjection *vp, const std::vector<uint64_t> &key_cols);

  /**
   * Accumulate input values into hash tables.
   */
  void Accumulate();

  /**
   * Iterate list of items in the table.
   */
  void IterateTable();

  /**
   * Find potential matching items.
   */
  void FindCandidates(const VectorProjection *vp);

  /**
   * Check and update matches in the table.
   */
  void UpdateMatches(const VectorProjection *vp);

  /**
   * Advance hash table chains.
   */
  void AdvanceChains(const VectorProjection *vp);

  /**
   * Initialize new entries for non matching items.
   */
  void InitNewEntry(const VectorProjection *vp, sel_t i);


 private:
  HashAggregationNode* node_;
  // Whether Accumulate has been called.
  bool accumulated_{false};
  // The aggregation table.
  AggrTable agg_table_;
  // Result filter and vectors.
  Filter result_filter_;
  std::vector<std::unique_ptr<Vector>> result_vecs_;
  // Matching and non matching filter.
  Filter non_match_filter_;
  Filter match_filter_;
  // Stores hash values.
  Vector hashes_{SqlType::Int64};
  // Types & offset of build columns within the HTEntry payload.
  std::vector<SqlType> build_types_;
  std::vector<uint64_t> build_offsets_;
  uint64_t entry_size_{0}; // Size of payload.
  // Stores & tracks entries with matching hashes.
  Vector candidates_{SqlType::Pointer};
  Filter cand_filter_;
  // Helper vector to store ht entries.
  Vector build_entries_;
  std::vector<std::vector<char>> build_alloc_space_;
  std::vector<const HTEntry *> entries_;
};
}
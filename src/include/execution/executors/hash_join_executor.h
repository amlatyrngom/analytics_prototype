#include "execution/nodes/hash_join_node.h"
#include "execution/executors/plan_executor.h"
#include "common/types.h"
#include "storage/filter.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "execution/execution_common.h"
#include "execution/bloom_filter.h"

namespace smartid {

using BloomFilterType = BlockedBloomFilter;

class HashJoinExecutor: public PlanExecutor {
 public:
  HashJoinExecutor(HashJoinNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children))
  , node_(node)
  , build_entries_(SqlType::Pointer)
  , result_filter_()
  {
    auto num_build_projection = 0;
    auto num_probe_projection = 0;
    for (const auto &proj: node_->Projections()) {
      num_build_projection += (proj.first == 0);
      num_probe_projection += (proj.first != 0);
    }
    build_vecs_.resize(num_build_projection);
    probe_vecs_.resize(num_probe_projection);
    result_ = std::make_unique<VectorProjection>(&result_filter_);
    // Init bloom filter
    if (node_->UseBloom()) {
      bloom_ = std::make_unique<BloomFilterType>(node_->ExpectedBloomCount(), 0.03);
    }
  }

  const VectorProjection * Next() override;

 private:
  // Clear
  void Clear();

  PlanExecutor* BuildExecutor() {
    return Child(0);
  }

  PlanExecutor* ProbeExecutor() {
    return Child(1);
  }

  /**
   * Computes the offsets at which build columns will be within the offset.
   * Columns are sorted by size to prevent padding.
   */
  void PrepareBuild(const VectorProjection *vp);

  /**
   * Compute the hash of the given columns.
   */
  void VectorHash(const VectorProjection *vp, const std::vector<uint64_t> &key_cols);

  /**
   * Construct the vector of hash table entries.
   */
  void MakeEntries(const VectorProjection *vp);

  /**
   * Insert rows into the hash table.
   */
  void VectorInsert(const VectorProjection *vp);

  /**
   * Build the join hash table.
   */
  void Build();

  /**
   * Perform a lookup to find potential matching candidates (hash collisions).
   */
  void FindCandidates(const VectorProjection *vp);

  /**
   * Checks if the current candidates are exact matches.
   */
  void CheckKeys(const VectorProjection *vp);

  void TestSetMarks(Bitmap* filter, Vector* mark);

  /**
   * Advance the chains.
   */
  void AdvanceChains();

  /**
   * Gather a build column from a hash table entry into a vector.
   */
  void GatherBuildCol(uint64_t col_idx, uint64_t vec_idx);

  /**
   * Gather a probe column into a vector.
   */
  void GatherProbeCol(const VectorProjection *vp, uint64_t col_idx, uint64_t vec_idx);

  /**
   * Create the final vectors of matches.
   */
  void GatherMatches(const VectorProjection *vp);

 private:
  HashJoinNode * node_;
  bool built_{false};
  // Helper vector to store ht entries.
  Vector build_entries_;
  std::vector<std::vector<char>> build_alloc_space_;
  // Join hash table.
  JoinTable join_table_;
  // Stores hash values.
  Vector hashes_{SqlType::Int64};
  // Stores marks.
  Vector build_marks_{SqlType::Char};
  Vector probe_marks_{SqlType::Char};
  // Stores & tracks entries with matching hashes.
  Vector candidates_{SqlType::Pointer};
  Bitmap cand_filter_;
  // Tracks entries with equal keys.
  Bitmap match_filter_;
  // List of matching {probe_idx, HTEntry}.
  std::vector<uint64_t> probe_matches_;
  std::vector<const HTEntry *> build_matches_;
  // Types & offset of build columns within the HTEntry payload.
  std::vector<SqlType> build_types_;
  std::vector<uint64_t> build_offsets_;
  uint64_t entry_size_{0}; // Size of payload.
  // Helper vectors to create the final results
  std::vector<std::unique_ptr<Vector>> build_vecs_;
  std::vector<std::unique_ptr<Vector>> probe_vecs_;
  bool first_probe_{true};
  // Final filter.
  Bitmap result_filter_;
  // Bloom filter
  std::unique_ptr<BloomFilterType> bloom_;
  // Join stats
  Bitmap build_out_filter_;
  Bitmap probe_out_filter_;
};
}
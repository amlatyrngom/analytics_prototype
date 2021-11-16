#include "execution/executors/plan_executor.h"
#include "execution/nodes/sort_node.h"

namespace smartid {
class SortExecutor : public PlanExecutor {
 public:
  SortExecutor(SortNode* node, std::vector<std::unique_ptr<PlanExecutor>> && children)
  : PlanExecutor(std::move(children))
  , node_(node) {}

  const VectorProjection * Next() override;

 private:
  /**
   * Compute offsets.
   */
  void Prepare(const VectorProjection *vp);

  /**
   * Add sort entries to the sort vector.
   */
  void Append(const VectorProjection *vp);

  void AppendLimit(const VectorProjection* vp, int64_t limit);

  /**
   * Sort the sort vector.
   */
  void Sort();

  /**
   * Collect entries from the sort vector.
   */
  void Collect();

 private:
  SortNode* node_;
  // Offset of elements within the vector entries,
  std::vector<uint64_t> sort_offsets_;
  // Types of the elements.
  std::vector<SqlType> sort_types_;
  // Vector of elements to sort.
  std::vector<char *> sort_vector_;
  // Space where elements are allocated.
  std::vector<std::vector<char>> sort_alloc_space_;
  // Total size of elements.
  uint64_t sort_size_;
  // Used for VectorOps. Shallow copy of the sort vector.
  Vector sort_entries_{SqlType::Pointer};
  // Result filter and vectors.
  Filter result_filter_;
  std::vector<std::unique_ptr<Vector>> result_vecs_;
  bool sorted_{false};
};
}
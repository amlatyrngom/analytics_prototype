#pragma once

#include "execution/execution_common.h"
#include "execution/vector_ops.h"
#include "plan_node.h"
#include "execution/bloom_filter.h"

namespace smartid {
struct JoinStats {
  double build_hash_time{0};
  double insert_time{0};
  double probe_hash_time{0};
  double probe_time{0};
  uint64_t bloom_before{0};
  uint64_t bloom_after{0};
  uint64_t ht_size{0};
  uint64_t build_in{0};
  uint64_t build_out{0};
  uint64_t probe_in{0};
  uint64_t probe_out{0};
};


class HashJoinNode : public PlanNode {
 public:
  HashJoinNode(PlanNode *build,
               PlanNode *probe,
               std::vector<uint64_t> &&build_key_cols,
               std::vector<uint64_t> &&probe_key_cols,
               std::vector<std::pair<uint64_t, uint64_t>> &&projections, JoinType join_type)
               : PlanNode(PlanType::HashJoin, {build, probe})
               , build_key_cols_(std::move(build_key_cols))
               , probe_key_cols_(std::move(probe_key_cols))
               , projections_(std::move(projections))
               , joint_type_(join_type) {}

  [[nodiscard]] const auto & BuildKeys() const {
    return build_key_cols_;
  }

  [[nodiscard]] const auto & ProbeKeys() const {
    return probe_key_cols_;
  }

  [[nodiscard]] const auto & Projections() const {
    return projections_;
  }

  [[nodiscard]] const auto & GetJoinType() const {
    return joint_type_;
  }

  [[nodiscard]] bool UseBloom() const {
    return use_bloom_;
  };

  [[nodiscard]] uint64_t ExpectedBloomCount() const {
    return expected_bloom_count_;
  };


  void UseBloomFilter(uint64_t expected_count) {
    use_bloom_ = true;
    expected_bloom_count_  = expected_count;
  }

  void ReportStats(const JoinStats& join_stats) {
    join_stats_.emplace_back(join_stats);
  }

  [[nodiscard]] const std::vector<JoinStats>& GetJoinStats() const {
    return join_stats_;
  }

 private:
  // The build and probe key columns.
  std::vector<uint64_t> build_key_cols_;
  std::vector<uint64_t> probe_key_cols_;
  // How to create the final result. <{side, col_idx}>
  std::vector<std::pair<uint64_t, uint64_t>> projections_;
  // The Join Type
  JoinType joint_type_;
  // Whether to use bloom filter.
  bool use_bloom_{false};
  uint64_t expected_bloom_count_{0};
  // Join stats
  std::vector<JoinStats> join_stats_;
};

}
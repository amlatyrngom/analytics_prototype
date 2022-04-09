#pragma once

#include <variant>
#include <unordered_map>
#include "execution/ops.h"
#include "tsl/robin_map.h"
#include "tsl/ordered_map.h"

class value;
namespace smartid {
/**
 * The types of joins.
 */
enum class JoinType {
  INNER,
  RIGHT_SEMI,
  LEFT_SEMI,
};

/**
 * The types of aggregations.
 */
enum class AggType {
  COUNT,
  SUM,
  MAX,
  MIN,
};

/**
 * The type of the sort.
 */
enum class SortType {
  ASC,
  DESC,
};


struct HTEntry {
  HTEntry *next;
  char payload[0];
};

// Keys are already hashed. No need to repeat.
struct ExecHasher {
  std::size_t operator()(const uint64_t& x) const noexcept {
    return x;
  }
};

using JoinTable = tsl::robin_map<uint64_t, HTEntry *, ExecHasher>;

using AggrTable = tsl::robin_map<uint64_t, HTEntry *, ExecHasher>;

using RowIDIndexTable = tsl::robin_map<uint64_t, std::vector<uint64_t>>;
}
#pragma once

#include <variant>
#include <unordered_map>
#include "execution/ops.h"
#include "tsl/robin_map.h"
#include "tsl/ordered_map.h"
#include "execution_types.h"

class value;
namespace smartid {



using JoinTable = tsl::robin_map<uint64_t, HTEntry *, ExecHasher>;

using JoinTable64 = tsl::robin_map<int64_t, std::vector<HTEntry *>>;
using JoinTable32 = tsl::robin_map<int32_t, std::vector<HTEntry *>>;

using AggrTable = tsl::robin_map<uint64_t, HTEntry *, ExecHasher>;
}
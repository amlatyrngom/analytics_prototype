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

using AggrTable = tsl::robin_map<uint64_t, HTEntry *, ExecHasher>;
}
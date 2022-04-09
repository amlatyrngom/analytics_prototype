#pragma once
#include "common/types.h"
#include <unordered_map>

namespace smartid {
class ExecutionContext {
 public:
  ExecutionContext() = default;

  void AddParam(const std::string& param_name, Value&& param_value, SqlType param_type) {
    params_[param_name] = std::pair<Value, SqlType>{param_value, param_type};
  }

  [[nodiscard]] const std::pair<Value, SqlType>& GetParam(const std::string& param_name) const {
    return params_.at(param_name);
  }
 private:
  std::unordered_map<std::string, std::pair<Value, SqlType>> params_;
};
}
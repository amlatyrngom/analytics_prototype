#pragma once

#include <fstream>

namespace smartid {

class Catalog;
class LogicalJoinNode;

struct Default {
  static void GenerateDefaultCosts(Catalog* catalog, std::ostream& os);
  static double RecursiveEstimateDefault(const LogicalJoinNode* logical_join);
  static void GenerateCostsForOptimization(Catalog *catalog);
};

}
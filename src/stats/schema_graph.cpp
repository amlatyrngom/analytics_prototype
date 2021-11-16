//#include "stats/schema_graph.h"
//#include "common/util.h"
//#include "common/catalog.h"
//#include <algorithm>
//
//namespace smartid {
//SchemaGraph::SchemaGraph(std::vector<std::string> &&table_names) {
//  auto catalog = Catalog::Instance();
//  for (const auto& name: table_names) {
//    auto table = catalog->GetTable(name);
//    tables_.emplace_back(table);
//    const auto& schema = table->GetSchema();
//    for (uint64_t i = 0; i < schema.NumCols(); i++) {
//      const auto& col = schema.GetColumn(i);
//      for (const auto& ref: col.FK().refs_) {
//        auto ref_table = Catalog::Instance()->GetTable(ref.first);
//        const auto& ref_schema = ref_table->GetSchema();
//        auto ref_col_idx = ref_schema.ColIdx(ref.second);
//        table_graph_[table][ref_table].emplace_back(i, ref_col_idx);
//        std::cout << "FK: "
//                  << name << "." << col.Name() << " ----> "
//                  << ref_table->Name() << "." << ref_schema.GetColumn(ref_col_idx).Name() << std::endl;
//      }
//    }
//  }
//}
//
//bool IsValidComparison(const BinaryCompNode* comp_node, OpType* flipped_comp) {
//  if (comp_node->GetOpType() == OpType::NE) return false;
//  auto lhs_type = comp_node->Child(0)->GetExprType();
//  auto rhs_type = comp_node->Child(1)->GetExprType();
//  auto is_col = [](ExprType e) {
//    return e == ExprType::Column;
//  };
//  auto is_param_or_const = [](ExprType e) {
//    return e == ExprType::Param || e == ExprType::Constant;
//  };
//  // No need to flip.
//  if (is_col(lhs_type) && is_param_or_const(rhs_type)) {
//    *flipped_comp = comp_node->GetOpType();
//    return true;
//  }
//  // Need to flip.
//  if (is_col(rhs_type) && is_param_or_const(lhs_type)) {
//    switch (comp_node->GetOpType()) {
//      case OpType::LT:
//        *flipped_comp = OpType::GT;
//        break;
//      case OpType::LE:
//        *flipped_comp = OpType::GE;
//        break;
//      case OpType::GT:
//        *flipped_comp = OpType::LT;
//        break;
//      case OpType::GE:
//        *flipped_comp = OpType::LE;
//        break;
//      default:
//        ASSERT(false, "Impossible");
//    }
//    return true;
//  }
//  return false;
//}
//
//// TODO: Implement reduction. For now, just consider last elements.
//double ReduceSelectivity(const std::vector<std::pair<uint64_t, uint64_t>>& sels) {
//  auto in_size = sels.back().first;
//  auto out_size = sels.back().second;
//  return static_cast<double>(out_size) / in_size;
//}
//
//void SchemaGraph::ExtractFilters(const ExprNode* expr, std::vector<FilterInfo>* infos, FilterInfo* curr_filter_info) {
//  if (auto e = dynamic_cast<const ColumnNode*>(expr)) {
//    curr_filter_info->col_idx = e->ColIdx();
//    return;
//  }
//  if (auto e = dynamic_cast<const ConstantNode*>(expr)) {
//    curr_filter_info->comp_vals = {e->Val()};
//    curr_filter_info->val_type = e->ValType();
//    return;
//  }
//  if (auto e = dynamic_cast<const ParamNode*>(expr)) {
//    const auto& param_stats = e->GetStats();
//    curr_filter_info->comp_vals = param_stats.vals;
//    curr_filter_info->val_type = param_stats.val_type;
//    return;
//  }
//  if (auto e = dynamic_cast<const BinaryCompNode*>(expr)) {
//    OpType flipped_comp_type;
//    if (!IsValidComparison(e, &flipped_comp_type)) return;
//    ExtractFilters(e->Child(0), infos, curr_filter_info);
//    ExtractFilters(e->Child(1), infos, curr_filter_info);
//    curr_filter_info->comp_type = flipped_comp_type;
//    curr_filter_info->selectivity = ReduceSelectivity(e->GetStats().sels_);
//    infos->emplace_back(*curr_filter_info);
//    std::cout << "FilterInfo: "
//              << curr_filter_info->col_idx << ", "
//              << curr_filter_info->selectivity << std::endl;
//    return;
//  }
//  // TODO(Amadou): Add more cases after implementing logical exprs.
//}
//
//
//// TODO(Amadou): Implement reduction. For now just consider last elements.
//JoinStats ReduceJoinStats(const std::vector<JoinStats>& all_join_stats) {
//  return all_join_stats.back();
//}
//
//
//void SchemaGraph::RecursiveMakeFilterGraph(const PlanNode *node,
//                                           FilterGraph * filter_graph, CompoundFilterGraph * compound_filter_graph,
//                                           TableFilter *curr_filters, TableCompoundFilter * curr_compound_filters,
//                                           const JoinStats* curr_join_stats, FilterInfo* curr_filter_info,
//                                           bool direction) {
//  // Just to shut now clang-tidy
//  if (node == nullptr) return;
//  if (auto n = dynamic_cast<const ScanNode*>(node); n != nullptr) {
//    // TODO(Amadou): Remove this after implementing irregular joins.
//    if (n->GetTable()->Name() == "supplier") return;
//    all_scans_.emplace_back(n);
//    // Append the current filters to those of the scanned table.
//    for (auto& f: *curr_filters) {
//      auto& n_f_filters = (*filter_graph)[n->GetTable()][f.first];
//      // WLOG assume direction=true (probe to build embedding.)
//      // This resets the join stats to closest build.
//      for (auto& info: f.second) {
//        info.join_stats = *curr_join_stats;
//      }
//      n_f_filters.insert(n_f_filters.end(), f.second.begin(), f.second.end());
//    }
//    // Same for compound filters.
//    for (auto& f: *curr_compound_filters) {
//      auto& compound_n_f_filters = (*compound_filter_graph)[n->GetTable()][f.first];
//      for (auto& compound_info: f.second) {
//        compound_info.join_stats = *curr_join_stats;
//      }
//      compound_n_f_filters.insert(compound_n_f_filters.end(), f.second.begin(), f.second.end());
//    }
//
//    // Extract all filter columns from the node.
//    std::vector<FilterInfo> filter_cols;
//    const auto& filters = n->GetFilters();
//    for (const auto& f: filters) ExtractFilters(f, &filter_cols, curr_filter_info);
//    if (filter_cols.empty()) return;
//    // Append those columns to the list of filters of this table.
//    auto& n_filters = (*curr_filters)[n->GetTable()];
//    // TODO(Amadou): Also add string columns after string histograms are implemented.
//    for (const auto& f: filter_cols) {
//      if (f.val_type != SqlType::Varchar) n_filters.emplace_back(f);
//    }
//    // Append compound filters.
//    auto& n_compound_filters = (*curr_compound_filters)[n->GetTable()];
//    CompoundFilterInfo new_compound_info{filter_cols};
//    n_compound_filters.emplace_back(std::move(new_compound_info));
//    return;
//  }
//  if (auto n = dynamic_cast<const HashJoinNode*>(node); n != nullptr) {
//    // WLOG assume direction=true (probe to build embedding).
//    // We are thus trying to reduce the size of built hash tables, so embedding should take into account build selectivity of the closest build.
//    // The build child (named 'second' below) should thus get the new join stats.
//    // But the probe child (named 'first') should get the stats of the closest node where a build occur (i.e. the old join stats).
//
//    // Recursive call order depends on direction.
//    auto first = direction ? n->Child(1) : n->Child(0);
//    auto second = direction ? n->Child(0) : n->Child(1);
//    // Recursive call with current join stats.
//    RecursiveMakeFilterGraph(first, filter_graph, compound_filter_graph, curr_filters, curr_compound_filters, curr_join_stats, curr_filter_info, direction);
//    auto new_join_stats = ReduceJoinStats(n->GetJoinStats());
//    RecursiveMakeFilterGraph(second, filter_graph, compound_filter_graph, curr_filters, curr_compound_filters, &new_join_stats, curr_filter_info, direction);
//    return;
//  }
//
//  // For other node types, reset the list of current filters. As pre-filter may make the final result incorrect.
//  if (node->NumChildren() > 0) {
//    TableFilter new_filters;
//    RecursiveMakeFilterGraph(node->Child(0), filter_graph, compound_filter_graph, &new_filters, curr_compound_filters, curr_join_stats, curr_filter_info, direction);
//  }
//}
//
//void SchemaGraph::FilterGraphs(const PlanNode *node) {
//  // Probe to build embedding.
//  TableFilter probe_table_filter;
//  TableCompoundFilter probe_table_compound_filter;
//  JoinStats probe_join_stats;
//  FilterInfo probe_filter_info;
//  RecursiveMakeFilterGraph(node, &probe_filter_graph_, &probe_compound_filter_graph_, &probe_table_filter, &probe_table_compound_filter, &probe_join_stats, &probe_filter_info, true);
//
//  // Build to probe embedding.
//  TableFilter build_table_filters;
//  TableCompoundFilter build_table_compound_filter;
//  JoinStats build_join_stats;
//  FilterInfo build_filter_info;
//  RecursiveMakeFilterGraph(node, &build_filter_graph_, &build_compound_filter_graph_, &build_table_filters, &build_table_compound_filter, &build_join_stats, &build_filter_info, false);
//}
//
//void SchemaGraph::MakeRanks() {
//  MakeCompoundRanks(&probe_compound_filter_graph_, true);
//  MakeCompoundRanks(&build_compound_filter_graph_, false);
//  std::unordered_map<const Table*, double> total_ranks;
//  MakeTableRank(&probe_filter_graph_, true);
//  MakeTableRank(&build_filter_graph_, false);
//  // Normalize and split
//  // Normalize ranks and scale to get close to 64.
//  auto normalize_split = [&](RankGraph& rank_graph, bool final) {
//    for (auto& p1: rank_graph) {
//      for (auto& p2: p1.second) {
//        for (auto& col_rank_pair: p2.second) {
//          col_rank_pair.second /= total_ranks[p1.first];
//          col_rank_pair.second *= 64 - (compound_probe_rank_graph_[p1.first].size()
//              + compound_build_rank_graph_[p1.first].size());
//          if (final) {
//            std::cout << "Number of bits: "
//                    << p1.first->Name() << ", "
//                    << p2.first->Name() << ", "
//                    << col_rank_pair.first << ", "
//                    << col_rank_pair.second << " bits" << std::endl;
//          }
//        }
//      }
//    }
//  };
//  auto remove_underscorer = [&](RankGraph& rank_graph) {
//    for (auto& p1: rank_graph) {
//      for (auto &p2: p1.second) {
//        std::erase_if(p2.second, [&](const auto &col_rank_pair) {
//          return static_cast<uint64_t>(col_rank_pair.second) < 2;
//        });
//      }
//    }
//  };
//  auto recompute_total = [&](RankGraph& rank_graph) {
//    for (auto& p1: rank_graph) {
//      for (auto &p2: p1.second) {
//        for (auto &col_rank_pair: p2.second) {
//          total_ranks[p1.first] += col_rank_pair.second;
//        }
//      }
//    }
//  };
//  recompute_total(probe_rank_graph_);
//  recompute_total(build_rank_graph_);
//  normalize_split(probe_rank_graph_, true);
//  normalize_split(build_rank_graph_, true);
//  std::cout << "Removing Underscorer" << std::endl;
//  remove_underscorer(probe_rank_graph_);
//  remove_underscorer(build_rank_graph_);
//  total_ranks.clear();
//  recompute_total(probe_rank_graph_);
//  recompute_total(build_rank_graph_);
//  std::cout << "Renormalizing" << std::endl;
//  normalize_split(probe_rank_graph_, true);
//  normalize_split(build_rank_graph_, true);
//}
//
//void SchemaGraph::MakeCompoundRanks(CompoundFilterGraph *filter_graph, bool direction) {
//  std::unordered_map<const Table*, uint64_t> allocated_bits;
//  for (auto& p1: *filter_graph) {
//    auto table1 = p1.first;
//    for (auto &p2: p1.second) {
//      auto table2 = p2.first;
//      for (auto& compound: p2.second) {
//        if (compound.estimated_reduction > 0.5) {
//          if (direction) {
//            double estimated_reduction = compound.estimated_reduction;
//            double build_sel = static_cast<double>(compound.join_stats.build_out) / compound.join_stats.build_in;
//            double maximum_reduction = 1.0 - build_sel;
//            // Probe-to-build embedding can reduce both build and probe time.
//            double total_time = compound.join_stats.build_hash_time + compound.join_stats.insert_time
//                + compound.join_stats.probe_hash_time + compound.join_stats.probe_time;
//            double cache_factor = 1.0;
//            if (compound.join_stats.ht_size > CACHE_SIZE && compound.join_stats.ht_size * build_sel <= CACHE_SIZE) {
//              cache_factor = CACHE_FACTOR;
//            }
//            if (cache_factor == CACHE_FACTOR || (maximum_reduction > 0.5)) {
//              auto rank = estimated_reduction * maximum_reduction * total_time * cache_factor;
//              std::cout << "COMPOUND PROBE Max, Est, Cache: "
//                        << table1->Name() << ", "
//                        << table2->Name() << ", "
//                        << maximum_reduction << ", "
//                        << estimated_reduction << ", "
//                        << cache_factor << ", "
//                        << rank << std::endl;
//              compound.rank = rank;
//              compound.table = table2;
//              compound_probe_rank_graph_[table1].emplace_back(compound);
//            }
//          } else {
//            double estimated_reduction = compound.estimated_reduction;
//            double probe_sel = static_cast<double>(compound.join_stats.probe_out) / compound.join_stats.probe_in;
//            double maximum_reduction = 1.0 - probe_sel;
//            // Build-to-probe embedding only reduces probe time.
//            double total_time = compound.join_stats.probe_hash_time + compound.join_stats.probe_time;
//            auto rank = estimated_reduction * maximum_reduction * total_time;
//            if (maximum_reduction > 0.1 && estimated_reduction > 0.1) {
//              std::cout << "COMPOUND BUILD Max, Est: "
//                        << table1->Name() << ", "
//                        << table2->Name() << ", "
//                        << maximum_reduction << ", "
//                        << estimated_reduction << ", "
//                        << rank << std::endl;
//
//              compound.rank = rank;
//              compound.table = table2;
//              compound_build_rank_graph_[table1].emplace_back(compound);
//            }
//          }
//        }
//      }
//    }
//  }
//
//  auto& compound_rank_graph = direction ? compound_probe_rank_graph_ : compound_build_rank_graph_;
//  for (auto& p1: compound_rank_graph) {
//    auto table1 = p1.first;
//    auto& compounds = p1.second;
//    std::sort(compounds.begin(), compounds.end(), [](const auto& c1, const auto& c2) {
//      return c1.rank > c2.rank;
//    });
//    // TODO(Amadou): Uncomment to pick for most promising ones.
//    // compounds.resize(std::min(4ul, compounds.size()));
//  }
//}
//
//void SchemaGraph::MakeTableRank(const FilterGraph *filter_graph, bool direction) {
//  // Compute the rank of each (table1, table2, column) triplet.
//  for (const auto& p1: *filter_graph) {
//    auto table1 = p1.first;
//    for (const auto& p2: p1.second) {
//      auto table2 = p2.first;
//      for (const auto& info: p2.second) {
//        if (direction) {
//          double estimated_reduction = 1.0 - info.selectivity;
//          double build_sel = static_cast<double>(info.join_stats.build_out) / info.join_stats.build_in;
//          double maximum_reduction = 1.0 - build_sel;
//          // Probe-to-build embedding can reduce both build and probe time.
//          double total_time = info.join_stats.build_hash_time + info.join_stats.insert_time + info.join_stats.probe_hash_time + info.join_stats.probe_time;
//          double cache_factor = 1.0;
//          if (info.join_stats.ht_size > CACHE_SIZE && info.join_stats.ht_size * build_sel <= CACHE_SIZE) {
//            cache_factor = CACHE_FACTOR;
//          }
//          if (cache_factor == CACHE_FACTOR || (maximum_reduction > 0.1 && estimated_reduction > 0.1)) {
//            std::cout << "PROBE Max, Est, Cache: "
//                      << table1->Name() << ", "
//                      << table2->Name() << ", "
//                      << maximum_reduction << ", "
//                      << estimated_reduction << ", "
//                      << cache_factor << std::endl;
//            auto rank = estimated_reduction * maximum_reduction * total_time * cache_factor;
//            probe_rank_graph_[table1][table2][info.col_idx] += rank;
//          }
//        } else {
//          double estimated_reduction = 1.0 - info.selectivity;
//          double probe_sel = static_cast<double>(info.join_stats.probe_out) / info.join_stats.probe_in;
//          double maximum_reduction = 1.0 - probe_sel;
//          // Build-to-probe embedding only reduces probe time.
//          double total_time = info.join_stats.probe_hash_time + info.join_stats.probe_time;
//          if (maximum_reduction > 0.1 && estimated_reduction > 0.1) {
//            std::cout << "BUILD Max, Est: "
//                      << table1->Name() << ", "
//                      << table2->Name() << ", "
//                      << maximum_reduction << ", "
//                      << estimated_reduction << std::endl;
//            auto rank = estimated_reduction * maximum_reduction * total_time;
//            build_rank_graph_[table1][table2][info.col_idx] += rank;
//          }
//        }
//      }
//    }
//  }
//}
//
//void SchemaGraph::MakeCompoundInfos(const CompoundFilterInfo &compound, EmbeddingInfo *origin_info, uint64_t* curr_offset, bool direction) {
//  uint64_t bit_idx = *curr_offset;
//  *curr_offset += 1;
//  std::vector<uint64_t> col_idxs;
//  std::vector<SqlType> col_types;
//  std::vector<OpType> comp_types;
//  std::vector<Value> vals;
//  for (const auto& f: compound.compound_filters) {
//    col_idxs.emplace_back(f.col_idx);
//    col_types.emplace_back(f.val_type);
//    comp_types.emplace_back(f.comp_type);
//    vals.emplace_back(f.comp_vals[0]);
//  }
//  if (direction) {
//    origin_info->compound_probe_infos.emplace_back(CompoundEmbeddingInfo(std::move(col_idxs), std::move(col_types), std::move(comp_types), std::move(vals), bit_idx));
//  } else {
//    origin_info->compound_build_infos.emplace_back(CompoundEmbeddingInfo(std::move(col_idxs), std::move(col_types), std::move(comp_types), std::move(vals), bit_idx));
//  }
//}
//
//void SchemaGraph::MakeColInfos(const RankList &rank_list, EmbeddingInfo *origin_info, uint64_t* curr_offset, bool direction) {
//  for (const auto& p: rank_list) {
//    auto col_idx = p.first;
//    auto num_bits = static_cast<uint64_t>(p.second);
//    auto bit_offet = *curr_offset;
//    *curr_offset += num_bits;
//    if (direction) {
//      origin_info->probe_col_infos.emplace_back(EmbeddingColInfo{col_idx, HistogramType::EquiWidth, num_bits, bit_offet});
//    } else {
//      origin_info->build_col_infos.emplace_back(EmbeddingColInfo{col_idx, HistogramType::EquiWidth, num_bits, bit_offet});
//    }
//  }
//}
//
//
//void SchemaGraph::MakeJoinPath(const Table *start_table, const Table *end_table, EmbeddingInfo *origin_info) {
//  // Attempts to create a build->probe order from start to end.
//  std::cout << "Search START" << std::endl;
//  auto min_path = RecursiveFindJoinPath(start_table, end_table, origin_info);
//  if (min_path.empty()) {
//    // Attempts to create a build->probe order from end to start.
//    min_path = RecursiveFindJoinPath(end_table, start_table, origin_info);
//    // TODO(Amadou): Implement zig zag.
//    if (min_path.empty()) {
//      return;
//    }
//  }
//  std::cout << "Search END" << std::endl;
//  // Reverse to get probe->build order.
//  std::reverse(min_path.begin(), min_path.end());
//
//  origin_info->interm_tables.resize(min_path.size());
//  for (uint64_t i = 0; i < min_path.size() - 1; i++) {
//    auto curr_probe_table = min_path[i];
//    auto curr_build_table = min_path[i+1];
//    std::vector<uint64_t> curr_probe_keys;
//    std::vector<uint64_t> curr_build_keys;
//    for (const auto&fk: table_graph_[curr_probe_table][curr_build_table]) {
//      curr_probe_keys.emplace_back(fk.first);
//      curr_build_keys.emplace_back(fk.second);
//    }
//    origin_info->interm_tables[i].table = curr_probe_table;
//    origin_info->interm_tables[i].probe_keys = curr_probe_keys;
//    origin_info->interm_tables[i+1].table = curr_build_table;
//    origin_info->interm_tables[i+1].build_keys = curr_build_keys;
//  }
//  // Reverse again for build->probe order.
//  std::reverse(origin_info->interm_tables.begin(), origin_info->interm_tables.end());
//
//  // Debug Info
//  for (uint64_t i = 0; i < origin_info->interm_tables.size() - 1; i++) {
//    const auto& curr_build = origin_info->interm_tables[i];
//    const auto& curr_probe = origin_info->interm_tables[i+1];
//
//    std::cout << "Join of: " << curr_build.table->Name() << ", " << curr_probe.table->Name() << std::endl;
//    std::cout << "Build Keys: ";
//    for (auto k: curr_build.build_keys) {
//      auto col_name = curr_build.table->GetSchema().GetColumn(k).Name();
//      std::cout << curr_build.table->Name() << "." << col_name << ", ";
//    }
//    std::cout << std::endl;
//    std::cout << "Probe Keys: ";
//    for (auto k: curr_probe.probe_keys) {
//      auto col_name = curr_probe.table->GetSchema().GetColumn(k).Name();
//      std::cout << curr_probe.table->Name() << "." << col_name << ", ";
//    }
//    std::cout << std::endl;
//  }
//  std::cout << "DONE!" << std::endl;
//}
//
//std::vector<const Table*> SchemaGraph::RecursiveFindJoinPath(const Table *build_table, const Table* curr_table, EmbeddingInfo *origin_info) {
//  if (curr_table == build_table) {
//    return {curr_table};
//  }
//  std::vector<const Table*> curr_min_path;
//  for (const auto& next_p: table_graph_[curr_table]) {
//    auto next_table = next_p.first;
//    if (auto next_path = RecursiveFindJoinPath(build_table, next_table, origin_info); !next_path.empty()) {
//      if (curr_min_path.empty() || next_path.size() < curr_min_path.size()) curr_min_path = next_path;
//    }
//  }
//  if (!curr_min_path.empty()) {
//    curr_min_path.emplace_back(curr_table);
//  }
//  return curr_min_path;
//}
//
//// I am too lazy to write hash function for a pair.
//// So I use this to fake a map from pair -> embedding
//using PairMap = std::unordered_map<const Table*, std::unordered_map<const Table*, std::unique_ptr<EmbeddingInfo>>>;
//
//EmbeddingInfo* GetEmbedding(PairMap& pair_map, const Table* t1, const Table* t2) {
//  if (pair_map.contains(t1) && pair_map.at(t1).contains(t2)) return pair_map.at(t1).at(t2).get();
//  if (pair_map.contains(t2) && pair_map.at(t2).contains(t1)) return pair_map.at(t2).at(t1).get();
//  auto new_embedding_info = std::make_unique<EmbeddingInfo>();
//  auto ret = new_embedding_info.get();
//  pair_map[t1][t2] = std::move(new_embedding_info);
//  return ret;
//}
//
//void SchemaGraph::MakeEmbeddingInfo() {
//  PairMap computed_infos;
//  std::unordered_map<const Table*, uint64_t> allocated_bits;
//  // Compound probe
//  std::cout << "COMPOUND PROBE" << std::endl;
//  for (const auto& p1: compound_probe_rank_graph_) {
//    auto table1 = p1.first;
//    const auto& compounds = p1.second;
//    for (const auto& compound: compounds) {
//      auto embedding_info = GetEmbedding(computed_infos, table1, compound.table);
//      MakeCompoundInfos(compound, embedding_info, &allocated_bits[table1], true);
//      if (embedding_info->interm_tables.empty()) MakeJoinPath(table1, compound.table, embedding_info);
//    }
//  }
//
//  std::cout << "COMPOUND BUILD" << std::endl;
//  for (const auto& p1: compound_build_rank_graph_) {
//    auto table1 = p1.first;
//    const auto& compounds = p1.second;
//    for (const auto& compound: compounds) {
//      auto embedding_info = GetEmbedding(computed_infos, table1, compound.table);
//      MakeCompoundInfos(compound, embedding_info, &allocated_bits[table1], false);
//      if (embedding_info->interm_tables.empty()) MakeJoinPath(table1, compound.table, embedding_info);
//    }
//  }
//
//  std::cout << "SINGLE PROBE" << std::endl;
//  for (const auto& p1: probe_rank_graph_) {
//    auto table1 = p1.first;
//    for (const auto& p2: p1.second) {
//      auto table2 = p2.first;
//      auto embedding_info = GetEmbedding(computed_infos, table1, table2);
//      MakeColInfos(p2.second, embedding_info, &allocated_bits[table1], true);
//      if (embedding_info->interm_tables.empty()) MakeJoinPath(table1, table2, embedding_info);
//    }
//  }
//
//  std::cout << "SINGLE BUILD" << std::endl;
//  for (const auto& p1: build_rank_graph_) {
//    auto table1 = p1.first;
//    for (const auto& p2: p1.second) {
//      auto table2 = p2.first;
//      auto embedding_info = GetEmbedding(computed_infos, table1, table2);
//      MakeColInfos(p2.second, embedding_info, &allocated_bits[table1], false);
//      if (embedding_info->interm_tables.empty()) MakeJoinPath(table1, table2, embedding_info);
//    }
//  }
//
//
//  std::cout << "COMPUTED" << std::endl;
//  for (const auto& p1: computed_infos) {
//    for (const auto& p2: p1.second) {
//      // TODO(Amadou): Remove if condition once irregular join paths are implemented.
//      if (!p2.second->interm_tables.empty()) embedding_infos_.emplace_back(std::move(*p2.second));
//    }
//  }
//}
//
//void SchemaGraph::PrintFilterGraphs() const {
//  std::cout << "==================== Probe Graph ======================" << std::endl;
//  for (const auto& p1: probe_filter_graph_) {
//    auto build_table = p1.first;
//    for (const auto& p2: p1.second) {
//      auto probe_table = p2.first;
//      std::cout << build_table->Name() << ", " << probe_table->Name() << std::endl;
//    }
//  }
//
//  std::cout << "=================== Compound Probe Graph ==================== " << std::endl;
//  for (const auto& p1: probe_compound_filter_graph_) {
//    auto build_table = p1.first;
//    for (const auto& p2: p1.second) {
//      auto probe_table = p2.first;
//      std::cout << build_table->Name() << ", " << probe_table->Name() << std::endl;
//      const auto& compounds = p2.second;
//      for (const auto& compound: compounds) {
//        std::cout << "Selectivity: " << compound.estimated_reduction << std::endl;
//        for (const auto& f: compound.compound_filters) {
//          std::cout << probe_table->GetSchema().GetColumn(f.col_idx).Name() << ", ";
//        }
//        std::cout << std::endl;
//      }
//    }
//  }
//}
//
//void SchemaGraph::ResetEmbeddings() {
//  for (auto table: tables_) {
//    for (uint64_t i = 0; i < table->NumBlocks(); i++) {
//      auto block = table->MutableBlockAt(i);
//      std::memset(block->MutableColumnData(table->EmbeddingIdx()), 0, block->NumElems() * sizeof(uint64_t));
//    }
//  }
//}
//}
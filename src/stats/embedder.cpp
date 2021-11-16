//#include "stats/embedder.h"
//#include "execution/execution_factory.h"
//
//namespace smartid {
//
//namespace {
//// TODO(Amadou): Place in some utility file.
//template<typename VectorReader>
//void ReadVPs(PlanExecutor *executor, VectorReader reader) {
//  const VectorProjection *vp;
//  while ((vp = executor->Next())) {
//    reader(vp->GetFilter(), vp);
//  }
//}
//}
//
//
//void Embedder::BuildEmbedding(EmbeddingInfo& embedding_info) {
//  auto node = embedding_info.join_node;
//
//  Vector indices(SqlType::Int64);
//  auto build_table = Catalog::Instance()->GetTable(embedding_info.interm_tables.front().table->Name());
//  auto probe_table = Catalog::Instance()->GetTable(embedding_info.interm_tables.back().table->Name());
//  auto build_stats = Catalog::Instance()->GetTableStats(build_table);
//  auto probe_stats = Catalog::Instance()->GetTableStats(probe_table);
//  auto build_embedding_col_idx = build_table->EmbeddingIdx();
//  auto probe_embedding_col_idx = probe_table->EmbeddingIdx();
//  // Helper to embed single columns.
//  auto embedding_single_col = [&](Table* table, const Filter* filter, const VectorProjection* vp,
//                                  const TableStats* table_stats, const EmbeddingColInfo& col_info,
//                                  uint64_t id_col_idx, uint64_t embedding_col_idx) {
//    auto vec = vp->VectorAt(col_info.vp_idx);
//    auto id_vec = vp->VectorAt(id_col_idx);
//    auto id_data = id_vec->DataAs<uint64_t>();
//    auto hist = table_stats->GetHistogram(col_info.col_idx, col_info.histogram_type);
//    hist->BitIndices(filter, vec, &indices, col_info.bit_offset, col_info.num_bits);
//    auto indices_data = indices.DataAs<uint64_t>();
//    filter->Map([&](sel_t i) {
//      auto id = id_data[i];
//      auto row_idx = id & 0xFFFFFFull;
//      auto block_idx = id >> 32ull;
//      auto block = table->MutableBlockAt(block_idx);
//      block->MutableColumnDataAs<uint64_t>(embedding_col_idx)[row_idx] |= (1ull << indices_data[i]);
//    });
//  };
//  // Helper to embed compounds.
//  auto embedding_compounds = [&](Table* table, const Filter* filter, const VectorProjection* vp,
//                                 const CompoundEmbeddingInfo& compound_info,
//                                 uint64_t id_col_idx, uint64_t embedding_col_idx) {
//    // Keep values that exactly satisfy filters.
//    Filter one_elem_filter;
//    Filter result_filter;
//    result_filter.SetFrom(filter);
//    one_elem_filter.Reset(1, FilterMode::SelVecFull);
//    for (uint64_t i = 0; i < compound_info.vp_idxs.size(); i++) {
//      auto lhs = vp->VectorAt(compound_info.vp_idxs[i]);
//      Vector rhs(compound_info.col_types[i], 1);
//      VectorOps::InitVector(&one_elem_filter, compound_info.vals[i], compound_info.col_types[i], &rhs);
//      VectorOps::BinaryCompVector(lhs, &rhs, &result_filter, compound_info.comp_types[i]);
//    }
//    // Set bit to one
//    auto id_vec = vp->VectorAt(id_col_idx);
//    auto id_data = id_vec->DataAs<uint64_t>();
//    result_filter.Map([&](sel_t i) {
//      auto id = id_data[i];
//      auto row_idx = id & 0xFFFFFFull;
//      auto block_idx = id >> 32ull;
//      auto block = table->MutableBlockAt(block_idx);
//      block->MutableColumnDataAs<uint64_t>(embedding_col_idx)[row_idx] |= (1ull << compound_info.bit_idx);
//    });
//  };
//  // Read output rows one by one and embed values.
//  auto vector_reader = [&](const Filter* filter, const VectorProjection* vp) {
//    // Probe-to-build single columns
//    for (const auto& e: embedding_info.probe_col_infos) {
//      embedding_single_col(build_table, filter, vp,
//                           probe_stats, e,
//                           embedding_info.build_id_vp_idx, build_embedding_col_idx);
//    }
//    // build-to-probe single columns
//    for (const auto& e: embedding_info.build_col_infos) {
//      embedding_single_col(probe_table, filter, vp,
//                           build_stats, e,
//                           embedding_info.probe_id_vp_idx, probe_embedding_col_idx);
//    }
//    // probe-to-build compound
//    for (const auto& c: embedding_info.compound_probe_infos) {
//      embedding_compounds(build_table, filter, vp, c, embedding_info.build_id_vp_idx, build_embedding_col_idx);
//    }
//    // build-to-probe compound
//    for (const auto& c: embedding_info.compound_build_infos) {
//      embedding_compounds(probe_table, filter, vp, c, embedding_info.probe_id_vp_idx, probe_embedding_col_idx);
//    }
//
//  };
//
//  // Run
//  auto executor = ExecutionFactory::MakePlanExecutor(node, nullptr);
//  ReadVPs(executor.get(), vector_reader);
//}
//
//void Embedder::UpdateScan(ExecutionFactory* factory, const ExecutionContext* ctx, const ScanNode *origin_scan, ScanNode *target_scan) {
//  for (const auto& info: GetEmbeddingInfos()) {
//    bool direction;
//    if (origin_scan->GetTable() == info.GetProbeTable() && target_scan->GetTable() == info.GetBuildTable()) {
////      std::cout << "Probe-to-Build Embedder from " << origin_scan->GetTable()->Name() << " to " << target_scan->GetTable()->Name() << std::endl;
//      direction = true;
//    } else if (origin_scan->GetTable() == info.GetBuildTable() && target_scan->GetTable() == info.GetProbeTable()) {
////      std::cout << "Build-to-probe Embedder from " << origin_scan->GetTable()->Name() << " to " << target_scan->GetTable()->Name() << std::endl;
//      direction = false;
//    } else {
//      continue;
//    }
//    auto compound_mask = info.GetCompoundMask(ctx, origin_scan->GetFilters(), direction);
//    auto single_masks = info.GetSingleMasks(ctx, origin_scan->GetFilters(), direction);
//    auto mutable_target_scan = const_cast<ScanNode*>(target_scan);
//    std::vector<ExprNode*> embedding_checks;
//    if (compound_mask != 0) {
////      std::cout << "Has compound mask." << std::endl;
//      embedding_checks.emplace_back(factory->MakeEmbeddingCheck(target_scan->GetTable(), compound_mask));
//    }
//    for (const auto& single_mask: single_masks) {
////      std::cout << "Has single mask." << std::endl;
//      embedding_checks.emplace_back(factory->MakeEmbeddingCheck(target_scan->GetTable(), single_mask));
//    }
//    mutable_target_scan->AddEmbeddingFilters(std::move(embedding_checks));
//  }
//}
//}
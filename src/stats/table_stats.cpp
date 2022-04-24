#include "stats/table_stats.h"
#include "execution/execution_factory.h"

namespace smartid {

namespace {


/**
 * Templated collection of min, max and count.
 */
template<typename T>
void TemplatedReadMinMaxCount(ColumnStats* stats, PlanExecutor* executor) {
  auto reader = [&](const VectorProjection* vp, sel_t i) {
    stats->count_ = vp->VectorAt(0)->DataAs<int64_t>()[0];
    stats->min_ = vp->VectorAt(1)->DataAs<T>()[0];
    stats->max_ = vp->VectorAt(2)->DataAs<T>()[0];
  };
  ReadRows(executor, reader);
}

#define TEMPLATED_READ_MIN_MAX_COUNT(sql_type, cpp_type, ...) \
  case SqlType::sql_type: {                                 \
    TemplatedReadMinMaxCount<cpp_type>(stats, executor);                \
    break;                                                    \
  }

/**
 * Collect min, max and count.
 */
void CollectMinMaxCount(const Table* table, ColumnNode* col, SqlType col_type, ColumnStats * stats, ExecutionFactory* factory) {
  std::vector<ExprNode*> scan_proj{col};
  std::vector<ExprNode*> scan_filter{};
  auto scan_node = factory->MakeScan(table, std::move(scan_proj), std::move(scan_filter));

  std::vector<std::pair<uint64_t, AggType>> aggs = {
      {0, AggType::COUNT},
      {0, AggType::MIN},
      {0, AggType::MAX},
  };
  auto static_aggregate_node = factory->MakeStaticAggregation(scan_node, std::move(aggs));

  // Retrieve results.
  auto executor = factory->MakeStoredPlanExecutor(static_aggregate_node, nullptr);
  switch (col_type) {
    SQL_TYPE(TEMPLATED_READ_MIN_MAX_COUNT, NOOP)
    case SqlType::Date:
      TemplatedReadMinMaxCount<Date>(stats, executor);                \
      break;
    default:
      std::cerr << "CollectStats only supported for arithmetic types!!!!!" << std::endl;
  }
}

/**
 * Collect COUNT(DISTINCT col)
 */
void CollectCountDistinct(const Table* table, ColumnNode* col, ColumnStats * stats, ExecutionFactory* factory) {
  std::vector<ExprNode*> scan_proj{col};
  std::vector<ExprNode*> scan_filter{};
  auto scan_node = factory->MakeScan(table, std::move(scan_proj), std::move(scan_filter));

  std::vector<uint64_t> group_bys{0};
  std::vector<std::pair<uint64_t, AggType>> aggs{};
  auto hash_aggregation_node = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));

  aggs = {
      {0, AggType::COUNT}
  };
  auto static_aggregate_node = factory->MakeStaticAggregation(hash_aggregation_node, std::move(aggs));

  // Retrieve result
  auto executor = factory->MakeStoredPlanExecutor(static_aggregate_node, nullptr);
  auto reader = [&](const VectorProjection* vp, sel_t i) {
    stats->count_distinct_ = vp->VectorAt(0)->DataAs<int64_t>()[0];
  };

  ReadRows(executor, reader);
}

/**
 * SELECT col, COUNT(col) AS freq FROM table ORDER BY col.
 */
void CollectByVal(const Table* table, ColumnNode* col, ColumnStats * stats, ExecutionFactory* factory) {
  std::vector<ExprNode*> scan_proj{col};
  std::vector<ExprNode*> scan_filter{};
  auto scan_node = factory->MakeScan(table, std::move(scan_proj), std::move(scan_filter));

  std::vector<uint64_t> group_bys{0};
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {0, AggType::COUNT}
  };
  auto hash_aggregation_node = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));

  std::vector<std::pair<uint64_t, SortType>> sort_cols {
      {0, SortType::ASC}
  };
  auto sort_node = factory->MakeSort(hash_aggregation_node, std::move(sort_cols));


  // Execute
  auto executor = factory->MakeStoredPlanExecutor(sort_node, nullptr);
  // Note: relies on the fact that SortNode materializes all its results at once.
  auto vp = executor->Next();
  stats->by_freq_.vals_ = vp->VectorAt(0);
  stats->by_freq_.weights_ = vp->VectorAt(1);
}

/**
 * SELECT col, SUM(usage) AS total_usage FROM table ORDER BY col.
 */
void CollectByUsage(const Table* table, ColumnNode* col, ColumnStats * stats, ExecutionFactory* factory) {
  auto usage_col = ColumnNode(table->UsageIdx());
  std::vector<ExprNode*> scan_proj{col, &usage_col};
  std::vector<ExprNode*> scan_filter{};
  auto scan_node = factory->MakeScan(table, std::move(scan_proj), std::move(scan_filter));

  std::vector<uint64_t> group_bys{0}; // group by col
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {1, AggType::SUM} // Sum usage
  };
  auto hash_aggregation_node = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));


  std::vector<std::pair<uint64_t, SortType>> sort_cols {
      {0, SortType::ASC} // sort by col
  };
  auto sort_node = factory->MakeSort(hash_aggregation_node, std::move(sort_cols));

  // Execute
  auto executor = factory->MakeStoredPlanExecutor(sort_node, nullptr);
  // Note: relies on the fact that SortNode materializes all its results at once.
  auto vp = executor->Next();
  stats->by_usage_.vals_ = vp->VectorAt(0);
  stats->by_usage_.weights_ = vp->VectorAt(1);
}

/**
 * Collect all the statistics of the given column.
 */
void CollectStats(const Table* table, uint64_t col_idx, SqlType col_type, ColumnStats* stats, ExecutionFactory* factory) {
  ColumnNode col(col_idx);
  CollectMinMaxCount(table, &col, col_type, stats, factory);
  CollectCountDistinct(table, &col, stats, factory);
  CollectByVal(table, &col, stats, factory);
  CollectByUsage(table, &col, stats, factory);
}
}

void TableStats::BuildHistograms(const std::vector<uint64_t>& wanted_cols, const std::vector<HistogramType>& wanted_hist_types) {
  const auto &schema = table_->GetSchema();
  ExecutionFactory factory;
  for (const auto& i: wanted_cols) {
    auto col_type = schema.GetColumn(i).Type();
    if (col_type == SqlType::Varchar) continue;

    ColumnStats stats;
    CollectStats(table_, i, col_type, &stats, &factory);

    for (const auto &histogram_type: wanted_hist_types) {
      auto hist = BuildHistogram(i, schema.GetColumn(i).Type(), histogram_type, stats);
      histograms_[i][histogram_type] = std::move(hist);
    }
  }
}

const Histogram * TableStats::GetHistogram(uint64_t col_idx, HistogramType histogram_type) const {
  return histograms_.at(col_idx).at(histogram_type).get();
}

std::unique_ptr<Histogram> TableStats::BuildHistogram(uint64_t col_idx,
                                                      SqlType sql_type,
                                                      HistogramType histogram_type, const ColumnStats& stats) {
  switch (histogram_type) {
    case HistogramType::EquiWidth:
      return std::make_unique<EquiWidthHistogram>(64, sql_type, stats);
    case HistogramType::EquiDepthFreq:
      return std::make_unique<EquiDepthHistogram>(64, sql_type, stats, true);
    case HistogramType::CompressedFreq:
      return std::make_unique<CompressedHistogram>(64, sql_type, stats, true);
    case HistogramType::EquiDepthUsage:
      return std::make_unique<EquiDepthHistogram>(64, sql_type, stats, false);
    case HistogramType::CompressedUsage:
      return std::make_unique<CompressedHistogram>(64, sql_type, stats, false);
    case HistogramType::MaxDiff:
      return std::make_unique<MaxDiffHistogram>(64, sql_type, stats);
  }
}

}
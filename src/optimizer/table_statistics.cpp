#include "optimizer/table_statistics.h"
#include "optimizer/histogram.h"
#include "execution/executors/scan_executor.h"
#include "execution/executors/static_aggr_executor.h"
#include "execution/executors/hash_aggr_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/execution_factory.h"
#include "storage/vector.h"
#include "storage/vector_projection.h"
#include "storage/filter.h"
#include "common/catalog.h"
#include "common/info_store.h"
#include "storage/table.h"
#include "optimizer/workload_metadata.h"
#include <random>
#include <fstream>
#include <filesystem>
#include <fmt/core.h>
#include <fmt/ranges.h>


namespace smartid {

// TODO(Amadou): Place in some utility file.
template<typename RowReader>
void ReadRows(PlanExecutor *executor, RowReader reader, const Bitmap* filter=nullptr) {
  const VectorProjection *vp;
  while ((vp = executor->Next())) {
    if (filter != nullptr) {
      filter->Map([&](sel_t i) {
        reader(vp, i);
      });
    } else {
      vp->GetFilter()->Map([&](sel_t i) {
        reader(vp, i);
      });
    }
  }
}

/**
 * Templated collection of min, max and count.
 */
template<typename T>
void TemplatedReadMinMaxCount(PlanExecutor* executor, ColumnStats* column_stats) {
  auto reader = [&](const VectorProjection* vp, sel_t i) {
    column_stats->count_ = vp->VectorAt(0)->DataAs<int64_t>()[0];
    column_stats->min_ = vp->VectorAt(1)->DataAs<T>()[0];
    column_stats->max_ = vp->VectorAt(2)->DataAs<T>()[0];
  };
  ReadRows(executor, reader);
}

#define TEMPLATED_READ_MIN_MAX_COUNT(sql_type, cpp_type, ...) \
  case SqlType::sql_type: {                                 \
  TemplatedReadMinMaxCount<cpp_type>(executor, column_stats);                \
    break;                                                    \
  }

/**
 * Templated collection of min, max and count.
 */
template<typename T>
void TemplatedSample(PlanExecutor* executor, ColumnStats* column_stats, double sample_rate, std::default_random_engine& generator, std::uniform_real_distribution<double>& distribution) {
  auto reader = [&](const VectorProjection* vp, sel_t i) {
    if (distribution(generator) < sample_rate) {
      column_stats->value_samples_.emplace_back(Value(vp->VectorAt(0)->DataAs<T>()[i]));
    }
  };
  ReadRows(executor, reader);
}

#define TEMPLATED_READ_MIN_MAX_COUNT(sql_type, cpp_type, ...) \
  case SqlType::sql_type: {                                 \
  TemplatedReadMinMaxCount<cpp_type>(executor, column_stats);                \
    break;                                                    \
  }

#define TEMPLATED_SAMPLE_VALUES(sql_type, cpp_type, ...) \
  case SqlType::sql_type: {                                 \
  TemplatedSample<cpp_type>(executor, column_stats, sample_rate, generator, distribution);                \
    break;                                                    \
  }




void ComputeMinMaxCount(ExecutionFactory* factory, Table* table, ColumnStats* column_stats) {
  ScanNode* scan_node;
  {
    // Scans
    std::vector<uint64_t> cols_to_read{uint64_t(column_stats->col_idx)};
    auto col_node = factory->MakeCol(0);
    std::vector<ExprNode*> projs;
    projs.emplace_back(col_node);
    std::vector<ExprNode*> filters{};
    scan_node = factory->MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  StaticAggregateNode* static_agg;
  {
    std::vector<std::pair<uint64_t, AggType>> aggs = {
        {0, AggType::COUNT},
        {0, AggType::MIN},
        {0, AggType::MAX},
    };
    static_agg = factory->MakeStaticAggregation(scan_node, std::move(aggs));
  }


  // Retrieve results.
  auto executor = factory->MakeStoredPlanExecutor(static_agg, nullptr);
  switch (column_stats->col_type) {
    SQL_TYPE(TEMPLATED_READ_MIN_MAX_COUNT, NOOP)
    case SqlType::Date:
      TemplatedReadMinMaxCount<Date>(executor, column_stats);
      break;
    default: {
      ASSERT(false, "CollectStats only supported for arithmetic types");
    }
  }
}

void ComputeNumDisctinctForCol(ExecutionFactory* factory, Table* table, ColumnStats* column_stats) {
  ScanNode* scan_node;
  {
    // Scans
    std::vector<uint64_t> cols_to_read{uint64_t(column_stats->col_idx)};
    auto col_node = factory->MakeCol(0);
    std::vector<ExprNode*> projs;
    projs.emplace_back(col_node);
    std::vector<ExprNode*> filters{};
    filters.emplace_back(factory->MakeNonNull(col_node));
    scan_node = factory->MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  HashAggregationNode* hash_agg;
  {
    std::vector<uint64_t> group_bys{0};
    std::vector<std::pair<uint64_t, AggType>> aggs{};
    hash_agg = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));
  }

  StaticAggregateNode* static_agg;
  {
    std::vector<std::pair<uint64_t, AggType>> aggs{
        {0, AggType::COUNT},
    };
    static_agg = factory->MakeStaticAggregation(hash_agg, std::move(aggs));
  }


  // Retrieve result
  auto executor = factory->MakeStoredPlanExecutor(static_agg, nullptr);
  auto reader = [&](const VectorProjection* vp, sel_t i) {
    column_stats->count_distinct_ = vp->VectorAt(0)->DataAs<int64_t>()[0];
  };
  ReadRows(executor, reader);
}

void ComputeMaxFreq(ExecutionFactory* factory, Table* table, ColumnStats* column_stats) {
  ScanNode* scan_node;
  {
    // Scans
    std::vector<uint64_t> cols_to_read{uint64_t(column_stats->col_idx)};
    auto col_node = factory->MakeCol(0);
    std::vector<ExprNode*> projs;
    projs.emplace_back(col_node);
    std::vector<ExprNode*> filters{};
    filters.emplace_back(factory->MakeNonNull(col_node));
    scan_node = factory->MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  HashAggregationNode* hash_agg;
  {
    std::vector<uint64_t> group_bys{0};
    std::vector<std::pair<uint64_t, AggType>> aggs{
        {0, AggType::COUNT},
    };
    hash_agg = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));
  }
  SortNode* sort_node;
  {
    std::vector<std::pair<uint64_t, SortType>> sort_keys{
        {1, SortType::DESC}, // Sort by count
    };
    sort_node = factory->MakeSort(hash_agg, std::move(sort_keys));
  }
  // Retrieve result
  auto executor = factory->MakeStoredPlanExecutor(sort_node, nullptr);
  std::vector<int64_t> max_freqs;
  int64_t num_samples = std::max(1000ll, int64_t(column_stats->count_ * 0.01));
  double sum_freqs{0.0};
  auto reader = [&](const VectorProjection* vp, sel_t i) {
    if (max_freqs.size() >= num_samples) return;
    max_freqs.emplace_back(vp->VectorAt(1)->DataAs<int64_t>()[i]);
    sum_freqs += max_freqs.back();
  };
  ReadRows(executor, reader);
  column_stats->max_freq_ = max_freqs[0];
//  fmt::print("Max Freq = {}; Max Freqs= {}\n", column_stats->max_freq_, max_freqs);
  column_stats->top_freq_ = sum_freqs / max_freqs.size();
}

void ComputeHistogram(ExecutionFactory* factory, Table* table, ColumnStats* column_stats) {
  ScanNode* scan_node;
  {
    // Scans
    std::vector<uint64_t> cols_to_read{uint64_t(column_stats->col_idx)};
    auto col_node = factory->MakeCol(0);
    std::vector<ExprNode*> projs;
    projs.emplace_back(col_node);
    std::vector<ExprNode*> filters{};
    filters.emplace_back(factory->MakeNonNull(col_node));
    scan_node = factory->MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  HashAggregationNode* hash_agg;
  {
    std::vector<uint64_t> group_bys{0};
    std::vector<std::pair<uint64_t, AggType>> aggs{
        {0, AggType::COUNT},
    };
    hash_agg = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));
  }

  SortNode* sort_node;
  {
    std::vector<std::pair<uint64_t, SortType>> sort_keys{
        {0, SortType::ASC}, // Sort by value
    };
    sort_node = factory->MakeSort(hash_agg, std::move(sort_keys));
  }

  // Execute
  auto executor = factory->MakeStoredPlanExecutor(sort_node, nullptr);
  auto vp = executor->Next();
  auto hist = std::make_unique<EquiDepthHistogram>(64, *column_stats, vp->VectorAt(0), vp->VectorAt(1));
  column_stats->hist_ = std::move(hist);
}

void ComputeValuesSample(ExecutionFactory* factory, Table* table, ColumnStats* column_stats) {
  ScanNode* scan_node;
  {
    // Scans
    std::vector<uint64_t> cols_to_read{uint64_t(column_stats->col_idx)};
    auto col_node = factory->MakeCol(0);
    std::vector<ExprNode*> projs;
    projs.emplace_back(col_node);
    std::vector<ExprNode*> filters{};
    filters.emplace_back(factory->MakeNonNull(col_node));
    scan_node = factory->MakeScan(table, std::move(cols_to_read), std::move(projs), std::move(filters));
  }
  HashAggregationNode* hash_agg;
  {
    std::vector<uint64_t> group_bys{0};
    std::vector<std::pair<uint64_t, AggType>> aggs{};
    hash_agg = factory->MakeHashAggregation(scan_node, std::move(group_bys), std::move(aggs));
  }

  // We want around 1000 samples of distinct values.
  double sample_rate = std::min(1.0, 1000.0 / column_stats->count_distinct_);
  // RNG for sampling.
  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution(0.0,1.0);
  // Retrieve results.
  auto executor = factory->MakeStoredPlanExecutor(hash_agg, nullptr);
  switch (column_stats->col_type) {
    SQL_TYPE(TEMPLATED_SAMPLE_VALUES, NOOP)
    case SqlType::Date:
      TemplatedSample<Date>(executor, column_stats, sample_rate, generator, distribution);
      break;
    default:
      std::cerr << "CollectStats only supported for arithmetic types!!!!!" << std::endl;
  }
}

std::string HistFileName(ColumnStats* column_stats) {
  auto data_folder = Settings::Instance()->DataFolder();
  std::stringstream ss;
  ss << data_folder << "/" << "hist_table" << column_stats->table_id << "_col" << column_stats->col_idx << ".csv" << std::endl;
  return ss.str();
}

std::string ValuesFileName(ColumnStats* column_stats) {
  auto data_folder = Settings::Instance()->DataFolder();
  std::stringstream ss;
  ss << data_folder << "/" << "values_table" << column_stats->table_id << "_col" << column_stats->col_idx << ".csv" << std::endl;
  return ss.str();
}

void WriteHist(ColumnStats* column_stats) {
  auto filename = HistFileName(column_stats);
  auto hist = column_stats->hist_.get();
  auto val_type = column_stats->col_type;
  std::ofstream os(filename, std::ofstream::trunc | std::ofstream::out);
  uint64_t num_parts = hist->GetParts().size();
  os << num_parts << std::endl;
  for (uint64_t i = 0; i < num_parts; i++) {
    auto& part = hist->GetParts()[i];
    double part_card = hist->GetPartCards()[i];
    ValueUtil::WriteVal(part.first, os, val_type);
    os << ' ';
    ValueUtil::WriteVal(part.second, os, val_type);
    os << ' ';
    os << part_card << std::endl;
  }
}

void ReadHist(ColumnStats* column_stats) {
  auto filename = HistFileName(column_stats);
  std::ifstream is(filename);
  std::vector<std::pair<Value, Value>> bounds;
  std::vector<double> cards;
  auto val_type = column_stats->col_type;
  uint64_t num_parts;
  is >> num_parts;
  for (uint64_t i = 0; i < num_parts; i++) {
    auto val1 = ValueUtil::ReadVal(is, val_type);
    auto val2 = ValueUtil::ReadVal(is, val_type);
    double card;
    is >> card;
    bounds.emplace_back(val1, val2);
    cards.emplace_back(card);
  }
  column_stats->hist_ = std::make_unique<EquiDepthHistogram>(std::move(bounds), std::move(cards), column_stats->col_type);
}

void WriteValues(ColumnStats* column_stats) {
  auto filename = ValuesFileName(column_stats);
  auto& vals = column_stats->value_samples_;
  auto val_type = column_stats->col_type;
  std::ofstream os(filename, std::ofstream::trunc | std::ofstream::out);
  os << vals.size() << std::endl;
  for (const auto& val: vals) {
    ValueUtil::WriteVal(val, os, val_type);
    os << std::endl;
  }
}

void ReadValues(ColumnStats* column_stats) {
  auto filename = ValuesFileName(column_stats);
  std::ifstream is(filename);
  std::vector<Value> vals;
  auto val_type = column_stats->col_type;
  uint64_t num_vals;
  is >> num_vals;
  for (uint64_t i = 0; i < num_vals; i++) {
    vals.emplace_back(ValueUtil::ReadVal(is, val_type));
  }
  column_stats->value_samples_ = std::move(vals);
}

void PersistTableStats(TableStatistics* table_stats, InfoStore* info_store) {
  try {
    auto & db = *info_store->db;
    {
      // Persist num_tuples, sample_size
      SQLite::Statement q(db, "INSERT INTO table_stats (table_id, num_tuples, sample_size) VALUES (?, ?, ?);");
      q.bind(1, table_stats->table_id);
      q.bind(2, table_stats->num_tuples);
      q.bind(3, table_stats->sample_size);
      std::cout << q.getExpandedSQL() << std::endl;
      q.exec();
    }
    {
      // Persist column_stats
      SQLite::Statement q(db, "INSERT INTO column_stats (table_id, col_idx, total_count, count_distinct, max_freq, top_freq) VALUES (?, ?, ?, ?, ?, ?)");
      for (const auto& column_stats: table_stats->column_stats) {
        q.bind(1, column_stats->table_id);
        q.bind(2, column_stats->col_idx);
        q.bind(3, column_stats->count_);
        q.bind(4, column_stats->count_distinct_);
        q.bind(5, column_stats->max_freq_);
        q.bind(6, column_stats->top_freq_);
        std::cout << q.getExpandedSQL() << std::endl;
        q.exec();
        q.reset();
      }
    }
    {
      // Persist histogram and persist values.
      for (const auto& column_stats: table_stats->column_stats) {
        WriteHist(column_stats.get());
        WriteValues(column_stats.get());
      }
    }
  }  catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

void TableStatistics::RestoreTableStats(Catalog* catalog, TableInfo* table_info) {
  auto table_stats = std::make_unique<TableStatistics>();
  auto table = catalog->GetTable(table_info->name);
  table_stats->table_id = table->TableID();
  // Read num_tuples and sample_size;
  try {
    auto & db = *catalog->GetInfoStore()->db;
    {
      // num_tuples, sample_size
      SQLite::Statement q(db, "SELECT num_tuples, sample_size FROM table_stats WHERE table_id=?");
      q.bind(1, table_stats->table_id);
      std::cout << q.getExpandedSQL() << std::endl;
      while (q.executeStep()) { // There should only be one result.
        table_stats->num_tuples = q.getColumn(0);
        table_stats->sample_size = q.getColumn(1);
      }
      // Restore table info stats just in case.
      table_info->base_size = table_stats->num_tuples;
      table_info->sample_size = table_stats->sample_size;
    }
    {
      // column_stats
      SQLite::Statement q(db, "SELECT total_count, count_distinct, max_freq, top_freq FROM column_stats WHERE table_id=? AND col_idx=?");
      int64_t col_idx = 0;
      for (const auto& col: table_info->schema.Cols()) {
        auto column_stats = std::make_unique<ColumnStats>();
        column_stats->table_id = table->TableID();
        column_stats->col_idx = col_idx;
        column_stats->col_type = col.Type();
        q.bind(1, column_stats->table_id);
        q.bind(2, column_stats->col_idx);
        std::cout << q.getExpandedSQL() << std::endl;
        while (q.executeStep()) { // Only one output
          column_stats->count_ = q.getColumn(0);
          column_stats->count_distinct_ = q.getColumn(1);
          column_stats->max_freq_ = q.getColumn(2);
          column_stats->top_freq_ = q.getColumn(3);
        }
        q.reset();
        ReadHist(column_stats.get());
        ReadValues(column_stats.get());
        table_stats->column_stats.emplace_back(std::move(column_stats));
        col_idx++;
      }
    }
    {
      // Persist histogram and persist values.
      for (const auto& column_stats: table_stats->column_stats) {
        WriteHist(column_stats.get());
        WriteValues(column_stats.get());
      }
    }
  }  catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
  table->SetStatistics(std::move(table_stats));
}


void TableStatistics::ComputeTableStats(Catalog* catalog, TableInfo* table_info) {
  ExecutionFactory factory(catalog);
  auto table = catalog->GetTable(table_info->name);
  uint64_t col_idx = 0;
  auto table_stats = std::make_unique<TableStatistics>();
  table_stats->table_id = table->TableID();
  table_stats->num_tuples = table_info->base_size;
  table_stats->sample_size = table_info->sample_size;
  for (const auto& col: table_info->schema.Cols()) {
    auto column_stats = std::make_unique<ColumnStats>();
    column_stats->table_id = table->TableID();
    column_stats->col_idx = int64_t(col_idx);
    column_stats->col_type = col.Type();
//    if (!(col.IsPK() && table_info->to_many)) {
    ComputeMinMaxCount(&factory, table, column_stats.get());
    fmt::print("Table {} Col {}: count={}\n", table->Name(), col.Name(), column_stats->count_);
    ComputeNumDisctinctForCol(&factory, table, column_stats.get());
    fmt::print("Table {} Col {}: count_distint={}\n", table->Name(), col.Name(), column_stats->count_distinct_);
    ComputeMaxFreq(&factory, table, column_stats.get());
    fmt::print("Table {} Col {}: max_freq={}\n", table->Name(), col.Name(), column_stats->max_freq_);
    fmt::print("Table {} Col {}: top_freq={}\n", table->Name(), col.Name(), column_stats->top_freq_);
    ComputeHistogram(&factory, table, column_stats.get());
    fmt::print("Table {} Col {}: Computed Histograms\n", table->Name(), col.Name(), column_stats->max_freq_);
    ComputeValuesSample(&factory, table, column_stats.get());
//    }
    table_stats->column_stats.emplace_back(std::move(column_stats));
    col_idx++;
  }

  PersistTableStats(table_stats.get(), catalog->GetInfoStore());
  table->SetStatistics(std::move(table_stats));
}


ColumnStats::~ColumnStats() = default;
TableStatistics::~TableStatistics() = default;

void ColumnStats::ToString(std::ostream &os) const {
  os << "ColumnStats("
    << "count=" << count_ << ", "
      << "distinct=" << count_distinct_ << ", "
      << "top_freq=" << top_freq_ << ", "
    << "max_freq=" << max_freq_ << ")" << std::endl;
  os << "Histogram Head (num_parts=" << hist_->GetParts().size() << ")" << std::endl;
  uint64_t num_to_print = std::min(uint64_t(5), uint64_t(hist_->GetParts().size()));
  for (uint64_t i = 0; i < num_to_print; i++) {
    const auto& part = hist_->GetParts()[i];
    double card = hist_->GetPartCards()[i];
    ValueUtil::WriteVal(part.first, os, col_type);
    os << ' ';
    ValueUtil::WriteVal(part.second, os, col_type);
    os << ' ';
    os << card << std::endl;
  }
  os << "Samples Values Head (num_vals=" << value_samples_.size() << ")" << std::endl;
  num_to_print = std::min(uint64_t(5), uint64_t(value_samples_.size()));
  for (uint64_t i = 0; i < num_to_print; i++) {
    ValueUtil::WriteVal(value_samples_[i], os, col_type);
    os << std::endl;
  }
}

void TableStatistics::GenerateAllStats(Catalog *catalog, WorkloadInfo *workload) {
  for (auto& [_, table_info]: workload->table_infos) {
    if (workload->reload) {
      TableStatistics::ComputeTableStats(catalog, table_info.get());
    } else {
      TableStatistics::RestoreTableStats(catalog, table_info.get());
    }
  }
}

void TableStatistics::GenerateMatViewStats(Catalog *catalog) {
}

void TableStatistics::ToString(std::ostream &os) const {
  os << "TableStatistics(num_tuples=" << num_tuples << ", sample_size=" << sample_size << ")" << std::endl;
  for (const auto& c: column_stats) {
    c->ToString(os);
  }
}
}
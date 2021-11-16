#pragma once

#include "stats/histogram.h"
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <memory>

namespace smartid {
/**
 * Helper class to collect table statistics and build histograms.
 */
class TableStats {
 public:
  /**
   * Constructor
   * @param table Table to summarize.
   * @param wanted_cols Columns to summarize.
   */
  TableStats(const Table* table, const std::vector<uint64_t>& wanted_cols, const std::vector<HistogramType>& wanted_hist_types) : table_(table) {
    hist_types_.insert(wanted_hist_types.begin(), wanted_hist_types.end());
    BuildHistograms(wanted_cols, wanted_hist_types);
  }

  /**
   * Return the histogram of the given column with the given type.
   */
  [[nodiscard]] const Histogram* GetHistogram(uint64_t col_idx, HistogramType histogram_type) const;

  /**
   * @return The table.
   */
  [[nodiscard]] const Table* GetTable() const {
    return table_;
  }

  /**
   * Check if the statistics from the given columns have already been collected.
   */
  [[nodiscard]] bool HasAll(const std::vector<uint64_t>& wanted_cols, const std::vector<HistogramType>& wanted_hist_types) const {
    for (const auto& i: wanted_cols) {
      if (histograms_.count(i) == 0) return false;
    }
    for (const auto& h: wanted_hist_types) {
      if (hist_types_.count(h) == 0) return false;
    }
    return true;
  }

 private:
  /**
   * Build all histograms for the given columns.
   */
  void BuildHistograms(const std::vector<uint64_t>& wanted_cols, const std::vector<HistogramType>& wanted_hist_types);
  /**
   * Build specific histogram with the given info.
   */
  static std::unique_ptr<Histogram> BuildHistogram(uint64_t col_idx, SqlType sql_type, HistogramType histogram_type, const ColumnStats& stats);

 private:
  const Table* table_;
  using HistMap = std::unordered_map<HistogramType, std::unique_ptr<Histogram>>;
  std::unordered_map<uint64_t, HistMap> histograms_;

  std::unordered_set<HistogramType> hist_types_;
};
}
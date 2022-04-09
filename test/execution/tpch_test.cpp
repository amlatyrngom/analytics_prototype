#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "test_util/test_table_info.h"
#include "execution/execution_factory.h"
#include "storage/table_loader.h"
using namespace smartid;


TEST_CASE("Q1") {
  // Load Data
  TableLoader::LoadTPCH("tpch_data");
  auto lineitem = Catalog::Instance()->GetTable("lineitem");
  // Scan
  Date date_val1 = Date::FromYMD(1998, 9, 2);
  // Base columns
  ColumnNode l_returnflag(l_returnflag_idx);
  ColumnNode l_linestatus(l_linestatus_idx);
  // Columns to be aggregated.
  ColumnNode l_quantity(l_quantity_idx);
  ColumnNode l_extendedprice(l_extendedprice_idx);
  ColumnNode l_discount(l_discount_idx);
  ColumnNode l_tax(l_tax_idx);
  ColumnNode l_shipdate(l_shipdate_idx);
  // Scan Projections
  ConstantNode one_const(Value(float(1)), SqlType::Float32);
  BinaryArithNode disc(&one_const, &l_discount, OpType::SUB, SqlType::Float32);
  BinaryArithNode tax(&one_const, &l_tax, OpType::ADD, SqlType::Float32);
  BinaryArithNode disc_price(&l_extendedprice, &disc, OpType::MUL, SqlType::Float32);
  BinaryArithNode charge(&disc_price, &tax, OpType::MUL, SqlType::Float32);
  std::vector<ExprNode*> projections{&l_returnflag, &l_linestatus, &l_quantity, &l_extendedprice, &l_discount, &disc_price, &charge};
  // Scan Filters
  ConstantNode date_rhs(date_val1, SqlType::Date);
  BinaryCompNode comp1(&l_shipdate, &date_rhs, OpType::LE);
  std::vector<ExprNode*> filters{&comp1};
  ScanNode scan(lineitem, std::move(projections), std::move(filters), false);
  // Aggregation
  std::vector<uint64_t> group_bys{0, 1};
  std::vector<std::pair<uint64_t, AggType>> aggs{
      {2, AggType::SUM}, // 2: qty
      {3, AggType::SUM}, // 3: extendedprice
      {4, AggType::SUM}, // 4: discount
      {5, AggType::SUM}, // 5: disc_price
      {6, AggType::SUM}, // 6: charge
      {0, AggType::COUNT}, // 7: count
  };
  HashAggregationNode hash_aggregation_node(&scan, std::move(group_bys), std::move(aggs));
  // Projection
  ColumnNode returnflag_output{0};
  ColumnNode linestatus_output{1};
  ColumnNode sum_qty{2};
  ColumnNode sum_base_price{3};
  ColumnNode sum_discount{4};
  ColumnNode sum_disc_price{5};
  ColumnNode sum_charge{6};
  ColumnNode count_order{7};
  BinaryArithNode avg_qty{&sum_qty, &count_order, OpType::DIV, SqlType::Float32};
  BinaryArithNode avg_price{&sum_base_price, &count_order, OpType::DIV, SqlType::Float32};
  BinaryArithNode avg_disc{&sum_discount, &count_order, OpType::DIV, SqlType::Float32};
  std::vector<ExprNode*> final_projections{
    &returnflag_output, &linestatus_output, &sum_qty, &sum_base_price, &sum_disc_price,
    &sum_charge, &avg_qty, &avg_price, &avg_disc, &count_order
  };
  ProjectionNode projection_node{&hash_aggregation_node, std::move(final_projections)};
  // Sorting
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
      {0, SortType::ASC},
      {1, SortType::ASC},
  };
  SortNode sort_node(&projection_node, std::move(sort_keys));
  // Output
  PrintNode node(&sort_node);
  auto exec = ExecutionFactory::MakePlanExecutor(&node);
  exec->Next();
}


TEST_CASE("Q4") {
  // Load Data
  TableLoader::LoadTPCH("tpch_data");
  auto lineitem = Catalog::Instance()->GetTable("lineitem");
  auto orders = Catalog::Instance()->GetTable("orders");
  // Orders scan
  // Cols
  ColumnNode o_orderkey(o_orderkey_idx);
  ColumnNode o_orderdate(o_orderdate_idx);
  ColumnNode o_orderpriority(o_orderpriority_idx);
  // Filters
  ConstantNode date_lower(Value(Date::FromYMD(1993, 7, 1)), SqlType::Date);
  ConstantNode date_upper(Value(Date::FromYMD(1993, 10, 1)), SqlType::Date);
  BinaryCompNode o_filter1(&o_orderdate, &date_lower, OpType::GE);
  BinaryCompNode o_filter2(&o_orderdate, &date_upper, OpType::LT);
  std::vector<ExprNode*> o_filters{&o_filter1, &o_filter2};
  // Projections
  std::vector<ExprNode*> o_projections{&o_orderkey, &o_orderpriority};

  // Scan
  ScanNode o_scan(orders, std::move(o_projections), std::move(o_filters), false);
  // Lineitem scan
  // Cols
  ColumnNode l_orderkey(l_orderkey_idx);
  ColumnNode l_commitdate(l_commitdate_idx);
  ColumnNode l_receiptdate(l_receiptdate_idx);
  // Filters
  BinaryCompNode l_filter1(&l_commitdate, &l_receiptdate, OpType::LT);
  std::vector<ExprNode*> l_filters{&l_filter1};
  // Projections
  std::vector<ExprNode*> l_projections{&l_orderkey};
  // Scan
  ScanNode l_scan(lineitem, std::move(l_projections), std::move(l_filters), false);

  // Join
  std::vector<uint64_t> join_build_cols{0};
  std::vector<uint64_t> join_probe_cols{0};
  std::vector<std::pair<uint64_t, uint64_t>> join_projections = {
      {0, 1},
  };
  HashJoinNode hash_join_node(&o_scan, &l_scan, std::move(join_build_cols), std::move(join_probe_cols), std::move(join_projections), JoinType::LEFT_SEMI);

  // Agg
  std::vector<uint64_t> group_bys{0};
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {0, AggType::COUNT},
  };
  HashAggregationNode hash_aggregation_node(&hash_join_node, std::move(group_bys), std::move(aggs));

  // Sort
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
      {0, SortType::ASC},
  };
  SortNode sort_node(&hash_aggregation_node, std::move(sort_keys));

  // Output
  PrintNode node(&sort_node);
  auto exec = ExecutionFactory::MakePlanExecutor(&node);
  exec->Next();
}


TEST_CASE("Inner Q4") {
  // Load Data
  TableLoader::LoadTPCH("tpch_data");
  auto lineitem = Catalog::Instance()->GetTable("lineitem");
  auto orders = Catalog::Instance()->GetTable("orders");
  // Orders scan
  // Cols
  ColumnNode o_orderkey(o_orderkey_idx);
  ColumnNode o_orderdate(o_orderdate_idx);
  ColumnNode o_orderpriority(o_orderpriority_idx);
  // Filters
  ConstantNode date_lower(Value(Date::FromYMD(1993, 7, 1)), SqlType::Date);
  ConstantNode date_upper(Value(Date::FromYMD(1993, 10, 1)), SqlType::Date);
  BinaryCompNode o_filter1(&o_orderdate, &date_lower, OpType::GE);
  BinaryCompNode o_filter2(&o_orderdate, &date_upper, OpType::LT);
  std::vector<ExprNode*> o_filters{&o_filter1, &o_filter2};
  // Projections
  std::vector<ExprNode*> o_projections{&o_orderkey, &o_orderpriority};

  // Scan
  ScanNode o_scan(orders, std::move(o_projections), std::move(o_filters), false);
  // Lineitem scan
  // Cols
  ColumnNode l_orderkey(l_orderkey_idx);
  ColumnNode l_commitdate(l_commitdate_idx);
  ColumnNode l_receiptdate(l_receiptdate_idx);
  // Filters
  BinaryCompNode l_filter1(&l_commitdate, &l_receiptdate, OpType::LT);
  std::vector<ExprNode*> l_filters{&l_filter1};
  // Projections
  std::vector<ExprNode*> l_projections{&l_orderkey};
  // Scan
  ScanNode l_scan(lineitem, std::move(l_projections), std::move(l_filters), true);

  // Join
  std::vector<uint64_t> join_build_cols{0};
  std::vector<uint64_t> join_probe_cols{0};
  std::vector<std::pair<uint64_t, uint64_t>> join_projections = {
      {0, 1},
  };
  HashJoinNode hash_join_node(&o_scan, &l_scan, std::move(join_build_cols), std::move(join_probe_cols), std::move(join_projections), JoinType::INNER);

  // Agg
  std::vector<uint64_t> group_bys{0};
  std::vector<std::pair<uint64_t, AggType>> aggs {
      {0, AggType::COUNT},
  };
  HashAggregationNode hash_aggregation_node(&hash_join_node, std::move(group_bys), std::move(aggs));

  // Sort
  std::vector<std::pair<uint64_t, SortType>> sort_keys{
      {0, SortType::ASC},
  };
  SortNode sort_node(&hash_aggregation_node, std::move(sort_keys));

  // Output
  PrintNode node(&sort_node);
  auto exec = ExecutionFactory::MakePlanExecutor(&node);
  exec->Next();
}

#pragma once

#include "common/types.h"
#include "execution/nodes/expr_node.h"

namespace smartid {
class Vector;
class VectorProjection;
class Bitmap;


struct VectorOps {
  /**
   * Gathers values pointed to by the input vector into the output vector.
   * @param in The input vector.
   * @param selvec The input selection vector.
   * @param in_type The type of the value pointed to.
   * @param val_offset The offset of the value pointed to.
   * @param out The output vector.
   */
//  static void GatherVector(const Vector *in, const Filter *filter, SqlType in_type, uint64_t val_offset, Vector *out);

  /**
   * Scatter elements from the input vector the pointers + val_offset within out.
   */
//  static void ScatterVector(const Vector *in, const Filter *filter, SqlType out_type, uint64_t val_offset, Vector *out);

  /**
   * Test and set mark value at given index.
   */
//  static void TestSetMarkVector(Filter *filter, uint64_t val_offset, Vector *out);

  /**
   * Gathers values pointed to by the first input vector and compares them to the second input vector.
   * @param in1 The input vector of pointers.
   * @param in2 The input vector of values.
   * @param selvec The input and output selection vector.
   * @param in_type The type of the value pointed to.
   * @param val_offset The offset of the value pointed to.
   */
//  static void GatherCompareVector(const Vector *in1,
//                                  const Vector *in2,
//                                  Filter *filter,
//                                  SqlType in_type,
//                                  uint64_t val_offset);

  /**
   * Hashes values from the input vector into the output vector.
   */
//  static void HashVector(const Vector *in, const Filter *filter, Vector *out);

  /**
   * Hash values from the input vector combined with the output vector.
   */
//  static void HashCombineVector(const Vector *in, const Filter *filter, Vector *out);

  /**
   * Compares values in the input vector to the value.
   */
  static void ConstantCompVector(const Vector *in, const Value &val, OpType op_type, Bitmap *filter);

  /**
   * Perform arithmetic between input and constant.
   */
//  static void ConstantArithVector(const Vector *in,
//                                  const Value &val,
//                                  const Filter *filter,
//                                  OpType op_type,
//                                  Vector *out);

  /**
   * Compares values in the two input vectors.
   */
  static void BinaryCompVector(const Vector *in1, const Vector *in2, Bitmap *filter, OpType op_type);

  /**
   * Perform arithmetic between the two input vectors.
   */
//  static void BinaryArithVector(const Vector *in1,
//                                const Vector *in2,
//                                const Filter *filter,
//                                OpType op_type,
//                                Vector *out);

  /**
   * Copies values from the input vector at the given indices.
   */
//  static void SelectVector(const Vector *in, const std::vector<uint64_t> &indices, Vector *out);

  /**
   * Reduce value from the input vector into the output value.
   */
//  static void ReduceVector(const Filter *filter, const Vector *in, Vector *out, AggType agg_type);

  /**
   * Reduce value from the input vector into the output value after scattering them to the right offset.
   */
//  static void ScatterReduceVector(const Filter *filter,
//                                  const Vector *in,
//                                  Vector *out,
//                                  uint64_t agg_offset,
//                                  AggType agg_type);

  /**
   * Initialize values to prepare for reduction.
   */
//  static void ScatterInitReduceScalar(const Vector *in, sel_t i, char *out, uint64_t agg_offset, AggType agg_type);

  /**
   * Scatter input element at the given index into the output array at the given offset.
   */
//  static void ScatterScalar(const Vector *in, sel_t i, char *out, uint64_t val_offset);

  /**
   * Sort with amortized interpretation.
   */
//  static void SortVector(std::vector<char *> *sort_vector, SortType sort_type, SqlType val_type, uint64_t val_offset);

  /**
   * Sort without amortized interpretation.
   */
//  static void MultiSortVector(std::vector<char *> *sort_vector,
//                              const std::vector<std::pair<uint64_t, SortType>> &sort_keys,
//                              const std::vector<SqlType> &val_types,
//                              const std::vector<uint64_t> &val_offsets);
//
//  static void HeapInsertVector(const Filter* filter, uint64_t limit, char** entries, std::vector<char*> sort_vector,
//                               const std::vector<std::pair<uint64_t, SortType>> &sort_keys,
//                               const std::vector<SqlType> &val_types,
//                               const std::vector<uint64_t> &val_offsets);


//  /**
//   * Initalize a vector with a value.
//   */
  static void InitVector(const Bitmap* filter, const Value& val, SqlType val_type, Vector* out);
//
//  static void CopyVector(const Filter* filter, const Vector* in, char* out);

  /**
   * Read a block element at a specific location.
   * @param block
   * @param row_idx
   * @param col_idx
   * @param out
   * @param out_idx
   */
//  static void ReadBlock(const Block* block, const std::vector<std::pair<uint64_t, uint64_t>>& row_idxs, uint64_t col_idx, Vector* out);
};
}
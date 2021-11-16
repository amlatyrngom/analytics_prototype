#include "execution/vector_ops.h"

namespace smartid {
///////////////////////////////////////////////
//// Sort
/////////////////////////////////////////////

template<typename cpp_type>
void TemplatedSort(std::vector<char *> *sort_vector, SortType sort_type, uint64_t val_offset) {
  if (sort_type == SortType::ASC) {
    auto sort_fn = [&](const char *ptr1, const char *ptr2) {
      const cpp_type &data1 = *reinterpret_cast<const cpp_type *>(ptr1 + val_offset);
      const cpp_type &data2 = *reinterpret_cast<const cpp_type *>(ptr2 + val_offset);
      return data1 < data2;
    };
    std::sort(sort_vector->begin(), sort_vector->end(), sort_fn);
  } else {
    auto sort_fn = [&](const char *ptr1, const char *ptr2) {
      const cpp_type &data1 = *reinterpret_cast<const cpp_type *>(ptr1 + val_offset);
      const cpp_type &data2 = *reinterpret_cast<const cpp_type *>(ptr2 + val_offset);
      return data1 > data2;
    };
    std::sort(sort_vector->begin(), sort_vector->end(), sort_fn);
  }
}

#define TEMPLATED_SORT(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                              \
    TemplatedSort<cpp_type>(sort_vector, sort_type, val_offset); \
    break;

void VectorOps::SortVector(std::vector<char *> *sort_vector,
                           SortType sort_type,
                           SqlType val_type,
                           uint64_t val_offset) {
  switch (val_type) {
    SQL_TYPE(TEMPLATED_SORT, TEMPLATED_SORT)
  }
}

////////////////////////////////////////////////
//// Multi Sort (interpreted). Avoid!!!!
///////////////////////////////////////////////

template<typename cpp_type>
int TemplatedMultiSort(const char *data1, const char *data2) {
  const auto &val1 = *reinterpret_cast<const cpp_type *>(data1);
  const auto &val2 = *reinterpret_cast<const cpp_type *>(data2);
  if constexpr (std::is_same_v<cpp_type, Varlen>) {
    return val1.Comp(val2);
  } else {
    return val1 < val2 ? -1 : (val1 > val2 ? 1 : 0);
  }
}

#define TEMPLATED_MULTI_SORT(sql_type, cpp_type, ...) \
  case SqlType::sql_type:                              \
    cmp = TemplatedMultiSort<cpp_type>(data1, data2); \
    break;

void VectorOps::MultiSortVector(std::vector<char *> *sort_vector,
                                const std::vector<std::pair<uint64_t, SortType>> &sort_keys,
                                const std::vector<SqlType> &val_types,
                                const std::vector<uint64_t> &val_offsets) {
  auto sort_fn = [&](const char *ptr1, const char *ptr2) {
    for (const auto &sort_key: sort_keys) {
      auto[key, type] = sort_key;
      auto val_type = val_types[key];
      auto val_offset = val_offsets[key];
      auto data1 = ptr1 + val_offset;
      auto data2 = ptr2 + val_offset;
      int cmp;
      switch (val_type) {
        SQL_TYPE(TEMPLATED_MULTI_SORT, TEMPLATED_MULTI_SORT)
      }
      if (cmp < 0) return type == SortType::ASC;
      if (cmp > 0) return type == SortType::DESC;
    }
    return false;
  };
  std::sort(sort_vector->begin(), sort_vector->end(), sort_fn);
}


////////////////////////
/// Heap Insertion
///////////////////////////
void VectorOps::HeapInsertVector(const Filter *filter,
                                 uint64_t limit,
                                 char **entries,
                                 std::vector<char *> sort_vector,
                                 const std::vector<std::pair<uint64_t, SortType>> &sort_keys,
                                 const std::vector<SqlType> &val_types,
                                 const std::vector<uint64_t> &val_offsets) {
  auto sort_fn = [&](const char *ptr1, const char *ptr2) {
    for (const auto &sort_key: sort_keys) {
      auto[key, type] = sort_key;
      auto val_type = val_types[key];
      auto val_offset = val_offsets[key];
      auto data1 = ptr1 + val_offset;
      auto data2 = ptr2 + val_offset;
      int cmp;
      switch (val_type) {
        SQL_TYPE(TEMPLATED_MULTI_SORT, TEMPLATED_MULTI_SORT)
      }
      if (cmp < 0) return type == SortType::ASC;
      if (cmp > 0) return type == SortType::DESC;
    }
    return false;
  };

  filter->Map([&](sel_t i) {
    sort_vector.emplace_back(entries[i]);
    std::push_heap(sort_vector.begin(), sort_vector.end(), sort_fn);
    if (sort_vector.size() > limit) {
      std::pop_heap(sort_vector.begin(), sort_vector.end(), sort_fn);
      sort_vector.pop_back();
    }
  });
}
}
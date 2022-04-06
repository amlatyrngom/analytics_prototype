#include "storage/table_block.h"

namespace smartid {

RawTableBlock *RawTableBlock::AllocateAndSetupRawBlock(const std::vector<SqlType>& col_types, uint64_t variable_data_size) {
  std::cout << "Num Cols: " << col_types.size() << std::endl;
  auto settings = Settings::Instance();
  uint64_t num_cols = col_types.size();
  uint64_t allocation_size = 0;
  // Add data offsets
  allocation_size += sizeof(uint64_t) * num_cols;
  // Add bitmap offsets
  allocation_size += sizeof(uint64_t) * num_cols;
  // Add presence bitmap
  std::cout << "Presence Offset: " << allocation_size << std::endl;
  allocation_size += Bitmap::AllocSize(settings->BlockSize());
  // Add data and bitmaps
  std::vector<uint64_t> col_offsets;
  std::vector<uint64_t> bitmap_offsets;
  for (const auto& col_type: col_types) {
    col_offsets.emplace_back(allocation_size);
    allocation_size += TypeUtil::TypeSize(col_type) * settings->BlockSize();
    bitmap_offsets.emplace_back(allocation_size);
    allocation_size += Bitmap::AllocSize(settings->BlockSize());
  }
  // Add variable data size
  auto variable_data_offset = allocation_size; // Store offset first.
  allocation_size += variable_data_size;

  // Add Header.
  allocation_size += sizeof(RawTableBlock);
  auto raw_block = reinterpret_cast<RawTableBlock*>(malloc(allocation_size));
  std::memset(reinterpret_cast<char*>(raw_block), 0, allocation_size); // Zero block.
  raw_block->block_id = -1;
  raw_block->total_size = allocation_size;
  raw_block->num_cols = num_cols;
  raw_block->variable_data_offset = variable_data_offset;
  for (uint64_t i = 0; i < num_cols; i++) {
    std::cout << "Col Offset: " << col_offsets[i] << std::endl;
    raw_block->ColOffsets()[i] = col_offsets[i];
    std::cout << "Bitmap Offset: " << bitmap_offsets[i] << std::endl;
    raw_block->BitmapOffsets()[i] = bitmap_offsets[i];
  }
  return raw_block;
}

void TableBlock::Consolidate() {
  ASSERT(hot_, "Consolidate called on cold block!");
  auto settings = Settings::Instance();
  uint64_t variable_data_size = 0;
  std::vector<uint64_t> active_rows_bitmap(settings->BlockSize(), 0);
  // First iteration to compute total size.
  for (uint64_t i = 0; i < col_types_.size(); i++) {
    if (col_types_[i] == SqlType::Varchar) {
      // Only compute non-null.
      auto col_bitmap = raw_block_->BitmapData(i);
      auto presence_bitmap = raw_block_->PresenceBitmap();
      Bitmap::Intersect(col_bitmap, presence_bitmap, settings->BlockSize(), active_rows_bitmap.data());
      auto col_data = raw_block_->ColDataAs<Varlen>(i);
      Bitmap::Map(active_rows_bitmap.data(), settings->BlockSize(), [&](sel_t row_idx) {
        const auto& v = col_data[row_idx];
        if (v.Info().IsCompact()) {
          variable_data_size += v.Info().CompactSize();
        } else {
          variable_data_size += v.Info().NormalSize();
        }
      });
    }
  }
  // Second iteration to write output.
  auto new_block = RawTableBlock::AllocateAndSetupRawBlock(col_types_, variable_data_size);
  char* variable_data = new_block->MutableVariableData();
  uint64_t curr_offset = 0;
  for (uint64_t col_idx = 0; col_idx < col_types_.size(); col_idx++) {
    // Copy presence and null data
    std::memcpy(new_block->MutablePresenceBitmap(), raw_block_->PresenceBitmap(), Bitmap::AllocSize(settings->BlockSize()));
    std::memcpy(new_block->MutableBitmapData(col_idx), raw_block_->BitmapData(col_idx), Bitmap::AllocSize(settings->BlockSize()));
    if (col_types_[col_idx] == SqlType::Varchar) {
      // For variable length types, copy non-null active varlens into variable data buffer.
      auto col_bitmap = new_block->BitmapData(col_idx);
      auto presence_bitmap = new_block->PresenceBitmap();
      Bitmap::Intersect(col_bitmap, presence_bitmap, settings->BlockSize(), active_rows_bitmap.data());
      auto old_col_data = raw_block_->ColDataAs<Varlen>(col_idx);
      auto new_col_data = new_block->MutableColDataAs<Varlen>(col_idx);
      Bitmap::Map(active_rows_bitmap.data(), settings->BlockSize(), [&](sel_t row_idx) {

        const auto& old_varlen = old_col_data[row_idx];
        const char* old_data{nullptr};
        uint64_t old_size{0};
        if (old_varlen.Info().IsCompact()) {
          old_size = old_varlen.Info().CompactSize();
          old_data = raw_block_->VariableData() + old_varlen.Info().CompactOffset();
        } else {
          old_size = old_varlen.Info().NormalSize();
          old_data = old_varlen.Data();
        }

        std::memcpy(variable_data, old_data, old_size);
        new_col_data[row_idx] = Varlen::MakeCompact(curr_offset, old_size);
        // Update write offset
        curr_offset += old_size;
        variable_data += old_size;
      });
    } else {
      // For scalar types, just copy all the data;
      std::memcpy(new_block->MutableColData(col_idx), raw_block_->ColData(col_idx), settings->BlockSize() * TypeUtil::TypeSize(col_types_[col_idx]));
    }
  }
  hot_ = false;
  auto tmp = raw_block_;
  raw_block_ = new_block;
  std::free(tmp);
}

template<typename T>
static void TemplatedPrintElem(const RawTableBlock* raw_block, const char* row_data, bool is_null) {
  if (is_null) {
    std::cout << "\\N";
    return;
  }
  T elem = *reinterpret_cast<const T*>(row_data);
  if constexpr (std::is_same_v<T, Varlen>) {
    if (elem.Info().IsCompact()) {
      const char* str_data = raw_block->VariableData() + elem.Info().CompactOffset();
      uint64_t str_size = elem.Info().CompactSize();
      std::string attr(str_data, str_size);
      std::cout << attr;
    } else {
      std::string attr(elem.Data(), elem.Info().NormalSize());
      std::cout << attr;
    }
  } else if constexpr (std::is_same_v<T, Date>) {
    std::cout << elem.ToString();
  } else {
    std::cout << elem;
  }
}

#define TEMPLATED_PRINT_ELEM(sql_type, cpp_type, ...) \
        case SqlType::sql_type: \
          TemplatedPrintElem<cpp_type>(raw_block, row_data, is_null); \
          break;

void PrintVectorElem(const RawTableBlock* raw_block, SqlType col_type, const char* row_data, bool is_null) {
  switch (col_type) {
    SQL_TYPE(TEMPLATED_PRINT_ELEM, TEMPLATED_PRINT_ELEM);
  }
}


void TableBlock::PrintBlock() {
  std::cout << "===================== Start BLOCK ID " << raw_block_->BlockID() << " ======================" << std::endl;
  auto settings = Settings::Instance();
  auto presence_bitmap = raw_block_->PresenceBitmap();
  Bitmap::Map(presence_bitmap, settings->BlockSize(), [&](sel_t row_idx) {
    for (uint64_t col_idx = 0; col_idx < raw_block_->NumCols(); col_idx++) {
      auto col_type = col_types_[col_idx];
      auto row_data = raw_block_->ColData(col_idx) + row_idx * TypeUtil::TypeSize(col_type);
      auto is_null = !Bitmap::IsSet(raw_block_->BitmapData(col_idx), row_idx);
      PrintVectorElem(raw_block_, col_type, row_data, is_null);
      if (col_idx < raw_block_->NumCols() - 1) {
        std::cout << ",";
      }
    }
    std::cout << std::endl;
  });
  std::cout << "===================== End BLOCK ID " << raw_block_->BlockID() << " ======================" << std::endl;
}

}
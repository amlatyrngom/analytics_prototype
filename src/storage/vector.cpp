#include "storage/vector.h"
#include "common/settings.h"
#include "storage/table_block.h"

namespace smartid {
Vector::Vector(SqlType type)
: elem_type_(type),
  num_elems_(0),
  owned_data_(0),
  null_bitmap_(std::make_unique<Bitmap>()),
  data_(owned_data_.data()),
  allocated_strings_(0),
  elem_size_(TypeUtil::TypeSize(type)),
  settings_vec_size_(Settings::Instance()->VecSize()){}

void Vector::Reset(uint64_t num_elems, const char *data, const uint64_t *null_bitmap, const RawTableBlock *block) {
  Resize(num_elems);
  null_bitmap_->Reset(num_elems, null_bitmap);
  if (elem_type_ == SqlType::Varchar) {
    auto src_varlens = reinterpret_cast<const Varlen*>(data);
    auto dst_varlens = reinterpret_cast<Varlen*>(owned_data_.data());
    for (uint64_t i = 0; i < num_elems_; i++) {
      std::vector<char>& buffer = allocated_strings_[i];
      if (src_varlens[i].Info().IsCompact()) {
        ASSERT(block != nullptr, "Reset with compact varlen without block!");
        auto offset = src_varlens[i].Info().CompactOffset();
        auto src_size = src_varlens[i].Info().CompactSize();
        auto src_data = block->VariableData() + offset;
        dst_varlens[i] = Varlen::MakeManagedNormal(src_size, src_data, buffer);
      } else {
        auto src_data = src_varlens[i].Data();
        auto src_size = src_varlens[i].Info().NormalSize();
        dst_varlens[i] = Varlen::MakeManagedNormal(src_size, src_data, buffer);
      }
    }
  } else {
    // Regular types.
    std::memcpy(owned_data_.data(), data, num_elems * elem_size_);
  }
}

void Vector::Resize(uint64_t new_size) {
  null_bitmap_->Reset(new_size);
  if (new_size == num_elems_) return;
  auto alloc_size = std::max(settings_vec_size_, new_size) * elem_size_;
  owned_data_.resize(alloc_size);
  if (elem_type_ == SqlType::Varchar) {
    allocated_strings_.resize(new_size);
  }
  data_ = owned_data_.data();
  num_elems_ = new_size;
}

}
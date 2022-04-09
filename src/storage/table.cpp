#include "storage/table.h"
#include "storage/vector.h"
#include "storage/buffer_manager.h"
#include "common/catalog.h"
#include "common/info_store.h"
#include "storage/dict_encoding.h"

namespace smartid {

Table::Table(int64_t table_id, std::string name, Schema&& schema, BufferManager* buffer_manager, InfoStore* info_store)
: table_id_(table_id)
, name_(std::move(name))
, schema_(std::move(schema))
, buffer_manager_(buffer_manager)
, info_store_(info_store) {
  // Check if already
  bool restore = schema_.NumCols() == 0;
  bool add_row_id = true;
  for (uint64_t col_idx = 0; col_idx < schema_.NumCols(); col_idx++) {
    if (schema_.GetColumn(col_idx).Name() == "row_id") {
      add_row_id = false;
      id_idx_= col_idx;
      break;
    }
  }

  if (restore) {
    // Restore from DB.
    RestoreFromDB();
  } else {
    // Add row id column.
    if (add_row_id) {
      id_idx_ = schema_.NumCols();
      schema_.AddColumn(Column("row_id", SqlType::Int64));
    }
    // Store Table Info;
    StoreCreateTableInfo();
  }

  // Make or restore dictionary encodings.
  for (uint64_t col_idx = 0; col_idx < schema_.NumCols(); col_idx++) {
    if (schema_.GetColumn(col_idx).DictEncode()) {
      dict_encodings_[col_idx] = std::make_unique<DictEncoding>(table_id, col_idx, info_store_);
    }
  }
}


void Table::InsertTableBlock(std::unique_ptr<TableBlock> &&table_block) {
  std::cout << "Inserting new block" << std::endl;
  auto settings = Settings::Instance();
  auto block_id = buffer_manager_->NewBlockID();
  StoreBlockInfo(block_id);
  table_block->RawBlock()->SetBlockID(block_id);
  // Write row_id
  if (id_idx_ != -1) {
    auto row_id_col = table_block->RawBlock()->MutableColDataAs<int64_t>(id_idx_);
    auto raw_id_bitmap = table_block->RawBlock()->MutableBitmapData(id_idx_);
    // Set all to non-null. The presence bitmap will compensate.
    std::memset(reinterpret_cast<char*>(raw_id_bitmap), 0xFF, Bitmap::AllocSize(settings->BlockSize()));
    for (int64_t i = 0; i < settings->BlockSize(); i++) {
      auto row_id = (block_id << settings->LogBlockSize()) | i;
      row_id_col[i] = row_id;
    }
  }

  auto block_info = buffer_manager_->AddBlock(table_block.get());
  buffer_manager_->Unpin(block_info, false, false);
  block_ids_.emplace_back(block_id);
}

void Table::FinalizeEncodings() {
  for (auto& [col_idx, dict_encoding]: dict_encodings_) {
    dict_encoding->Finalize(this);
  }
}


BlockIterator::BlockIterator(const Table *table, const BlockInfo *block, const std::vector<uint64_t>& cols_to_read)
    : table_(table), block_(block), cols_to_read_(cols_to_read), vecs_(cols_to_read_.size()) {
  presence_ = std::make_unique<Bitmap>();
  for (uint64_t i = 0; i < cols_to_read.size(); i++) {
    auto schema_col = table->GetSchema().GetColumn(cols_to_read_[i]);
    vecs_[i] = std::make_unique<Vector>(schema_col.Type());
  }
}

bool BlockIterator::Advance() {
  auto raw_block = TableBlock::FromBlockInfo(block_);
  auto settings = Settings::Instance();
  auto block_size = settings->BlockSize();
  auto vec_size = settings->VecSize();

  bool any_present = false;

  while (curr_idx_ < block_size && !any_present) {
    auto presence_data = raw_block->PresenceBitmap() + (curr_idx_ / Settings::BITMAP_BITS_PER_WORD);
    presence_->Reset(block_size, presence_data);
    auto num_ones = Bitmap::NumOnes(presence_->Words(), vec_size);
    any_present = num_ones > 0;
    if (any_present) {
      // TODO: Avoid deep copy.
      for (uint64_t i = 0; i < cols_to_read_.size(); i++) {
        auto col_idx = cols_to_read_[i];
        auto null_bitmap = raw_block->BitmapData(col_idx) + (curr_idx_ / Settings::BITMAP_BITS_PER_WORD);
        auto read_data = raw_block->ColData(col_idx) + curr_idx_ * vecs_[i]->ElemSize();
        vecs_[i]->Reset(vec_size, read_data, null_bitmap, raw_block);
      }
    }
    curr_idx_ += vec_size;
  }
  return any_present;
}

Table::~Table() {
  // Uncomment to delete table each time run completes.
//  for (auto &block_id: block_ids_) {
//    std::cout << "Trying to delete: " << block_id << std::endl;
//    auto block_info = buffer_manager_->Pin(block_id);
//    buffer_manager_->Unpin(block_info, false, true);
//  }
}

std::vector<const Vector *> BlockIterator::Vectors() const {
  std::vector<const Vector *> res{vecs_.size()};
  for (uint64_t i = 0; i < cols_to_read_.size(); i++) {
    res[i] = vecs_[i].get();
  }
  return res;
}

TableIterator::TableIterator(const Table *table, BufferManager* bm, const std::vector<uint64_t>& cols_to_read)
: table_(table)
, cols_to_read_(cols_to_read)
, buffer_manager_(bm)
, curr_block_idx_(0) {
  if (curr_block_idx_ < table->NumBlocks()) {
    auto block_id = table_->BlockIDS()[curr_block_idx_];
    curr_block_ = buffer_manager_->Pin(block_id);
    block_iter_ = std::make_unique<BlockIterator>(table_, curr_block_, cols_to_read);
  }
}

bool TableIterator::Advance() {
  // Empty Table.
  if (block_iter_ == nullptr) return false;
  // Read Vector from current block if possible.
  if (block_iter_->Advance()) return true;
  // Move to next block otherwise.
  buffer_manager_->Unpin(curr_block_, false);
  curr_block_idx_++;
  // Check if this end of blocks reached.
  if (curr_block_idx_ >= table_->NumBlocks()) return false;
  // Load next blocks.
  auto block_id = table_->BlockIDS()[curr_block_idx_];
  curr_block_ = buffer_manager_->Pin(block_id);
  block_iter_->Reset(curr_block_);
  return block_iter_->Advance();
}

void Table::StoreCreateTableInfo() {
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    {
      SQLite::Statement q(db, "INSERT INTO tables (table_id, table_name) VALUES (?, ?)");
      q.bind(1, table_id_);
      q.bind(2, name_);
      std::cout << q.getExpandedSQL() << std::endl;
      q.exec();
    }
    {
      SQLite::Statement q(db, "INSERT INTO table_schemas (table_id, col_name, col_idx, col_type) VALUES (?, ?, ?, ?)");
      for (uint64_t i = 0; i < schema_.NumCols(); i++) {
        auto & col = schema_.GetColumn(i);
        q.bind(1, table_id_);
        q.bind(2, col.Name());
        q.bind(3, static_cast<int32_t>(i));
        q.bind(4, TypeUtil::TypeToName(col.Type()));
        std::cout << q.getExpandedSQL() << std::endl;
        q.exec();
        q.reset();
      }
    }
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

void Table::StoreBlockInfo(int64_t block_id) {
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    {
      SQLite::Statement q(db, "INSERT INTO table_blocks (table_id, block_id) VALUES (?, ?)");
      q.bind(1, table_id_);
      q.bind(2, block_id);
      std::cout << q.getExpandedSQL() << std::endl;
      q.exec();
    }
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

void Table::RestoreSchema() {
  std::vector<std::pair<uint64_t, Column>> unordered_cols;
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    {
      SQLite::Statement q(db, "SELECT col_name, col_idx, col_type FROM table_schemas WHERE table_id=?");
      q.bind(1, table_id_);
      while (q.executeStep()) {
        const char* name = q.getColumn(0);
        int col_idx = q.getColumn(1);
        const char* type_name = q.getColumn(2);
        if (name == std::string("row_id")) {
          id_idx_ = col_idx;
        }
        SqlType sql_type = TypeUtil::NameToType(type_name);
        unordered_cols.emplace_back(col_idx, Column(name, sql_type));
      }
    }
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
  std::vector<Column> cols(unordered_cols.size());
  for (const auto& [col_idx, col]: unordered_cols) {
    cols[col_idx] = col;
  }
  schema_ = Schema(std::move(cols));
}

void Table::RestoreBlocks() {
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    {
      SQLite::Statement q(db, "SELECT block_id FROM table_blocks WHERE table_id=?");
      q.bind(1, table_id_);
      while (q.executeStep()) {
        int64_t block_id = q.getColumn(0);
        if (buffer_manager_->GetInfo(block_id) != nullptr) {
          std::cout << "Table restoring block: " << block_id << std::endl;
          block_ids_.emplace_back(block_id);
        }
      }
    }
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

void Table::RestoreFromDB() {
  // Restore schema.
  RestoreSchema();
  // Restore blocks.
  RestoreBlocks();
}
}
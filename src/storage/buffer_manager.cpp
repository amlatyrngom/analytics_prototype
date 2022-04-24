#include "storage/buffer_manager.h"
#include <lrucache11/LRUCache11.hpp>
#include <limits>
#include <filesystem>
#include <unordered_set>
#include <fstream>
#include "common/util.h"
#include "common/info_store.h"



namespace smartid {

// Simple LRU class for buffer manager.
struct BlockLRU {
  BlockLRU() = default;

  // Insert in cache.
  void Insert(BlockInfo* block_info) {
    ASSERT(lru_map.count(block_info->block_id) == 0, "Inserting existing block in LRU!");
    lru_list.emplace_front(block_info->block_id);
    lru_map[block_info->block_id] = block_info;
  }

  // Pop LRU.
  BlockInfo* Pop() {
    ASSERT(!lru_map.empty(), "Popping in empty list!");
    int64_t block_id = lru_list.back();
    lru_list.pop_back();
    auto ret = lru_map[block_id];
    lru_map.erase(block_id);
    return ret;
  }

  // Check if contains.
  [[nodiscard]] bool Contains(int64_t block_id) const {
    return lru_map.count(block_id) > 0;
  }

  // Get a item.
  BlockInfo* Get(int64_t block_id) {
    ASSERT(lru_map.count(block_id) > 0, "Removing non-existing block in LRU!");
    return lru_map.at(block_id);
  }

  // Completely remove from list.
  void Remove(int64_t block_id) {
    ASSERT(lru_map.count(block_id) > 0, "Removing non-existing block in LRU!");
    lru_list.remove(block_id);
    lru_map.erase(block_id);
  }


  std::list<int64_t> lru_list;
  std::unordered_map<int64_t, BlockInfo*> lru_map;
};


void BufferManager::Unpin(BlockInfo *block_info, bool dirty, bool should_delete) {
  int64_t block_id{-1};
  {
    // Unpin block.
    auto g = std::lock_guard(global_m);
    block_id = block_info->block_id;
    // Safeguards.
    ASSERT(block_info != nullptr, "Unpinning non existent block!!!");
    ASSERT(!block_info->deleted, "Unpinning deleted block!!!");
    ASSERT(pin_counts_[block_id] > 0, "Unpinning unpinned block!!!");
    ASSERT(!should_delete || pin_counts_[block_id] == 1, "Deleted block pinned by more than one thread!");
    // Make as dirty.
    if (dirty) {
      // Take local lock just in case. dirty is accessed elsewhere behind this lock.
      auto l = std::lock_guard(block_info->local_m);
      block_info->dirty = true;
    }
    block_info->deleted = should_delete;
    // Decrease pin count.
    pin_counts_[block_id]--;
    if (pin_counts_[block_id] == 0) {
      // Remove from pinned in mem;
      auto block = pinned_[block_id];
      pinned_.erase(block_id);
      pin_counts_.erase(block_id);
      if (!should_delete) {
        // Unpin in mem.
        unpinned_in_mem_->Insert(block_info);
      } else {
        // Free space. File will be permanently deleted after global lock is unlocked.
        std::free(block_info->data);
        used_mem_space_ -= block_info->size;
        used_disk_space_ -= block_info->size;
        block_infos_.erase(block_info->block_id);
      }
    }
  }
  {
    // Persist block info.
    if (should_delete) {
      // No need to take local lock, since other threads do have this block.
      DeleteBlockInfo(block_id);
      PermanentDeleteBlock(block_id);
    }
  }
}

BlockInfo *BufferManager::GetInfo(int64_t block_id) {
  auto g = std::lock_guard(global_m);
  if (block_infos_.count(block_id) == 0) {
    return nullptr;
  }
  return block_infos_.at(block_id).get();
}

BlockInfo *BufferManager::Pin(int64_t block_id) {
  global_m.lock();
  auto block_info = block_infos_[block_id].get();
  ASSERT(block_info != nullptr, "Pinning non existent block!!!");
  ASSERT(!block_info->deleted, "Pinning deleted block!!!");
  auto found_in_mem = false;

  // Check if pinned.
  if (pin_counts_.count(block_id) > 0) {
    pin_counts_[block_id]++;
    found_in_mem = true;
  }
  // Check if cached but unpinned.
  if (unpinned_in_mem_->Contains(block_id)) {
    pin_counts_[block_id] = 1;
    auto block = unpinned_in_mem_->Get(block_id);
    unpinned_in_mem_->Remove(block_id);
    pinned_[block_id] = block;
    found_in_mem = true;
  }


  if (!found_in_mem) {
    // Mark blocks to evict.
    std::vector<BlockInfo*> evicted_to_disk;
    std::vector<BlockInfo*> evicted_to_cloud;
    MakeSpace(block_info, evicted_to_disk, evicted_to_cloud);
    // Acquire local.
    block_info->local_m.lock();
    block_info->fetching = true; // To prevent concurrent remove and fetch.
    global_m.unlock();
    // Perform evictions and fetching.
    PerformEvictions(evicted_to_disk, evicted_to_cloud);
    FetchBlock(block_info);
    UpdateBlockInfo(block_info); // for on_disk=true within FetchBlock.
    block_info->local_m.unlock();
  } else {
    // Unlock global and wait for fetching.
    global_m.unlock();
    block_info->local_m.lock();
    block_info->local_m.unlock();
  }

  return block_info;
}

BufferManager::BufferManager(InfoStore* info_store) : info_store_(info_store) {
  unpinned_in_mem_ = std::make_unique<BlockLRU>();
  unpinned_in_disk_ = std::make_unique<BlockLRU>();

  if (!std::filesystem::exists(GetBlockDir())) {
    std::filesystem::create_directory(GetBlockDir());
  }

  try {
    auto & db = *info_store_->db;
    std::stringstream ss;
    ss
        << "CREATE TABLE IF NOT EXISTS buffer_info ("
        << "block_id BIGINT PRIMARY KEY,"
        << "size BIGINT,"
        << "deleted INT,"
        << "on_disk INT"
        << ");";
    auto q = ss.str();
    std::cout << q << std::endl;
    SQLite::Statement query(db, q);
    query.exec();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }

  RestoreFromDB();
}

void BufferManager::RestoreFromDB() {
  std::unordered_map<int64_t, std::unique_ptr<BlockInfo>> db_blocks;
  std::unordered_map<int64_t, std::string> disk_blocks;
  // TODO: Also read cloud blocks.

  // Read db block infos.
  try {
    auto& db = *info_store_->db;
    {
      SQLite::Statement q(db, "SELECT block_id, size, deleted, on_disk FROM buffer_info");
      while (q.executeStep()) {
        int64_t block_id = q.getColumn(0);
        int64_t block_size = q.getColumn(1);
        int deleted = q.getColumn(2);
        int on_disk = q.getColumn(3);
        auto block_info_uniq = std::make_unique<BlockInfo>(block_id, block_size, false);
        block_info_uniq->deleted = bool(deleted);
        block_info_uniq->on_disk = bool(on_disk);
        db_blocks.emplace(block_id, std::move(block_info_uniq));
        if (block_id >= curr_block_id_) {
          curr_block_id_ = block_id + 1;
        }
      }
    }
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
  // Read disk block.
  auto block_dir = GetBlockDir();
  auto prefix_size = GetBlockFilenamePrefix().size();
  for (const auto& block_file: std::filesystem::directory_iterator{block_dir}) {
    auto filename = block_file.path().filename().string();
    auto block_id = std::stoi(filename.substr(prefix_size, filename.size() - prefix_size));
    disk_blocks[block_id] = block_file.path().string();
  }

  // Reconcile
  // Any disk file not in DB should be deleted.
  for (const auto& [block_id, block_path]: disk_blocks) {
    if (!db_blocks.contains(block_id)) {
      std::filesystem::remove(block_path);
      // TODO: also remove from cloud.
    }
  }
  // Any disk file marked as deleted or !on_disk should be deleted.
  for (auto& [block_id, block_info]: db_blocks) {
    bool on_disk = disk_blocks.contains(block_id);
    if (block_info->deleted) {
      // Deleted from could and disk.
      if (on_disk) {
        const auto& block_path = disk_blocks[block_id];
        std::filesystem::remove(block_path);
      }
      // TODO: also deleted from cloud.
      continue;
    }
    if (!block_info->on_disk) {
      // Delete from disk if db says not on disk.
      if (on_disk) {
        const auto& block_path = disk_blocks[block_id];
        std::filesystem::remove(block_path);
      }
    } else {
      // Add to unpinned list on disk.
      unpinned_in_disk_->Insert(block_info.get());
    }
    // At this point, information is consistent.
    block_infos_[block_id] = std::move(block_info);
  }
}


BufferManager::~BufferManager() {
  std::cout << "Destroying Buffer Manager: Writing out dirty blocks" << std::endl;
  auto global = std::lock_guard(global_m);
  for (auto& [block_id, block_info]: block_infos_) {
    if (block_info->dirty) {
      ASSERT(pinned_.contains(block_id) || unpinned_in_mem_->Contains(block_id),"Dirty block should be in mem!");
      WriteBlock(block_info.get());
    }
    // Free memory.
    if (pinned_.contains(block_id) || unpinned_in_mem_->Contains(block_id)) {
      std::free(block_info->data);
    }
  }
}




BlockInfo* BufferManager::AddBlock(BufferBlock* buffer_block) {
  BlockInfo* block_info(nullptr);
  std::vector<BlockInfo*> to_disk;
  std::vector<BlockInfo*> to_cloud;
  {
    // Place block in memory.
    auto global = std::lock_guard(global_m);
    auto block_id = buffer_block->BlockID();
    ASSERT(block_infos_.count(block_id) == 0, "Adding block that already exists!");
    // Take data.
    uint64_t data_size{0};
    char* block_data = buffer_block->Finalize(&data_size);
    auto block_info_uniq = std::make_unique<BlockInfo>(block_id, data_size, false); // dirty is false because block is automatically written.
    block_info = block_info_uniq.get();
    block_info->data = block_data;
    block_infos_[block_id] = std::move(block_info_uniq);
    // Make space and pin in mem and disk.
    MakeSpace(block_info, to_disk, to_cloud);
    block_info->on_disk = true; // Will be written.
  }

  {
    // No need to take local lock, since other threads do have this block.
    PerformEvictions(to_disk, to_cloud);
    WriteBlock(block_info);
    InsertBlockInfo(block_info); // for creation.
  }
  return block_info;
}

void BufferManager::InsertBlockInfo(BlockInfo* block_info) {
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    {
      SQLite::Statement q(db, "INSERT INTO buffer_info (block_id, size, deleted, on_disk) VALUES (?, ?, ?, ?);");
      q.bind(1, block_info->block_id);
      q.bind(2, block_info->size);
      q.bind(3, int(block_info->deleted));
      q.bind(4, int(block_info->on_disk));
//      std::cout << q.getExpandedSQL() << std::endl;
      q.exec();
    }
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

void BufferManager::UpdateBlockInfo(BlockInfo* block_info) {
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    {
      SQLite::Statement q(db, "UPDATE buffer_info SET size=?, deleted=?, on_disk=? WHERE block_id=?;");
      q.bind(1, block_info->size);
      q.bind(2, int(block_info->deleted));
      q.bind(3, int(block_info->on_disk));
      q.bind(4, block_info->block_id);
//      std::cout << q.getExpandedSQL() << std::endl;
      q.exec();
    }
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

void BufferManager::DeleteBlockInfo(int64_t block_id) {
  try {
    auto& db = *info_store_->db;
    SQLite::Transaction transaction(*info_store_->db);
    SQLite::Statement q(db, "DELETE FROM buffer_info WHERE block_id=?;");
    q.bind(1, block_id);
//    std::cout << q.getExpandedSQL() << std::endl;
    q.exec();
    transaction.commit();
  } catch (std::exception& e) {
    std::cout << e.what() << std::endl;
    throw e;
  }
}

// Assumes global lock is held.
void BufferManager::MakeSpace(BlockInfo* block_info, std::vector<BlockInfo*>& to_disk, std::vector<BlockInfo*>& to_cloud) {
  auto setting = Settings::Instance();
  auto buffer_mem = setting->BufferMem();
  auto buffer_disk = setting->BufferDisk();
//  std::cout << "Before making space: mem=" << used_mem_space_ << "; disk=" << used_disk_space_ << std::endl;
  auto curr_used_mem = used_mem_space_;
  auto curr_used_disk = used_disk_space_;
  // Do evictions from memory to disk.
  while (curr_used_mem > 0 && curr_used_mem + block_info->size > buffer_mem) {
    auto evicted = unpinned_in_mem_->Pop();
    evicted->fetching = false; // Will prevent concurrent write and fetch.
    curr_used_mem -= evicted->size;
    to_disk.emplace_back(evicted);
    unpinned_in_disk_->Insert(evicted);
//    std::cout << "Marking for disk eviction: id =" << evicted->block_id << " mem=" << curr_used_mem << "; disk=" << curr_used_disk << std::endl;
  }
  if (!block_info->on_disk) {
    // Do evictions from disk to cloud.
    while (curr_used_disk > 0 && curr_used_disk + block_info->size > buffer_disk) {
      auto evicted = unpinned_in_disk_->Pop();
      evicted->fetching = false; // Will prevent concurrent write and fetch.
      curr_used_disk -= evicted->size;
      to_cloud.emplace_back(evicted);
      block_info->on_disk = false;
//      std::cout << "Marking for cloud eviction: id =" << evicted->block_id << " mem=" << curr_used_mem << "; disk=" << curr_used_disk << std::endl;
    }
  }
  // Eviction successful.
  pin_counts_[block_info->block_id] = 1;
  pinned_[block_info->block_id] = block_info;
  used_mem_space_ = curr_used_mem + block_info->size;
  if (!block_info->on_disk) {
    used_disk_space_ = curr_used_disk + block_info->size;
  } else {
    // Is now in mem.
    unpinned_in_disk_->Remove(block_info->block_id);
  }
//  std::cout << "After making space: mem=" << used_mem_space_ << "; disk=" << used_disk_space_ << std::endl;
}

// Assumes global lock is held.
void BufferManager::PerformEvictions(std::vector<BlockInfo*>& to_disk, std::vector<BlockInfo*>& to_cloud) {
  // Sort to prevent locking deadlock.
  std::sort(to_disk.begin(), to_disk.end(), [](const BlockInfo* b1,  const BlockInfo* b2) {
    return b1->block_id < b2->block_id;
  });
  std::sort(to_cloud.begin(), to_cloud.end(), [](const BlockInfo* b1,  const BlockInfo* b2) {
    return b1->block_id < b2->block_id;
  });
  // From mem to disk & cloud. Can probably be done in parallel.
  for (auto & block_info: to_disk) {
    // TODO: Possible race condition???? Verify clearly later.
    auto local = std::lock_guard(block_info->local_m);
    if (block_info->dirty) {
      // Write to disk and to cloud.
      block_info->dirty = false;
      WriteBlock(block_info);
    }
    // Free allocation
    std::free(block_info->data);
    block_info->data = nullptr;
  }
  // From disk.
  for (auto & block_info: to_cloud) {
    // Because disk and cloud are always in sync, no need to remove from cloud.
    auto local = std::lock_guard(block_info->local_m);
    if (!block_info->fetching) { // Fetching was set to prevent concurrent remove and fetch.
      block_info->on_disk = false; // Set again. Should not be necessary, but is for clarity.
      UpdateBlockInfo(block_info); // For on_disk=false.
      std::filesystem::remove(GetBlockFilename(block_info->block_id));
    }
  }
}

// Write to disk and cloud. Assumes exclusive access to block.
void BufferManager::WriteBlock(BlockInfo* block_info) {
  block_info->on_disk = true;
  std::ofstream os(GetBlockFilename(block_info->block_id));
  os.write(block_info->data, block_info->size);
  // TODO: Write to cloud too.
}

// Assumes exclusive access to block
void BufferManager::FetchBlock(BlockInfo *block_info) {
  block_info->fetching = true; // Prevent concurrent remove and fetch.
  if (!block_info->on_disk) {
    block_info->on_disk = true;
    // TODO: Fetch from cloud first.
  }
  // Read from disk.
  block_info->data = reinterpret_cast<char*>(std::malloc(block_info->size));
  std::ifstream is(GetBlockFilename(block_info->block_id));
  is.read(block_info->data, block_info->size);
}

// Assumes exclusive access to block
void BufferManager::PermanentDeleteBlock(int64_t block_id) {
  std::filesystem::remove(GetBlockFilename(block_id));
  // TODO: delete from cloud too.
}

std::string BufferManager::GetBlockFilename(uint64_t block_id) {
  std::stringstream ss;
  ss << GetBlockDir() << "/" << GetBlockFilenamePrefix() << block_id;
  return ss.str();
}

std::string BufferManager::GetBlockFilenamePrefix() {
  return "block_data";
}

std::string BufferManager::GetBlockDir() {
  auto settings = Settings::Instance();
  std::stringstream ss;
  ss << settings->DataFolder() << "/buffer_data" << settings->NodeID();
  return ss.str();
}




}
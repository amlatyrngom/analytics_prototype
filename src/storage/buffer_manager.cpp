#include "storage/buffer_manager.h"
#include <sqlite_modern_cpp/hdr/sqlite_modern_cpp.h>
#include <lrucache11/LRUCache11.hpp>
#include <limits>
#include <filesystem>
#include <unordered_set>
#include <fstream>
#include "common/util.h"



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
  {
    // Unpin block.
    auto g = std::lock_guard(global_m);
    auto block_id = block_info->block_id;
    // Safeguards.
    ASSERT(block_info != nullptr, "Unpinning non existent block!!!");
    ASSERT(!block_info->deleted, "Unpinning deleted block!!!");
    ASSERT(pin_counts_[block_id] > 0, "Unpinning unpinned block!!!");
    ASSERT(!should_delete || pin_counts_[block_id] == 1, "Deleted block pinned by more than one thread!");
    // Make as dirty.
    if (dirty) {
      // Take local lock just in case. dirty is accessed elsewhere.
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
        block_infos_.erase(block_info->block_id);
        used_mem_space_ -= block_info->size;
        used_disk_space_ -= block_info->size;
      }
    }
  }
  {
    // Persist block info.
    if (should_delete) {
      // No need to take local lock, since other threads do have this block.
      PersistBlockInfo(block_info);
      PermanentDeleteBlock(block_info);
      DeleteBlockInfo(block_info);
    }
  }
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
    PersistBlockInfo(block_info); // for on_disk=true within FetchBlock.
  } else {
    // Unlock global and wait for fetching.
    global_m.unlock();
    block_info->local_m.lock();
    block_info->local_m.unlock();
  }

  return block_info;
}

BufferManager::BufferManager() {
  db_ = std::make_unique<sqlite::database>(GetDBFilename());
  *db_
  << "CREATE TABLE IF NOT EXISTS buffer_info ("
  << "block_id BIGINT PRIMARY KEY,"
  << "size BIGINT,"
  << "deleted BOOL,"
  << "on_disk BOOL"
  << ");";
}

BufferManager::~BufferManager() {}




void BufferManager::AddBlock(BufferBlock* buffer_block) {
  BlockInfo* block_info(nullptr);
  {
    // Place block in memory.
    auto global = std::lock_guard(global_m);
    auto block_id = buffer_block->BlockID();
    ASSERT(block_infos_.count(block_id) == 0, "Adding block that already exists!");
    // Take data.
    uint64_t data_size{0};
    char* block_data = buffer_block->Finalize(&data_size);
    auto block_info_uniq = std::make_unique<BlockInfo>(block_id, data_size, false);
    block_info = block_info_uniq.get();
    block_info->data = block_data;
    block_infos_[block_id] = std::move(block_info_uniq);
    pinned_[block_id] = block_info;
    // Increase space usage. Note that this may cause slight excess, but that's fine.
    used_mem_space_ += block_info->size;
    used_disk_space_ += block_info->size;
  }

  {
    // No need to take local lock, since other threads do have this block.
    WriteBlock(block_info);
    PersistBlockInfo(block_info); // for creation.
  }
}

void BufferManager::PersistBlockInfo(BlockInfo* block_info) {
  *db_ << "begin;";
  *db_ << "DELETE FROM buffer_info WHERE block_id=?;" << block_info->block_id;
  *db_ << "INSERT INTO buffer_info (block_id, size, deleted, on_disk) VALUES (?, ?, ?, ?)"
        << block_info->block_id << block_info->size << block_info->deleted << block_info->on_disk;
  *db_ << "commit;";
  // TODO: persist db and/or db commands to cloud.
}

void BufferManager::DeleteBlockInfo(BlockInfo* block_info) {
  *db_ << "begin;";
  *db_ << "DELETE FROM buffer_info WHERE block_id=?;" << block_info->block_id;
  *db_ << "commit;";
  // TODO: persist db and/or db commands to cloud.
}

// Assumes global lock is held.
void BufferManager::MakeSpace(BlockInfo* block_info, std::vector<BlockInfo*>& to_disk, std::vector<BlockInfo*>& to_cloud) {
  auto setting = Settings::Instance();
  auto buffer_mem = setting->BufferMem();
  auto buffer_disk = setting->BufferDisk();
  auto curr_used_mem = used_mem_space_;
  auto curr_used_disk = used_disk_space_;
  // Do evictions from memory to disk.
  while (curr_used_mem + block_info->size > buffer_mem) {
    auto evicted = unpinned_in_mem_->Pop();
    evicted->fetching = false; // Will prevent concurrent write and fetch.
    curr_used_mem -= evicted->size;
    to_disk.emplace_back(evicted);
    unpinned_in_disk_->Insert(evicted);
  }
  if (!block_info->on_disk) {
    // Do evictions from disk to cloud.
    while (used_disk_space_ + block_info->size > buffer_disk) {
      auto evicted = unpinned_in_disk_->Pop();
      evicted->fetching = false; // Will prevent concurrent write and fetch.
      used_disk_space_ -= evicted->size;
      to_cloud.emplace_back(evicted);
      block_info->on_disk = false;
    }
  }
  // Eviction successful.
  pin_counts_[block_info->block_id] = 1;
  pinned_[block_info->block_id] = block_info;
  used_mem_space_ = curr_used_mem + block_info->size;
  if (!block_info->on_disk) used_disk_space_ = curr_used_disk + block_info->size;
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
  // From mem to disk.
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
  }
  // From disk.
  for (auto & block_info: to_cloud) {
    // Because disk and cloud are always in sync, no need to remove from cloud.
    auto local = std::lock_guard(block_info->local_m);
    if (!block_info->fetching) { // Fetching was set to prevent concurrent remove and fetch.
      block_info->on_disk = false; // Set again. Should not be necessary, but is for clarity.
      PersistBlockInfo(block_info); // For on_disk=false.
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
  std::ifstream is(GetBlockFilename(block_info->block_id));
  is.read(block_info->data, block_info->size);
}

// Assumes exclusive access to block
void BufferManager::PermanentDeleteBlock(BlockInfo *block_info) {
  std::filesystem::remove(GetBlockFilename(block_info->block_id));
  // TODO: delete from cloud too.
}

std::string BufferManager::GetBlockFilename(uint64_t block_id) {
  auto settings = Settings::Instance();
  std::stringstream ss;
  ss << settings->DataFolder() << "/buffer_data/block_data_" << settings->NodeID() << "_" << block_id;
  return ss.str();
}

std::string BufferManager::GetDBFilename() {
  auto settings = Settings::Instance();
  std::stringstream ss;
  ss << settings->DataFolder() << "/buffer_info.db";
  return ss.str();
}



}
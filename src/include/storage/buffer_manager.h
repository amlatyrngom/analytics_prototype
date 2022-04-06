#pragma once
#include <mutex>
#include <unordered_map>
#include "common/settings.h"

// Pre-declare
namespace sqlite {
class database;
}


namespace smartid {

class BlockLRU;

class BufferBlock {
 public:
  // Give ownership of raw data to buffer manager.
  // DO NOT CALL BUFFER MANAGER WITHIN THIS FUNCTION.
  virtual char* Finalize(uint64_t* data_size) = 0;


  virtual int64_t BlockID() const = 0;
};

struct BlockInfo {
  BlockInfo(int64_t id, int64_t total_size, bool is_dirty) : block_id(id), size(total_size), dirty(is_dirty) {}

  // Persited in DB.
  int64_t block_id;
  int64_t size;
  bool deleted{true}; // False when block is deleted. Use for garbage collection.
  bool on_disk{false};

  // In Mem
  bool dirty;
  bool fetching{false};
  char *data{nullptr};
  BufferBlock* buffer_block{nullptr};
  std::mutex local_m;
};

class BufferManager {
 public:

  // Create singleton instance.
  static BufferManager *Instance() {
    static BufferManager instance;
    return &instance;
  }

  // Get a new unique block id.
  int64_t NewBlockID() {
    auto g = std::lock_guard(global_m);
    return curr_block_id_++;
  }

  // Add a block to the buffer manager, which takes ownership of associated data.
  void AddBlock(BufferBlock* buffer_block);

  // Unpin a block.
  void Unpin(BlockInfo *block_info, bool dirty, bool should_delete=false);

  // Pin a block.
  BlockInfo* Pin(int64_t block_id);

  // Make outline.
  ~BufferManager();

 private:
  // Private Constructor prevents initialization.
  BufferManager();

  static std::string GetBlockFilename(uint64_t block_id);

  static std::string GetDBFilename();

  // Update DB
  void PersistBlockInfo(BlockInfo* block_info);
  // Update DB
  void DeleteBlockInfo(BlockInfo* block_info);


  // Assumes lock is held.
  void MakeSpace(BlockInfo* block_info, std::vector<BlockInfo*>& to_disk, std::vector<BlockInfo*>& to_cloud);

  // Assumes lock is held.
  void PerformEvictions(std::vector<BlockInfo*>& to_disk, std::vector<BlockInfo*>& to_cloud);

  // Write to disk and cloud.
  void WriteBlock(BlockInfo* block_info);

  // Fetch from disk and/or cloud.
  void FetchBlock(BlockInfo* block_info);

  // Permanently delete block on disk and cloud.
  void PermanentDeleteBlock(BlockInfo* block_info);




  std::mutex global_m;
  std::unique_ptr<sqlite::database> db_;
  int64_t curr_block_id_{0};
  int64_t used_mem_space_{0};
  int64_t used_disk_space_{0};
  std::unordered_map<int64_t, BlockInfo*> pinned_;
  std::unordered_map<int64_t, int64_t> pin_counts_;
  std::unique_ptr<BlockLRU> unpinned_in_mem_;
  std::unique_ptr<BlockLRU> unpinned_in_disk_;
  std::unordered_map<int64_t, std::unique_ptr<BlockInfo>> block_infos_;
};


}
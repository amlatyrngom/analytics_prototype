#pragma once

namespace smartid {

struct Settings {
  static constexpr uint64_t BITMAP_BITS_PER_WORD = 64;
  static constexpr uint64_t BITMAP_BYTES_PER_WORD = 8;
  static constexpr uint64_t LOG_MAX_TUPLES_PER_TABLE = 48;


  static Settings *Instance() {
    static Settings instance;
    return &instance;
  }

  [[nodiscard]] uint64_t VecSize() const {
    return vec_size_;
  }

  void SetVecSize(uint64_t s) {
    vec_size_ = s;
  }

  [[nodiscard]] uint64_t BlockSize() const {
    return 1 << log_block_size_;
  }

  [[nodiscard]] uint64_t LogBlockSize() const {
    return log_block_size_;
  }

  void SetLogBlockSize(uint64_t s) {
    log_block_size_ = s;
  }

  [[nodiscard]] uint64_t BufferMem() const {
    return buffer_mem_;
  }

  void SetBufferMem(uint64_t s) {
    buffer_mem_ = s;
  }

  [[nodiscard]] uint64_t BufferDisk() const {
    return buffer_disk_;
  }

  void SetBufferDisk(uint64_t s) {
    buffer_disk_ = s;
  }

  [[nodiscard]] uint64_t NodeID() const {
    return node_id_;
  }

  void SetNodeID(uint64_t s) {
    node_id_ = s;
  }

  [[nodiscard]] const auto& DataFolder() const {
    return data_folder_;
  }

  void SetDataFolder(const std::string& s) {
    data_folder_ = s;
  }


 private:
  uint64_t vec_size_{1 << 10}; // Keep small to prevent too sparse bitmap.
  uint64_t log_block_size_{18};
  uint64_t buffer_mem_{(1ull << 30) << 3}; // 8GB of memory.
  uint64_t buffer_disk_{(1ull << 30) << 8}; // 256GB of disk.
  std::string data_folder_{"smartids_data"};
  uint64_t node_id_ = 0; // For supporting many nodes.
  Settings() = default;
};

}
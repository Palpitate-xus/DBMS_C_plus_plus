#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace dbms {

// Bloom filter index with an exact key->rid side table.
//
// The bloom part answers "might any row carry this key?" in O(k) probes and
// lets scans skip persistence entirely for absent keys; the side map keeps
// the candidate rid list exact (PostgreSQL's bloom access method stores
// signatures per-tuple instead — the observable contract here is the same:
// equality probe in, rid set out, no false negatives).
//
// Layout (little endian):
//   u32 magic 'BLM1'
//   u32 m            bits
//   u32 k            hash functions
//   u32 entryCount
//   entryCount entries: u32 keyLen, key bytes, u32 ridCount, ridCount x u64 rid
// The bit array is rebuilt from the entries on load.
//
// All public methods are serialized by an internal mutex like HashIndex.
class BloomIndex {
public:
    // Signature parameters follow PostgreSQL's bloom defaults in spirit:
    // bits per entry ~ 128 with a 1% false-positive target needs k=7; the
    // array is sized from the expected entry count at creation time.
    BloomIndex(const std::filesystem::path& indexFile,
               uint32_t bitsPerEntry = 128, uint32_t hashes = 7);

    bool open();
    bool close();
    bool flush();

    // Insert key->rid (duplicates allowed).
    bool insert(const std::string& key, int64_t rid);
    // Remove one occurrence of rid for key.
    bool remove(const std::string& key, int64_t rid);

    // Exact candidate list for an equality probe (may include stale rids
    // the heap recheck filters out).
    std::vector<int64_t> search(const std::string& key) const;
    // O(k) membership probe: true when the key may exist (false positives
    // possible), false when it definitely does not.
    bool mightContain(const std::string& key) const;
    bool containsExact(const std::string& key) const;

    void clear();
    bool isOpen() const { return loaded_; }
    const std::filesystem::path& filePath() const { return filePath_; }
    bool hasDirtyData() const { return dirty_; }
    size_t size() const;

    uint32_t bitCount() const { return m_; }
    uint32_t hashCount() const { return k_; }

private:
    std::filesystem::path filePath_;
    uint32_t bitsPerEntry_;
    uint32_t k_;
    uint32_t m_ = 0;   // bit array length, sized on first insert
    std::vector<uint64_t> bits_;
    std::map<std::string, std::vector<int64_t>> entries_;
    bool loaded_ = false;
    bool dirty_ = false;
    mutable std::mutex mutex_;

    void addKeyBits(const std::string& key);
    bool probeKeyBits(const std::string& key) const;
    void rebuildBitsLocked();
    void sizeBitsLocked(size_t entryCount);
    bool loadFromFile();
    bool saveToFile();
};

}  // namespace dbms

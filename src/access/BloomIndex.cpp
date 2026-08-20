#include "access/BloomIndex.h"

#include <cstring>

namespace dbms {

namespace {
constexpr uint32_t kBloomMagic = 0x314D4C42u;  // 'BLM1'

// FNV-1a 64-bit.
uint64_t fnv1a(const std::string& s, uint64_t seed = 0xcbf29ce484222325ULL) {
    uint64_t h = seed;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ULL;
    }
    return h;
}

void putU32(std::string& out, uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void putU64(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
struct Reader {
    const std::string& buf;
    size_t pos = 0;
    bool ok = true;
    uint32_t u32() {
        if (pos + 4 > buf.size()) { ok = false; return 0; }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(static_cast<unsigned char>(buf[pos + i])) << (8 * i);
        pos += 4;
        return v;
    }
    uint64_t u64() {
        if (pos + 8 > buf.size()) { ok = false; return 0; }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(static_cast<unsigned char>(buf[pos + i])) << (8 * i);
        pos += 8;
        return v;
    }
    std::string bytes(size_t n) {
        if (pos + n > buf.size()) { ok = false; return {}; }
        std::string s = buf.substr(pos, n);
        pos += n;
        return s;
    }
};
}  // namespace

BloomIndex::BloomIndex(const std::filesystem::path& indexFile,
                       uint32_t bitsPerEntry, uint32_t hashes)
    : filePath_(indexFile), bitsPerEntry_(bitsPerEntry), k_(hashes) {
    if (k_ == 0) k_ = 1;
    if (k_ > 32) k_ = 32;
    if (bitsPerEntry_ == 0) bitsPerEntry_ = 1;
}

void BloomIndex::sizeBitsLocked(size_t entryCount) {
    // m = entries * bitsPerEntry, rounded up to word granularity; a minimum
    // floor keeps tiny indexes from degenerating into all-ones.
    const uint64_t want = static_cast<uint64_t>(entryCount + 8) * bitsPerEntry_;
    const uint32_t words = static_cast<uint32_t>((want + 63) / 64);
    m_ = static_cast<uint32_t>(words) * 64;
    bits_.assign(words, 0);
}

void BloomIndex::addKeyBits(const std::string& key) {
    if (m_ == 0) return;
    // Double hashing (Kirsch-Mitzenmacher): h_i = h1 + i*h2 mod m.
    const uint64_t h1 = fnv1a(key);
    const uint64_t h2 = fnv1a(key, 0x9E3779B97F4A7C15ULL) | 1ULL;
    for (uint32_t i = 0; i < k_; ++i) {
        uint64_t bit = (h1 + static_cast<uint64_t>(i) * h2) % m_;
        bits_[bit / 64] |= (1ULL << (bit % 64));
    }
}

bool BloomIndex::probeKeyBits(const std::string& key) const {
    if (m_ == 0) return false;
    const uint64_t h1 = fnv1a(key);
    const uint64_t h2 = fnv1a(key, 0x9E3779B97F4A7C15ULL) | 1ULL;
    for (uint32_t i = 0; i < k_; ++i) {
        uint64_t bit = (h1 + static_cast<uint64_t>(i) * h2) % m_;
        if (!(bits_[bit / 64] & (1ULL << (bit % 64)))) return false;
    }
    return true;
}

void BloomIndex::rebuildBitsLocked() {
    sizeBitsLocked(entries_.size());
    for (const auto& kv : entries_) addKeyBits(kv.first);
}

bool BloomIndex::loadFromFile() {
    std::ifstream in(filePath_, std::ios::binary);
    if (!in) {
        // Absent file: start empty (create path).
        loaded_ = true;
        return true;
    }
    std::string buf((std::istreambuf_iterator<char>(in)),
                    std::istreambuf_iterator<char>());
    Reader r{buf};
    if (r.u32() != kBloomMagic) return false;
    const uint32_t m = r.u32();
    const uint32_t k = r.u32();
    const uint32_t count = r.u32();
    if (k == 0 || k > 32) return false;
    k_ = k;
    entries_.clear();
    for (uint32_t i = 0; i < count && r.ok; ++i) {
        const uint32_t keyLen = r.u32();
        if (!r.ok) break;
        std::string key = r.bytes(keyLen);
        const uint32_t ridCount = r.u32();
        if (!r.ok) break;
        std::vector<int64_t> rids;
        rids.reserve(ridCount);
        for (uint32_t j = 0; j < ridCount && r.ok; ++j) rids.push_back(static_cast<int64_t>(r.u64()));
        if (!r.ok) break;
        entries_[std::move(key)] = std::move(rids);
    }
    if (!r.ok) return false;
    rebuildBitsLocked();
    if (m_ != m) {
        // Bit array derived from entry count; keep the loaded size when it is
        // larger so probes stay stable across saves.
        if (m > m_) {
            const uint32_t words = (m + 63) / 64;
            bits_.resize(words, 0);
            m_ = static_cast<uint32_t>(words) * 64;
            rebuildBitsLocked();
        }
    }
    loaded_ = true;
    dirty_ = false;
    return true;
}

bool BloomIndex::saveToFile() {
    std::string buf;
    putU32(buf, kBloomMagic);
    putU32(buf, m_);
    putU32(buf, k_);
    putU32(buf, static_cast<uint32_t>(entries_.size()));
    for (const auto& kv : entries_) {
        putU32(buf, static_cast<uint32_t>(kv.first.size()));
        buf += kv.first;
        putU32(buf, static_cast<uint32_t>(kv.second.size()));
        for (int64_t rid : kv.second) putU64(buf, static_cast<uint64_t>(rid));
    }
    std::filesystem::path tmp = filePath_;
    tmp += ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) return false;
        out.write(buf.data(), static_cast<std::streamsize>(buf.size()));
        if (!out) return false;
    }
    std::error_code ec;
    std::filesystem::rename(tmp, filePath_, ec);
    if (ec) return false;
    dirty_ = false;
    return true;
}

bool BloomIndex::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_) return true;
    return loadFromFile();
}

bool BloomIndex::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_ && dirty_ && !saveToFile()) return false;
    loaded_ = false;
    return true;
}

bool BloomIndex::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_ || !dirty_) return true;
    return saveToFile();
}

bool BloomIndex::insert(const std::string& key, int64_t rid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    entries_[key].push_back(rid);
    // Rebuild only when the entry count crosses the sizing of the bit
    // array; otherwise flipping bits in place is enough.
    if (m_ == 0) {
        rebuildBitsLocked();
    } else {
        const uint64_t want = static_cast<uint64_t>(entries_.size() + 8) * bitsPerEntry_;
        if (want > m_) {
            rebuildBitsLocked();
        } else {
            addKeyBits(key);
        }
    }
    dirty_ = true;
    return true;
}

bool BloomIndex::remove(const std::string& key, int64_t rid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    auto it = entries_.find(key);
    if (it == entries_.end()) return true;
    auto& rids = it->second;
    for (size_t i = 0; i < rids.size(); ++i) {
        if (rids[i] == rid) {
            rids.erase(rids.begin() + static_cast<long>(i));
            break;
        }
    }
    if (rids.empty()) {
        entries_.erase(it);
        // Bits stay set: bloom filters never un-set, false positives on the
        // removed key are filtered by the exact side map.
        dirty_ = true;
    }
    dirty_ = true;
    return true;
}

std::vector<int64_t> BloomIndex::search(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return {};
    if (!probeKeyBits(key)) return {};   // definite miss: skip the map
    auto it = entries_.find(key);
    if (it == entries_.end()) return {};
    return it->second;
}

bool BloomIndex::mightContain(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    return probeKeyBits(key);
}

bool BloomIndex::containsExact(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    return entries_.count(key) > 0;
}

void BloomIndex::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
    bits_.clear();
    m_ = 0;
    dirty_ = true;
}

size_t BloomIndex::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

}  // namespace dbms

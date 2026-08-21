#include "storage/PageCrypto.h"

#include <atomic>
#include <cstring>
#include <fstream>
#include <random>
#include <sys/stat.h>
#include <vector>

#include "sha256.h"

namespace dbms {

namespace {

std::array<uint8_t, PageCrypto::kKeySize>& keyStorage() {
    static std::array<uint8_t, PageCrypto::kKeySize> k{};
    return k;
}

std::atomic<bool>& enabledFlag() {
    static std::atomic<bool> on{false};
    return on;
}

std::string toHex(const uint8_t* data, size_t len) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(digits[data[i] >> 4]);
        out.push_back(digits[data[i] & 0xF]);
    }
    return out;
}

bool fromHex(const std::string& hex, uint8_t* out, size_t outLen) {
    if (hex.size() != outLen * 2) return false;
    auto nibble = [](char c, int& v) {
        if (c >= '0' && c <= '9') { v = c - '0'; return true; }
        if (c >= 'a' && c <= 'f') { v = c - 'a' + 10; return true; }
        if (c >= 'A' && c <= 'F') { v = c - 'A' + 10; return true; }
        return false;
    };
    for (size_t i = 0; i < outLen; ++i) {
        int hi = 0, lo = 0;
        if (!nibble(hex[2 * i], hi) || !nibble(hex[2 * i + 1], lo)) return false;
        out[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

std::string randomHexKey() {
    std::random_device rd;
    std::uniform_int_distribution<uint32_t> dist;
    uint8_t raw[PageCrypto::kKeySize];
    for (size_t i = 0; i < PageCrypto::kKeySize; i += sizeof(uint32_t)) {
        const uint32_t v = dist(rd);
        std::memcpy(raw + i, &v, sizeof(uint32_t));
    }
    return toHex(raw, sizeof(raw));
}

// MAC over nonce || ciphertext || key || pageId.  Page binding makes replay
// of a sealed page at a different pageId fail the MAC.
std::array<uint8_t, PageCrypto::kMacSize> computeMac(
    uint32_t pageId, const uint8_t* nonce, const char* ciphertext,
    size_t cipherLen) {
    const auto& k = keyStorage();
    std::string macInput;
    macInput.reserve(PageCrypto::kNonceSize + cipherLen + PageCrypto::kKeySize +
                     sizeof(pageId));
    macInput.append(reinterpret_cast<const char*>(nonce), PageCrypto::kNonceSize);
    macInput.append(ciphertext, cipherLen);
    macInput.append(reinterpret_cast<const char*>(k.data()), PageCrypto::kKeySize);
    const uint32_t pid = pageId;
    macInput.append(reinterpret_cast<const char*>(&pid), sizeof(pid));
    const auto digest = SHA256::digestBytes(macInput);
    std::array<uint8_t, PageCrypto::kMacSize> out{};
    std::memcpy(out.data(), digest.data(), PageCrypto::kMacSize);
    return out;
}

}  // namespace

std::array<uint8_t, PageCrypto::kKeySize>& PageCrypto::key() { return keyStorage(); }

bool PageCrypto::loadKeyFromFile(const std::string& keyringPath, std::string& error) {
    std::ifstream in(keyringPath);
    if (!in) {
        // First use: create the keyring with fresh randomness.
        const std::string hex = randomHexKey();
        std::ofstream out(keyringPath, std::ios::trunc);
        if (!out) {
            error = "cannot create keyring file " + keyringPath;
            return false;
        }
        out << hex << "\n";
        out.flush();
        if (!out) {
            error = "cannot write keyring file " + keyringPath;
            return false;
        }
        ::chmod(keyringPath.c_str(), 0600);
        return enable(hex);
    }
    std::string hex;
    std::getline(in, hex);
    hex.erase(hex.find_last_not_of(" \t\r\n") + 1);
    if (!enable(hex)) {
        error = "keyring file does not contain a 64-hex-char key";
        return false;
    }
    return true;
}

bool PageCrypto::enable(const std::string& hexKey) {
    std::array<uint8_t, kKeySize> parsed{};
    if (!fromHex(hexKey, parsed.data(), parsed.size())) return false;
    key() = parsed;
    enabledFlag().store(true, std::memory_order_release);
    return true;
}

void PageCrypto::disable() { enabledFlag().store(false, std::memory_order_release); }

bool PageCrypto::enabled() { return enabledFlag().load(std::memory_order_acquire); }

bool PageCrypto::isPlaintextRecord(const uint8_t record[kRecordSize]) {
    for (size_t i = 0; i < kRecordSize; ++i)
        if (record[i] != 0) return false;
    return true;
}

void PageCrypto::clearRecord(uint8_t record[kRecordSize]) {
    std::memset(record, 0, kRecordSize);
}

std::vector<char> PageCrypto::keystream(uint32_t pageId, const uint8_t* nonce,
                                        size_t length) {
    // SHA-256-CTR: blocks of SHA256(key || pageId || nonce || counter).
    std::vector<char> stream(length);
    const auto& k = key();
    uint32_t counter = 0;
    size_t filled = 0;
    while (filled < length) {
        std::string block;
        block.reserve(kKeySize + kNonceSize + 8);
        block.append(reinterpret_cast<const char*>(k.data()), kKeySize);
        const uint32_t pid = pageId;
        block.append(reinterpret_cast<const char*>(&pid), sizeof(pid));
        block.append(reinterpret_cast<const char*>(nonce), kNonceSize);
        const uint32_t ctr = counter++;
        block.append(reinterpret_cast<const char*>(&ctr), sizeof(ctr));
        const auto digest = SHA256::digestBytes(block);
        const size_t take = std::min(digest.size(), length - filled);
        std::memcpy(stream.data() + filled, digest.data(), take);
        filled += take;
    }
    return stream;
}

bool PageCrypto::sealPage(uint32_t pageId, char* page, size_t pageSize,
                          uint8_t record[kRecordSize]) {
    if (!enabled() || pageSize == 0) return false;

    uint8_t nonce[kNonceSize];
    std::random_device rd;
    for (size_t i = 0; i < kNonceSize; i += sizeof(uint32_t)) {
        const uint32_t v = rd();
        std::memcpy(nonce + i, &v, sizeof(uint32_t));
    }

    const auto stream = keystream(pageId, nonce, pageSize);
    for (size_t i = 0; i < pageSize; ++i) {
        page[i] = static_cast<char>(static_cast<uint8_t>(page[i]) ^
                                    static_cast<uint8_t>(stream[i]));
    }

    const auto mac = computeMac(pageId, nonce, page, pageSize);
    std::memcpy(record, nonce, kNonceSize);
    std::memcpy(record + kNonceSize, mac.data(), kMacSize);
    return true;
}

bool PageCrypto::openPage(uint32_t pageId, char* page, size_t pageSize,
                          const uint8_t record[kRecordSize]) {
    if (!enabled()) return false;

    uint8_t nonce[kNonceSize];
    std::memcpy(nonce, record, kNonceSize);

    // Verify MAC before releasing any plaintext (encrypt-then-MAC).
    const auto expect = computeMac(pageId, nonce, page, pageSize);
    if (std::memcmp(expect.data(), record + kNonceSize, kMacSize) != 0)
        return false;

    const auto stream = keystream(pageId, nonce, pageSize);
    for (size_t i = 0; i < pageSize; ++i) {
        page[i] = static_cast<char>(static_cast<uint8_t>(page[i]) ^
                                    static_cast<uint8_t>(stream[i]));
    }
    return true;
}

}  // namespace dbms

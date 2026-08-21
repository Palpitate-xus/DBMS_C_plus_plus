#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// Page-level transparent data encryption (P2-8, pg_tde-style)
//
// Composition: SHA-256-CTR keystream for confidentiality plus a SHA-256
// encrypt-then-MAC tag for tamper detection — the same security composition
// AES-GCM provides, built from the codebase's existing hash primitive
// (no external crypto dependency).
//
// Storage model: the ENTIRE page is ciphertext at rest; the per-page
// envelope (16-byte random nonce + 32-byte MAC) lives in a sidecar file
// (<datafile>.tde, 48 bytes per page, indexed by pageId).  A heap page
// offers no safe in-page envelope location — the line-pointer array grows
// down from the header and tuples grow up from the page end — so keeping
// the envelope out of the page preserves the physical layout exactly.
//
// A sidecar record of all zeroes marks a plaintext (unencrypted) page,
// which lets a database be encrypted in place as pages are rewritten.
//
// The key comes from a keyring file (64 hex chars = 32 bytes), created with
// 0600 permissions on first use when absent.  Page 0 (the structural file
// header) is never encrypted: it carries no user data and must stay legible
// to open the file.
// ============================================================================

class PageCrypto {
public:
    static constexpr size_t kNonceSize = 16;
    static constexpr size_t kMacSize = 32;
    static constexpr size_t kKeySize = 32;
    // One sidecar record per page: nonce || mac.
    static constexpr size_t kRecordSize = kNonceSize + kMacSize;

    // Load (or create) the keyring and derive the page key. Returns false
    // when the file is unreadable or malformed.
    static bool loadKeyFromFile(const std::string& keyringPath, std::string& error);

    // Enable TDE process-wide with the given hex key (64 chars). Returns
    // false when the key is not valid hex.
    static bool enable(const std::string& hexKey);
    static void disable();
    static bool enabled();

    // Seal (encrypt) one page in place.  `record` receives the 48-byte
    // sidecar record (nonce || mac) the caller must persist alongside the
    // page.  Returns false when TDE is disabled or the page is empty.
    static bool sealPage(uint32_t pageId, char* page, size_t pageSize,
                         uint8_t record[kRecordSize]);

    // Open (verify + decrypt) one page in place using its sidecar record.
    // Returns false on MAC mismatch (tampering or wrong key).
    static bool openPage(uint32_t pageId, char* page, size_t pageSize,
                         const uint8_t record[kRecordSize]);

    // True when a sidecar record is the all-zero plaintext marker.
    static bool isPlaintextRecord(const uint8_t record[kRecordSize]);

    // Zero out a record to mark a page as plaintext.
    static void clearRecord(uint8_t record[kRecordSize]);

private:
    static std::array<uint8_t, kKeySize>& key();
    static std::vector<char> keystream(uint32_t pageId,
                                       const uint8_t* nonce, size_t length);
};

}  // namespace dbms

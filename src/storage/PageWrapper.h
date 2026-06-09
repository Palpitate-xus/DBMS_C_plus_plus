#pragma once

#include "Page.h"
#include "PgPage.h"
#include <cstdint>
#include <cstring>

namespace dbms {

// ========================================================================
// PageWrapper - Unified page interface that dispatches to Page or PgPage
// ========================================================================
//
// Backward compatibility:
//   formatVersion 0/1 -> uses legacy Page class (4KB fixed-length rows)
//   formatVersion 2   -> uses PgPage class (8KB PostgreSQL-style tuples)
//
// Slot ID mapping:
//   Page    slotId      = 0, 1, 2, ... (0-based)
//   PgPage  linePtr     = 1, 2, 3, ... (1-based, OffsetNumber)
//   PageWrapper exposes 0-based slotIds externally for both backends.

class PageWrapper {
public:
    PageWrapper(char* buf, size_t pageSize, uint32_t formatVersion);

    void init(uint32_t pageId);

    // Insert a row, return slotId (0-based)
    bool insert(const char* data, size_t len, uint16_t& slotId);

    // Read a row by slotId
    bool read(uint16_t slotId, const char*& data, size_t& len) const;

    // Update a row in-place if possible, otherwise remove + re-insert.
    // newSlotId receives the new slot id if re-inserted.
    bool update(uint16_t slotId, const char* data, size_t len, uint16_t& newSlotId);

    // Delete a row (mark as deleted / LP_UNUSED)
    bool remove(uint16_t slotId);

    // Compact: defragment page, reclaim deleted space
    void compact();

    // Statistics
    uint16_t liveCount() const;
    uint16_t slotCount() const;
    size_t freeSpace() const;

    // Checksum
    bool verifyChecksum() const;
    void writeChecksum();

    // Page chaining (used by legacy Page only)
    uint32_t nextPage() const;
    void setNextPage(uint32_t next);

    // Iterate over all live records: calls fn(slotId, data, len) for each
    template<typename Fn>
    void forEachLive(Fn&& fn) const {
        if (usePgPage()) {
            PgPage pg(buf_);
            pg.forEachLive([&fn](OffsetNumber lp, const char* data, size_t len) {
                fn(toExternalSlot(lp), data, len);
            });
        } else {
            Page page(buf_, pageSize_);
            page.forEachLive([&fn](uint16_t slotId, const char* data, size_t len) {
                fn(slotId, data, len);
            });
        }
    }

private:
    char* buf_;
    size_t pageSize_;
    uint32_t formatVersion_;

    bool usePgPage() const { return formatVersion_ >= 2; }

    // Convert external 0-based slotId to internal representation
    // PgPage uses 1-based OffsetNumber
    static uint16_t toInternalSlot(uint16_t slotId) { return slotId + 1; }
    static uint16_t toExternalSlot(uint16_t internalSlot) { return internalSlot - 1; }
};

} // namespace dbms

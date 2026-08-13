#pragma once

#include "DataFileHeader.h"
#include "PgPage.h"

namespace dbms {

// Stable heap-page facade.  The storage engine has one page layout now:
// PostgreSQL-style 8 KiB pages with one-based line pointers internally and
// zero-based row IDs at the engine boundary.
class PageWrapper {
public:
    PageWrapper(char* buf, size_t pageSize, uint32_t formatVersion);

    void init(uint32_t pageId);
    bool insert(const char* data, size_t len, uint16_t& slotId);
    bool read(uint16_t slotId, const char*& data, size_t& len) const;
    bool update(uint16_t slotId, const char* data, size_t len, uint16_t& newSlotId);
    bool update(uint16_t slotId, const char* data, size_t len) {
        uint16_t dummy = slotId;
        return update(slotId, data, len, dummy);
    }
    bool remove(uint16_t slotId);
    bool restore(uint16_t slotId);
    bool redirect(uint16_t fromSlotId, uint16_t toSlotId);
    void compact();

    uint16_t liveCount() const;
    uint16_t slotCount() const;
    size_t freeSpace() const;
    bool canFit(size_t len) const;
    bool verifyChecksum() const;
    bool isValid() const;
    void writeChecksum();
    uint32_t nextPage() const;
    void setNextPage(uint32_t next);

    template<typename Fn>
    void forEachLive(Fn&& fn) const {
        PgPage page(buf_);
        page.forEachLive([&fn](OffsetNumber linePtr, const char* data, size_t len) {
            fn(toExternalSlot(linePtr), data, len);
        });
    }

private:
    char* buf_;

    static uint16_t toInternalSlot(uint16_t slotId) { return slotId + 1; }
    static uint16_t toExternalSlot(uint16_t linePtr) { return linePtr - 1; }
};

} // namespace dbms

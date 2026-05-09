#pragma once

#include <cstdint>
#include <cstring>

namespace dbms {

// Slotted Page layout for heap file storage.
// A page is divided into three regions:
//   [Header | Slot Array (grows down) | Free Space | Data (grows up from bottom)]
//
//   offset 0          sizeof(Header)      freeOffset    dataOffset      PAGE_SIZE
//   +-----------------+-------------------+-------------+---------------+
//   | Header          | Slot 0 | Slot 1.. |  free       | Record N..0   |
//   +-----------------+-------------------+-------------+---------------+
//
// Records are stored from the bottom (high offset) upward.
// The slot array grows from the header downward.
//
// Page 0 of each .dt file is reserved as a file header page containing:
//   magic, numPages, freeListHead, rowSize

class Page {
public:
    static constexpr size_t PAGE_SIZE = 4096;
    static constexpr uint32_t MAGIC = 0x44415441; // 'DATA'

    // File header stored in page 0 of the .dt file
#pragma pack(push, 1)
    struct FileHeader {
        uint32_t magic;
        uint32_t numPages;      // total number of pages in file
        uint32_t freeListHead;  // head of free page list (0 = none)
        uint32_t rowSize;       // fixed row size for this table
    };
#pragma pack(pop)

    // Slot descriptor for each record on the page
#pragma pack(push, 1)
    struct Slot {
        uint16_t offset;   // byte offset of record within page
        uint16_t length;   // length of record in bytes
        uint16_t flags;    // bit0: isDeleted
    };
#pragma pack(pop)
    static constexpr uint16_t SLOT_DELETED = 0x01;

    // Page header stored at the start of every data page (page >= 1)
#pragma pack(push, 1)
    struct Header {
        uint32_t pageId;       // page number
        uint16_t numSlots;     // number of slots currently on page
        uint16_t freeOffset;   // offset where slot array ends (= start of free space)
        uint16_t dataOffset;   // offset where data region starts (= end of free space)
        uint16_t reserved;
        uint32_t nextPage;     // next page in chain (for overflow / free list)
    };
#pragma pack(pop)

    explicit Page(char* buf) : buf_(buf) {}

    // Initialize a newly allocated page
    void init(uint32_t pageId);

    // Insert a record into the page. Returns slot id via out param.
    // For fixed-length rows, attempts to reuse a deleted slot first.
    bool insert(const char* data, size_t len, uint16_t& slotId);

    // Mark a slot as deleted (tombstone). Returns false if slot invalid.
    bool remove(uint16_t slotId);

    // Read a record. data points into buf_, no copy.
    bool get(uint16_t slotId, const char*& data, size_t& len) const;

    // Update a record in-place if len fits, otherwise remove + insert.
    bool update(uint16_t slotId, const char* data, size_t len);

    // Compact the page: move all live records together and rebuild slot array.
    // This removes fragmentation caused by deleted records.
    void compact();

    // Utility accessors
    size_t freeSpace() const;
    bool canFit(size_t len) const;
    uint16_t slotCount() const { return header()->numSlots; }
    uint32_t id() const { return header()->pageId; }
    uint32_t nextPage() const { return header()->nextPage; }
    void setNextPage(uint32_t next);

    // Number of live (non-deleted) records
    uint16_t liveCount() const;

    // Iterate over all live records: calls fn(slotId, data, len) for each
    template<typename Fn>
    void forEachLive(Fn&& fn) const {
        const Header* h = header();
        for (uint16_t i = 0; i < h->numSlots; ++i) {
            const Slot* s = slot(i);
            if ((s->flags & SLOT_DELETED) == 0) {
                fn(i, buf_ + s->offset, s->length);
            }
        }
    }

private:
    char* buf_;

    Header* header() const { return reinterpret_cast<Header*>(buf_); }
    Slot* slot(uint16_t idx) const {
        return reinterpret_cast<Slot*>(buf_ + sizeof(Header) + idx * sizeof(Slot));
    }
};

} // namespace dbms

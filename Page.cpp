#include "Page.h"

#include <algorithm>

namespace dbms {

// ========================================================================
// Page initialization
// ========================================================================

void Page::init(uint32_t pageId) {
    std::memset(buf_, 0, PAGE_SIZE);
    Header* h = header();
    h->pageId = pageId;
    h->numSlots = 0;
    h->freeOffset = static_cast<uint16_t>(sizeof(Header));
    h->dataOffset = static_cast<uint16_t>(PAGE_SIZE);
    h->reserved = 0;
    h->nextPage = 0;
}

// ========================================================================
// Insert
// ========================================================================

bool Page::insert(const char* data, size_t len, uint16_t& slotId) {
    if (len == 0 || len > PAGE_SIZE - sizeof(Header) - sizeof(Slot)) return false;

    Header* h = header();

    // For fixed-length rows: try to reuse a deleted slot first
    for (uint16_t i = 0; i < h->numSlots; ++i) {
        Slot* s = slot(i);
        if ((s->flags & SLOT_DELETED) != 0 && s->length >= len) {
            // Reuse this slot's space
            std::memcpy(buf_ + s->offset, data, len);
            s->length = static_cast<uint16_t>(len);
            s->flags = 0;
            slotId = i;
            return true;
        }
    }

    // Need new slot + data space
    size_t need = sizeof(Slot) + len;
    if (h->dataOffset < h->freeOffset + need) return false;

    // Write data at the bottom
    h->dataOffset -= static_cast<uint16_t>(len);
    std::memcpy(buf_ + h->dataOffset, data, len);

    // Append slot
    Slot* s = slot(h->numSlots);
    s->offset = h->dataOffset;
    s->length = static_cast<uint16_t>(len);
    s->flags = 0;
    slotId = h->numSlots;
    h->numSlots++;
    h->freeOffset = static_cast<uint16_t>(sizeof(Header) + h->numSlots * sizeof(Slot));
    return true;
}

// ========================================================================
// Remove (tombstone)
// ========================================================================

bool Page::remove(uint16_t slotId) {
    Header* h = header();
    if (slotId >= h->numSlots) return false;
    Slot* s = slot(slotId);
    if ((s->flags & SLOT_DELETED) != 0) return false;
    s->flags |= SLOT_DELETED;
    return true;
}

// ========================================================================
// Get
// ========================================================================

bool Page::get(uint16_t slotId, const char*& data, size_t& len) const {
    const Header* h = header();
    if (slotId >= h->numSlots) return false;
    const Slot* s = slot(slotId);
    if ((s->flags & SLOT_DELETED) != 0) return false;
    data = buf_ + s->offset;
    len = s->length;
    return true;
}

// ========================================================================
// Update
// ========================================================================

bool Page::update(uint16_t slotId, const char* data, size_t len) {
    Header* h = header();
    if (slotId >= h->numSlots) return false;
    Slot* s = slot(slotId);
    if ((s->flags & SLOT_DELETED) != 0) return false;

    if (len <= s->length) {
        // In-place update
        std::memcpy(buf_ + s->offset, data, len);
        if (len < s->length) {
            // Zero pad remainder to avoid stale data
            std::memset(buf_ + s->offset + len, 0, s->length - len);
        }
        return true;
    }

    // Does not fit: remove old + insert new
    if (!remove(slotId)) return false;
    uint16_t newSlotId = 0;
    if (!insert(data, len, newSlotId)) {
        // Failed to insert: restore old slot (best effort)
        s->flags &= ~SLOT_DELETED;
        return false;
    }
    return true;
}

// ========================================================================
// Compact: defragment page by moving live records together
// ========================================================================

void Page::compact() {
    Header* h = header();
    if (h->numSlots == 0) return;

    // Build array of live slots
    struct LiveItem { uint16_t slotId; Slot slot; };
    LiveItem items[256]; // MAX_SLOTS roughly PAGE_SIZE / (min row ~ 10)
    uint16_t liveCount = 0;
    for (uint16_t i = 0; i < h->numSlots; ++i) {
        Slot* s = slot(i);
        if ((s->flags & SLOT_DELETED) == 0 && liveCount < 256) {
            items[liveCount++] = {i, *s};
        }
    }
    if (liveCount == 0) {
        // All deleted: reset page
        init(h->pageId);
        return;
    }

    // Sort by original offset descending (bottom-up order)
    std::sort(items, items + liveCount,
              [](const LiveItem& a, const LiveItem& b) {
                  return a.slot.offset > b.slot.offset;
              });

    // Move records to new compact positions
    uint16_t newDataOffset = static_cast<uint16_t>(PAGE_SIZE);
    for (uint16_t i = 0; i < liveCount; ++i) {
        newDataOffset -= items[i].slot.length;
        std::memmove(buf_ + newDataOffset, buf_ + items[i].slot.offset, items[i].slot.length);
        items[i].slot.offset = newDataOffset;
    }

    // Rebuild slot array: assign slots 0..liveCount-1
    for (uint16_t i = 0; i < liveCount; ++i) {
        Slot* s = slot(i);
        *s = items[i].slot;
    }

    h->numSlots = liveCount;
    h->freeOffset = static_cast<uint16_t>(sizeof(Header) + liveCount * sizeof(Slot));
    h->dataOffset = newDataOffset;
}

// ========================================================================
// Utilities
// ========================================================================

size_t Page::freeSpace() const {
    const Header* h = header();
    if (h->dataOffset <= h->freeOffset) return 0;
    return h->dataOffset - h->freeOffset;
}

bool Page::canFit(size_t len) const {
    const Header* h = header();
    size_t need = sizeof(Slot) + len;
    return h->dataOffset >= h->freeOffset + need;
}

void Page::setNextPage(uint32_t next) {
    header()->nextPage = next;
}

uint16_t Page::liveCount() const {
    const Header* h = header();
    uint16_t count = 0;
    for (uint16_t i = 0; i < h->numSlots; ++i) {
        if ((slot(i)->flags & SLOT_DELETED) == 0) ++count;
    }
    return count;
}

} // namespace dbms

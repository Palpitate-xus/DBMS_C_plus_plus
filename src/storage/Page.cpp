#include "Page.h"

#include <algorithm>

namespace dbms {

// ========================================================================
// Checksum
// ========================================================================

uint16_t Page::computeChecksum(const char* data, size_t len) {
    uint8_t sum1 = 0, sum2 = 0;
    for (size_t i = 0; i < len; ++i) {
        sum1 = (sum1 + static_cast<uint8_t>(data[i])) % 255;
        sum2 = (sum2 + sum1) % 255;
    }
    return (static_cast<uint16_t>(sum2) << 8) | sum1;
}

void Page::writeChecksum() {
    Header* h = header();
    h->checksum = 0;
    h->checksum = computeChecksum(buf_, PAGE_SIZE);
}

bool Page::verifyChecksum() const {
    const Header* h = header();
    uint16_t saved = h->checksum;
    if (saved == 0) return true; // unchecked / legacy page
    const_cast<Header*>(h)->checksum = 0;
    uint16_t computed = computeChecksum(buf_, PAGE_SIZE);
    const_cast<Header*>(h)->checksum = saved;
    return saved == computed;
}

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
    h->checksum = 0;
    h->nextPage = 0;
    writeChecksum();
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
            writeChecksum();
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
    writeChecksum();
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
    writeChecksum();
    return true;
}

bool Page::restore(uint16_t slotId) {
    Header* h = header();
    if (slotId >= h->numSlots) return false;
    Slot* s = slot(slotId);
    if ((s->flags & SLOT_DELETED) == 0) return false;
    s->flags &= ~SLOT_DELETED;
    writeChecksum();
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

bool Page::update(uint16_t slotId, const char* data, size_t len, uint16_t* newSlotId) {
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
        writeChecksum();
        if (newSlotId) *newSlotId = slotId;
        return true;
    }

    // Does not fit: remove old + insert new
    if (!remove(slotId)) return false;
    uint16_t insertedSlotId = 0;
    if (!insert(data, len, insertedSlotId)) {
        // Failed to insert: restore old slot (best effort)
        s->flags &= ~SLOT_DELETED;
        writeChecksum();
        if (newSlotId) *newSlotId = slotId;
        return false;
    }
    if (newSlotId) *newSlotId = insertedSlotId;
    return true;
}

// ========================================================================
// Compact: defragment page by moving live records together
// ========================================================================

void Page::compact() {
    Header* h = header();
    if (h->numSlots == 0) return;

    // Build array of live slots (preserve original slotId)
    struct LiveItem { uint16_t slotId; Slot slot; };
    LiveItem items[256];
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

    // Move records to new compact positions, updating offsets in-place
    uint16_t newDataOffset = static_cast<uint16_t>(PAGE_SIZE);
    for (uint16_t i = 0; i < liveCount; ++i) {
        newDataOffset -= items[i].slot.length;
        std::memmove(buf_ + newDataOffset, buf_ + items[i].slot.offset, items[i].slot.length);
        // Update the original slot's offset (slotId unchanged)
        Slot* origSlot = slot(items[i].slotId);
        origSlot->offset = newDataOffset;
    }

    // Clear deleted slot entries
    for (uint16_t i = 0; i < h->numSlots; ++i) {
        Slot* s = slot(i);
        if (s->flags & SLOT_DELETED) {
            s->offset = 0;
            s->length = 0;
        }
    }

    // Trim trailing deleted slots to shrink slot array
    while (h->numSlots > 0) {
        Slot* s = slot(h->numSlots - 1);
        if ((s->flags & SLOT_DELETED) == 0) break;
        s->offset = 0;
        s->length = 0;
        s->flags = 0;
        h->numSlots--;
    }

    h->freeOffset = static_cast<uint16_t>(sizeof(Header) + h->numSlots * sizeof(Slot));
    h->dataOffset = newDataOffset;
    writeChecksum();
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

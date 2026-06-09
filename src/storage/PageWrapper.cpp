#include "PageWrapper.h"

namespace dbms {

PageWrapper::PageWrapper(char* buf, size_t pageSize, uint32_t formatVersion)
    : buf_(buf), pageSize_(pageSize), formatVersion_(formatVersion) {}

void PageWrapper::init(uint32_t pageId) {
    if (usePgPage()) {
        PgPage pg(buf_);
        pg.init(static_cast<PageId>(pageId));
    } else {
        Page page(buf_, pageSize_);
        page.init(pageId);
    }
}

bool PageWrapper::insert(const char* data, size_t len, uint16_t& slotId) {
    if (usePgPage()) {
        PgPage pg(buf_);
        OffsetNumber lp = 0;
        if (!pg.insert(data, len, lp)) return false;
        slotId = toExternalSlot(lp);
        return true;
    } else {
        Page page(buf_, pageSize_);
        return page.insert(data, len, slotId);
    }
}

bool PageWrapper::read(uint16_t slotId, const char*& data, size_t& len) const {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.get(toInternalSlot(slotId), data, len);
    } else {
        Page page(buf_, pageSize_);
        return page.get(slotId, data, len);
    }
}

bool PageWrapper::update(uint16_t slotId, const char* data, size_t len, uint16_t& newSlotId) {
    if (usePgPage()) {
        PgPage pg(buf_);
        OffsetNumber newLp = 0;
        if (!pg.update(toInternalSlot(slotId), data, len, &newLp)) return false;
        newSlotId = toExternalSlot(newLp);
        return true;
    } else {
        Page page(buf_, pageSize_);
        uint16_t* pNewSlot = &newSlotId;
        return page.update(slotId, data, len, pNewSlot);
    }
}

bool PageWrapper::remove(uint16_t slotId) {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.remove(toInternalSlot(slotId));
    } else {
        Page page(buf_, pageSize_);
        return page.remove(slotId);
    }
}

void PageWrapper::compact() {
    if (usePgPage()) {
        PgPage pg(buf_);
        pg.compact();
    } else {
        Page page(buf_, pageSize_);
        page.compact();
    }
}

uint16_t PageWrapper::liveCount() const {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.liveCount();
    } else {
        Page page(buf_, pageSize_);
        return page.liveCount();
    }
}

uint16_t PageWrapper::slotCount() const {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.numLinePointers();
    } else {
        Page page(buf_, pageSize_);
        return page.slotCount();
    }
}

size_t PageWrapper::freeSpace() const {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.freeSpace();
    } else {
        Page page(buf_, pageSize_);
        return page.freeSpace();
    }
}

bool PageWrapper::canFit(size_t len) const {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.canFit(len);
    } else {
        Page page(buf_, pageSize_);
        return page.canFit(len);
    }
}

bool PageWrapper::verifyChecksum() const {
    if (usePgPage()) {
        PgPage pg(buf_);
        return pg.verifyChecksum();
    } else {
        Page page(buf_, pageSize_);
        return page.verifyChecksum();
    }
}

void PageWrapper::writeChecksum() {
    if (usePgPage()) {
        PgPage pg(buf_);
        pg.writeChecksum();
    } else {
        Page page(buf_, pageSize_);
        page.writeChecksum();
    }
}

uint32_t PageWrapper::nextPage() const {
    if (usePgPage()) {
        // PgPage does not store nextPage in page header; return 0
        return 0;
    } else {
        Page page(buf_, pageSize_);
        return page.nextPage();
    }
}

void PageWrapper::setNextPage(uint32_t next) {
    if (usePgPage()) {
        // PgPage does not support nextPage; no-op
        (void)next;
    } else {
        Page page(buf_, pageSize_);
        page.setNextPage(next);
    }
}

} // namespace dbms

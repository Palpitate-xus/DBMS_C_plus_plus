#include "PageWrapper.h"

#include <stdexcept>

namespace dbms {

PageWrapper::PageWrapper(char* buf, size_t pageSize, uint32_t formatVersion)
    : buf_(buf) {
    if (pageSize != PgPage::PAGE_SIZE || formatVersion != DATA_FILE_FORMAT_VERSION) {
        throw std::invalid_argument("PageWrapper requires the v2 8 KiB heap format");
    }
}

void PageWrapper::init(uint32_t pageId) { PgPage(buf_).init(static_cast<PageId>(pageId)); }

bool PageWrapper::insert(const char* data, size_t len, uint16_t& slotId) {
    OffsetNumber linePtr = 0;
    if (!PgPage(buf_).insert(data, len, linePtr)) return false;
    slotId = toExternalSlot(linePtr);
    return true;
}

bool PageWrapper::read(uint16_t slotId, const char*& data, size_t& len) const {
    return PgPage(buf_).get(toInternalSlot(slotId), data, len);
}

bool PageWrapper::update(uint16_t slotId, const char* data, size_t len, uint16_t& newSlotId) {
    OffsetNumber newLinePtr = 0;
    if (!PgPage(buf_).update(toInternalSlot(slotId), data, len, &newLinePtr)) return false;
    newSlotId = toExternalSlot(newLinePtr);
    return true;
}

bool PageWrapper::remove(uint16_t slotId) { return PgPage(buf_).remove(toInternalSlot(slotId)); }

bool PageWrapper::restore(uint16_t slotId) { return PgPage(buf_).restore(toInternalSlot(slotId)); }

bool PageWrapper::redirect(uint16_t fromSlotId, uint16_t toSlotId) {
    return PgPage(buf_).redirect(toInternalSlot(fromSlotId), toInternalSlot(toSlotId));
}

void PageWrapper::compact() { PgPage(buf_).compact(); }

uint16_t PageWrapper::liveCount() const { return PgPage(buf_).liveCount(); }

uint16_t PageWrapper::slotCount() const { return PgPage(buf_).numLinePointers(); }

size_t PageWrapper::freeSpace() const { return PgPage(buf_).freeSpace(); }

bool PageWrapper::canFit(size_t len) const { return PgPage(buf_).canFit(len); }

bool PageWrapper::verifyChecksum() const { return PgPage(buf_).verifyChecksum(); }

bool PageWrapper::isValid() const { return PgPage(buf_).isValid(); }

void PageWrapper::writeChecksum() { PgPage(buf_).writeChecksum(); }

uint32_t PageWrapper::nextPage() const { return PgPage(buf_).nextPage(); }

void PageWrapper::setNextPage(uint32_t next) { PgPage(buf_).setNextPage(next); }

} // namespace dbms

#include "PageAllocator.h"

#include <iostream>

namespace dbms {

PageAllocator::PageAllocator(const std::string& filename, size_t rowSize)
    : filename_(filename), rowSize_(rowSize), bp_(std::make_unique<BufferPool>(filename, 16)) {}

PageAllocator::~PageAllocator() {
    close();
}

bool PageAllocator::open() {
    if (bp_->isOpen()) return true;
    if (!bp_->open()) return false;

    // Check if page 0 exists and has valid magic
    char* buf = bp_->fetchPage(0);
    Page::FileHeader* fh = reinterpret_cast<Page::FileHeader*>(buf);
    if (fh->magic != Page::MAGIC) {
        // New file: initialize file header
        std::memset(buf, 0, Page::PAGE_SIZE);
        fh->magic = Page::MAGIC;
        fh->numPages = 1;  // only page 0 (header)
        fh->freeListHead = 0;
        fh->rowSize = static_cast<uint32_t>(rowSize_);
        bp_->markDirty(0);
    }
    bp_->unpinPage(0);
    return true;
}

void PageAllocator::close() {
    if (bp_) {
        bp_->flush();
        bp_->close();
    }
}

bool PageAllocator::isOpen() const {
    return bp_ && bp_->isOpen();
}

uint32_t PageAllocator::allocPage() {
    if (!isOpen()) return 0;

    char* fhBuf = bp_->fetchPage(0);
    Page::FileHeader* fh = reinterpret_cast<Page::FileHeader*>(fhBuf);
    uint32_t pageId = 0;

    if (fh->freeListHead != 0) {
        // Reuse a page from the free list
        pageId = fh->freeListHead;
        char* pageBuf = bp_->fetchPage(pageId);
        Page page(pageBuf);
        uint32_t nextFree = page.nextPage();
        bp_->unpinPage(pageId);

        fh->freeListHead = nextFree;
    } else {
        // Extend file with a new page
        pageId = fh->numPages;
        fh->numPages++;

        // Initialize the new page
        char* newBuf = bp_->fetchPage(pageId);
        Page newPage(newBuf);
        newPage.init(pageId);
        bp_->markDirty(pageId);
        bp_->unpinPage(pageId);
    }

    bp_->markDirty(0);
    bp_->unpinPage(0);
    return pageId;
}

void PageAllocator::freePage(uint32_t pageId) {
    if (!isOpen() || pageId == 0) return;

    // Read file header
    char* fhBuf = bp_->fetchPage(0);
    Page::FileHeader* fh = reinterpret_cast<Page::FileHeader*>(fhBuf);

    // Initialize the freed page and link it to free list
    char* pageBuf = bp_->fetchPage(pageId);
    Page page(pageBuf);
    page.init(pageId);
    page.setNextPage(fh->freeListHead);
    bp_->markDirty(pageId);
    bp_->unpinPage(pageId);

    // Update free list head
    fh->freeListHead = pageId;
    bp_->markDirty(0);
    bp_->unpinPage(0);
}

uint32_t PageAllocator::numPages() const {
    if (!isOpen()) return 0;
    char* fhBuf = const_cast<BufferPool*>(bp_.get())->fetchPage(0);
    Page::FileHeader* fh = reinterpret_cast<Page::FileHeader*>(fhBuf);
    uint32_t n = fh->numPages;
    bp_->unpinPage(0);
    return n;
}

char* PageAllocator::fetchPage(uint32_t pageId) {
    if (!isOpen()) return nullptr;
    char* buf = bp_->fetchPage(pageId);
    if (buf && pageId >= 1) {
        Page page(buf);
        if (!page.verifyChecksum()) {
            std::cerr << "[CHECKSUM ERROR] Page " << pageId << " checksum mismatch" << std::endl;
        }
    }
    return buf;
}

void PageAllocator::unpinPage(uint32_t pageId) {
    if (isOpen()) bp_->unpinPage(pageId);
}

void PageAllocator::markDirty(uint32_t pageId) {
    if (isOpen()) bp_->markDirty(pageId);
}

void PageAllocator::flush() {
    if (isOpen()) bp_->flush();
}

bool PageAllocator::readFileHeader(Page::FileHeader& fh) {
    if (!isOpen()) return false;
    char* buf = bp_->fetchPage(0);
    std::memcpy(&fh, buf, sizeof(Page::FileHeader));
    bp_->unpinPage(0);
    return true;
}

void PageAllocator::writeFileHeader(const Page::FileHeader& fh) {
    if (!isOpen()) return;
    char* buf = bp_->fetchPage(0);
    std::memcpy(buf, &fh, sizeof(Page::FileHeader));
    bp_->markDirty(0);
    bp_->unpinPage(0);
}

} // namespace dbms

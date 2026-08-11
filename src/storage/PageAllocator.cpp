#include "PageAllocator.h"

#include <filesystem>
#include <iostream>

namespace dbms {

PageAllocator::PageAllocator(const std::string& filename, size_t rowSize, size_t pageSize, uint32_t formatVersion)
    : filename_(filename), rowSize_(rowSize), pageSize_(pageSize), formatVersion_(formatVersion), bp_(std::make_unique<BufferPool>(filename, 16, pageSize)) {}

PageAllocator::~PageAllocator() {
    close();
}

bool PageAllocator::open() {
    if (pageSize_ != PgPage::PAGE_SIZE || formatVersion_ != DATA_FILE_FORMAT_VERSION) {
        std::cerr << "[storage] unsupported heap format: pageSize=" << pageSize_
                  << ", formatVersion=" << formatVersion_ << std::endl;
        return false;
    }
    bool existingFile = false;
    try {
        existingFile = std::filesystem::exists(filename_) &&
                       std::filesystem::file_size(filename_) != 0;
    } catch (...) {
        return false;
    }
    if (bp_->isOpen()) return true;
    if (!bp_->open()) return false;

    // Check if page 0 exists and has valid magic
    char* buf = bp_->fetchPage(0);
    if (!buf) {
        bp_->close();
        return false;
    }
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(buf);
    if (fh->magic != DATA_FILE_MAGIC) {
        if (existingFile) {
            std::cerr << "[storage] unsupported heap file header: " << filename_ << std::endl;
            bp_->unpinPage(0);
            bp_->close();
            return false;
        }
        // New file: initialize file header
        std::memset(buf, 0, pageSize_);
        fh->magic = DATA_FILE_MAGIC;
        fh->numPages = 1;  // only page 0 (header)
        fh->freeListHead = 0;
        fh->rowSize = static_cast<uint32_t>(rowSize_);
        fh->formatVersion = DATA_FILE_FORMAT_VERSION;
        bp_->markDirty(0);
    } else if (fh->formatVersion != DATA_FILE_FORMAT_VERSION) {
        std::cerr << "[storage] unsupported data file format version: "
                  << fh->formatVersion << std::endl;
        bp_->unpinPage(0);
        bp_->close();
        return false;
    }
    bp_->unpinPage(0);
    return true;
}

void PageAllocator::close() {
    if (bp_) {
        bp_->close();
    }
}

bool PageAllocator::isOpen() const {
    return bp_ && bp_->isOpen();
}

uint32_t PageAllocator::allocPage() {
    if (!isOpen()) return 0;

    char* fhBuf = bp_->fetchPage(0);
    if (!fhBuf) return 0;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);
    uint32_t pageId = 0;

    if (fh->freeListHead != 0) {
        // Reuse a page from the free list
        pageId = fh->freeListHead;
        char* pageBuf = bp_->fetchPage(pageId);
        if (!pageBuf) {
            bp_->unpinPage(0);
            return 0;
        }
        PageWrapper page(pageBuf, pageSize_, formatVersion_);
        uint32_t nextFree = page.nextPage();
        bp_->unpinPage(pageId);

        fh->freeListHead = nextFree;
    } else {
        // Extend file with a new page
        pageId = fh->numPages;
        fh->numPages++;

        // Initialize the new page
        char* newBuf = bp_->fetchPage(pageId);
        if (!newBuf) {
            fh->numPages--;
            bp_->unpinPage(0);
            return 0;
        }
        PageWrapper newPage(newBuf, pageSize_, formatVersion_);
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
    if (!fhBuf) return;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);

    // Initialize the freed page and link it to free list
    char* pageBuf = bp_->fetchPage(pageId);
    if (!pageBuf) {
        bp_->unpinPage(0);
        return;
    }
    PageWrapper page(pageBuf, pageSize_, formatVersion_);
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
    if (!fhBuf) return 0;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);
    uint32_t n = fh->numPages;
    bp_->unpinPage(0);
    return n;
}

char* PageAllocator::fetchPage(uint32_t pageId) {
    if (!isOpen()) return nullptr;
    char* buf = bp_->fetchPage(pageId);
    if (buf && pageId >= 1) {
        PageWrapper page(buf, pageSize_, formatVersion_);
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

bool PageAllocator::flush() {
    return isOpen() && bp_->flush();
}

bool PageAllocator::readFileHeader(DataFileHeader& fh) {
    if (!isOpen()) return false;
    char* buf = bp_->fetchPage(0);
    std::memcpy(&fh, buf, sizeof(DataFileHeader));
    bp_->unpinPage(0);
    return true;
}

void PageAllocator::writeFileHeader(const DataFileHeader& fh) {
    if (!isOpen()) return;
    char* buf = bp_->fetchPage(0);
    std::memcpy(buf, &fh, sizeof(DataFileHeader));
    bp_->markDirty(0);
    bp_->unpinPage(0);
}

} // namespace dbms

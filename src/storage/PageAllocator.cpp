#include "PageAllocator.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>

namespace dbms {

size_t heapBufferFrameCount() {
    static const size_t frames = [] {
        const char* env = std::getenv("DBMS_BUFFER_FRAMES");
        if (env && *env) {
            char* end = nullptr;
            const long value = std::strtol(env, &end, 10);
            if (end && *end == '\0' && value >= 16 && value <= 4096) {
                return static_cast<size_t>(value);
            }
        }
        return static_cast<size_t>(256);
    }();
    return frames;
}

PageAllocator::PageAllocator(const std::string& filename, size_t rowSize, size_t pageSize, uint32_t formatVersion)
    : filename_(filename), rowSize_(rowSize), pageSize_(pageSize), formatVersion_(formatVersion), bp_(std::make_unique<BufferPool>(filename, heapBufferFrameCount(), pageSize)) {
    // Verify heap pages when they are first loaded from disk. Page 0 is the
    // file header (own checksum, validated by validateFileHeader) and is
    // explicitly excluded from the heap-page check.
    const uint32_t fv = formatVersion_;
    bp_->setPageValidator([fv](uint32_t pageId, const char* data) -> bool {
        if (pageId == 0) return true;
        const PageWrapper page(const_cast<char*>(data), PgPage::PAGE_SIZE, fv);
        if (!page.isValid()) {
            std::cerr << "[storage] invalid heap page " << pageId
                      << "; refusing to load corrupted data" << std::endl;
            return false;
        }
        return true;
    });
}

PageAllocator::~PageAllocator() {
    close();
}

bool PageAllocator::open() {
    if (pageSize_ != PgPage::PAGE_SIZE || formatVersion_ != DATA_FILE_FORMAT_VERSION ||
        rowSize_ > std::numeric_limits<uint32_t>::max()) {
        std::cerr << "[storage] unsupported heap format: pageSize=" << pageSize_
                  << ", formatVersion=" << formatVersion_ << ", rowSize=" << rowSize_ << std::endl;
        return false;
    }
    std::error_code fileEc;
    const bool fileExists = std::filesystem::exists(filename_, fileEc);
    if (fileEc) return false;
    const uintmax_t fileBytes = fileExists ? std::filesystem::file_size(filename_, fileEc) : 0;
    if (fileEc) return false;
    const bool existingFile = fileBytes != 0;
    if (existingFile && (fileBytes < pageSize_ || fileBytes % pageSize_ != 0)) {
        std::cerr << "[storage] truncated or misaligned heap file: " << filename_ << std::endl;
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
        fh->headerChecksum = computeDataFileHeaderChecksum(*fh);
        bp_->markDirty(0);
    } else if (!validateFileHeader(*fh)) {
        std::cerr << "[storage] invalid heap file header: " << filename_ << std::endl;
        bp_->unpinPage(0);
        bp_->close();
        return false;
    }
    numPages_ = fh->numPages;
    bp_->unpinPage(0);
    return true;
}

void PageAllocator::close() {
    if (bp_) {
        bp_->close();
    }
    numPages_ = 0;
}

bool PageAllocator::isOpen() const {
    return bp_ && bp_->isOpen();
}

uint32_t PageAllocator::allocPage() {
    if (!isOpen()) return 0;
    // The header page (free-list head, numPages) is read-modify-write state
    // shared by every allocator user; concurrent inserts may run under mere
    // intent locks, so serialize the whole allocation decision.
    std::lock_guard<std::mutex> lock(allocMutex_);

    char* fhBuf = bp_->fetchPage(0);
    if (!fhBuf) return 0;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);
    uint32_t pageId = 0;

    if (fh->freeListHead != 0) {
        // Reuse a page from the free list
        pageId = fh->freeListHead;
        if (pageId >= fh->numPages || pageId >= numPages_) {
            bp_->unpinPage(0);
            return 0;
        }
        char* pageBuf = fetchPage(pageId);
        if (!pageBuf) {
            bp_->unpinPage(0);
            return 0;
        }
        PageWrapper page(pageBuf, pageSize_, formatVersion_);
        uint32_t nextFree = page.nextPage();
        bp_->unpinPage(pageId);

        if (nextFree >= fh->numPages) {
            bp_->unpinPage(0);
            return 0;
        }
        fh->freeListHead = nextFree;
    } else {
        // Extend file with a new page
        if (fh->numPages == std::numeric_limits<uint32_t>::max()) {
            bp_->unpinPage(0);
            return 0;
        }
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
        numPages_ = fh->numPages;
    }

    fh->headerChecksum = computeDataFileHeaderChecksum(*fh);
    bp_->markDirty(0);
    bp_->unpinPage(0);
    return pageId;
}

void PageAllocator::freePage(uint32_t pageId) {
    if (!isOpen() || pageId == 0) return;

    std::lock_guard<std::mutex> lock(allocMutex_);
    // Read file header
    char* fhBuf = bp_->fetchPage(0);
    if (!fhBuf) return;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);

    if (pageId >= numPages_) {
        bp_->unpinPage(0);
        return;
    }

    // Initialize the freed page and link it to free list
    char* pageBuf = fetchPage(pageId);
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
    fh->headerChecksum = computeDataFileHeaderChecksum(*fh);
    bp_->markDirty(0);
    bp_->unpinPage(0);
}

uint32_t PageAllocator::numPages() const {
    if (!isOpen()) return 0;
    std::lock_guard<std::mutex> lock(allocMutex_);
    return numPages_;
}


char* PageAllocator::fetchPage(uint32_t pageId) {
    if (!isOpen()) return nullptr;
    if (pageId >= numPages_) return nullptr;
    // Page integrity (Fletcher-16 checksum + structural checks) is verified
    // once by the buffer pool when a page is loaded from disk; a cached page
    // cannot change underneath a pin, so re-validating on every access only
    // repeated an 8KB checksum per row operation.
    return bp_->fetchPage(pageId);
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

bool PageAllocator::validateFileHeader(const DataFileHeader& fh) const {
    if (fh.magic != DATA_FILE_MAGIC || fh.formatVersion != DATA_FILE_FORMAT_VERSION ||
        fh.numPages == 0 || fh.freeListHead >= fh.numPages ||
        fh.headerChecksum != computeDataFileHeaderChecksum(fh)) {
        return false;
    }
    std::error_code ec;
    const uintmax_t bytes = std::filesystem::file_size(filename_, ec);
    if (ec || bytes < pageSize_ || bytes % pageSize_ != 0) return false;
    const uintmax_t diskPages = bytes / pageSize_;
    return diskPages == fh.numPages &&
           fh.numPages <= std::numeric_limits<uint32_t>::max();
}

} // namespace dbms

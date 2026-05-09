#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "BufferPool.h"
#include "Page.h"

namespace dbms {

// Manages page allocation for a heap data file (.dt).
// Page 0 is reserved as the file header page.
// Pages 1..N are data pages managed via a free list.
class PageAllocator {
public:
    PageAllocator(const std::string& filename, size_t rowSize);
    ~PageAllocator();

    // Open or create the data file.
    bool open();
    void close();
    bool isOpen() const;

    // Allocate a new data page (pageId >= 1).
    // Reuses a page from the free list if available.
    uint32_t allocPage();

    // Free a data page, add it to the free list.
    void freePage(uint32_t pageId);

    // Total number of pages in the file (including page 0).
    uint32_t numPages() const;

    // Direct access to the underlying buffer pool.
    BufferPool* bufferPool() { return bp_.get(); }

    size_t rowSize() const { return rowSize_; }

    // Fetch a page (pinned). Caller must unpin.
    char* fetchPage(uint32_t pageId);
    void unpinPage(uint32_t pageId);
    void markDirty(uint32_t pageId);

    // Flush all dirty pages.
    void flush();

private:
    std::string filename_;
    size_t rowSize_;
    std::unique_ptr<BufferPool> bp_;

    // Read/write the file header stored in page 0.
    bool readFileHeader(Page::FileHeader& fh);
    void writeFileHeader(const Page::FileHeader& fh);
};

} // namespace dbms

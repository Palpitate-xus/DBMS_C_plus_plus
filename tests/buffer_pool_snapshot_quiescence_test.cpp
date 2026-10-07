// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
#include "storage/BufferPool.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

int main() {
    namespace fs = std::filesystem;
    dbms::BufferPool pool("snapshot-buffer.data", 1, 128);
    assert(!pool.quiescentForSnapshot());
    assert(pool.open());
    assert(pool.quiescentForSnapshot());
    char* page = pool.fetchPage(1);
    assert(page);
    assert(!pool.quiescentForSnapshot());
    page[0] = 'v';
    pool.markDirty(1);
    pool.unpinPage(1);
    assert(!pool.quiescentForSnapshot());
    assert(pool.flush());
    assert(pool.quiescentForSnapshot());
    page = pool.fetchPage(1);
    assert(page && page[0] == 'v');
    pool.invalidateAll();
    // Invalidated pins do not appear as ordinary mapped frames.
    assert(!pool.quiescentForSnapshot());
    pool.unpinPage(1);
    assert(pool.quiescentForSnapshot());
    fs::rename("snapshot-buffer.data", "snapshot-buffer.retired");
    {
        std::ofstream out("snapshot-buffer.data", std::ios::binary);
        out << "a different physical owner";
    }
    assert(!pool.quiescentForSnapshot());
    pool.close();
    assert(!pool.quiescentForSnapshot());
    std::cout << "buffer snapshot rejects dirty, pinned, orphaned and retired owners\n";
}

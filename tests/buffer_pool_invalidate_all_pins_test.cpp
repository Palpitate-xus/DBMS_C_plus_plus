// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp

#include "storage/BufferPool.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>

using dbms::BufferPool;

int main() {
    const std::filesystem::path path = "buffer_pool_invalidate_all_pins.dat";
    std::filesystem::remove(path);
    std::filesystem::remove(path.string() + ".tde");

    BufferPool pool(path.string(), 1, 128);
    assert(pool.open());

    char* page = pool.fetchPage(1);
    assert(page != nullptr);
    std::memcpy(page, "persistent", 10);
    pool.markDirty(1);
    pool.unpinPage(1);
    assert(pool.flush());

    // Invalidation may discard this clean mapping, but it must keep the
    // frame unavailable until this caller releases its outstanding pin.
    char* pinned = pool.fetchPage(1);
    assert(pinned != nullptr);
    assert(std::memcmp(pinned, "persistent", 10) == 0);
    pool.invalidateAll();

    // A one-frame pool must not overwrite the pointer still owned by `pinned`.
    assert(pool.fetchPage(2) == nullptr);

    pool.unpinPage(1);
    char* other = pool.fetchPage(2);
    assert(other != nullptr);
    pool.unpinPage(2);

    char* reloaded = pool.fetchPage(1);
    assert(reloaded != nullptr);
    assert(std::memcmp(reloaded, "persistent", 10) == 0);
    pool.unpinPage(1);
    pool.close();

    std::filesystem::remove(path);
    std::filesystem::remove(path.string() + ".tde");
    std::cout << "[BUFFER_POOL] invalidateAll preserves pinned frames\n";
    return 0;
}

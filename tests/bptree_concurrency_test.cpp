// test_sources: src/access/BPTree.cpp src/storage/BufferPool.cpp

#include "access/BPTree.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

static void cleanup(const std::string& path) {
    std::error_code error;
    std::filesystem::remove(path, error);
    error.clear();
    std::filesystem::remove(path + ".tde", error);
}

int main() {
    const std::string indexPath = "/tmp/btree_concurrent_test.idx";
    cleanup(indexPath);
    dbms::BPTree tree(indexPath);
    assert(tree.open());

    const auto keyFor = [](int value) {
        char key[16];
        std::snprintf(key, sizeof(key), "k%06d", value);
        return std::string(key);
    };
    constexpr int initialKeys = 512;
    constexpr int totalKeys = 2048;
    for (int i = 0; i < initialKeys; ++i) assert(tree.insert(keyFor(i), i));

    std::atomic<int> published{initialKeys};
    std::atomic<int> readyReaders{0};
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<bool> failed{false};
    std::vector<std::thread> readers;
    for (int reader = 0; reader < 2; ++reader) {
        readers.emplace_back([&] {
            readyReaders.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            for (int iteration = 0;
                 iteration < 5000 && !done.load(std::memory_order_acquire);
                 ++iteration) {
                const int visible = published.load(std::memory_order_acquire);
                const int candidates[] = {0, visible / 2, visible - 1};
                for (const int candidate : candidates) {
                    int64_t value = -1;
                    if (!tree.search(keyFor(candidate), value) || value != candidate) {
                        failed.store(true, std::memory_order_release);
                        return;
                    }
                }
                if (iteration % 8 == 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            }
        });
    }
    while (readyReaders.load(std::memory_order_acquire) != 2) {
        std::this_thread::yield();
    }

    std::thread writer([&] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        for (int i = initialKeys; i < totalKeys; ++i) {
            if (!tree.insert(keyFor(i), i)) {
                failed.store(true, std::memory_order_release);
                break;
            }
            published.store(i + 1, std::memory_order_release);
        }
        done.store(true, std::memory_order_release);
    });
    start.store(true, std::memory_order_release);

    writer.join();
    for (auto& reader : readers) reader.join();
    assert(!failed.load(std::memory_order_acquire));
    assert(tree.allValues().size() == totalKeys);
    tree.close();
    cleanup(indexPath);
    std::cout << "[BPTREE CONCURRENCY] concurrent readers/writer OK\n";
    return 0;
}

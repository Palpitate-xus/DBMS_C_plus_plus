#include "access/IndexChecksum.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#if defined(__unix__)
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
size_t checks = 0;
void need(bool value, const char* label) {
    ++checks;
    if (!value) throw std::runtime_error(label);
}

// Independent original bit recurrence, without tables or CPU intrinsics.
uint32_t scalar(uint32_t state, const char* data, size_t length) {
    for (size_t i = 0; i < length; ++i) {
        state ^= static_cast<unsigned char>(data[i]);
        for (unsigned bit = 0; bit < 8; ++bit) {
            const uint32_t mask = 0u - (state & 1u);
            state = (state >> 1) ^ (0x82F63B78u & mask);
        }
    }
    return state;
}

uint32_t randomWord(uint32_t& value) {
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;
    return value;
}

void compare(uint32_t state, const char* data, size_t length) {
    using namespace dbms::index_checksum;
    const auto wanted = scalar(state, data, length);
    need(crc32cUpdate(state, data, length) == wanted, "dispatched raw state matches scalar");
    need(detail::crc32cPortableUpdate(state, data, length) == wanted, "portable raw state matches scalar");
#if (defined(__GNUC__) || defined(__clang__)) && \
    (defined(__x86_64__) || defined(__i386__))
    if (detail::hardwareCrc32cAvailable())
        need(detail::crc32cSse42Update(state, data, length) == wanted, "hardware raw state matches scalar");
#endif
    const size_t split = length/3;
    const char* tail = length == 0 ? data : data+split;
    need(crc32cUpdate(crc32cUpdate(state, data, split), tail, length-split) == wanted,
         "arbitrary split preserves unfinalized state");
    need(crc32cFinish(wanted) == (wanted ^ 0xFFFFFFFFu), "one final XOR only");
}
} // namespace

int main() try {
    using namespace dbms::index_checksum;
    uint32_t random = 0x31415926u;
    std::vector<char> data(262144+32);
    for (auto& byte : data) byte = static_cast<char>(randomWord(random));

    // In the standalone component build this is the first public CRC call.
    // No engine/global startup initializes its dispatcher before these threads.
    std::atomic<unsigned> ready{0};
    std::atomic<bool> go{false};
    std::array<bool, 8> results{};
    const auto wanted = scalar(0xFFFFFFFFu, data.data(), 65536);
    std::vector<std::thread> workers;
    for (size_t i = 0; i < results.size(); ++i)
        workers.emplace_back([&, i] {
            ready.fetch_add(1, std::memory_order_release);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            results[i] = crc32cUpdate(0xFFFFFFFFu, data.data(), 65536) == wanted;
        });
    while (ready.load(std::memory_order_acquire) != results.size()) std::this_thread::yield();
    go.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    for (bool result : results) need(result, "concurrent first use");

#if defined(__unix__)
    std::atomic<bool> stop{false}, active{false}, backgroundOk{true};
    std::thread background([&] {
        while (!stop.load(std::memory_order_acquire)) {
            if (crc32cUpdate(0xFFFFFFFFu, data.data(), 65536) != wanted)
                backgroundOk.store(false, std::memory_order_release);
            active.store(true, std::memory_order_release);
        }
    });
    while (!active.load(std::memory_order_acquire)) std::this_thread::yield();
    std::array<bool, 4> forkResults{};
    for (auto& result : forkResults) {
        const auto child = fork();
        if (child == 0) {
            // No inherited engine/thread teardown or C++ allocation in the child.
            _exit(crc32cUpdate(0xFFFFFFFFu, data.data(), 65536) == wanted ? 0 : 2);
        }
        int status = 0;
        result = child > 0 && waitpid(child, &status, 0) == child &&
                 WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }
    stop.store(true, std::memory_order_release);
    background.join();
    for (bool result : forkResults) need(result, "checksum in forked child while parent thread runs");
    need(backgroundOk.load(std::memory_order_acquire), "parent concurrent checksum preserved");
#endif

    need(crc32c(nullptr, 0) == 0, "empty CRC32C known value");
    need(crc32c("123456789", 9) == 0xE3069283u, "standard CRC32C check vector");
    need(crc32c("abc", 3) == 0x364B3FB7u, "independent abc check vector");
    for (uint32_t state : {0u, 1u, 0xFFFFFFFFu, 0xA5C39017u})
        compare(state, nullptr, 0);
    for (size_t offset = 0; offset < 32; ++offset) {
        for (size_t length = 0; length <= 257; ++length)
            compare(randomWord(random), data.data()+offset, length);
        for (size_t length : {511u, 512u, 513u, 4095u, 4096u, 4097u,
                             65535u, 65536u, 65537u, 262143u, 262144u})
            compare(randomWord(random), data.data()+offset, length);
    }
    // Deliberately stream different chunk sizes, including short tails, through
    // the exact raw-state API used by the existing index file readers.
    for (size_t chunk : {1u, 3u, 4u, 7u, 8u, 9u, 31u, 32u, 63u, 64u, 4096u, 65536u}) {
        uint32_t state = 0x89231745u;
        for (size_t pos = 0; pos < 65537; pos += chunk) {
            const size_t length = std::min(chunk, size_t{65537}-pos);
            state = crc32cUpdate(state, data.data()+pos, length);
        }
        need(state == scalar(0x89231745u, data.data(), 65537), "full streamed raw-state identity");
    }
#if defined(__unix__)
    const long pageSize = sysconf(_SC_PAGESIZE);
    need(pageSize >= 4096, "actual OS page size");
    need(static_cast<size_t>(pageSize) <= data.size(), "guard-page seed fits complete page");
    void* mapping = mmap(nullptr, size_t(pageSize)*3, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    need(mapping != MAP_FAILED, "owned guard pages");
    struct Unmap { void* data; size_t size; ~Unmap() { munmap(data, size); } } unmap{mapping, size_t(pageSize)*3};
    char* begin = static_cast<char*>(mapping)+pageSize;
    need(mprotect(begin, pageSize, PROT_READ|PROT_WRITE) == 0, "only middle page readable");
    std::copy_n(data.data(), size_t(pageSize), begin);
    for (size_t length = 0; length <= 129; ++length) {
        compare(0xFFFFFFFFu, begin+pageSize-length, length);
        compare(0x5A13C709u, begin, length);
    }
#endif
    std::cout << "INDEX_CHECKSUM_DISPATCH checks=" << checks << " failures=0 hardware="
              << detail::hardwareCrc32cAvailable() << '\n';
    return 0;
} catch (const std::exception& error) {
    std::cerr << "INDEX_CHECKSUM_DISPATCH_FAIL checks=" << checks << ' ' << error.what() << '\n';
    return 1;
}

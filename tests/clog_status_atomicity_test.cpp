// test_sources: src/storage/CommitLog.cpp
// Exhaust registration allocations without injecting the separate flush phase.
#include "storage/CommitLog.h"
#include <algorithm>
#include <array>
#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <new>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {
using Log = dbms::CommitLog;
using Status = Log::Status;
using Xid = dbms::TxnId;
constexpr Xid targetBase = 10000, peerXid = 50000, primeBase = 80000, prefixXid = 120000;
thread_local bool tracing = false;
thread_local size_t allocationCount = 0, failOrdinal = 0;
thread_local std::array<size_t, 128> allocationSizes {};
thread_local pthread_mutex_t* tracedMutex = nullptr;
thread_local size_t tracedLocks = 0;
enum class State { Empty, Capacity, SameByte, OtherByte, Growth, Cold, PrefixNew, PrefixExisting };
struct Configuration { bool batch; unsigned shift; Status oldStatus, newStatus; State state; };
struct Result { size_t allocations; std::array<size_t,128> sizes; bool good; };
void require(bool good, const char* message) { if (!good) throw std::runtime_error(message); }
Status status(unsigned bits) { return static_cast<Status>(bits & 3); }
struct PrivateDirectory {
    std::string path;
    PrivateDirectory() {
        char pattern[] = "./clog-status-XXXXXX";
        const char* directory = ::mkdtemp(pattern);
        require(directory != nullptr, "private directory");
        path = directory;
    }
    ~PrivateDirectory() {
        tracing = false;
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
};
std::string bytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    require(input.good(), "fixture snapshot");
    return {std::istreambuf_iterator<char>(input), {}};
}
void arm(size_t ordinal) {
    allocationCount = 0; failOrdinal = ordinal; tracedMutex = nullptr; tracedLocks = 0;
    allocationSizes.fill(0); tracing = true;
}
}

void* operator new(std::size_t size) {
    if (tracing) {
        const auto number = allocationCount++;
        if (number < allocationSizes.size()) allocationSizes[number] = size;
        if (failOrdinal && allocationCount == failOrdinal) throw std::bad_alloc();
    }
    if (void* pointer = std::malloc(size ? size : 1)) return pointer;
    throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { std::free(pointer); }

extern "C" int pthread_mutex_lock(pthread_mutex_t* mutex) noexcept {
    using Original = int(*)(pthread_mutex_t*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_mutex_lock"));
    if (!original) ::_exit(86);
    if (tracing) {
        if (!tracedMutex) tracedMutex = mutex;
        if (mutex == tracedMutex && ++tracedLocks == 2) {
            // setStatuses has released its entry-update lock and is entering
            // flush(). Do not fold the separate save/IO exception issue into
            // registration coverage; the actual flush still runs normally.
            tracing = false;
        }
    }
    return original(mutex);
}

static Result runCase(const Configuration& config, size_t ordinal) {
    PrivateDirectory temporary;
    const auto segment = temporary.path + "/pg_xact/0";
    const Xid target = targetBase + config.shift;
    std::vector<Xid> rows;
    std::vector<Status> before;
    for (unsigned i = 0; i < 4; ++i) {
        rows.push_back(targetBase + i);
        before.push_back(i == config.shift ? config.oldStatus : status(i + 1));
    }
    rows.push_back(peerXid); before.push_back(Status::Committed);
    for (unsigned i = 0; i < 13; ++i) { rows.push_back(primeBase + 4 * i); before.push_back(Status::InProgress); }
    rows.push_back(prefixXid); before.push_back(Status::InProgress);
    {
        Log seed(temporary.path);
        for (size_t i = 0; i < rows.size(); ++i) seed.setStatus(rows[i], before[i]);
        require(seed.flush(), "seed statuses");
    }
    Log backend(temporary.path);
    if (config.state != State::Cold) require(backend.getStatus(target) == config.oldStatus, "warm segment");
    const auto remember = [&](Xid xid, Status value) {
        backend.setStatus(xid, value);
        before[static_cast<size_t>(std::find(rows.begin(), rows.end(), xid) - rows.begin())] = value;
    };
    if (config.state == State::Capacity) {
        remember(primeBase, Status::Aborted);
        require(backend.flush(), "retain pending bucket capacity");
    }
    if (config.state == State::SameByte)
        remember(targetBase + (config.shift + 1) % 4, Status::Aborted);
    if (config.state == State::OtherByte) remember(primeBase, Status::Aborted);
    if (config.state == State::Growth)
        for (unsigned i = 0; i < 13; ++i) remember(primeBase + 4 * i, Status::Aborted);
    if (config.state == State::PrefixExisting) remember(prefixXid, Status::Aborted);
    const bool prefix = config.state == State::PrefixNew || config.state == State::PrefixExisting;
    std::vector<std::pair<Xid, Status>> entries;
    if (prefix) entries.emplace_back(prefixXid, Status::Committed);
    entries.emplace_back(target, config.newStatus);
    const auto originalBytes = bytes(segment);
    bool caught = false, wrongException = false, batchSaved = true;
    arm(ordinal);
    try {
        if (config.batch) batchSaved = backend.setStatuses(entries);
        else backend.setStatus(target, config.newStatus);
    } catch (const std::bad_alloc&) { caught = true; }
      catch (...) { wrongException = true; }
    tracing = false;
    const auto allocations = allocationCount;
    const auto sizes = allocationSizes;
    require(allocations <= sizes.size(), "registration allocation trace exceeded inventory");
    const bool injectionGood = !wrongException && caught == (ordinal != 0) && (!caught || allocations == ordinal);
    const auto after = bytes(segment);
    const bool unchangedDisk = !caught || after == originalBytes;
    std::vector<Status> observed;
    for (const auto xid : rows) observed.push_back(backend.getStatus(xid));
    bool entryGood = true;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (rows[i] == target) entryGood = entryGood && observed[i] == (caught ? config.oldStatus : config.newStatus);
        else if (prefix && rows[i] == prefixXid)
            entryGood = entryGood && (observed[i] == (caught ? before[i] : Status::Committed) ||
                                     (caught && observed[i] == Status::Committed));
        else entryGood = entryGood && observed[i] == before[i];
    }
    // Every state actually exposed by a failed operation must remain tracked
    // and survive a successful flush, including completed batch prefixes.
    bool roundTrip = backend.flush();
    {
        Log verifier(temporary.path);
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto disk = verifier.getStatus(rows[i]);
            const auto cache = backend.getStatus(rows[i]);
            roundTrip = roundTrip && disk == observed[i] && cache == observed[i];
        }
    }
    // A retry must still merge a peer's intervening durable update.
    {
        Log peer(temporary.path);
        peer.setStatus(peerXid, Status::Aborted);
        require(peer.flush(), "peer commit before retry");
    }
    if (config.batch) require(backend.setStatuses(entries), "batch retry");
    else { backend.setStatus(target, config.newStatus); require(backend.flush(), "scalar retry"); }
    bool retried = true;
    {
        Log verifier(temporary.path);
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto expected = rows[i] == target ? config.newStatus : rows[i] == peerXid ? Status::Aborted :
                prefix && rows[i] == prefixXid ? Status::Committed : before[i];
            const auto disk = verifier.getStatus(rows[i]);
            const auto cache = backend.getStatus(rows[i]);
            retried = retried && disk == expected && cache == expected;
        }
    }
    const bool good = injectionGood && batchSaved && unchangedDisk && entryGood && roundTrip && retried;
    std::cout << "CLOG_STATUS_ATOMIC_CASE api=" << config.batch << " state=" << static_cast<int>(config.state)
        << " shift=" << config.shift << " old=" << static_cast<int>(config.oldStatus)
        << " new=" << static_cast<int>(config.newStatus) << " allocation=" << ordinal
        << " good=" << good << " injected=" << injectionGood << " unchanged=" << unchangedDisk
        << " entry=" << entryGood << " roundtrip=" << roundTrip << " retry=" << retried;
    if (!ordinal) { std::cout << " trace=" << allocations; for (size_t i = 0; i < allocations; ++i) std::cout << ':' << sizes[i]; }
    std::cout << '\n';
    return {allocations, sizes, good};
}

static bool invalidCase(bool batch) {
    PrivateDirectory temporary;
    Log backend(temporary.path);
    backend.setStatus(targetBase, Status::Aborted);
    require(backend.flush(), "invalid seed");
    const std::vector<std::pair<Xid, Status>> entries{{dbms::INVALID_TXN_ID, Status::Committed}};
    arm(1);
    bool saved = true;
    if (batch) saved = backend.setStatuses(entries);
    else backend.setStatus(dbms::INVALID_TXN_ID, Status::Committed);
    tracing = false;
    const bool good = saved && allocationCount == 0 && backend.getStatus(dbms::INVALID_TXN_ID) == Status::InProgress &&
        backend.getStatus(targetBase) == Status::Aborted;
    std::cout << "CLOG_STATUS_ATOMIC_INVALID api=" << batch << " good=" << good << '\n';
    return good;
}

int main() {
    try {
        std::vector<Configuration> configurations;
        for (bool batch : {false, true}) for (unsigned shift = 0; shift < 4; ++shift)
            for (unsigned old = 0; old < 4; ++old) for (unsigned value = 0; value < 4; ++value)
                configurations.push_back({batch, shift, status(old), status(value), State::Empty});
        for (bool batch : {false, true}) {
            for (const auto state : {State::Capacity, State::SameByte, State::OtherByte, State::Growth, State::Cold})
                configurations.push_back({batch, 2, Status::Aborted, Status::Committed, state});
        }
        for (const auto state : {State::PrefixNew, State::PrefixExisting})
            configurations.push_back({true, 2, Status::Aborted, Status::Committed, state});
        require(configurations.size() == 140, "complete configuration inventory");
        size_t controls = 0, failures = 0, injected = 0;
        for (const auto& config : configurations) {
            const auto baseline = runCase(config, 0);
            ++controls; if (!baseline.good) ++failures;
            for (size_t ordinal = 1; ordinal <= baseline.allocations; ++ordinal) {
                const auto result = runCase(config, ordinal);
                ++controls; ++injected; if (!result.good) ++failures;
            }
        }
        for (bool batch : {false, true}) { ++controls; if (!invalidCase(batch)) ++failures; }
        std::cout << "CLOG_STATUS_ATOMIC_NATIVE_COMPLETE " << controls << " FAILURES " << failures
                  << " CONFIGS " << configurations.size() << " INJECTED_ALLOCS " << injected << '\n';
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        tracing = false;
        std::cerr << "CLOG_STATUS_ATOMIC_DRIVER_EXCEPTION " << error.what() << '\n';
        return 120;
    }
}

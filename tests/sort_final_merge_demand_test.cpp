// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite,--wrap=ftruncate
#include "executor/ExecutionPlan.h"
#include "executor/SortSpill.h"
#include "Config.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <unistd.h>

extern dbms::Config g_config;

namespace {
constexpr size_t rowCount = 8, payloadBytes = 4096;
constexpr int64_t keyBase = 9007199254740993LL;
enum class Fault { None, FinalHead, Refill };
struct Observation {
    size_t creates = 0, truncates = 0, maxDescriptors = 0;
    uint64_t initialExtent = 0, initialWritten = 0, intermediateWritten = 0, read = 0;
    size_t readCalls = 0, finalizedIntermediateRuns = 0;
    bool sourceEof = false, armed = false, triggered = false;
    Fault fault = Fault::None;
} observed;

bool sortFile(int fd) {
    char link[64], name[4096];
    std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    const auto count = ::readlink(link, name, sizeof(name) - 1);
    if (count < 0) return false;
    name[count] = '\0';
    return std::strstr(name, "/.dbms-sort-") != nullptr;
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd"))
        count += sortFile(std::stoi(entry.path().filename().string()));
    return count;
}
bool noPaths() {
    for (const auto& entry : std::filesystem::directory_iterator("."))
        if (entry.path().filename().string().rfind(".dbms-sort-", 0) == 0) return false;
    return true;
}
std::string rawRow(size_t id) {
    const int64_t key = keyBase + static_cast<int64_t>(id);
    const int32_t identity = static_cast<int32_t>(id);
    std::string raw(sizeof(key) + sizeof(identity) + payloadBytes, 'a' + id % 23);
    std::memcpy(raw.data(), &key, sizeof(key));
    std::memcpy(raw.data() + sizeof(key), &identity, sizeof(identity));
    // Preserve binary payload bytes even when an unrelated column is NULL.
    raw.replace(sizeof(key) + sizeof(identity), 8, std::string("\0\nNULL\xce\xb2", 8));
    return raw;
}
std::vector<bool> bitmap(size_t id, bool withNulls) {
    return {withNulls && id % 5 == 0, false, withNulls && id % 3 == 0};
}
class Source final : public dbms::Operator {
    size_t position_ = 0;
    size_t& closed_;
    bool withNulls_;
public:
    Source(size_t& closed, bool withNulls) : closed_(closed), withNulls_(withNulls) {}
    bool open() override { position_ = 0; clearError(); return true; }
    bool next(std::string& raw) override {
        if (position_ == rowCount) { observed.sourceEof = true; return false; }
        raw = rawRow(rowCount - 1 - position_++);
        return true;
    }
    void close() override { ++closed_; }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override {
        if (!position_) return false;
        nulls = bitmap(rowCount - position_, withNulls_);
        return true;
    }
    bool lastColumnIsNull(size_t column) const override {
        return position_ && column < 3 && bitmap(rowCount - position_, withNulls_)[column];
    }
    // This synthetic source has no stored-row owner. Do not fabricate a heap
    // engine/RID: actual persisted provenance is covered by sort_spill_heap_test.
};

dbms::TableSchema schema() {
    dbms::TableSchema table;
    table.tablename = "sort_final_merge_demand";
    table.append(dbms::makeIntColumn("k", true, 4));
    table.append(dbms::makeIntColumn("id", false, 2));
    table.append(dbms::makeStringColumn("payload", true, payloadBytes));
    return table;
}
bool rowMatches(dbms::SortOp& sort, const std::string& raw, size_t id, bool withNulls) {
    std::vector<bool> nulls;
    const auto origin = sort.scanOrigin();
    bool good = raw == rawRow(id) && sort.lastStoredNullBitmap(nulls) && nulls == bitmap(id, withNulls) &&
        origin.engine == nullptr && origin.dbname.empty() && origin.tablename.empty() && origin.rid == 0;
    for (size_t column = 0; column < 3; ++column)
        good = good && sort.lastColumnIsNull(column) == bitmap(id, withNulls)[column];
    return good;
}
bool cleaned() { return descriptors() == 0 && noPaths(); }
bool exactIoError(const dbms::DbError& error) {
    return error.sqlState() == "58030" &&
        error.message() == std::string("could not read sort temporary file: ") + std::strerror(EIO);
}
bool terminalReadFailure(dbms::SortOp& sort, const std::exception_ptr& primary,
                         const dbms::StorageEngine::NullRowBindingState& callerBinding) {
    // A partially read record cannot be retried from its advanced tape
    // position. Failure must release the tapes before the caller closes it.
    bool good = primary && cleaned();
    const auto binding = dbms::StorageEngine::captureNullRowBinding();
    good = good && binding.engine == callerBinding.engine && binding.database == callerBinding.database &&
        binding.table == callerBinding.table && binding.rid == callerBinding.rid &&
        binding.natts == callerBinding.natts;
    const auto reads = observed.readCalls;
    const auto bytes = observed.read;
    const auto writes = observed.intermediateWritten;
    bool firstEof = false, secondEof = false;
    std::exception_ptr retryFailure;
    try {
        std::string unused;
        firstEof = !sort.next(unused);
        secondEof = !sort.next(unused);
    } catch (...) { retryFailure = std::current_exception(); }
    if (retryFailure)
        std::cerr << "Unexpected next() exception after terminal sort failure; same_primary="
                  << (retryFailure == primary) << '\n';
    const auto origin = sort.scanOrigin();
    good = good && firstEof && secondEof && !retryFailure &&
        origin.engine == nullptr && origin.dbname.empty() && origin.tablename.empty() && origin.rid == 0 &&
        observed.readCalls == reads && observed.read == bytes && observed.intermediateWritten == writes;
    sort.close(); sort.close();
    return good && cleaned() && observed.readCalls == reads && observed.read == bytes &&
        observed.intermediateWritten == writes;
}
void reset(Fault fault = Fault::None) { observed = {}; observed.fault = fault; }
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int, void*, size_t, off_t);
extern "C" ssize_t __real_pwrite(int, const void*, size_t, off_t);
extern "C" int __real_ftruncate(int, off_t);

extern "C" int __wrap_mkstemp(char* name) {
    if (std::strncmp(name, ".dbms-sort-", 11) != 0) return __real_mkstemp(name);
    ++observed.creates;
    const auto fd = __real_mkstemp(name);
    observed.maxDescriptors = std::max(observed.maxDescriptors, descriptors());
    return fd;
}
extern "C" ssize_t __wrap_pwrite(int fd, const void* data, size_t size, off_t offset) {
    if (!sortFile(fd)) return __real_pwrite(fd, data, size, offset);
    const auto result = __real_pwrite(fd, data, size, offset);
    if (result <= 0) return result;
    if (!observed.sourceEof) {
        observed.initialWritten += static_cast<uint64_t>(result);
        observed.initialExtent = std::max(observed.initialExtent,
            static_cast<uint64_t>(offset) + static_cast<uint64_t>(result));
    } else {
        observed.intermediateWritten += static_cast<uint64_t>(result);
        // A real finalized header, rather than a zero placeholder. Eight
        // oversized input rows produce 8 -> 4 -> 2 runs with fan-in two.
        if (size == sizeof(dbms::sort_detail::RunHeader) && result == static_cast<ssize_t>(size)) {
            dbms::sort_detail::RunHeader header;
            std::memcpy(&header, data, sizeof(header));
            if (header.rows && header.bytes > sizeof(header)) {
                ++observed.finalizedIntermediateRuns;
                if (observed.fault == Fault::FinalHead && observed.finalizedIntermediateRuns == 6)
                    observed.armed = true;
            }
        }
    }
    return result;
}
extern "C" ssize_t __wrap_pread(int fd, void* data, size_t size, off_t offset) {
    if (!sortFile(fd)) return __real_pread(fd, data, size, offset);
    ++observed.readCalls;
    if (observed.armed && !observed.triggered) {
        observed.triggered = true;
        errno = EIO;
        return -1;
    }
    const auto result = __real_pread(fd, data, size, offset);
    if (result > 0) observed.read += static_cast<uint64_t>(result);
    return result;
}
extern "C" int __wrap_ftruncate(int fd, off_t size) {
    if (sortFile(fd)) ++observed.truncates;
    return __real_ftruncate(fd, size);
}

int main() {
    using namespace dbms;
    const auto originalBudget = g_config.workMemKb;
    g_config.workMemKb = 1;
    const auto table = schema();
    size_t checked = 0, failures = 0;
    const auto check = [&](const char* label, bool good) {
        ++checked;
        failures += !good;
        std::cout << "SORT_FINAL_MERGE_DEMAND " << label << " passed=" << good
                  << " initial_written=" << observed.initialWritten
                  << " intermediate_written=" << observed.intermediateWritten
                  << " read=" << observed.read << " read_calls=" << observed.readCalls
                  << " intermediate_runs=" << observed.finalizedIntermediateRuns
                  << " maxfds=" << observed.maxDescriptors << '\n';
    };

    // Keys exceed exact double integer precision; NULL and payload bitmaps are
    // independent. The NULL peers retain source ordinal order (5 before 0).
    const std::array<std::array<size_t, rowCount>, 4> orders{{
        {{7, 6, 4, 3, 2, 1, 5, 0}}, {{5, 0, 7, 6, 4, 3, 2, 1}},
        {{1, 2, 3, 4, 6, 7, 5, 0}}, {{5, 0, 1, 2, 3, 4, 6, 7}}
    }};
    size_t order = 0;
    for (bool ascending : {false, true}) for (bool first : {false, true}) {
        reset();
        size_t closed = 0, count = 0;
        SortOp sort(std::make_unique<Source>(closed, true), table, "k", ascending, first, true);
        bool good = sort.open() && observed.sourceEof && closed == 1 && descriptors() == 1;
        std::string raw;
        while (sort.next(raw)) {
            good = count < rowCount && rowMatches(sort, raw, orders[order][count], true) && good;
            ++count;
        }
        const uint64_t payload = observed.initialExtent - rowCount * sizeof(sort_detail::RunHeader);
        // Actual syscall bytes, without copying the private RecordHeader ABI.
        // Two intermediate passes write 2*payload; the final merge reads it
        // once, so total reads are 3*payload plus small run headers. A fully
        // materialized final pass would add another payload write AND read.
        good = good && count == rowCount && !sort.hasError() && payload > rowCount * payloadBytes &&
            observed.intermediateWritten >= 2 * payload && observed.intermediateWritten < 2 * payload + 1024 &&
            observed.read >= 3 * payload && observed.read < 3 * payload + 1024 &&
            observed.creates == 2 && observed.truncates == 1 && observed.finalizedIntermediateRuns == 6 &&
            observed.maxDescriptors <= 2;
        sort.close(); sort.close();
        good = good && closed == 1 && cleaned();
        check("full exact rows/NULL/empty origin and one fewer I/O pass", good);
        ++order;
    }

    // Returning a preloaded winner must not replenish its run until the next
    // caller demand. This is our precise I/O demand policy, not a claim about
    // PostgreSQL's prefix after injected filesystem errors. Its source must
    // still be completely consumed, and its key evaluated, before any output.
    {
        reset(Fault::Refill);
        size_t closed = 0;
        SortOp sort(std::make_unique<Source>(closed, false), table, "k", true);
        bool good = sort.open() && observed.sourceEof && closed == 1 && descriptors() == 1;
        const auto reads = observed.readCalls, bytes = observed.read, writes = observed.intermediateWritten;
        const uint64_t payload = observed.initialExtent - rowCount * sizeof(sort_detail::RunHeader);
        observed.armed = true;
        std::string raw;
        try { good = sort.next(raw) && rowMatches(sort, raw, 0, false) && good; }
        catch (const DbError&) { good = false; }
        sort.close(); sort.close();
        good = good && observed.readCalls == reads && observed.read == bytes &&
            observed.intermediateWritten == writes && !observed.triggered &&
            bytes > 2 * payload && bytes < 5 * payload / 2 + 1024 &&
            closed == 1 && cleaned();
        check("early close skips successor row I/O", good);
    }
    {
        reset(Fault::Refill);
        size_t closed = 0, count = 0;
        SortOp sort(std::make_unique<Source>(closed, false), table, "k", true);
        bool good = sort.open() && closed == 1;
        const auto reads = observed.readCalls;
        const auto callerBinding = StorageEngine::captureNullRowBinding();
        observed.armed = true;
        bool exactFailure = false;
        std::exception_ptr primary;
        std::string raw;
        try {
            if (sort.next(raw)) { ++count; good = rowMatches(sort, raw, 0, false) && good; }
            good = good && count == 1 && observed.readCalls == reads && !observed.triggered;
            // The winning run has additional rows. Its >4KiB next record
            // cannot fit in the 64-byte preread buffer, regardless of alignment.
            if (sort.next(raw)) ++count;
        } catch (const DbError& error) {
            primary = std::current_exception();
            exactFailure = exactIoError(error);
        }
        const bool terminal = terminalReadFailure(sort, primary, callerBinding);
        good = good && exactFailure && observed.triggered && count == 1 &&
            observed.readCalls == reads + 1 && closed == 1 && terminal;
        check("first winner then exact next-refill EIO prefix", good);
    }
    {
        reset(Fault::FinalHead);
        size_t closed = 0, count = 0;
        SortOp sort(std::make_unique<Source>(closed, false), table, "k", true);
        bool opened = false, exactFailure = false;
        const auto callerBinding = StorageEngine::captureNullRowBinding();
        std::exception_ptr primary;
        std::string raw;
        try {
            opened = sort.open();
            while (opened && sort.next(raw)) ++count;
        } catch (const DbError& error) {
            primary = std::current_exception();
            exactFailure = exactIoError(error);
        }
        // SortOp::open must release both intermediate and final-head state
        // itself when a final-head read fails; caller cleanup remains idempotent.
        bool good = !opened && count == 0 && observed.triggered && exactFailure && cleaned();
        const bool terminal = terminalReadFailure(sort, primary, callerBinding);
        good = good && closed == 1 && terminal && observed.finalizedIntermediateRuns == 6;
        check("final-head EIO emits zero rows and preserves read error", good);
    }
    {
        reset();
        size_t closed = 0, count = 0;
        SortOp sort(std::make_unique<Source>(closed, false), table, "k", true);
        bool good = sort.open();
        std::string raw;
        good = sort.next(raw) && rowMatches(sort, raw, 0, false) && good;
        sort.close();
        good = cleaned() && good;
        reset();
        good = sort.open() && good;
        while (sort.next(raw)) { good = rowMatches(sort, raw, count, false) && good; ++count; }
        const auto reads = observed.readCalls, writes = observed.intermediateWritten;
        good = !sort.next(raw) && !sort.next(raw) && observed.readCalls == reads &&
            observed.intermediateWritten == writes && good;
        sort.close(); sort.close();
        good = good && count == rowCount && closed == 2 && cleaned();
        check("same graph reopen and repeated EOF require no extra I/O", good);
    }
    g_config.workMemKb = originalBudget;
    assert(checked == 8);
    std::cout << "SORT_FINAL_MERGE_DEMAND_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}

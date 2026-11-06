// test_link_flags: -Wl,--wrap=_ZNSi4readEPcl -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
// Native mechanism observation. Use the same source with both generations.
// The TLS counters observe the entire public typed-probe call, not only GiST.
// std::istream::read counts are C++ calls, never claimed to be kernel syscalls.
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/Config.h"
#include "storage/PageAllocator.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <time.h>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace observer {
struct Counts {
    uint64_t cpp_new_requests = 0, cpp_new_requested_bytes = 0;
    uint64_t stream_read_calls = 0, stream_read_requested_bytes = 0;
    uint64_t stream_read_delivered_bytes = 0;
    uint64_t all_allocator_fetches = 0, target_heap_fetches = 0;
};
thread_local bool enabled = false;
thread_local dbms::PageAllocator* target = nullptr;
thread_local Counts counts{};
void* allocate(size_t bytes, size_t alignment = 0) {
    if (enabled) { ++counts.cpp_new_requests; counts.cpp_new_requested_bytes += bytes; }
    const size_t actual = bytes ? bytes : 1;
    for (;;) {
        void* result = nullptr;
        if (alignment) {
            if (posix_memalign(&result, std::max(alignment, sizeof(void*)), actual) != 0) result = nullptr;
        } else result = std::malloc(actual);
        if (result) return result;
        const auto handler = std::get_new_handler();
        if (!handler) throw std::bad_alloc();
        handler();
    }
}
struct Scope {
    explicit Scope(dbms::PageAllocator* allocator) {
        if (enabled) throw std::runtime_error("nested observer scope");
        counts = {}; target = allocator; enabled = true;
    }
    ~Scope() { enabled = false; target = nullptr; }
};
} // namespace observer

// All allocation entry points use the same real malloc/free family. No header
// is injected into production objects and no C-library allocation is inferred.
void* operator new(size_t n) { return observer::allocate(n); }
void* operator new[](size_t n) { return observer::allocate(n); }
void* operator new(size_t n, const std::nothrow_t&) noexcept { try { return observer::allocate(n); } catch (...) { return nullptr; } }
void* operator new[](size_t n, const std::nothrow_t&) noexcept { try { return observer::allocate(n); } catch (...) { return nullptr; } }
void* operator new(size_t n, std::align_val_t a) { return observer::allocate(n, size_t(a)); }
void* operator new[](size_t n, std::align_val_t a) { return observer::allocate(n, size_t(a)); }
void* operator new(size_t n, std::align_val_t a, const std::nothrow_t&) noexcept { try { return observer::allocate(n, size_t(a)); } catch (...) { return nullptr; } }
void* operator new[](size_t n, std::align_val_t a, const std::nothrow_t&) noexcept { try { return observer::allocate(n, size_t(a)); } catch (...) { return nullptr; } }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }

extern "C" std::istream& realRead(std::istream*, char*, std::streamsize)
    asm("__real__ZNSi4readEPcl");
extern "C" std::istream& wrapRead(std::istream*, char*, std::streamsize)
    asm("__wrap__ZNSi4readEPcl");
extern "C" std::istream& wrapRead(std::istream* stream, char* destination, std::streamsize amount) {
    if (observer::enabled) {
        ++observer::counts.stream_read_calls;
        if (amount > 0) observer::counts.stream_read_requested_bytes += uint64_t(amount);
    }
    auto& result = realRead(stream, destination, amount);
    if (observer::enabled && result.gcount() > 0)
        observer::counts.stream_read_delivered_bytes += uint64_t(result.gcount());
    return result;
}
extern "C" char* realFetch(dbms::PageAllocator*, uint32_t)
    asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*, uint32_t)
    asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* allocator, uint32_t page) {
    if (observer::enabled) {
        ++observer::counts.all_allocator_fetches;
        if (allocator == observer::target) ++observer::counts.target_heap_fetches;
    }
    return realFetch(allocator, page);
}

namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Bound = StorageEngine::GiSTBound;
using Bounds = StorageEngine::GiSTBounds;
using Rids = std::vector<int64_t>;
struct Model { int id; Cell key, number, padding; };
struct Actual { Model value; int64_t rid; };
const std::string table = "gist_cost";
const std::string database = testDbPath("gist_probe_mechanism");
size_t checks = 0, failures = 0, probes = 0;
void check(bool pass, const std::string& label) {
    ++checks;
    if (!pass) { ++failures; std::cerr << "GIST_PROBE_FAIL " << label << '\n'; }
}
void require(bool pass, const std::string& label) {
    check(pass, label);
    if (!pass) throw std::runtime_error("unsafe continuation: " + label);
}
std::string hex(const std::string& value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result; result.reserve(value.size() * 2);
    for (unsigned char byte : value) { result += digits[byte >> 4]; result += digits[byte & 15]; }
    return result;
}
int compare(const std::string& a, const std::string& b) {
    for (size_t i = 0; i < std::min(a.size(), b.size()); ++i) {
        const auto x = static_cast<unsigned char>(a[i]), y = static_cast<unsigned char>(b[i]);
        if (x != y) return x < y ? -1 : 1;
    }
    return a.size() < b.size() ? -1 : a.size() > b.size() ? 1 : 0;
}
std::vector<Model> population() {
    std::vector<Model> rows;
    for (int id = 1; id <= 4096; ++id) {
        Cell key;
        if (id != 2 && id % 17 != 0) {
            if (id == 1 || id % 13 == 0) key = "";
            else if (id == 3) key = "NULL";
            else if (id == 4) key = " ";
            else if (id == 5) key = "key 界\n|;,";
            else if (id == 6) key = std::string(1, '\x7f');
            else if (id >= 4089) key = "needle" + std::to_string(id);
            else { std::ostringstream out; out << "key" << std::setw(4) << std::setfill('0') << id % 1024; key = out.str(); }
            // Deliberate component-only extension: real >SSO physical keys.
            // The fixed P26 normal9 workload is unchanged and is not rerun.
            if (!key->empty() && id % 101 == 0) *key += std::string(96, 'q') + "界";
        }
        rows.push_back({id, std::move(key), id % 19 ? Cell(std::to_string(id - 2048)) : Cell{},
                        id % 11 ? Cell("payload 界\n|;," + std::to_string(id) + std::string(512, 'x')) : Cell{}});
    }
    return rows;
}
std::string readBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary); require(bool(input), "real index image opens");
    input.seekg(0, std::ios::end); const auto size = input.tellg(); require(size >= 0, "real image extent");
    std::string bytes(size_t(size), '\0'); input.seekg(0);
    input.read(bytes.data(), std::streamsize(bytes.size()));
    require(size_t(input.gcount()) == bytes.size(), "whole owned index read"); return bytes;
}
uint32_t crc(const std::string& bytes, size_t end) {
    uint32_t state = 0xffffffffu;
    for (size_t i = 0; i < end; ++i) {
        state ^= static_cast<unsigned char>(bytes[i]);
        for (unsigned bit = 0; bit < 8; ++bit) state = (state >> 1) ^ ((state & 1) ? 0x82f63b78u : 0);
    }
    return state ^ 0xffffffffu;
}
uint64_t integer(const std::string& bytes, size_t& offset, unsigned width, size_t end) {
    require(offset <= end && width <= end - offset, "independent frame extent"); uint64_t result = 0;
    for (unsigned i = 0; i < width; ++i) result |= uint64_t(static_cast<unsigned char>(bytes[offset++])) << (8 * i);
    return result;
}
void verifyImage(const std::string& bytes, const std::vector<Actual>& rows) {
    require(bytes.size() >= 24 && bytes.compare(0, 8, std::string("\0DBMSS\0" "3", 8)) == 0, "actual positive V3 image");
    const size_t end = bytes.size() - 4; size_t trailer = end;
    require(integer(bytes, trailer, 4, bytes.size()) == crc(bytes, end), "independent full CRC32C");
    size_t offset = 8; require(integer(bytes, offset, 4, end) == 3, "exact V3 version");
    const auto count = integer(bytes, offset, 8, end); require(count <= (end - offset) / 24, "bounded count");
    std::map<int64_t, std::string> expected;
    for (const auto& row : rows) if (row.value.key) expected.emplace(row.rid, *row.value.key);
    require(count == expected.size(), "all nonNULL keys including actual encoded empty");
    std::set<int64_t> seen; size_t empty = 0, longKeys = 0;
    for (uint64_t i = 0; i < count; ++i) {
        const auto rid = integer(bytes, offset, 8, end), lo = integer(bytes, offset, 8, end), hi = integer(bytes, offset, 8, end);
        require(rid > 0 && rid <= uint64_t(INT64_MAX) && lo <= end - offset && hi <= end - offset - lo, "genuine RID and key spans");
        const std::string low = bytes.substr(offset, size_t(lo)); offset += size_t(lo);
        const std::string high = bytes.substr(offset, size_t(hi)); offset += size_t(hi);
        require(seen.insert(int64_t(rid)).second && expected.count(int64_t(rid)) && low == high && low == expected.at(int64_t(rid)), "full actual RID/value/NULL exclusion");
        empty += low.empty(); longKeys += low.size() > 15;
    }
    require(offset == end && empty > 0 && longKeys > 0, "exact end, genuine empty and >SSO positives");
    std::cout << "GIST_PROBE_IMAGE bytes=" << bytes.size() << " entries=" << count << " empty=" << empty << " long=" << longKeys << '\n';
}
std::vector<Actual> heapOracle(const std::vector<Model>& model, bool print) {
    const auto schema = g_engine.getTableSchema(database, table); require(schema.len == 4, "actual four-column source");
    auto cursor = g_engine.beginHeapScan(database, table, schema); HeapScanRow row;
    std::vector<Actual> result; std::set<int> ids; std::set<int64_t> rids;
    while (cursor->next(row)) {
        std::vector<std::string> values; std::vector<bool> nulls;
        detail::decodeStoredRowValues(g_engine, database, row.data, schema, row.nulls, values, nulls);
        require(values.size() == 4 && nulls.size() == 4 && nulls == row.nulls && !nulls[0], "same physical tuple and captured NULL bitmap");
        const int id = std::stoi(values[0]); require(id >= 1 && id <= 4096 && ids.insert(id).second && rids.insert(row.rid).second, "actual unique physical PK/RID");
        Model actual{id, nulls[1] ? Cell{} : Cell(values[1]), nulls[2] ? Cell{} : Cell(values[2]), nulls[3] ? Cell{} : Cell(values[3])};
        const auto& expected = model.at(size_t(id - 1)); require(actual.key == expected.key && actual.number == expected.number && actual.padding == expected.padding, "all actual values/NULL match independent model");
        if (print) std::cout << "GIST_PROBE_HEAP id=" << id << " rid=" << row.rid << " key_null=" << nulls[1] << " key_hex=" << hex(values[1]) << " n_null=" << nulls[2] << " n_hex=" << hex(values[2]) << " pad_null=" << nulls[3] << " pad_hex=" << hex(values[3]) << '\n';
        result.push_back({std::move(actual), row.rid});
    }
    require(result.size() == model.size(), "all 4096 committed physical rows"); return result;
}
Rids expected(const std::vector<Actual>& rows, const Bounds& bounds) {
    Rids result;
    for (const auto& row : rows) {
        if (!row.value.key) continue;
        if (bounds.low) { const int c = compare(*row.value.key, bounds.low->value); if (c < 0 || (c == 0 && !bounds.low->inclusive)) continue; }
        if (bounds.high) { const int c = compare(*row.value.key, bounds.high->value); if (c > 0 || (c == 0 && !bounds.high->inclusive)) continue; }
        result.push_back(row.rid);
    }
    std::sort(result.begin(), result.end()); return result;
}
uint64_t nanos(clockid_t clock) { timespec value{}; require(clock_gettime(clock, &value) == 0, "real diagnostic clock"); return uint64_t(value.tv_sec) * 1000000000ULL + uint64_t(value.tv_nsec); }
void printCounts(const std::string& name, const observer::Counts& c) {
    std::cout << "GIST_PROBE_COUNTS " << name << " cpp_new_counting_enabled=1 cpp_new_requests=" << c.cpp_new_requests << " cpp_new_requested_bytes=" << c.cpp_new_requested_bytes << " cpp_istream_read_calls=" << c.stream_read_calls << " cpp_istream_requested_bytes=" << c.stream_read_requested_bytes << " cpp_istream_delivered_bytes=" << c.stream_read_delivered_bytes << " all_allocator_fetches=" << c.all_allocator_fetches << " target_heap_fetches=" << c.target_heap_fetches;
}
void positiveControls(PageAllocator* allocator, const std::filesystem::path& index) {
    observer::Counts a, b, c;
    { observer::Scope scope(allocator); std::string actual(4096, 'p'); volatile char use = actual[17]; (void)use; a = observer::counts; }
    require(a.cpp_new_requests > 0 && a.cpp_new_requested_bytes >= 4096, "real std::string4096 positive allocation control"); printCounts("positive_stdstring4096", a); std::cout << '\n';
    std::ifstream input(index, std::ios::binary); require(bool(input), "positive read source"); char byte = 1;
    { observer::Scope scope(allocator); input.read(&byte, 1); b = observer::counts; }
    require(byte == 0 && b.stream_read_calls == 1 && b.stream_read_requested_bytes == 1 && b.stream_read_delivered_bytes == 1, "real exact one-byte istream read positive control"); printCounts("positive_read1", b); std::cout << '\n';
    size_t seen = 0; bool ok = false;
    { observer::Scope scope(allocator); ok = g_engine.forEachRow(database, table, [&](uint32_t, uint16_t, const char*, size_t) { ++seen; }); c = observer::counts; }
    require(ok && seen == 4096 && c.target_heap_fetches > 0, "same real target allocator positive full heap scan"); printCounts("positive_heap4096", c); std::cout << '\n';
}
void probe(PageAllocator* allocator, const std::vector<Actual>& rows, const std::string& name, const Bounds& bounds, bool contained) {
    const auto wanted = expected(rows, bounds); std::optional<Rids> actual; observer::Counts c;
    const auto wall = std::chrono::steady_clock::now(); const auto process = nanos(CLOCK_PROCESS_CPUTIME_ID), thread = nanos(CLOCK_THREAD_CPUTIME_ID);
    { observer::Scope scope(allocator); actual = contained ? g_engine.tryGiSTSearchContainedBy(database, table, "k", bounds) : g_engine.tryGiSTSearchOverlap(database, table, "k", bounds); c = observer::counts; }
    const auto threadNs = nanos(CLOCK_THREAD_CPUTIME_ID) - thread, processNs = nanos(CLOCK_PROCESS_CPUTIME_ID) - process;
    const auto wallNs = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - wall).count();
    ++probes; check(bool(actual), "actual typed API engaged: " + name); check(c.target_heap_fetches == 0, "no target heap fallback: " + name);
    if (actual) { std::sort(actual->begin(), actual->end()); check(std::adjacent_find(actual->begin(), actual->end()) == actual->end() && *actual == wanted, "full exact RID result: " + name); }
    printCounts(name, c); std::cout << " contained=" << contained << " engaged=" << bool(actual) << " expected=" << wanted.size() << " actual=" << (actual ? actual->size() : 0) << " diagnostic_wall_ns=" << wallNs << " diagnostic_process_cpu_ns=" << processNs << " diagnostic_thread_cpu_ns=" << threadNs << " rids=";
    if (actual) for (auto rid : *actual) std::cout << rid << ',';
    std::cout << '\n';
}
} // namespace

int main() try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); g_config.autoAnalyzeEnabled = false; g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* old; ~Restore() { observer::enabled = false; observer::target = nullptr; if (g_engine.inTransaction()) g_engine.rollbackTransaction(); setCurrentSession(old); } } restore{previous};
    require(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "new private fixture database");
    DdlExecutor ddl;
    require(!ddl.executeSql("CREATE TABLE gist_cost(id integer PRIMARY KEY,k text COLLATE \"C\",n bigint,pad text)", session), "genuine physical four-column declaration");
    const auto model = population(); require(g_engine.beginTransaction(database) == DBStatus::OK, "actual seed transaction");
    for (const auto& row : model) require(g_engine.insertRow(database, table, StorageEngine::SqlRow{{"id", std::to_string(row.id)}, {"k", row.key}, {"n", row.number}, {"pad", row.padding}}) == DBStatus::OK, "real physical insert");
    require(g_engine.commitTransaction() == DBStatus::OK, "actual durable seed commit");
    const auto rows = heapOracle(model, true); const auto schema = g_engine.getTableSchema(database, table);
    require(schema.len == 4 && schema.cols[1].dataType == "text" && schema.cols[1].resolvedCollationIsBinary && !schema.cols[1].resolvedCollationUsesLocale, "actual C text, no forged type/owner");
    auto* allocator = g_engine.getPageAllocator(database, table); require(allocator != nullptr, "actual target heap allocator");
    require(g_engine.createGiSTIndex(database, table, "k") == DBStatus::OK, "actual public GiST producer");
    const auto index = g_engine.dbPath(database) / "gist_cost_k.gist"; const auto original = readBytes(index); verifyImage(original, rows);
    positiveControls(allocator, index);
    const auto exact = [](std::string value) { return Bounds{Bound{value, true}, Bound{std::move(value), true}}; };
    const auto longKey = std::find_if(model.begin(), model.end(), [](const auto& row) { return row.key && row.key->size() > 32; });
    require(longKey != model.end(), "real long key model");
    const std::vector<std::pair<std::string, Bounds>> inputs{
        {"high_inclusive", Bounds{Bound{"needle", true}, {}}},
        {"absent_closed", Bounds{Bound{"absent", true}, Bound{"absenu", true}}},
        {"equal_nonempty", exact("needle4096")}, {"low_inclusive", Bounds{{}, Bound{"key0007", true}}},
        {"dense_nonNULL", Bounds{Bound{"", true}, {}}}, {"equal_empty", exact("")},
        {"strict_above_empty", Bounds{Bound{"", false}, {}}}, {"literal_DEL", exact(std::string(1, '\x7f'))},
        {"long_exact", exact(*longKey->key)}, {"all_unbounded", Bounds{}}
    };
    // Each finite immutable control occurs once per role. No timing distribution,
    // warmup selection, retry, normalSQL speed, or counter-based result oracle.
    for (const auto& item : inputs) { probe(allocator, rows, item.first, item.second, false); probe(allocator, rows, item.first, item.second, true); }
    const auto after = heapOracle(model, false); std::map<int, int64_t> oldRids;
    for (const auto& row : rows) oldRids.emplace(row.value.id, row.rid);
    for (const auto& row : after) check(oldRids.at(row.value.id) == row.rid, "actual heap RID unchanged");
    require(readBytes(index) == original, "full physical index bytes unchanged by probes");
    positiveControls(allocator, index);
    std::cout << "GIST PROBE MECHANISM COMPLETE checks=" << checks << " failures=" << failures << " probes=" << probes << " rows=" << rows.size() << '\n';
    return failures ? 1 : 0;
} catch (const std::exception& error) {
    observer::enabled = false; observer::target = nullptr;
    std::cerr << "GIST_PROBE_EXCEPTION " << error.what() << '\n'; return 1;
}

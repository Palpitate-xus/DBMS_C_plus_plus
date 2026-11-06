// test_link_flags: -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
// P27 native reader validation; real heap and strict generation checks.
// Runner must prove the TM undefined / PageAllocator defined fetchPage symbol
// in separate matching objects with nm, and this program's positive controls.
#include "access/GiSTIndexFormat.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "storage/PageAllocator.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Rids = std::vector<int64_t>;
using Bound = StorageEngine::GiSTBound;
using Bounds = StorageEngine::GiSTBounds;
constexpr size_t cap = 1024 * 1024;
const auto db = testDbPath("gist_probe_reader_validation");
thread_local PageAllocator* watched = nullptr;
thread_local bool armed = false;
thread_local size_t fetches = 0;
size_t checks = 0, failures = 0, probes = 0, zeroFetch = 0, positives = 0, groups = 0;
void check(bool ok, const std::string& label) {
    ++checks;
    if (!ok) { ++failures; std::cerr << "GIST_READER_FAIL " << label << '\n'; }
}
void need(bool ok, const std::string& label) {
    check(ok, label);
    if (!ok) throw std::runtime_error("unsafe fixture continuation: " + label);
}
void group(const std::string& label) { ++groups; std::cout << "GIST_READER_GROUP " << groups << ' ' << label << '\n'; }
std::string readBytes(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary); need(bool(in), "owned image open");
    std::string s{std::istreambuf_iterator<char>(in), {}};
    need(!in.bad(), "owned image complete read"); return s;
}
void writeBytes(const std::filesystem::path& p, const std::string& s) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(s.data(), static_cast<std::streamsize>(s.size())); out.close();
    need(!out.fail(), "owned image complete replacement");
}
struct RestoreImage {
    std::filesystem::path path; std::string bytes;
    ~RestoreImage() {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); out.close();
        if (out.fail()) { ++failures; std::cerr << "GIST_READER_RESTORE_FAIL " << path << '\n'; }
    }
};
uint32_t crc(const std::string& s, size_t length) {
    uint32_t x = 0xffffffffu;
    for (size_t i = 0; i < length; ++i) {
        x ^= static_cast<unsigned char>(s[i]);
        for (unsigned b = 0; b < 8; ++b) x = (x >> 1) ^ ((x & 1) ? 0x82f63b78u : 0);
    }
    return x ^ 0xffffffffu;
}
void append(std::string& s, uint64_t x, unsigned n) {
    for (unsigned i = 0; i < n; ++i) s.push_back(char((x >> (8 * i)) & 255));
}
void overwrite(std::string& s, size_t at, uint64_t x, unsigned n) {
    need(at <= s.size() && n <= s.size() - at, "fixed mutation extent");
    for (unsigned i = 0; i < n; ++i) s[at + i] = char((x >> (8 * i)) & 255);
}
uint64_t number(const std::string& s, size_t& at, unsigned n, size_t end) {
    need(at <= end && n <= end - at, "independent integer extent"); uint64_t x = 0;
    for (unsigned i = 0; i < n; ++i) x |= uint64_t(static_cast<unsigned char>(s[at++])) << (8 * i);
    return x;
}
void renewCrc(std::string& s) {
    need(s.size() >= 4, "CRC trailer exists"); overwrite(s, s.size() - 4, crc(s, s.size() - 4), 4);
}
struct Entry { uint64_t rid; std::string low, high; };
std::string image(unsigned version, const std::vector<Entry>& entries) {
    need(version == 2 || version == 3, "independent image generation");
    std::string s("\0DBMSS\0", 7); s.push_back(char('0' + version));
    append(s, version, 4); append(s, entries.size(), 8);
    for (const auto& e : entries) {
        append(s, e.rid, 8); append(s, e.low.size(), 8); append(s, e.high.size(), 8); s += e.low; s += e.high;
    }
    append(s, crc(s, s.size()), 4); return s;
}
std::vector<Entry> decodeActual(const std::string& s) {
    need(s.size() >= 24 && s.compare(0, 8, std::string("\0DBMSS\0" "3", 8)) == 0, "actual V3 frame");
    const size_t end = s.size() - 4; size_t tail = end;
    need(number(s, tail, 4, s.size()) == crc(s, end), "independent actual V3 CRC");
    size_t at = 8; need(number(s, at, 4, end) == 3, "actual V3 version");
    const auto count = number(s, at, 8, end); need(count <= (end - at) / 24, "actual count extent");
    std::set<uint64_t> rids; std::vector<Entry> entries;
    for (uint64_t i = 0; i < count; ++i) {
        const auto rid = number(s, at, 8, end), lo = number(s, at, 8, end), hi = number(s, at, 8, end);
        need(rid && rid <= uint64_t(INT64_MAX) && lo <= end - at && hi <= end - at - lo,
             "actual RID/payload extents");
        Entry e{rid, s.substr(at, size_t(lo)), {}}; at += size_t(lo);
        e.high = s.substr(at, size_t(hi)); at += size_t(hi);
        need(rids.insert(rid).second, "actual unique heap RID"); entries.push_back(std::move(e));
    }
    need(at == end, "actual exact payload end");
    need(image(3, entries) == s, "independent serializer equals actual writer bytes");
    std::vector<gist_index_format::Entry> publicEntries; uint32_t version = 0;
    need(gist_index_format::decode(s, publicEntries, &version) && version == 3 && publicEntries.size() == entries.size(),
         "existing public codec agrees with genuine V3");
    for (size_t i = 0; i < entries.size(); ++i)
        need(publicEntries[i].rid == entries[i].rid && publicEntries[i].low == entries[i].low &&
             publicEntries[i].high == entries[i].high, "public/independent complete encoded-positive entry");
    return entries;
}
struct Fixture {
    std::string table; bool numeric = false;
    std::vector<Cell> input;
    std::map<int64_t, Cell> rows;
    PageAllocator* allocator = nullptr;
    std::filesystem::path path;
    std::string original;
    std::vector<Entry> entries;
};
struct Watch {
    explicit Watch(PageAllocator* p) { watched = p; fetches = 0; armed = true; }
    ~Watch() { armed = false; watched = nullptr; }
};
void positive(Fixture& f) {
    size_t count = 0; bool ok = false;
    { Watch watch(f.allocator); ok = g_engine.forEachRow(db, f.table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }); }
    need(ok && count == f.input.size() && fetches > 0, "real target-allocator heap positive control");
    positives += fetches; std::cout << "GIST_READER_HEAP_POSITIVE table=" << f.table << " fetches=" << fetches << '\n';
}
void verifyHeap(Fixture& f, bool first) {
    const auto schema = g_engine.getTableSchema(db, f.table); need(schema.len == 3, "genuine id/key/payload declaration");
    auto cursor = g_engine.beginHeapScan(db, f.table, schema); HeapScanRow row;
    std::map<int64_t, Cell> rows; std::set<int> ids;
    while (cursor->next(row)) {
        std::vector<std::string> values; std::vector<bool> nulls;
        detail::decodeStoredRowValues(g_engine, db, row.data, schema, row.nulls, values, nulls);
        need(row.nulls.size() == 3 && values.size() == 3 && nulls == row.nulls, "actual tuple and bitmap owner");
        const int id = std::stoi(values[0]); need(id > 0 && size_t(id) <= f.input.size() && ids.insert(id).second,
                                               "actual unique seeded PK");
        const Cell key = nulls[1] ? Cell{} : Cell(values[1]);
        check(!nulls[0] && !nulls[2] && key == f.input[size_t(id - 1)] && values[2] == "payload-" + std::to_string(id),
              "full real key/payload/NULL oracle: " + f.table + " PK " + std::to_string(id));
        uint32_t page = 0; uint16_t slot = 0; StorageEngine::decodeRid(row.rid, page, slot);
        need(page > 0 && StorageEngine::encodeRid(page, slot) == row.rid && rows.emplace(row.rid, key).second,
             "real physical RID and uniqueness");
        std::cout << "GIST_READER_ROW table=" << f.table << " id=" << id << " rid=" << row.rid
                  << " is_null=" << !key << " key_bytes=" << (key ? key->size() : 0)
                  << " key_crc=" << (key ? crc(*key, key->size()) : 0) << '\n';
    }
    need(rows.size() == f.input.size(), "complete genuine heap population");
    if (first) f.rows = std::move(rows); else check(f.rows == rows, "final complete values/NULLs/RIDs unchanged");
}
Fixture create(DdlExecutor& ddl, Session& session, std::string name, bool numeric, std::vector<Cell> input) {
    Fixture f; f.table = std::move(name); f.numeric = numeric; f.input = std::move(input);
    const std::string type = numeric ? "bigint" : "text COLLATE \"C\"";
    need(!ddl.executeSql("CREATE TABLE " + f.table + "(id integer PRIMARY KEY,k " + type + ",payload text)", session),
         "real physical relation");
    need(g_engine.beginTransaction(db) == DBStatus::OK, "actual seed owner");
    for (size_t i = 0; i < f.input.size(); ++i)
        need(g_engine.insertRow(db, f.table, StorageEngine::SqlRow{{"id", std::to_string(i + 1)},
             {"k", f.input[i]}, {"payload", "payload-" + std::to_string(i + 1)}}) == DBStatus::OK, "real SQL NULL-aware insert");
    need(g_engine.commitTransaction() == DBStatus::OK, "actual seed commit");
    verifyHeap(f, true); f.allocator = g_engine.getPageAllocator(db, f.table);
    need(f.allocator != nullptr, "actual PageAllocator"); positive(f);
    need(g_engine.createGiSTIndex(db, f.table, "k") == DBStatus::OK, "genuine physical GiST writer");
    f.path = g_engine.dbPath(db) / (f.table + "_k.gist"); f.original = readBytes(f.path); f.entries = decodeActual(f.original);
    size_t nonnull = 0; for (const auto& row : f.rows) if (row.second) ++nonnull;
    need(nonnull == f.entries.size(), "all and only actual non-NULL rows encoded including empty");
    for (const auto& e : f.entries) need(f.rows.count(int64_t(e.rid)) && f.rows.at(int64_t(e.rid)) &&
        *f.rows.at(int64_t(e.rid)) == e.low && e.low == e.high, "each index scalar is the genuine complete heap datum");
    std::cout << "GIST_READER_IMAGE table=" << f.table << " bytes=" << f.original.size()
              << " entries=" << f.entries.size() << " cap=" << cap << '\n'; return f;
}
int compare(const Fixture& f, const std::string& a, const std::string& b) {
    if (f.numeric) { const auto x = std::stoll(a), y = std::stoll(b); return x < y ? -1 : x > y ? 1 : 0; }
    const auto c = a.compare(b); return c < 0 ? -1 : c > 0 ? 1 : 0;
}
Bounds equal(const std::string& s) { return {Bound{s, true}, Bound{s, true}}; }
Rids expected(const Fixture& f, const std::vector<Entry>& fileOrder, const Bounds& b) {
    Rids rids;
    for (const auto& e : fileOrder) {
        need(f.rows.count(int64_t(e.rid)) && f.rows.at(int64_t(e.rid)), "oracle uses real non-NULL heap RID");
        const auto& key = *f.rows.at(int64_t(e.rid));
        if (b.low) { const int c = compare(f, key, b.low->value); if (c < 0 || (c == 0 && !b.low->inclusive)) continue; }
        if (b.high) { const int c = compare(f, key, b.high->value); if (c > 0 || (c == 0 && !b.high->inclusive)) continue; }
        rids.push_back(int64_t(e.rid));
    }
    return rids;
}
void probe(Fixture& f, const Bounds& b, bool engaged, const Rids& exactOrder, const std::string& label) {
    for (bool contained : {false, true}) {
        ++probes; need(g_engine.getPageAllocator(db, f.table) == f.allocator, "same actual allocator before watch");
        std::optional<Rids> result;
        { Watch watch(f.allocator); result = contained ? g_engine.tryGiSTSearchContainedBy(db, f.table, "k", b)
                                                     : g_engine.tryGiSTSearchOverlap(db, f.table, "k", b); }
        check(fetches == 0, "zero target-user-heap fetches: " + label); if (fetches == 0) ++zeroFetch;
        check(bool(result) == engaged, "strict optional admission: " + label);
        if (result) check(engaged && *result == exactOrder, "full exact genuine RID vector in original file order: " + label);
        std::cout << "GIST_READER_PROBE table=" << f.table << " label=" << label << " contained=" << contained
                  << " engaged=" << bool(result) << " heap_fetches=" << fetches << " rids=";
        if (result) for (auto rid : *result) std::cout << rid << ','; std::cout << '\n';
    }
}
void valid(Fixture& f, const Bounds& b, const std::vector<Entry>& order, const std::string& label) {
    probe(f, b, true, expected(f, order, b), label);
}
void invalid(Fixture& f, const std::string& bad, const std::string& label) {
    RestoreImage restore{f.path, f.original}; writeBytes(f.path, bad);
    // Both queries must inspect the late fault: an early match cannot leak
    // partial candidates, and a high-end miss cannot skip full validation.
    probe(f, Bounds{}, false, {}, label + "-early-match");
    probe(f, equal(f.numeric ? "9223372036854775807" : "zzzzzzzzzzzz"), false, {}, label + "-absent");
}
} // namespace
extern "C" char* realFetch(dbms::PageAllocator*, uint32_t) asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*, uint32_t) asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* p, uint32_t page) {
    if (armed && p == watched) ++fetches; return realFetch(p, page);
}
int main() try {
    using namespace dbms; TypeRegistry::instance().bootstrap();
    g_config.autoAnalyzeEnabled = false; g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = db;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Scope { Session* old; ~Scope() { armed = false; watched = nullptr;
        if (g_engine.inTransaction()) g_engine.rollbackTransaction(); setCurrentSession(old); } } scope{previous};
    need(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "owned private database"); DdlExecutor ddl;
    group("genuine small V3 / high-end miss / optional finite empty / exact file order");
    auto f = create(ddl, session, "reader_small", false,
        {Cell(""), Cell{}, Cell("alpha"), Cell(""), Cell(std::string(1, '\x7f')), Cell("界"),
         Cell("__TOAST__1"), Cell("NULL"), Cell("omega"), Cell("a界"), Cell("z")});
    need(f.original.size() < cap, "genuine small fast-file lane");
    valid(f, Bounds{}, f.entries, "all-nonNULL"); valid(f, equal(""), f.entries, "finite-empty");
    valid(f, Bounds{Bound{"z", false}, {}}, f.entries, "strict-high");
    valid(f, equal("zzzzzzzzzzzz"), f.entries, "absent-engaged-empty");
    group("unique descending and nonmonotonic RID orders remain legal and unchanged");
    for (bool descending : {true, false}) {
        auto order = f.entries;
        std::sort(order.begin(), order.end(), [](const auto& a, const auto& b) { return a.rid > b.rid; });
        if (!descending) std::rotate(order.begin(), order.begin() + 3, order.end());
        RestoreImage restore{f.path, f.original}; writeBytes(f.path, image(3, order));
        valid(f, Bounds{}, order, descending ? "descending-file-order" : "nonmonotonic-file-order");
        valid(f, Bounds{{}, Bound{"omega", true}}, order, "unordered-upper-restriction");
    }
    group("CRC-valid late faults / no partial outputs / complete tail and version validation");
    auto bad = f.entries; bad.back().high = bad.back().low + "z";
    invalid(f, image(3, bad), "late-ordered-unequal-scalar");
    bad = f.entries; bad.back().low = "z"; bad.back().high = "a";
    invalid(f, image(3, bad), "late-reversed-range");
    bad = f.entries; bad.back().rid = bad.front().rid; invalid(f, image(3, bad), "late-duplicate-RID");
    bad = f.entries; bad.back().rid = 0; invalid(f, image(3, bad), "late-zero-RID");
    bad = f.entries; bad.back().rid = uint64_t(INT64_MAX) + 1; invalid(f, image(3, bad), "late-out-of-range-RID");
    auto bytes = f.original; overwrite(bytes, 12, f.entries.size() - 1, 8); renewCrc(bytes);
    invalid(f, bytes, "too-small-count-leaves-entry-tail");
    bytes = f.original; overwrite(bytes, 12, f.entries.size() + 1, 8); renewCrc(bytes);
    invalid(f, bytes, "too-large-count");
    bytes = f.original; bytes.insert(bytes.size() - 4, 1, 'x'); renewCrc(bytes);
    invalid(f, bytes, "CRC-valid-unclaimed-payload-byte");
    bytes = f.original; overwrite(bytes, 8, 2, 4); renewCrc(bytes);
    invalid(f, bytes, "V3-magic-V2-version");
    bytes = f.original; bytes[bytes.size() - 5] ^= 1; invalid(f, bytes, "late-bad-CRC");
    // The final entry's length fields begin after every preceding full entry.
    size_t finalAt = 20; for (size_t i = 0; i + 1 < f.entries.size(); ++i)
        finalAt += 24 + f.entries[i].low.size() + f.entries[i].high.size();
    bytes = f.original; overwrite(bytes, finalAt + 16, UINT64_MAX, 8); renewCrc(bytes);
    invalid(f, bytes, "late-length-overflow");
    // A headerless zero-byte image has no complete-coverage proof. Both
    // typed APIs must ask for heap fallback for a genuine nonempty datum.
    { RestoreImage restore{f.path, f.original}; writeBytes(f.path, "");
      probe(f, equal("omega"), false, {}, "headerless-zero-byte-must-fallback"); }
    need(readBytes(f.path) == f.original, "each failed optional leaves original generation restored");
    valid(f, Bounds{}, f.entries, "full-good-result-after-late-faults");
    group("V2 genuinely omits empty / no coverage upgrade / legacy text unique order");
    std::vector<Entry> nonempty; for (const auto& e : f.entries) if (!e.low.empty()) nonempty.push_back(e);
    need(nonempty.size() + 2 == f.entries.size(), "two genuine non-NULL empty keys omitted only in legacy model");
    { RestoreImage restore{f.path, f.original}; writeBytes(f.path, image(2, nonempty));
      probe(f, equal(""), false, {}, "V2-finite-empty-must-fallback");
      probe(f, Bounds{}, false, {}, "V2-unbounded-must-fallback");
      valid(f, Bounds{Bound{"", false}, {}}, nonempty, "V2-strict-nonempty-can-engage");
      valid(f, equal("omega"), nonempty, "V2-nonempty-equality-can-engage"); }
    { auto order = nonempty; std::reverse(order.begin(), order.end()); std::ostringstream legacy;
      for (const auto& e : order) legacy << e.rid << ' ' << std::quoted(e.low) << ' ' << std::quoted(e.high) << '\n';
      RestoreImage restore{f.path, f.original}; writeBytes(f.path, legacy.str());
      valid(f, Bounds{Bound{"", false}, {}}, order, "legacy-text-unique-order"); }
    group("genuine zero-count all-NULL image");
    auto empty = create(ddl, session, "reader_nulls", false, {Cell{}, Cell{}});
    need(empty.entries.empty() && empty.original.size() == 24, "actual writer zero-count generation");
    valid(empty, Bounds{}, empty.entries, "zero-count-unbounded-engaged-empty");
    valid(empty, equal(""), empty.entries, "zero-count-finite-empty-engaged-empty");
    group("actual writer images immediately below and above the 1MiB capture cap");
    std::vector<Cell> belowKeys, aboveKeys;
    for (size_t i = 0; i < 32; ++i) {
        std::string s(16371, char('a' + i % 26)); s.replace(0, 2, (i < 10 ? "0" : "") + std::to_string(i));
        belowKeys.emplace_back(s); s.push_back('x'); aboveKeys.emplace_back(std::move(s));
    }
    auto below = create(ddl, session, "reader_below", false, belowKeys);
    auto above = create(ddl, session, "reader_above", false, aboveKeys);
    need(below.original.size() == cap - 40 && above.original.size() == cap + 24,
         "actual complete writer sizes prove bounded and original-stream lanes");
    for (auto* lane : {&below, &above}) {
        valid(*lane, Bounds{}, lane->entries, "cap-lane-full-genuine-RIDs");
        valid(*lane, equal(*lane->input.front()), lane->entries, "cap-lane-real-long-TOAST-key");
        valid(*lane, equal("zzzzzzzzzzzz"), lane->entries, "cap-lane-absent");
        auto late = lane->entries; late.back().rid = late.front().rid;
        invalid(*lane, image(3, late), "cap-lane-late-duplicate");
    }
    group("numeric comparison and invalid-value partition remain original");
    auto numeric = create(ddl, session, "reader_numeric", true,
        {Cell("-9223372036854775808"), Cell("-9"), Cell{}, Cell("0"), Cell("2"), Cell("19"), Cell("9223372036854775807")});
    need(numeric.original.size() < cap, "actual numeric image uses buffered lane");
    valid(numeric, Bounds{Bound{"-9", false}, Bound{"19", true}}, numeric.entries, "signed-integer-range");
    valid(numeric, Bounds{Bound{"9223372036854775807", false}, {}}, numeric.entries, "numeric-absent");
    auto invalidNumeric = numeric.entries; invalidNumeric.back().low = invalidNumeric.back().high = "not-an-integer";
    invalid(numeric, image(3, invalidNumeric), "late-unparseable-integer");
    probe(numeric, equal("bad-bound"), false, {}, "invalid-numeric-query-bound");
    valid(numeric, Bounds{}, numeric.entries, "numeric-good-after-invalid");
    group("final full heap/RID/NULL and original-image preservation");
    for (auto* fixture : {&f, &empty, &below, &above, &numeric}) {
        check(readBytes(fixture->path) == fixture->original, "final original immutable image");
        verifyHeap(*fixture, false); positive(*fixture);
    }
    check(groups == 8 && probes == 110 && probes == zeroFetch && positives > 0, "all fixed groups and positive/zero fetch evidence");
    std::cout << "GIST_PROBE_READER checks=" << checks << " failures=" << failures << " groups=" << groups
              << " probes=" << probes << " zero_heap_fetch_probes=" << zeroFetch
              << " positive_heap_fetches=" << positives << '\n';
    return failures ? 1 : 0;
} catch (const std::exception& e) {
    armed = false; watched = nullptr;
    std::cerr << "GIST_PROBE_READER_FATAL checks=" << checks << " failures=" << failures << ' ' << e.what() << '\n';
    return 1;
}

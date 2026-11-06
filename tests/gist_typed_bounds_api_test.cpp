// test_link_flags: -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
// After-only typed seam. Compile against the complete matching candidate ABI.
// PageAllocator::fetchPage is defined in a different TU from TableManage.cpp.
// Before accepting a zero count, the runner must verify the unresolved TM
// reference / separate PA definition with nm, and this test's real heap scan
// must exercise the exact same wrapped external symbol with a positive count.
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
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Bound = StorageEngine::GiSTBound;
using Bounds = StorageEngine::GiSTBounds;
using Rids = std::vector<int64_t>;
thread_local PageAllocator* watchedAllocator = nullptr;
thread_local bool watching = false;
thread_local size_t heapFetches = 0;
size_t checks = 0, failures = 0, groups = 0, probes = 0;
size_t zeroFetchProbes = 0, positiveControlFetches = 0;
const auto db = testDbPath("gist_typed_bounds_api");
const std::string table = "typed_gist";

void check(bool pass, const std::string& label) {
    ++checks;
    if (!pass) {
        ++failures;
        std::cerr << "GIST_TYPED_CHECK_FAIL " << label << '\n';
    }
}
void require(bool pass, const std::string& label) {
    check(pass, label);
    if (!pass) throw std::runtime_error("unsafe fixture continuation: " + label);
}
void group(const std::string& label) {
    ++groups;
    std::cout << "GIST_TYPED_GROUP " << groups << ' ' << label << '\n';
}
std::string readBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    require(bool(input), "actual owned image exists: " + path.filename().string());
    std::string result{std::istreambuf_iterator<char>(input), {}};
    require(!input.bad(), "whole actual image read");
    return result;
}
void writeBytes(const std::filesystem::path& path, const std::string& value) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(value.data(), static_cast<std::streamsize>(value.size()));
    output.close();
    require(!output.fail(), "whole private image replacement");
}
struct RestoreImage {
    std::filesystem::path path;
    std::string original;
    ~RestoreImage() {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output.write(original.data(), static_cast<std::streamsize>(original.size()));
        output.close();
        if (output.fail()) {
            ++failures;
            std::cerr << "GIST_TYPED_RESTORE_FAIL " << path << '\n';
        }
    }
};
std::string hex(const std::string& value) {
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (const unsigned char c : value) output << std::setw(2) << unsigned(c);
    return output.str();
}
uint32_t independentCrc(const std::string& value, size_t length) {
    uint32_t state = 0xffffffffu;
    for (size_t i = 0; i < length; ++i) {
        state ^= static_cast<unsigned char>(value[i]);
        for (unsigned bit = 0; bit < 8; ++bit)
            state = (state >> 1) ^ ((state & 1) ? 0x82f63b78u : 0);
    }
    return state ^ 0xffffffffu;
}
void appendNumber(std::string& output, uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i)
        output.push_back(static_cast<char>((value >> (8 * i)) & 0xffu));
}
uint64_t readNumber(const std::string& input, size_t& offset, unsigned width,
                    size_t end) {
    require(offset <= end && width <= end - offset, "independent integer extent");
    uint64_t result = 0;
    for (unsigned i = 0; i < width; ++i)
        result |= uint64_t(static_cast<unsigned char>(input[offset++])) << (8 * i);
    return result;
}
struct ImageEntry { int64_t rid; std::string low, high; };
std::vector<ImageEntry> independentV3(const std::string& input) {
    const std::string magic("\0DBMSS\0" "3", 8);
    require(input.size() >= 24 && input.compare(0, magic.size(), magic) == 0,
            "actual V3 magic and minimum physical frame");
    const size_t end = input.size() - 4;
    size_t crcOffset = end;
    require(readNumber(input, crcOffset, 4, input.size()) == independentCrc(input, end),
            "independent bitwise CRC of actual V3 image");
    size_t offset = magic.size();
    require(readNumber(input, offset, 4, end) == 3, "actual V3 version");
    const uint64_t count = readNumber(input, offset, 8, end);
    require(count <= (end - offset) / 24, "independent entry count extent");
    std::set<int64_t> unique;
    std::vector<ImageEntry> result;
    for (uint64_t i = 0; i < count; ++i) {
        const auto rawRid = readNumber(input, offset, 8, end);
        const auto lower = readNumber(input, offset, 8, end);
        const auto upper = readNumber(input, offset, 8, end);
        require(rawRid > 0 && rawRid <= uint64_t(INT64_MAX) &&
                lower <= end - offset && upper <= end - offset - lower,
                "independent genuine RID and payload extents");
        ImageEntry entry{static_cast<int64_t>(rawRid), input.substr(offset, size_t(lower)), {}};
        offset += size_t(lower);
        entry.high = input.substr(offset, size_t(upper));
        offset += size_t(upper);
        require(unique.insert(entry.rid).second, "independent unique real index RID");
        result.push_back(std::move(entry));
    }
    require(offset == end, "independent exact end before CRC");
    return result;
}
// The exact old framing with the genuine nonempty entries copied from the
// new image. It deliberately models the historical producer's omitted empty
// rows; it does not pretend that decoding a zero length proves old coverage.
std::string independentImage(unsigned version, const std::vector<ImageEntry>& entries) {
    require(version == 2 || version == 3, "fixed independent image generation");
    std::string output("\0DBMSS\0", 7);
    output.push_back(char('0' + version));
    appendNumber(output, version, 4);
    appendNumber(output, entries.size(), 8);
    for (const auto& entry : entries) {
        appendNumber(output, uint64_t(entry.rid), 8);
        appendNumber(output, entry.low.size(), 8);
        appendNumber(output, entry.high.size(), 8);
        output += entry.low;
        output += entry.high;
    }
    appendNumber(output, independentCrc(output, output.size()), 4);
    return output;
}
int byteCompare(const std::string& left, const std::string& right) {
    for (size_t i = 0; i < std::min(left.size(), right.size()); ++i) {
        const auto a = static_cast<unsigned char>(left[i]);
        const auto b = static_cast<unsigned char>(right[i]);
        if (a != b) return a < b ? -1 : 1;
    }
    return left.size() < right.size() ? -1 : left.size() > right.size() ? 1 : 0;
}
struct ActualRow { int id; int64_t rid; Cell key, payload; std::string opaque; };
using Population = std::map<int, std::pair<Cell, Cell>>;
std::vector<ActualRow> heapOracle(const Population& population) {
    const auto schema = g_engine.getTableSchema(db, table);
    require(schema.len == 4, "genuine id/key/jsonb/payload width");
    std::vector<ActualRow> result;
    std::set<int64_t> rids;
    auto cursor = g_engine.beginHeapScan(db, table, schema);
    HeapScanRow row;
    while (cursor->next(row)) {
        require(row.nulls.size() == schema.len, "actual cursor owns all physical NULL flags");
        std::vector<std::string> values;
        std::vector<bool> nulls;
        detail::decodeStoredRowValues(g_engine, db, row.data, schema, row.nulls, values, nulls);
        require(values.size() == 4 && nulls == row.nulls, "decode this real tuple with its captured bitmap");
        const int id = std::stoi(values[0]);
        require(population.count(id) && rids.insert(row.rid).second, "real unique inserted PK and heap RID");
        uint32_t page = 0; uint16_t slot = 0;
        StorageEngine::decodeRid(row.rid, page, slot);
        require(page > 0 && StorageEngine::encodeRid(page, slot) == row.rid, "genuine physical RID roundtrip");
        const auto& expected = population.at(id);
        const Cell key = nulls[1] ? Cell{} : Cell(values[1]);
        const Cell payload = nulls[3] ? Cell{} : Cell(values[3]);
        check(!nulls[0] && !nulls[2] && key == expected.first && payload == expected.second,
              "complete values and NULL identity: PK " + std::to_string(id));
        check(values[2] == "{}", "actual non-NULL empty-json-object storage: PK " + std::to_string(id));
        std::cout << "GIST_TYPED_ROW id=" << id << " rid=" << row.rid
                  << " null=" << nulls[1] << " key_hex=" << hex(values[1])
                  << " payload_null=" << nulls[3] << " payload_bytes=" << values[3].size() << '\n';
        result.push_back({id, row.rid, key, payload, values[2]});
    }
    require(result.size() == population.size(), "all committed physical seed rows");
    return result;
}
Rids oracle(const std::vector<ActualRow>& rows, const Bounds& bounds) {
    Rids result;
    for (const auto& row : rows) {
        if (!row.key) continue;
        if (bounds.low) {
            const auto compare = byteCompare(*row.key, bounds.low->value);
            if (compare < 0 || (compare == 0 && !bounds.low->inclusive)) continue;
        }
        if (bounds.high) {
            const auto compare = byteCompare(*row.key, bounds.high->value);
            if (compare > 0 || (compare == 0 && !bounds.high->inclusive)) continue;
        }
        result.push_back(row.rid);
    }
    std::sort(result.begin(), result.end());
    return result;
}
Bounds equal(const std::string& value) { return {Bound{value, true}, Bound{value, true}}; }
struct Watch {
    explicit Watch(PageAllocator* allocator) {
        watchedAllocator = allocator;
        heapFetches = 0;
        watching = true;
    }
    ~Watch() { watching = false; watchedAllocator = nullptr; }
};
size_t positiveControl(PageAllocator* allocator, size_t expectedRows) {
    size_t rows = 0;
    bool ok = false;
    {
        Watch watch(allocator);
        ok = g_engine.forEachRow(db, table,
            [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    }
    require(ok && rows == expectedRows, "wrapped positive control is a genuine complete heap scan");
    require(heapFetches > 0, "external fetchPage wrapper observes this actual allocator");
    positiveControlFetches += heapFetches;
    std::cout << "GIST_TYPED_WRAP_POSITIVE rows=" << rows << " target_heap_fetches=" << heapFetches << '\n';
    return heapFetches;
}
void observe(PageAllocator* allocator, bool contained, const std::string& column,
             const Bounds& bounds, bool engaged, const Rids& expected, const std::string& label) {
    ++probes;
    // These metadata/owner warmups are intentionally outside the watch. The
    // API call itself must not fetch any page of the genuine user heap.
    const auto schema = g_engine.getTableSchema(db, table);
    require(schema.len == 4 && g_engine.getPageAllocator(db, table) == allocator,
            "same actual physical allocator before typed probe");
    std::optional<Rids> result;
    {
        Watch watch(allocator);
        result = contained ? g_engine.tryGiSTSearchContainedBy(db, table, column, bounds)
                           : g_engine.tryGiSTSearchOverlap(db, table, column, bounds);
    }
    check(heapFetches == 0, "typed API never silently scans user heap: " + label);
    if (heapFetches == 0) ++zeroFetchProbes;
    check(bool(result) == engaged, "typed optional admission role: " + label);
    if (result) {
        std::sort(result->begin(), result->end());
        check(std::adjacent_find(result->begin(), result->end()) == result->end(), "no duplicate returned RID: " + label);
        if (engaged) check(*result == expected, "all exact genuine heap RIDs: " + label);
    }
    std::cout << "GIST_TYPED_PROBE " << label << " contained=" << contained
              << " engaged=" << bool(result) << " count=" << (result ? result->size() : 0)
              << " target_heap_fetches=" << heapFetches << " rids=";
    if (result) for (const auto rid : *result) std::cout << rid << ',';
    std::cout << '\n';
}
void pairProbe(PageAllocator* allocator, const std::vector<ActualRow>& rows,
               const Bounds& bounds, bool engaged, const std::string& label) {
    const auto expected = oracle(rows, bounds);
    observe(allocator, false, "k", bounds, engaged, expected, label);
    observe(allocator, true, "k", bounds, engaged, expected, label);
}
} // namespace

extern "C" char* realFetch(dbms::PageAllocator*, uint32_t)
    asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*, uint32_t)
    asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* allocator, uint32_t page) {
    if (watching && allocator == watchedAllocator) ++heapFetches;
    return realFetch(allocator, page);
}

namespace {
// Separate real default-locale relation: this is LIKE's byte-prefix
// necessity, not permission to run scalar SQL comparisons in byte order.
void defaultLocaleLikePrefixGroup(DdlExecutor& ddl, Session& session) {
    group("default-locale LIKE prefix role / scalar decline / true heap oracle");
    const std::string localTable = "typed_gist_locale";
    require(!ddl.executeSql("CREATE TABLE typed_gist_locale(id integer PRIMARY KEY,k text,opaque jsonb,payload text)", session),
            "actual default-locale text table without COLLATE override");
    const std::string maximum("\xf4\x8f\xbf\xbf", 4);
    const std::string del(1, '\x7f');
    const Population population{
        {1,{Cell(""),Cell("true empty")}},
        {2,{Cell{},Cell{}}},
        {3,{Cell("a"),Cell("plain prefix")}},
        {4,{Cell("a"+del),Cell("finite DEL")}},
        {5,{Cell("a界"),Cell("UTF8 suffix")}},
        {6,{Cell("a%b"),Cell("literal percent")}},
        {7,{Cell("a_b"),Cell("literal underscore")}},
        {8,{Cell(del+"suffix"),Cell("leading DEL")}},
        {9,{Cell("界"),Cell("UTF8 prefix")}},
        {10,{Cell(maximum),Cell("maximum unicode")}},
        {11,{Cell(maximum+"界"),Cell("maximum unicode suffix")}},
        {12,{Cell("a"+del+"suffix"),Cell(std::string(4096,'q'))}}
    };
    require(g_engine.beginTransaction(db)==DBStatus::OK,"one real default-locale seed owner");
    for(const auto& row:population)
        require(g_engine.insertRow(db,localTable,StorageEngine::SqlRow{
            {"id",std::to_string(row.first)},{"k",row.second.first},
            {"opaque","{}"},{"payload",row.second.second}})==DBStatus::OK,
            "real default-locale physical INSERT: PK "+std::to_string(row.first));
    require(g_engine.commitTransaction()==DBStatus::OK,"commit real default-locale population");
    const auto schema=g_engine.getTableSchema(db,localTable);
    require(schema.len==4 && schema.cols[1].dataType=="text" &&
            schema.cols[1].collation.empty() && schema.cols[1].domainName.empty() &&
            !schema.cols[1].isArray && !schema.cols[1].generatedKind,
            "actual stored default-locale declaration, not a forged binary column");
    auto* localAllocator=g_engine.getPageAllocator(db,localTable);
    require(localAllocator!=nullptr,"real default-locale target PageAllocator");
    const auto physicalRows=[&]() {
        std::vector<ActualRow> result;
        std::set<int64_t> unique;
        auto cursor=g_engine.beginHeapScan(db,localTable,schema);
        HeapScanRow row;
        while(cursor->next(row)) {
            require(row.nulls.size()==schema.len,"default-locale actual full NULL bitmap");
            std::vector<std::string> values;
            std::vector<bool> nulls;
            detail::decodeStoredRowValues(g_engine,db,row.data,schema,row.nulls,values,nulls);
            require(values.size()==4 && nulls==row.nulls,"same real tuple decode and NULL owner");
            const int id=std::stoi(values[0]);
            require(population.count(id) && unique.insert(row.rid).second,"genuine default-locale PK/RID");
            const Cell key=nulls[1]?Cell{}:Cell(values[1]);
            const Cell payload=nulls[3]?Cell{}:Cell(values[3]);
            check(!nulls[0] && !nulls[2] && values[2]=="{}" &&
                    key==population.at(id).first && payload==population.at(id).second,
                    "default-locale full physical values/NULL: PK "+std::to_string(id));
            std::cout<<"GIST_PREFIX_ROW id="<<id<<" rid="<<row.rid
                     <<" null="<<nulls[1]<<" key_hex="<<hex(values[1])<<'\n';
            result.push_back({id,row.rid,key,payload,values[2]});
        }
        require(result.size()==population.size(),"all real default-locale heap rows");
        return result;
    };
    const auto actualRows=physicalRows();
    const auto positive=[&]() {
        size_t count=0;bool ok=false;
        {
            Watch watch(localAllocator);
            ok=g_engine.forEachRow(db,localTable,
                [&](uint32_t,uint16_t,const char*,size_t){++count;});
        }
        require(ok && count==population.size() && heapFetches>0,
                "same default-locale target wrapper positive heap control");
        positiveControlFetches+=heapFetches;
        std::cout<<"GIST_PREFIX_WRAP_POSITIVE rows="<<count
                 <<" target_heap_fetches="<<heapFetches<<'\n';
    };
    positive();
    require(g_engine.createGiSTIndex(db,localTable,"k")==DBStatus::OK,
            "genuine default-locale text GiST");
    require(g_engine.createGiSTIndex(db,localTable,"opaque")==DBStatus::OK,
            "genuine unsupported-jsonb sidecar for prefix rejection");
    const auto image=g_engine.dbPath(db)/(localTable+"_k.gist");
    const auto original=readBytes(image);
    const auto entries=independentV3(original);
    std::map<int64_t,std::string> expectedEntries;
    for(const auto& row:actualRows)if(row.key)expectedEntries.emplace(row.rid,*row.key);
    check(entries.size()==expectedEntries.size(),"default-locale actual V3 nonNULL coverage");
    for(const auto& entry:entries)
        check(expectedEntries.count(entry.rid) && entry.low==entry.high &&
                entry.low==expectedEntries.at(entry.rid),
                "default-locale V3 empty/UTF8 payload has genuine heap RID");
    enum class Role { Prefix, Overlap, Contained };
    const auto probe=[&](Role role,const std::string& column,const std::string& prefix,
                          bool engaged,const std::string& label) {
        ++probes;
        require(g_engine.getPageAllocator(db,localTable)==localAllocator &&
                g_engine.getTableSchema(db,localTable).len==4,
                "same actual default-locale physical owner outside observation");
        Rids expected;
        if(role==Role::Prefix && engaged) {
            for(const auto& row:actualRows)
                if(row.key && row.key->compare(0,prefix.size(),prefix)==0)
                    expected.push_back(row.rid);
            std::sort(expected.begin(),expected.end());
        }
        std::optional<Rids> result;
        {
            Watch watch(localAllocator);
            if(role==Role::Prefix)
                result=g_engine.tryGiSTSearchLikePrefix(db,localTable,column,prefix);
            else if(role==Role::Overlap)
                result=g_engine.tryGiSTSearchOverlap(db,localTable,column,equal(prefix));
            else result=g_engine.tryGiSTSearchContainedBy(db,localTable,column,equal(prefix));
        }
        check(heapFetches==0,"default-locale typed role has no target heap fetch: "+label);
        if(heapFetches==0)++zeroFetchProbes;
        check(bool(result)==engaged,"default-locale actual optional role: "+label);
        if(result) {
            std::sort(result->begin(),result->end());
            check(std::adjacent_find(result->begin(),result->end())==result->end(),
                    "default-locale no duplicate candidate RID: "+label);
            if(engaged)check(*result==expected,"all exact true byte-prefix heap RIDs: "+label);
        }
        std::cout<<"GIST_PREFIX_PROBE "<<label<<" role="<<int(role)
                 <<" engaged="<<bool(result)<<" count="<<(result?result->size():0)
                 <<" target_heap_fetches="<<heapFetches<<'\n';
    };
    for(const auto& value:{std::string(""),std::string("a"),std::string("a")+del}) {
        probe(Role::Overlap,"k",value,false,"default-locale scalar overlap remains unsupported");
        probe(Role::Contained,"k",value,false,"default-locale scalar contained remains unsupported");
    }
    const std::vector<std::pair<std::string,std::string>> prefixes{
        {"a","ASCII prefix"},{"a%","percent is literal bytes"},
        {"a_","underscore is literal bytes"},{"a"+del,"DEL is literal suffix"},
        {del,"DEL is literal leading byte"},{"界","UTF8 prefix"},
        {maximum,"maximum UTF8 has no finite valid-codepoint ceiling"},
        {"not-present","proven empty answer"}
    };
    for(const auto& input:prefixes)probe(Role::Prefix,"k",input.first,true,input.second);
    probe(Role::Prefix,"k","",false,"empty necessary prefix is not selective admission");
    probe(Role::Prefix,"k",std::string("\xc0\xaf",2),false,"invalid overlong UTF8 declines");
    probe(Role::Prefix,"k",std::string("\xf4\x8f",2),false,"truncated UTF8 declines");
    probe(Role::Prefix,"opaque","a",false,"actual jsonb declaration unsupported for LIKE role");
    probe(Role::Prefix,"absent_column","a",false,"absent column does not invent a declaration");
    {
        RestoreImage restore{image,original};
        auto corrupt=original;corrupt.back()^=1;writeBytes(image,corrupt);
        probe(Role::Prefix,"k","a",false,"CRC-corrupt generation declines");
        writeBytes(image,original);
        require(std::filesystem::remove(image),"remove only actual default-locale GiST file");
        probe(Role::Prefix,"k","a",false,"missing generation declines");
    }
    require(readBytes(image)==original,"actual default-locale image restored");
    {
        RestoreImage restore{image,original};
        std::vector<ImageEntry> old;
        for(const auto& entry:entries)if(!entry.low.empty())old.push_back(entry);
        writeBytes(image,independentImage(2,old));
        probe(Role::Prefix,"k","a",true,"legacy omitted-empty V2 is complete for nonempty prefix");
        probe(Role::Prefix,"k",maximum,true,"legacy V2 MAX prefix remains conservative and complete");
    }
    const auto dirty=g_engine.dbPath(db)/(localTable+".specialized_index_dirty");
    require(!std::filesystem::exists(dirty),"actual default-locale generation initially clean");
    {
        struct RemoveMarker {
            std::filesystem::path path;
            ~RemoveMarker(){std::error_code ignored;std::filesystem::remove(path,ignored);}
        } remove{dirty};
        writeBytes(dirty,"DBMS_SPECIALIZED_INDEX_DIRTY_V1\n");
        probe(Role::Prefix,"k","a",false,"dirty generation retains original heap owner");
        require(std::filesystem::remove(dirty),"remove only own default-locale dirty marker");
    }
    require(g_engine.setIsolationLevel(IsolationLevel::SERIALIZABLE),"actual default-locale SSI setting");
    require(g_engine.beginTransaction(db)==DBStatus::OK,"actual default-locale SSI read owner");
    probe(Role::Prefix,"k","a",false,"SSI matching prefix keeps real predicate read owner");
    probe(Role::Prefix,"k","absent",false,"SSI empty prefix answer keeps real predicate owner");
    require(g_engine.commitTransaction()==DBStatus::OK,"finish actual default-locale SSI owner");
    require(g_engine.setIsolationLevel(IsolationLevel::READ_COMMITTED),"restore actual isolation after prefix controls");
    const auto finalRows=physicalRows();
    for(const auto& row:finalRows) {
        const auto found=std::find_if(actualRows.begin(),actualRows.end(),
            [&](const ActualRow& old){return old.id==row.id;});
        check(found!=actualRows.end() && found->rid==row.rid && found->key==row.key &&
                found->payload==row.payload && found->opaque==row.opaque,
                "default-locale complete heap/RID/NULL unchanged after all probes");
    }
    require(readBytes(image)==original,"all prefix controls leave actual V3 image unchanged");
    positive();
}
} // namespace

int main() try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    g_config.autoAnalyzeEnabled = false;
    g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = db;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct RestoreSession {
        Session* previous;
        ~RestoreSession() {
            watching = false; watchedAllocator = nullptr;
            if (g_engine.inTransaction()) g_engine.rollbackTransaction();
            setCurrentSession(previous);
        }
    } restoreSession{previous};

    group("real heap / PK / NULL / literal marker / wrapper positive control");
    require(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "owned private database");
    DdlExecutor ddl;
    require(!ddl.executeSql("CREATE TABLE typed_gist(id integer PRIMARY KEY,k text COLLATE \"C\",opaque jsonb,payload text)", session),
            "genuine typed four-column physical table");
    const Population population{
        {1, {Cell(""), Cell(std::string(2048, 'p'))}},
        {2, {Cell{}, Cell{}}},
        {3, {Cell(std::string(1, '\x7f')), Cell("DEL")}},
        {4, {Cell(std::string(1, '\x7f') + "suffix"), Cell("DEL suffix")}},
        {5, {Cell("界"), Cell("UTF8")}},
        {6, {Cell("__TOAST__1"), Cell("literal marker")}},
        {7, {Cell("NULL"), Cell("literal NULL")}},
        {8, {Cell("alpha"), Cell("ASCII")}},
        {9, {Cell(""), Cell("")}},
        {10, {Cell(std::string("a") + '\x7f'), Cell("finite suffix")}},
        {11, {Cell(std::string(4096, 'z')), Cell("actual long key")}}
    };
    require(g_engine.beginTransaction(db) == DBStatus::OK, "one actual seed transaction");
    for (const auto& row : population)
        require(g_engine.insertRow(db, table, StorageEngine::SqlRow{
            {"id", std::to_string(row.first)}, {"k", row.second.first},
            {"opaque", "{}"}, {"payload", row.second.second}}) == DBStatus::OK,
            "complete real row INSERT: PK " + std::to_string(row.first));
    require(g_engine.commitTransaction() == DBStatus::OK, "committed actual heap fixture");
    const auto rows = heapOracle(population);
    auto* allocator = g_engine.getPageAllocator(db, table);
    require(allocator != nullptr, "actual user heap PageAllocator owner");
    positiveControl(allocator, population.size());

    group("actual V3 writer / independent CRC+framing / full empty coverage");
    require(g_engine.createGiSTIndex(db, table, "k") == DBStatus::OK, "genuine current GiST build");
    const auto index = g_engine.dbPath(db) / (table + "_k.gist");
    const auto original3 = readBytes(index);
    const auto entries = independentV3(original3);
    check(independentImage(3, entries) == original3, "independent V3 serializer reproduces actual writer bytes");
    std::vector<gist_index_format::Entry> decoded;
    uint32_t version = 0;
    require(gist_index_format::decode(original3, decoded, &version) && version == 3,
            "public codec recognizes actual V3 generation");
    std::map<int64_t, std::string> expectedEntries;
    for (const auto& row : rows) if (row.key) expectedEntries.emplace(row.rid, *row.key);
    check(entries.size() == expectedEntries.size() && decoded.size() == entries.size(),
          "all and only genuine non-NULL values are encoded, including duplicate empties");
    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& entry = entries[i];
        require(expectedEntries.count(entry.rid), "encoded RID is an actual non-NULL heap tuple");
        check(entry.low == expectedEntries.at(entry.rid) && entry.high == expectedEntries.at(entry.rid),
              "actual encoded-positive scalar payload, including empty/DEL/UTF8/TOAST literal");
        check(decoded[i].rid == uint64_t(entry.rid) && decoded[i].low == entry.low && decoded[i].high == entry.high,
              "independent decoder agrees with public V3 codec");
    }

    group("present finite empty / DEL / strictness / absent endpoint positive probes");
    const std::vector<std::pair<std::string, Bounds>> bounds{
        {"equal empty", equal("")},
        {"present inclusive finite upper empty", Bounds{{}, Bound{"", true}}},
        {"present strict finite upper empty", Bounds{{}, Bound{"", false}}},
        {"strict lower empty", Bounds{Bound{"", false}, {}}},
        {"literal finite DEL equality", equal(std::string(1, '\x7f'))},
        {"literal finite DEL upper", Bounds{{}, Bound{std::string(1, '\x7f'), true}}},
        {"both endpoints absent", Bounds{}},
        {"present inclusive lower empty", Bounds{Bound{"", true}, {}}}
    };
    for (const auto& query : bounds) pairProbe(allocator, rows, query.second, true, query.first);

    group("historical V2 omits genuine empty tuples; no coverage upgrade by decoding");
    std::vector<ImageEntry> oldEntries;
    for (const auto& entry : entries) if (!entry.low.empty()) oldEntries.push_back(entry);
    require(oldEntries.size() + 2 == entries.size(), "exact two historical omitted-empty RIDs");
    {
        RestoreImage restore{index, original3};
        const auto old2 = independentImage(2, oldEntries);
        writeBytes(index, old2);
        std::vector<gist_index_format::Entry> legacy;
        uint32_t oldVersion = 0;
        require(gist_index_format::decode(old2, legacy, &oldVersion) && oldVersion == 2 && legacy.size() == oldEntries.size(),
                "genuine V2 framing, complete nonempty entries retained");
        pairProbe(allocator, rows, equal(""), false, "V2 finite empty must decline");
        pairProbe(allocator, rows, Bounds{}, false, "V2 unbounded can include omitted empty");
        pairProbe(allocator, rows, equal(std::string(1, '\x7f')), true, "V2 finite nonempty equality remains usable");
        pairProbe(allocator, rows, Bounds{Bound{"", false}, {}}, true, "V2 strict greater empty excludes omitted values");
    }
    require(readBytes(index) == original3, "original V3 restored after V2 compatibility controls");

    group("legacy text image finite nonempty versus omitted-empty compatibility");
    {
        RestoreImage restore{index, original3};
        std::ostringstream text;
        for (const auto& entry : oldEntries)
            text << entry.rid << ' ' << std::quoted(entry.low) << ' ' << std::quoted(entry.high) << '\n';
        writeBytes(index, text.str());
        pairProbe(allocator, rows, equal(""), false, "text legacy finite empty must decline");
        pairProbe(allocator, rows, equal("alpha"), true, "text legacy finite nonempty still usable");
    }
    require(readBytes(index) == original3, "original V3 restored after legacy text controls");

    group("corrupt / late-invalid / missing / dirty all decline without user heap I/O");
    {
        RestoreImage restore{index, original3};
        auto corrupt = original3; corrupt.back() ^= 1; writeBytes(index, corrupt);
        pairProbe(allocator, rows, equal(""), false, "bad CRC cannot prove even an empty answer");
        auto late = entries;
        require(late.size() > 1, "real multi-entry late invalid image");
        late.back().low = "z"; late.back().high = "a";
        writeBytes(index, independentImage(3, late));
        pairProbe(allocator, rows, equal(""), false, "CRC-valid late reversed range checked after early match");
        writeBytes(index, original3);
        require(std::filesystem::remove(index), "remove only this owned GiST sidecar");
        pairProbe(allocator, rows, equal("alpha"), false, "missing file returns nullopt, not hidden heap fallback");
    }
    require(readBytes(index) == original3, "original V3 restored after corrupt/missing controls");
    const auto dirty = g_engine.dbPath(db) / (table + ".specialized_index_dirty");
    require(!std::filesystem::exists(dirty), "fixture starts with no dirty-generation marker");
    writeBytes(dirty, "DBMS_SPECIALIZED_INDEX_DIRTY_V1\n");
    struct RemoveDirty {
        std::filesystem::path path;
        ~RemoveDirty() { std::error_code ignored; std::filesystem::remove(path, ignored); }
    } removeDirty{dirty};
    pairProbe(allocator, rows, equal("alpha"), false, "dirty generation cannot return trusted candidates");
    require(std::filesystem::remove(dirty), "remove only this owned diagnostic dirty marker");

    group("actual unsupported scalar declaration and absent column never guess ordering");
    require(g_engine.createGiSTIndex(db, table, "opaque") == DBStatus::OK, "real GiST sidecar for actual jsonb declaration");
    const auto opaque = g_engine.dbPath(db) / (table + "_opaque.gist");
    require(!readBytes(opaque).empty(), "unsupported typed-order test has a genuine index file");
    for (const bool contained : {false, true}) {
        observe(allocator, contained, "opaque", Bounds{}, false, {}, "actual jsonb order is unsupported");
        observe(allocator, contained, "not_a_column", Bounds{}, false, {}, "unknown column is not a guessed type");
    }

    group("serializable owner declines; legacy string owner remains real heap source");
    require(g_engine.setIsolationLevel(IsolationLevel::SERIALIZABLE), "select actual serializable isolation");
    require(g_engine.beginTransaction(db) == DBStatus::OK, "real serializable read owner");
    pairProbe(allocator, rows, equal(""), false, "serializable try API retains original predicate owner");
    require(g_engine.commitTransaction() == DBStatus::OK, "finish actual serializable owner");
    require(g_engine.setIsolationLevel(IsolationLevel::READ_COMMITTED), "restore actual read-committed setting");
    const auto expectedAll = oracle(rows, Bounds{});
    for (const bool contained : {false, true}) {
        auto legacy = contained ? g_engine.giSTSearchContainedBy(db, table, "k", "", "")
                                : g_engine.giSTSearchOverlap(db, table, "k", "", "");
        std::sort(legacy.begin(), legacy.end());
        check(legacy == expectedAll, "legacy string empty/empty still returns all non-NULL heap RIDs");
    }
    const auto after = heapOracle(population);
    require(after.size() == rows.size(), "final complete original heap remains present");
    for (const auto& row : after) {
        const auto old = std::find_if(rows.begin(), rows.end(), [&](const ActualRow& before) { return before.id == row.id; });
        require(old != rows.end(), "final actual PK existed in baseline");
        check(old->rid == row.rid && old->key == row.key && old->payload == row.payload && old->opaque == row.opaque,
              "all original physical RIDs/values/NULLs preserved");
    }
    require(readBytes(index) == original3, "all compatibility/negative probes retain original V3 bytes");
    positiveControl(allocator, population.size());
    check(groups == 8 && zeroFetchProbes == probes && probes > 0,
          "all eight groups complete and every try API has zero user-heap fetches");
    defaultLocaleLikePrefixGroup(ddl, session);
    check(groups == 9 && zeroFetchProbes == probes && probes > 0,
          "all nine groups complete with actual default-locale prefix role and zero target heap fetches");
    std::cout << "GIST_TYPED_BOUNDS checks=" << checks << " failures=" << failures
              << " groups=" << groups << " probes=" << probes
              << " zero_heap_fetch_probes=" << zeroFetchProbes
              << " heap_positive_fetches=" << positiveControlFetches << '\n';
    return failures ? 1 : 0;
} catch (const std::exception& error) {
    watching = false; watchedAllocator = nullptr;
    std::cerr << "GIST_TYPED_BOUNDS_FATAL checks=" << checks << " failures=" << failures
              << " groups=" << groups << " probes=" << probes << ' ' << error.what() << '\n';
    return 1;
}

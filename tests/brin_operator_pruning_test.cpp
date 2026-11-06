// test_link_flags: -Wl,--wrap=_ZN4dbms10ExprHelper23canonicalResultTypeNameENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
// compareValues is defined in TableManage.cpp beside tryBrinPageRanges: GNU
// --wrap cannot reliably observe those internal calls. Its 2/3-call budget is
// a source-review contract, not a zero-count assertion in this test.
#include "access/BrinIndexFormat.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Ranges = std::vector<std::pair<uint32_t, uint32_t>>;
using Truth = StorageEngine::PredicateTruth;
size_t checks = 0, failures = 0, probes = 0;
thread_local bool watchCanonical = false;
thread_local size_t canonicalCalls = 0;
const auto db = testDbPath("brin_operator_pruning");

// Accumulate semantic and cost failures, so a slower old implementation does
// not suppress the remaining fixed input/oracle observations.
void need(bool value, const std::string& label) {
    ++checks;
    if (!value) {
        ++failures;
        std::cerr << "BRIN_OPERATOR_CHECK_FAIL " << label << '\n';
    }
}
void require(bool value, const std::string& label) {
    need(value, label);
    if (!value) throw std::runtime_error("unsafe fixture continuation: " + label);
}
std::string readBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    require(bool(input), "read actual index file");
    std::string result{std::istreambuf_iterator<char>(input), {}};
    require(!input.bad(), "complete actual index read");
    return result;
}
void writeBytes(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    require(!output.fail(), "complete owned index replacement");
}
uint32_t independentCrc(const std::string& bytes, size_t length) {
    uint32_t result = 0xffffffffu;
    for (size_t i = 0; i < length; ++i) {
        result ^= static_cast<unsigned char>(bytes[i]);
        for (unsigned bit = 0; bit < 8; ++bit)
            result = (result >> 1) ^ ((result & 1) ? 0x82f63b78u : 0);
    }
    return result ^ 0xffffffffu;
}
void appendNumber(std::string& bytes, uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i)
        bytes.push_back(static_cast<char>((value >> (8 * i)) & 0xffu));
}
void appendText(std::string& bytes, const std::string& value) {
    appendNumber(bytes, value.size(), 8);
    bytes += value;
}
// An independent V3 writer/CRC lets semantic-bound mutations retain valid
// framing. It keeps the genuine physical relation/schema/extent identities.
std::string mutationImage(const brin_index_format::TypedIndex& index) {
    std::string bytes;
    appendNumber(bytes, 0x5342524eu, 4);
    appendNumber(bytes, 3, 4);
    appendNumber(bytes, index.ranges.size(), 8);
    appendNumber(bytes, index.pagesPerRange, 4);
    appendNumber(bytes, index.heapPages, 4);
    appendNumber(bytes, index.flags, 4);
    appendNumber(bytes, index.physicalRelation, 8);
    appendText(bytes, index.orderingIdentity);
    for (const auto& range : index.ranges) {
        appendNumber(bytes, range.first, 4);
        appendNumber(bytes, range.last, 4);
        appendNumber(bytes, range.flags, 4);
        appendText(bytes, range.min);
        appendText(bytes, range.max);
    }
    appendNumber(bytes, independentCrc(bytes, bytes.size()), 4);
    return bytes;
}
bool validIndependentCrc(const std::string& bytes) {
    if (bytes.size() < 4) return false;
    uint32_t stored = 0;
    for (unsigned i = 0; i < 4; ++i)
        stored |= uint32_t(static_cast<unsigned char>(bytes[bytes.size() - 4 + i])) << (8 * i);
    return stored == independentCrc(bytes, bytes.size() - 4);
}
struct RestoreImage {
    std::filesystem::path path;
    std::string bytes;
    ~RestoreImage() {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
};
bool covers(const Ranges& ranges, uint32_t page) {
    return std::any_of(ranges.begin(), ranges.end(), [page](auto range) {
        return range.first <= page && page <= range.second;
    });
}
struct Entry { Cell text; int rank; };
struct InputCase {
    std::string name, declaration;
    std::vector<Entry> entries;
};
struct ActualRow {
    int id;
    uint32_t page;
    uint16_t slot;
    int64_t rid;
    bool isNull;
    int rank;
};
std::vector<ActualRow> actualRows(const InputCase& input, size_t repetitions, size_t padWidth) {
    const auto schema = g_engine.getTableSchema(db, input.name);
    require(schema.len == 3, "actual id/value/pad schema width: " + input.name);
    std::vector<ActualRow> rows;
    auto cursor = g_engine.beginHeapScan(db, input.name, schema);
    HeapScanRow row;
    std::set<int64_t> rids;
    while (cursor->next(row)) {
        require(row.nulls.size() == 3, "actual full tuple NULL bitmap");
        std::vector<std::string> values;
        std::vector<bool> nulls;
        detail::decodeStoredRowValues(g_engine, db, row.data, schema, row.nulls, values, nulls);
        require(values.size() == 3 && nulls == row.nulls, "decode uses this actual row's captured bitmap");
        const int id = std::stoi(values[0]);
        require(id >= 1 && size_t(id) <= repetitions * input.entries.size(), "real inserted row identity");
        const auto& entry = input.entries[(size_t(id) - 1) % input.entries.size()];
        const auto rid = row.rid;
        uint32_t page = 0;
        uint16_t slot = 0;
        StorageEngine::decodeRid(rid, page, slot);
        require(page > 0 && StorageEngine::encodeRid(page, slot) == rid, "actual heap RID roundtrip");
        require(rids.insert(rid).second, "distinct genuine heap RID");
        const bool isNull = nulls[1];
        const auto& value = values[1];
        need(isNull == !entry.text && row.nulls[1] == !entry.text,
             "literal NULL/empty text remain distinct: " + input.name);
        need(!row.nulls[0] && !nulls[2] && !row.nulls[2] &&
             values[2] == std::string(padWidth, char('a' + id % 23)), "complete stored pad value");
        std::cout << "BRIN_OPERATOR_ROW table=" << input.name << " id=" << id
                  << " page=" << page << " slot=" << slot
                  << " rid=" << rid << " null=" << isNull << " bytes=" << value.size()
                  << " value=" << value << '\n';
        rows.push_back({id, page, slot, rid, isNull, entry.rank});
    }
    require(rows.size() == repetitions * input.entries.size(), "all physical input rows present");
    return rows;
}
std::vector<ActualRow> build(const InputCase& input, DdlExecutor& ddl, Session& session,
                           size_t repetitions = 4, size_t padWidth = 192) {
    require(!ddl.executeSql("CREATE TABLE " + input.name +
            "(id integer PRIMARY KEY,v " + input.declaration + ",pad text)", session),
            "actual typed DDL: " + input.name);
    require(g_engine.beginTransaction(db) == DBStatus::OK, "actual private seed transaction");
    for (size_t i = 0; i < repetitions * input.entries.size(); ++i) {
        const auto id = i + 1;
        require(g_engine.insertRow(db, input.name,
            {{"id", std::to_string(id)}, {"v", input.entries[i % input.entries.size()].text},
             {"pad", std::string(padWidth, char('a' + id % 23))}}) == DBStatus::OK,
            "actual typed INSERT: " + input.name);
    }
    require(g_engine.commitTransaction() == DBStatus::OK, "durable private seed");
    const auto before = actualRows(input, repetitions, padWidth);
    require(g_engine.createBrinIndex(db, input.name, "v", 1) == DBStatus::OK, "actual pages_per_range=1 build");
    const auto path = g_engine.dbPath(db) / (input.name + "_v.brin");
    const auto bytes = readBytes(path);
    require(validIndependentCrc(bytes), "independent CRC of actual V3 writer");
    brin_index_format::TypedIndex index;
    require(brin_index_format::decodeTyped(bytes, index), "actual V3 shape");
    need(mutationImage(index) == bytes, "independent V3 serialization matches actual file");
    need(index.physicalRelation == g_engine.getTableSchema(db, input.name).physicalRelationId &&
         index.heapPages == g_engine.tableNumPages(db, input.name), "genuine identity and actual extent");
    return before;
}
struct Observation { std::optional<Ranges> ranges; size_t canonical = 0; };
Observation observe(const std::string& table, const std::string& op, const std::string& value) {
    ++probes;
    canonicalCalls = 0;
    struct Watch { Watch() { watchCanonical = true; } ~Watch() { watchCanonical = false; } } watch;
    try {
        auto ranges = g_engine.tryBrinPageRanges(db, table, "v", op, value);
        const auto count = canonicalCalls;
        std::cout << "BRIN_OPERATOR_PROBE table=" << table << " op=" << op << " value=" << value
                  << " trusted=" << bool(ranges) << " ranges=" << (ranges ? ranges->size() : 0)
                  << " inclusive_canonical=" << count << '\n';
        return {std::move(ranges), count};
    } catch (const DbError& error) {
        need(false, "tryBrin unexpected exception: " + table + " " + op + " " + error.sqlState());
        return {{}, canonicalCalls};
    }
}
bool oracleMatch(const ActualRow& row, const std::string& op, int rank) {
    if (op == "IS NULL") return row.isNull;
    if (op == "IS NOT NULL") return !row.isNull;
    if (row.isNull) return false;
    if (op == "=") return row.rank == rank;
    if (op == "<") return row.rank < rank;
    if (op == "<=") return row.rank <= rank;
    if (op == ">") return row.rank > rank;
    return row.rank >= rank;
}
void completeProbe(const InputCase& input, const std::vector<ActualRow>& rows,
                   const std::string& op, const Entry& value, bool budget = false) {
    const auto result = observe(input.name, op, value.text.value_or(""));
    need(bool(result.ranges), "trusted candidate set: " + input.name + " " + op);
    if (result.ranges) {
        size_t matchingRows = 0;
        for (const auto& row : rows) {
            if (!oracleMatch(row, op, value.rank)) continue;
            ++matchingRows;
            need(covers(*result.ranges, row.page), "no matching genuine RID lost: " + input.name + " id=" + std::to_string(row.id));
        }
        std::cout << "BRIN_OPERATOR_MATCHES table=" << input.name << " op=" << op
                  << " expected=" << matchingRows << '\n';
    }
    if (op == "IS NULL" || op == "IS NOT NULL")
        need(result.canonical == 0, "NULL predicate never asks for declared scalar order");
    else if (budget) {
        // Only the modern bigint column uses this inclusive budget. Temporal
        // normalization may legitimately call canonicalResultTypeName itself.
        need(result.canonical > 0, "actual public tryBrin reaches external canonical wrapper");
        need(result.canonical <= 1, "one scalar-order admission per whole bigint probe");
    }
}
void semanticCase(const InputCase& input, const std::vector<ActualRow>& rows) {
    std::vector<Entry> nonNull;
    for (const auto& entry : input.entries) if (entry.text) nonNull.push_back(entry);
    for (size_t index : std::array<size_t, 3>{0, nonNull.size() / 2, nonNull.size() - 1})
        for (const auto& op : {"=", "<", "<=", ">", ">="})
            completeProbe(input, rows, op, nonNull[index]);
    completeProbe(input, rows, "IS NULL", {{}, 0});
    completeProbe(input, rows, "IS NOT NULL", {{}, 0});
    // These are proven empty candidate vectors, not nullopt fallbacks. Every
    // physical range is ordered and no datum is below/above its true extremum.
    const auto low = observe(input.name, "<", nonNull.front().text.value());
    const auto high = observe(input.name, ">", nonNull.back().text.value());
    need(low.ranges && low.ranges->empty(), "proven empty below actual minimum: " + input.name);
    need(high.ranges && high.ranges->empty(), "proven empty above actual maximum: " + input.name);
}
} // namespace

extern "C" std::string realCanonical(std::string)
    asm("__real__ZN4dbms10ExprHelper23canonicalResultTypeNameENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE");
extern "C" std::string wrapCanonical(std::string)
    asm("__wrap__ZN4dbms10ExprHelper23canonicalResultTypeNameENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE");
extern "C" std::string wrapCanonical(std::string value) {
    if (watchCanonical) ++canonicalCalls;
    return realCanonical(std::move(value));
}

int main() try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const bool oldAnalyze = g_config.autoAnalyzeEnabled, oldVacuum = g_config.autoVacuumEnabled;
    g_config.autoAnalyzeEnabled = false;
    g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = db;
    auto* previous = currentSession();
    setCurrentSession(&session);
    struct Restore {
        Session* previous; bool analyze, vacuum;
        ~Restore() {
            watchCanonical = false;
            if (g_engine.inTransaction()) g_engine.rollbackTransaction();
            setCurrentSession(previous);
            g_config.autoAnalyzeEnabled = analyze; g_config.autoVacuumEnabled = vacuum;
        }
    } restore{previous, oldAnalyze, oldVacuum};
    canonicalCalls = 0; watchCanonical = true;
    const auto canary = ExprHelper::canonicalResultTypeName("int8");
    watchCanonical = false;
    require(canary == "bigint" && canonicalCalls == 1, "external canonical --wrap canary is live");
    require(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "private real database");
    DdlExecutor ddl;
    const std::vector<InputCase> cases{
        {"smallints", "smallint", {{"-32768", 0}, {"-2", 1}, {"0", 2}, {"2", 3}, {"32767", 4}, {{}, 0}}},
        {"integers", "integer", {{"-2147483648", 0}, {"-2", 1}, {"0", 2}, {"2", 3}, {"2147483647", 4}, {{}, 0}}},
        {"bigints", "bigint", {{"-9223372036854775808", 0}, {"-9007199254740993", 1}, {"9007199254740992", 2}, {"9007199254740993", 3}, {"9223372036854775807", 4}, {{}, 0}}},
        {"decimals", "numeric", {{"-999999999999999999999999999999.1", 0}, {"-2.25", 1}, {"0.00", 2}, {"2.500", 3}, {"999999999999999999999999999999.1", 4}, {"NaN", 5}, {{}, 0}}},
        {"floats", "double precision", {{"-10.5", 0}, {"-0", 1}, {"0", 1}, {"10.25", 2}, {"NaN", 3}, {{}, 0}}},
        {"booleans", "boolean", {{"false", 0}, {"true", 1}, {{}, 0}}},
        {"strings", "text COLLATE \"C\"", {{"", 0}, {"10", 1}, {"2", 2}, {"NULL", 3}, {"界\n|;,", 4}, {{}, 0}}},
        {"dates", "date", {{"1999-12-31", 0}, {"2000-01-01", 1}, {"2020-06-03", 2}, {{}, 0}}},
        {"times", "time", {{"00:00:00", 0}, {"12:34:56.123456", 1}, {"23:59:59.999999", 2}, {{}, 0}}},
        {"timestamps", "timestamp", {{"1999-12-31 23:59:59.999999", 0}, {"2000-01-01 00:00:00", 1}, {"2020-01-01 00:00:00.000001", 2}, {{}, 0}}},
        {"zoned", "timestamptz", {{"1999-12-31 23:59:59+00", 0}, {"2000-01-01 00:00:00+00", 1}, {"2020-01-01 00:00:00+00", 2}, {{}, 0}}},
        {"uuids", "uuid", {{"00000000-0000-0000-0000-000000000001", 0}, {"00000000-0000-0000-0000-000000000010", 1}, {"ffffffff-ffff-ffff-ffff-ffffffffffff", 2}, {{}, 0}}},
        {"bits", "bit varying(8)", {{"", 0}, {"0", 1}, {"00", 2}, {"01", 3}, {"1", 4}, {"10", 5}, {{}, 0}}}
    };
    for (const auto& input : cases) semanticCase(input, build(input, ddl, session));

    const InputCase wide{"wide", "bigint", {{"10", 0}, {"20", 1}, {"30", 2}, {{}, 0}}};
    const auto wideRows = build(wide, ddl, session, 256, 512);
    const auto path = g_engine.dbPath(db) / "wide_v.brin";
    const auto original = readBytes(path);
    RestoreImage restoreIndex{path, original};
    brin_index_format::TypedIndex real;
    require(brin_index_format::decodeTyped(original, real), "decode genuine many-range index");
    require(real.ranges.size() >= 16, "budget fixture has at least 16 real page ranges");
    require(g_engine.tryBrinPageRanges(db, "wide", "v", ">=", "20").has_value(), "unmeasured real nonempty warm probe");
    for (const auto& op : {"=", "<", "<=", ">", ">="})
        for (const auto& value : std::vector<Entry>{{"9", -1}, {"10", 0}, {"20", 1}, {"30", 2}, {"31", 3}})
            completeProbe(wide, wideRows, op, value, true);
    completeProbe(wide, wideRows, "IS NULL", {{}, 0});
    completeProbe(wide, wideRows, "IS NOT NULL", {{}, 0});
    for (const auto& value : {"not-an-integer", "9223372036854775808", "20garbage"})
        for (const auto& op : {"=", "<", "<=", ">", ">="})
            need(!observe("wide", op, value).ranges, "invalid requested input declines whole proof");

    const auto mutated = [&](const std::string& label, brin_index_format::TypedIndex changed,
                             const std::string& op, const std::string& value) {
        const auto encoded = mutationImage(changed);
        require(validIndependentCrc(encoded), "independent mutation CRC: " + label);
        brin_index_format::TypedIndex decoded;
        require(brin_index_format::decodeTyped(encoded, decoded), "mutation keeps genuine V3 frame: " + label);
        writeBytes(path, encoded);
        need(!observe("wide", op, value).ranges, "invalid proof is nullopt, never empty/partial candidates: " + label);
        writeBytes(path, original);
    };
    for (const auto& op : {"=", "<", "<=", ">", ">="}) {
        auto changed = real;
        changed.ranges.front().min = "bad-min";
        mutated("invalid lower " + std::string(op), changed, op, "9");
        changed = real; changed.ranges.front().max = "bad-max";
        mutated("invalid upper " + std::string(op), changed, op, "9");
        changed = real; changed.ranges.front().min = "30"; changed.ranges.front().max = "10";
        mutated("reversed bounds " + std::string(op), changed, op, "9");
    }
    // Whole-file proof must continue past both a genuine early match and a
    // genuine early mismatch. No candidate or partial proof may escape first.
    for (const auto& request : std::vector<std::pair<std::string, std::string>>{{"<", "9"}, {">=", "10"}, {"=", "9"}}) {
        auto changed = real; changed.ranges.back().max = "late-invalid";
        mutated("late malformed bound", changed, request.first, request.second);
        changed = real; changed.ranges.back().min = "30"; changed.ranges.back().max = "10";
        mutated("late reversed bounds", changed, request.first, request.second);
    }
    { auto changed = real; changed.ranges.back().flags |= brin_index_format::kUnbounded;
      mutated("late unbounded range", changed, "=", "9"); }
    { auto changed = real; changed.orderingIdentity += "stale";
      mutated("ordering generation mismatch", changed, "=", "9"); }
    { auto changed = real; ++changed.physicalRelation;
      mutated("physical relation mismatch", changed, "=", "9"); }
    { auto changed = real; ++changed.heapPages;
      mutated("actual extent mismatch", changed, "=", "9"); }
    { auto damaged = original; damaged.back() ^= 1; writeBytes(path, damaged);
      need(!observe("wide", "=", "9").ranges, "bad CRC declines instead of proving empty"); writeBytes(path, original); }
    // NULL-only predicates intentionally consume flags, not scalar bounds.
    { auto changed = real; changed.ranges.back().min = "bad"; changed.ranges.back().max = "bad-other";
      writeBytes(path, mutationImage(changed));
      completeProbe(wide, wideRows, "IS NULL", {{}, 0});
      completeProbe(wide, wideRows, "IS NOT NULL", {{}, 0});
      writeBytes(path, original); }

    const InputCase allNull{"all_null", "bigint", {{{}, 0}}};
    (void)build(allNull, ddl, session);
    for (const auto& op : {"=", "<", "<=", ">", ">="}) {
        const auto result = observe("all_null", op, "invalid-unrequested-input");
        need(result.ranges && result.ranges->empty(), "all-NULL summary needs no scalar datum comparison");
        need(result.canonical == 0, "all-NULL summary does not request declared order");
    }
    const InputCase arrays{"arrays", "integer[]", {{"{}", 0}, {"{1,NULL}", 1}, {{}, 0}}};
    const auto arrayRows = build(arrays, ddl, session);
    completeProbe(arrays, arrayRows, "IS NULL", {{}, 0});
    completeProbe(arrays, arrayRows, "IS NOT NULL", {{}, 0});
    need(!observe("arrays", "=", "{}").ranges, "unsupported array order keeps original fallback");

    // Component proof of the real legacy comparator's pair-dependent Unknown.
    // This is deliberately not mislabelled as a modern physical timestamp.
    auto legacy = g_engine.getTableSchema(db, "timestamps").cols[1];
    legacy.storageCodec = Column::StorageCodec::Legacy;
    legacy.isVariableLength = false;
    legacy.dataType = "timestamp";
    need(StorageEngine::compareValues(legacy, "bad-A", false, "2000-01-01 00:00:00", false, "<=") == Truth::True,
         "legacy parser maps malformed epoch-zero operand below genuine later bound");
    need(StorageEngine::compareValues(legacy, "bad-B", false, "bad-A", false, "<") == Truth::Unknown,
         "legacy pair-dependent Unknown cannot be replaced by a total-order assumption");
    need(StorageEngine::compareValues(legacy, "bad-B", false, "2000-01-01 00:00:00", false, "<") == Truth::True,
         "legacy other-end comparison alone is insufficient proof");

    std::cout << "BRIN_OPERATOR_OBSERVER compareValues_internal=UNAVAILABLE canonical=inclusive_real_external_wrap\n";
    std::cout << "BRIN_OPERATOR_PRUNING checks=" << checks << " probes=" << probes << " failures=" << failures << '\n';
    return failures ? 1 : 0;
} catch (const std::exception& error) {
    watchCanonical = false;
    std::cerr << "BRIN_OPERATOR_PRUNING_FATAL checks=" << checks << " failures=" << failures << ' ' << error.what() << '\n';
    return 1;
}

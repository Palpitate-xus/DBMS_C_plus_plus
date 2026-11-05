#include "commands/AnalyzedStatisticsCapture.h"
#include "commands/DdlExecutor.h"
#include "common/Config.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
namespace stats = dbms::analyzed_statistics;
using namespace dbms;
using SqlRow = StorageEngine::SqlRow;
size_t checks = 0;
void require(bool value, const char* message) {
    ++checks;
    if (!value) throw std::runtime_error(message);
}
std::string hex(const std::string& bytes) {
    const char* digits = "0123456789abcdef"; std::string result;
    for (unsigned char c : bytes) { result += digits[c >> 4]; result += digits[c & 15]; }
    return result.empty() ? "-" : result;
}
uint32_t bits(float value) { uint32_t result; std::memcpy(&result, &value, 4); return result; }
void dump(const std::string& table, const std::string& name, const stats::Key& key, const stats::Column& c) {
    std::cout << "SCALAR_BASE " << table << ' ' << name << ' ' << key.inherited << ' '
        << c.attributeType->actualOid << ' ' << c.sampleRows << ' ' << c.nullRows << ' '
        << uint64_t(c.totalRows) << ' ' << c.averageWidth << ' ' << bits(c.nullFraction) << ' '
        << bits(c.distinctEstimate) << ' ' << key.relation << ' ' << key.attribute << ' '
        << c.physicalRelation << '\n';
    for (const auto& slot : c.slots) {
        const auto kind = static_cast<unsigned>(slot.kind);
        std::cout << "SCALAR_SLOT " << table << ' ' << name << ' ' << kind << ' '
            << slot.comparisonOperator << ' ' << slot.collation << ' '
            << (slot.values ? slot.values->elementType->actualOid : 0) << ' '
            << (slot.numbers ? long(slot.numbers->size()) : -1) << ' '
            << (slot.values ? long(slot.values->cells.size()) : -1) << '\n';
        if (slot.numbers) for (size_t i = 0; i < slot.numbers->size(); ++i)
            std::cout << "SCALAR_NUMBER " << table << ' ' << name << ' ' << kind << ' ' << i << ' ' << bits((*slot.numbers)[i]) << '\n';
        if (slot.values) for (size_t i = 0; i < slot.values->cells.size(); ++i) {
            const auto& v = slot.values->cells[i];
            std::cout << "SCALAR_VALUE " << table << ' ' << name << ' ' << kind << ' ' << i << ' ' << v.isNull << ' ' << hex(v.value) << '\n';
        }
    }
}
std::string quote(const std::optional<std::string>& value) {
    if (!value) return "NULL";
    std::string result = "'";
    for (char c : *value) { result += c; if (c == '\'') result += c; }
    return result + "'";
}
void ddl(DdlExecutor& owner, Session& session, const std::string& sql, bool reference = true) {
    require(!owner.executeSql(sql, session), "real DDL succeeds");
    if (reference) std::cout << "SCALAR_REFERENCE_SQL " << sql << ";\n";
}
void insert(const std::string& db, const std::string& table, const SqlRow& row) {
    require(g_engine.insertRow(db, table, row) == DBStatus::OK, "real typed heap insert succeeds");
    std::string names, values;
    for (const auto& x : row) {
        if (!names.empty()) { names += ','; values += ','; }
        names += x.first; values += quote(x.second);
    }
    std::cout << "SCALAR_REFERENCE_SQL INSERT INTO " << table << '(' << names << ") VALUES(" << values << ");\n";
}
const stats::Slot* find(const stats::Column& c, stats::Kind kind) {
    for (const auto& slot : c.slots) if (slot.kind == kind) return &slot;
    return nullptr;
}
struct Actual {
    stats::Key key;
    stats::Column column;
};
Actual actual(const std::string& db, const std::string& table, const std::string& name) {
    const auto snapshot = g_engine.catalogService().metadataSnapshot(db);
    uint32_t ns = 0, relation = 0;
    for (const auto& n : snapshot.namespaces) if (n.nspname == "public") ns = n.oid;
    for (const auto& r : snapshot.relations) if (r.relnamespace == ns && r.relname == table) relation = r.oid;
    require(relation != 0, "real catalog relation identity");
    int16_t attribute = 0;
    for (const auto& a : snapshot.attributes) if (a.attrelid == relation && a.attname == name && !a.attisdropped) attribute = a.attnum;
    const auto image = stats::read(g_engine.dbPath(db) / ".analyzed_statistics");
    require(image.has_value(), "real ANALYZE published typed snapshot");
    const stats::Key key{relation, attribute, false};
    const auto it = image->columns.find(key);
    require(it != image->columns.end(), "actual column statistics row exists");
    const auto physical = g_engine.getTableSchema(db, table).physicalRelationId;
    require(physical && physical == it->second.physicalRelation, "actual physical relation generation");
    dump(table, name, key, it->second);
    return {key, it->second};
}
void analyze(const std::string& db, const std::string& table) {
    require(g_engine.analyzeTable(db, table), "real ANALYZE succeeds");
    std::cout << "SCALAR_REFERENCE_SQL ANALYZE " << table << ";\n";
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 3 && std::string(argv[1]) == "--cold") {
            const auto image = stats::read(argv[2]);
            require(image && image->columns.size() == 15, "cold read has every real analyzed row");
            std::ifstream in(argv[2], std::ios::binary);
            const std::string bytes{std::istreambuf_iterator<char>(in), {}};
            require(stats::encode(*image) == bytes, "cold image preserves every field and exact frame");
            std::cout << "ANALYZED_SCALAR_COLD checks=" << checks << " failures=0\n";
            return 0;
        }
        require(argc == 1, "fixed regression protocol");
        TypeRegistry::instance().bootstrap();
        const auto db = testDbPath("analyzed_scalar_statistics");
        require(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "real database creation");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = db;
        struct Scope {
            Session* before; bool automatic;
            ~Scope() { setCurrentSession(before); g_config.autoAnalyzeEnabled = automatic; }
        } scope{currentSession(), g_config.autoAnalyzeEnabled};
        setCurrentSession(&session); g_config.autoAnalyzeEnabled = false;
        DdlExecutor owner;
        ddl(owner, session, "CREATE TABLE scalar_scan(i integer,u integer,r integer,t text,n numeric,b boolean,f double precision,s integer GENERATED ALWAYS AS(i*2) STORED,v integer GENERATED ALWAYS AS(1/(u-u)) VIRTUAL)", false);
        // PostgreSQL18 has virtual columns too; the invalid expression remains
        // unselected during both engines' ANALYZE operations.
        std::cout << "SCALAR_REFERENCE_SQL CREATE TABLE scalar_scan(i integer,u integer,r integer,t text,n numeric,b boolean,f double precision,s integer GENERATED ALWAYS AS(i*2) STORED,v integer GENERATED ALWAYS AS(1/(u-u)) VIRTUAL);\n";
        for (int i = 1; i <= 40; ++i) {
            const bool null = i % 7 == 0;
            SqlRow row{{"i", null ? std::nullopt : std::optional<std::string>(std::to_string(i % 4))},
                {"u", std::to_string(i)}, {"r", std::to_string(41 - i)},
                {"t", null ? std::nullopt : std::optional<std::string>(i % 3 == 0 ? "a,;|\"界" : i % 3 == 1 ? "" : "NULL")},
                {"n", null ? std::nullopt : std::optional<std::string>(i % 3 == 0 ? "123456789012345678901.2500" : i % 3 == 1 ? "123456789012345678901.25" : "-0.50")},
                {"b", null ? std::nullopt : std::optional<std::string>(i % 2 ? "t" : "f")},
                {"f", null ? std::nullopt : std::optional<std::string>(i % 11 == 0 ? "NaN" : i % 13 == 0 ? "Infinity" : i % 5 ? "1.25" : "-2.5")}};
            insert(db, "scalar_scan", row);
        }
        analyze(db, "scalar_scan");
        for (const auto* name : {"i", "u", "r", "t", "n", "b", "f", "s"}) {
            const auto x = actual(db, "scalar_scan", name);
            require(x.column.sampleRows == 40 && x.column.totalRows == 40, "full real small population");
        }
        const auto up = actual(db, "scalar_scan", "u").column;
        const auto down = actual(db, "scalar_scan", "r").column;
        require(up.distinctEstimate == -1 && find(up, stats::Kind::Correlation)->numbers->at(0) == 1,
            "ascending unique physical order");
        require(find(down, stats::Kind::Correlation)->numbers->at(0) == -1, "descending physical order");
        ddl(owner, session, "CREATE TABLE scalar_nulls(i integer,t text,f double precision)");
        for (int i = 0; i < 5; ++i) insert(db, "scalar_nulls", {{"i", {}}, {"t", {}}, {"f", {}}});
        analyze(db, "scalar_nulls");
        for (const auto* name : {"i", "t", "f"}) {
            const auto x = actual(db, "scalar_nulls", name).column;
            require(x.nullFraction == 1 && x.distinctEstimate == 0 && x.slots.empty(), "all NULL applicability");
        }
        ddl(owner, session, "CREATE TABLE scalar_single(i integer)");
        insert(db, "scalar_single", {{"i", "7"}}); analyze(db, "scalar_single");
        require(actual(db, "scalar_single", "i").column.slots.empty(), "singleton has no fabricated slots");
        ddl(owner, session, "CREATE TABLE scalar_constant(i integer)");
        for (int i = 0; i < 40; ++i) insert(db, "scalar_constant", {{"i", "7"}});
        analyze(db, "scalar_constant");
        const auto constant = actual(db, "scalar_constant", "i").column;
        require(constant.distinctEstimate == 1 && find(constant, stats::Kind::MostCommon)->numbers->at(0) == 1,
            "complete finite-domain MCV");
        ddl(owner, session, "CREATE TABLE scalar_wide(t text)");
        for (int i = 0; i < 3; ++i) insert(db, "scalar_wide", {{"t", std::string(1500, 'x')}});
        analyze(db, "scalar_wide");
        const auto wide = actual(db, "scalar_wide", "t").column;
        require(wide.distinctEstimate == -1 && wide.slots.empty() && wide.averageWidth > 0,
            "wide inputs count toward width but have no invented MCV/correlation");
        ddl(owner, session, "CREATE TABLE scalar_sample(i integer)");
        ddl(owner, session, "ALTER TABLE scalar_sample ALTER COLUMN i SET STATISTICS 1");
        for (int i = 0; i < 450; ++i) insert(db, "scalar_sample", {{"i", std::to_string(i)}});
        analyze(db, "scalar_sample");
        const auto sampled = actual(db, "scalar_sample", "i").column;
        require(sampled.sampleRows == 300 && sampled.totalRows == 450 && sampled.distinctEstimate == -1,
            "actual bounded reservoir sample and exact population count");
        require(find(sampled, stats::Kind::Histogram)->values->cells.size() == 2 &&
            find(sampled, stats::Kind::Correlation)->numbers->at(0) == 1, "sample uses physical ranks and target");
        const auto path = std::filesystem::absolute(g_engine.dbPath(db) / ".analyzed_statistics");
        const auto image = stats::read(path);
        require(image && image->columns.size() == 15, "one real generation per analyzed owner");
        std::cout << "ANALYZED_SCALAR_IMAGE " << path << '\n';
        std::cout << "ANALYZED_SCALAR_COMPLETE checks=" << checks << " failures=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ANALYZED_SCALAR_FAIL checks=" << checks << ' ' << error.what() << '\n'; return 1;
    }
}

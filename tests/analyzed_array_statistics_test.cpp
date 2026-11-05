#include "catalog/AnalyzedStatisticsStore.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "common/Config.h"
#include "test_utils.h"
#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
namespace stats = dbms::analyzed_statistics;
size_t checks = 0;
void require(bool good, const char* message) { ++checks; if (!good) throw std::runtime_error(message); }
void ddl(DdlExecutor& owner, Session& session, const std::string& sql) {
    require(!owner.executeSql(sql, session), "real array DDL succeeds");
    std::cout << "ARRAY_REFERENCE_SQL " << sql << ";\n";
}
std::string literal(const std::optional<std::string>& value) {
    if (!value) return "NULL";
    std::string out = "'";
    for (char c : *value) { out += c; if (c == '\'') out += c; }
    return out + "'";
}
void insert(const std::string& db, const std::string& table, const StorageEngine::SqlRow& row) {
    require(g_engine.insertRow(db, table, row) == DBStatus::OK, "real array heap insert succeeds");
    std::string names, values;
    for (const auto& cell : row) {
        if (!names.empty()) { names += ','; values += ','; }
        names += cell.first; values += literal(cell.second);
    }
    std::cout << "ARRAY_REFERENCE_SQL INSERT INTO " << table << '(' << names << ") VALUES(" << values << ");\n";
}
void analyze(const std::string& db, const std::string& table) {
    require(g_engine.analyzeTable(db, table), "real array ANALYZE succeeds");
    std::cout << "ARRAY_REFERENCE_SQL ANALYZE " << table << ";\n";
}
const stats::Slot* slot(const stats::Column& c, stats::Kind kind) {
    for (const auto& s : c.slots) if (s.kind == kind) return &s;
    return nullptr;
}
stats::Column actual(const std::string& db, const std::string& table, const std::string& column) {
    const auto metadata = g_engine.catalogService().metadataSnapshot(db);
    uint32_t ns = 0, relation = 0; int16_t attribute = 0;
    for (const auto& n : metadata.namespaces) if (n.nspname == "public") ns = n.oid;
    for (const auto& r : metadata.relations) if (r.relnamespace == ns && r.relname == table) relation = r.oid;
    require(relation, "array relation has actual catalog identity");
    for (const auto& a : metadata.attributes)
        if (a.attrelid == relation && !a.attisdropped && a.attname == column) attribute = a.attnum;
    const auto image = stats::read(g_engine.dbPath(db) / ".analyzed_statistics");
    require(image.has_value(), "actual ANALYZE snapshot exists");
    const auto found = image->columns.find({relation, attribute, false});
    require(found != image->columns.end(), "actual array attribute statistics were collected");
    require(found->second.physicalRelation == g_engine.getTableSchema(db, table).physicalRelationId,
            "statistics retain the actual physical relation generation");
    std::cout << "ARRAY_IDENTITY " << table << ' ' << column << ' ' << relation << ' ' << attribute << '\n';
    return found->second;
}
void validateArrays(const stats::Column& c, uint32_t array, uint32_t element) {
    require(c.attributeType->actualOid == array && c.attributeType->arrayElement &&
            c.attributeType->arrayElement->actualOid == element, "actual array and scalar element OIDs");
    for (const auto& s : c.slots) {
        require(s.collation == c.collation, "slot collation belongs to this actual attribute");
        if (s.kind == stats::Kind::CommonElements) {
            require(s.values && s.numbers && s.values->elementType->actualOid == element &&
                    s.numbers->size() == s.values->cells.size() + 3, "element slot value identity and frequency tail");
            for (const auto& v : s.values->cells) require(!v.isNull && !v.compound, "common elements are genuine scalar cells");
        } else if (s.kind == stats::Kind::MostCommon || s.kind == stats::Kind::Histogram) {
            require(s.values && s.values->elementType->actualOid == array, "whole-array slot values use array element identity");
            for (const auto& v : s.values->cells)
                require(!v.isNull && v.compound && v.compound->type->actualOid == array &&
                        v.compound->arrayElements.has_value(), "whole-array slot owns its actual structured array");
        }
    }
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 3 && std::string(argv[1]) == "--cold") {
            const auto image = stats::read(argv[2]);
            require(image && image->columns.size() == 14, "cold reader sees all real scalar and array rows");
            std::ifstream file(argv[2], std::ios::binary); const std::string bytes{std::istreambuf_iterator<char>(file), {}};
            require(stats::encode(*image) == bytes, "cold roundtrip preserves every owning child, dimension and field");
            for (const auto& pair : image->columns) if (pair.second.attributeType->kind == DatumKind::Array)
                validateArrays(pair.second, pair.second.attributeType->actualOid,
                                pair.second.attributeType->arrayElement->actualOid);
            std::cout << "ANALYZED_ARRAY_COLD checks=" << checks << " failures=0\n"; return 0;
        }
        require(argc == 1, "fixed native array regression protocol");
        TypeRegistry::instance().bootstrap(); const auto db = testDbPath("analyzed_array_statistics");
        require(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "real database creation");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = db;
        struct Scope { Session* before; bool automatic;
            ~Scope() { setCurrentSession(before); g_config.autoAnalyzeEnabled = automatic; }
        } scope{currentSession(), g_config.autoAnalyzeEnabled};
        setCurrentSession(&session); g_config.autoAnalyzeEnabled = false; DdlExecutor owner;
        ddl(owner, session, "CREATE TABLE array_scan(i integer[],t text[],n numeric[],b boolean[],f double precision[],x integer,v integer[] GENERATED ALWAYS AS(ARRAY[1/(x-x)]) VIRTUAL)");
        for (int r = 0; r < 40; ++r) {
            static const std::vector<std::optional<std::string>> ints = {
                {}, "{}", "{NULL,NULL}", "{1,1,2,NULL}", "{2,3}", "[-2:-1]={10,2}", "{{2,10},{3,NULL}}", "{10}"};
            const std::optional<std::string> text = r % 4 == 0 ? std::optional<std::string>{} :
                r % 4 == 1 ? "{}" : r % 4 == 2 ? "{\"a b\",\"a,b\",\"NULL\",\"\",NULL,\"a b\"}" : "{z,z,a}";
            insert(db, "array_scan", {{"i", ints[r % 8]}, {"t", text},
                {"n", r % 3 ? "{1.00,1.0,2.50}" : "{10000000000000000000000000000,0.1,-1}"},
                {"b", "{true,false,true,NULL}"}, {"f", r % 2 ? "{NaN,Infinity,-Infinity,0,-0,1}" : "{1.5,1.50,-2.5,NULL}"},
                {"x", std::to_string(r)}});
        }
        analyze(db, "array_scan");
        auto c = actual(db, "array_scan", "i"); validateArrays(c, 1007, 23);
        require(c.sampleRows == 40 && c.nullRows == 5 && c.nullFraction == .125f,
                "real NULL arrays counted independently from empty/NULL-only arrays");
        const auto* common = slot(c, stats::Kind::CommonElements);
        require(common && common->comparisonOperator == 96 && common->values->cells.size() == 4,
                "real integer common-element slot");
        const std::vector<std::string> expected = {"1", "2", "3", "10"};
        for (size_t i = 0; i < expected.size(); ++i)
            require(common->values->cells[i].value == expected[i], "real numeric element order");
        require(*common->numbers == std::vector<float>({1.0f/7, 4.0f/7, 2.0f/7, 3.0f/7, 1.0f/7, 4.0f/7, 3.0f/7}),
                "real row-membership frequencies, min/max and NULL-presence frequency");
        const auto& counts = *slot(c, stats::Kind::ElementCounts)->numbers;
        require(counts.size() == 101 && counts.front() == 0 && counts[99] == 3 && counts.back() == 10.0f/7,
                "real distinct-element histogram endpoints and mean");
        c = actual(db, "array_scan", "t"); validateArrays(c, 1009, 25);
        const auto* textCommon = slot(c, stats::Kind::CommonElements);
        bool empty = false, literalNull = false;
        for (const auto& value : textCommon->values->cells) { empty |= value.value.empty(); literalNull |= value.value == "NULL"; }
        require(empty && literalNull, "empty text and literal NULL survive as non-NULL array elements");
        validateArrays(actual(db, "array_scan", "n"), 1231, 1700);
        validateArrays(actual(db, "array_scan", "b"), 1000, 16);
        validateArrays(actual(db, "array_scan", "f"), 1022, 701);
        c = actual(db, "array_scan", "x");
        require(c.attributeType->actualOid == 23 && !slot(c, stats::Kind::CommonElements),
                "ordinary scalar statistics stay scalar");

        ddl(owner, session, "CREATE TABLE array_order(i integer[],t text[])");
        ddl(owner, session, "ALTER TABLE array_order ALTER COLUMN i SET STATISTICS 3");
        ddl(owner, session, "ALTER TABLE array_order ALTER COLUMN t SET STATISTICS 3");
        for (int r = 0; r < 40; ++r) {
            const auto values = std::to_string(r % 3) + ',' + std::to_string(r);
            const auto i = r % 5 == 0 ? "[0:1]={" + values + '}' : r % 5 == 1 ? "{{" + values + "}}" : '{' + values + '}';
            insert(db, "array_order", {{"i", i}, {"t", "{x," + std::to_string(r) + '}'} });
        }
        analyze(db, "array_order");
        c = actual(db, "array_order", "i"); validateArrays(c, 1007, 23);
        require(!slot(c, stats::Kind::MostCommon) && slot(c, stats::Kind::Histogram)->values->cells.size() == 4 &&
                slot(c, stats::Kind::Correlation), "unique arrays have real whole-array histogram and physical correlation");
        validateArrays(actual(db, "array_order", "t"), 1009, 25);
        ddl(owner, session, "CREATE TABLE array_empty(a integer[],z text[])"); analyze(db, "array_empty");
        ddl(owner, session, "CREATE TABLE array_nulls(a integer[],z text[])");
        ddl(owner, session, "CREATE TABLE array_zero(a integer[],z text[])");
        for (int r = 0; r < 3; ++r) {
            insert(db, "array_nulls", {{"a", {}}, {"z", {}}});
            insert(db, "array_zero", {{"a", "{}"}, {"z", "{}"}});
        }
        analyze(db, "array_nulls"); analyze(db, "array_zero");
        for (const auto& name : {"a", "z"}) {
            c = actual(db, "array_nulls", name);
            require(c.nullFraction == 1 && c.slots.empty(), "all-NULL actual array column has no invented slots");
            c = actual(db, "array_zero", name);
            require(!slot(c, stats::Kind::CommonElements) && slot(c, stats::Kind::ElementCounts)->numbers->back() == 0,
                    "real empty arrays retain zero distinct-element counts");
        }
        ddl(owner, session, "CREATE TABLE array_one(a integer[])");
        insert(db, "array_one", {{"a", "{NULL,1,1}"}}); analyze(db, "array_one");
        c = actual(db, "array_one", "a");
        require(!slot(c, stats::Kind::Correlation) && !slot(c, stats::Kind::MostCommon) &&
                slot(c, stats::Kind::CommonElements)->numbers->back() == 1,
                "single actual row has only applicable whole-array and element slots");
        ddl(owner, session, "CREATE TABLE array_sample(a integer[])");
        ddl(owner, session, "ALTER TABLE array_sample ALTER COLUMN a SET STATISTICS 1");
        for (int r = 0; r < 450; ++r)
            insert(db, "array_sample", {{"a", "{1," + std::to_string(r % 5) + ',' + std::to_string(r) + '}'}});
        analyze(db, "array_sample"); c = actual(db, "array_sample", "a");
        require(c.sampleRows == 300 && c.totalRows == 450 && c.target == 1 &&
                slot(c, stats::Kind::CommonElements)->values->cells.size() <= 10,
                "real bounded reservoir and target-times-ten element cap");
        const auto imagePath = g_engine.dbPath(db) / ".analyzed_statistics";
        const auto image = stats::read(imagePath);
        require(image && image->columns.size() == 14, "all actual scalar/array rows, without empty-table or VIRTUAL inventions");
        std::cout << "ANALYZED_ARRAY_IMAGE " << imagePath << '\n';
        std::cout << "ANALYZED_ARRAY_STATISTICS checks=" << checks << " failures=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ANALYZED_ARRAY_STATISTICS_FAIL checks=" << checks << ' ' << error.what() << '\n'; return 1;
    }
}

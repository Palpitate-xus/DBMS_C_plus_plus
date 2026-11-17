// Run in a fresh private CWD: test_stubs constructs its engine before main.
#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "expression/CompoundValueOps.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Frozen = std::map<std::string, Cell>;
unsigned checked = 0;
void need(bool value, const std::string& reason) {
    ++checked;
    if (!value) throw std::runtime_error(reason);
}
const ExprValue& value(const ExprEvalResult& result) {
    need(result.ok && result.owningValue.has_value(), "actual owning helper result: " + result.error);
    return *result.owningValue;
}
void scalar(const ExprEvalResult& result, const std::string& type, const Cell& expected) {
    const auto& datum = value(result);
    need(datum.typeName == type && datum.isNull == !expected &&
         (!expected || datum.value == *expected), "actual scalar type/value/NULL");
}
bool sameType(const DatumType* a, const DatumType* b) {
    if (!a || !b) return a == b;
    if (a->actualOid != b->actualOid || a->codecOid != b->codecOid || a->kind != b->kind ||
        a->codecSize != b->codecSize || a->effectiveTypeMod != b->effectiveTypeMod ||
        a->hasBinaryOutput != b->hasBinaryOutput || a->enumType != b->enumType ||
        a->recordFields.size() != b->recordFields.size() || !sameType(a->arrayElement.get(), b->arrayElement.get())) return false;
    for (size_t i = 0; i < a->recordFields.size(); ++i)
        if (!sameType(&a->recordFields[i], &b->recordFields[i])) return false;
    return true;
}
bool sameValue(const ExprValue& a, const ExprValue& b) {
    if (a.typeName != b.typeName || a.value != b.value || a.isNull != b.isNull ||
        a.collation != b.collation || a.objectOid != b.objectOid || a.elementOids != b.elementOids ||
        bool(a.recordFields) != bool(b.recordFields) || bool(a.compound) != bool(b.compound)) return false;
    if (a.recordFields) {
        if (a.recordFields->size() != b.recordFields->size()) return false;
        for (size_t i = 0; i < a.recordFields->size(); ++i)
            if (!sameValue(a.recordFields->at(i), b.recordFields->at(i))) return false;
    }
    if (!a.compound) return true;
    const auto& x = *a.compound;
    const auto& y = *b.compound;
    if (!sameType(x.type.get(), y.type.get()) || x.arrayNullBitmap != y.arrayNullBitmap ||
        x.dimensions.size() != y.dimensions.size() || bool(x.arrayElements) != bool(y.arrayElements)) return false;
    for (size_t i = 0; i < x.dimensions.size(); ++i)
        if (x.dimensions[i].length != y.dimensions[i].length || x.dimensions[i].lower != y.dimensions[i].lower) return false;
    if (x.arrayElements) {
        if (x.arrayElements->size() != y.arrayElements->size()) return false;
        for (size_t i = 0; i < x.arrayElements->size(); ++i)
            if (!sameValue(x.arrayElements->at(i), y.arrayElements->at(i))) return false;
    }
    return true;
}
void array(const ExprEvalResult& result, const std::string& type,
           uint32_t oid, const std::vector<Cell>& expected, int lower = 1) {
    const auto& datum = value(result);
    need(datum.typeName == type && !datum.isNull && datum.compound && datum.compound->arrayElements,
         "array keeps actual owning elements");
    const auto& data = *datum.compound;
    need(data.type && data.type->actualOid == oid && data.type->kind == DatumKind::Array,
         "array keeps its real catalog declaration");
    need(data.arrayElements->size() == expected.size(), "array cardinality");
    if (expected.empty()) need(data.dimensions.empty() && data.arrayNullBitmap == false,
                               "empty array has neither dimensions nor a NULL bitmap");
    else need(data.dimensions.size() == 1 && data.dimensions[0].lower == lower &&
              data.dimensions[0].length == static_cast<int32_t>(expected.size()), "array lower bound and length");
    for (size_t i = 0; i < expected.size(); ++i)
        need(data.arrayElements->at(i).isNull == !expected[i] &&
             (!expected[i] || data.arrayElements->at(i).value == *expected[i]), "array leaf value/NULL");
}
} // namespace

int main() {
    try {
        TypeRegistry::instance().bootstrap();
        const std::string database = testDbPath("lazy_scalar_metadata_" + std::to_string(::getpid()));
        need(!std::filesystem::exists(database), "fresh private database name");
        need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual database creation");
        Session session;
        session.username = "admin"; session.permission = 1; session.currentDB = database;
        struct Restore { Session* prior = currentSession(); ~Restore() { setCurrentSession(prior); } } restore;
        setCurrentSession(&session);
        need(!DdlExecutor{}.executeSql(
            "CREATE TABLE rich_inputs(id integer PRIMARY KEY,n integer,r regtype,z regtype,ra regtype[],"
            "ia integer[],ta text[],e integer[],q integer[],g integer GENERATED ALWAYS AS(n+1) VIRTUAL,"
            "bad integer GENERATED ALWAYS AS(1/(n-n)) VIRTUAL,gr regtype GENERATED ALWAYS AS(r) VIRTUAL,"
            "ga integer[] GENERATED ALWAYS AS(ia) VIRTUAL)", session), "actual rich source declaration");
        need(g_engine.insertRow(database, "rich_inputs", {{"id", "1"}, {"n", "4"}, {"r", "integer"}, {"z", "-"},
            {"ra", "[0:2]={integer,NULL,-}"}, {"ia", "[0:2]={7,NULL,9}"},
            {"ta", "[0:2]={\"NULL\",NULL,\"\"}"}, {"e", "{}"}, {"q", std::nullopt}}) == DBStatus::OK,
            "actual rich stored row");
        need(g_engine.insertRow(database, "rich_inputs", {{"id", "2"}, {"n", std::nullopt}, {"r", std::nullopt}, {"z", "-"},
            {"ra", "{}"}, {"ia", "{}"}, {"ta", "{}"}, {"e", "{}"}, {"q", std::nullopt}}) == DBStatus::OK,
            "actual NULL/empty stored row");
        const auto schema = g_engine.getTableSchema(database, "rich_inputs");
        std::map<std::string, std::string> types;
        for (size_t i = 0; i < schema.len; ++i)
            types.emplace(schema.cols[i].dataName, ExprHelper::canonicalResultTypeName(
                schema.cols[i].dataType + (schema.cols[i].isArray ? "[]" : "")));
        std::map<int, Frozen> rows;
        {
            auto cursor = g_engine.beginHeapScan(database, "rich_inputs", schema);
            HeapScanRow item;
            while (cursor->next(item)) {
                need(item.rid > 0 && item.nulls.size() == schema.len, "actual owned heap row and bitmap");
                detail::ScopedStoredRowNullBitmap bitmap(schema, item.nulls);
                Frozen row;
                for (size_t i = 0; i < schema.len; ++i) {
                    if (schema.cols[i].generatedKind == 'v') continue;
                    row.emplace(schema.cols[i].dataName, item.nulls[i] ? Cell{} :
                        Cell{g_engine.extractColumnValue(item.data, schema, i, database, false)});
                }
                const int id = std::stoi(*row.at("id"));
                need(rows.emplace(id, std::move(row)).second, "unique actual row identity");
            }
        }
        need(rows.size() == 2 && rows.at(1).at("r") == Cell{"23"} && rows.at(1).at("z") == Cell{"0"},
             "frozen callbacks receive real stored object OIDs, including zero");
        auto metadata = g_engine.prepareDatumMetadataContext(database, "rich_inputs");
        need(metadata && metadata->sourceColumns && metadata->generatedSource &&
             metadata->storedColumns.count("r") && !metadata->storedColumns.count("gr"),
             "actual metadata separates stored and virtual inputs");
        std::map<std::string, unsigned> calls;
        const auto evaluate = [&](const std::string& expression, const Frozen& row) {
            calls.clear();
            const auto reader = [&](const std::string& name) -> Cell {
                ++calls[name];
                const auto found = row.find(name);
                if (found == row.end()) throw DbError("XX001", "virtual or undeclared source must not be fetched: " + name);
                return found->second;
            };
            return ExprHelper::evalStringWithLazyColumns(expression, types, reader, database, "admin", &g_engine,
                                                         {}, true, metadata.get());
        };
        auto result = evaluate("r", rows.at(1));
        scalar(result, "regtype", "integer");
        need(value(result).objectOid == 23 && calls.size() == 1 && calls["r"] == 1, "stored REGTYPE keeps physical identity");
        result = evaluate("z", rows.at(1));
        scalar(result, "regtype", "-");
        need(value(result).objectOid.has_value() && *value(result).objectOid == 0 && calls["z"] == 1,
             "OID zero is a present non-NULL identity");
        result = evaluate("r", rows.at(2));
        scalar(result, "regtype", std::nullopt);
        need(!value(result).objectOid && calls["r"] == 1, "NULL reference has no object identity");
        result = evaluate("ra", rows.at(1));
        array(result, "regtype[]", 2211, {"integer", {}, "-"}, 0);
        const auto& references = *value(result).compound->arrayElements;
        need(references[0].objectOid == 23 && !references[1].objectOid && references[2].objectOid.has_value() &&
             *references[2].objectOid == 0 && result.owningValue->elementOids.size() == 3,
             "reference array preserves OID/NULL/zero leaf roles");
        result = evaluate("ia", rows.at(1));
        array(result, "integer[]", 1007, {"7", {}, "9"}, 0);
        result = evaluate("ta", rows.at(1));
        array(result, "text[]", 1009, {"NULL", {}, ""}, 0);
        result = evaluate("e", rows.at(1));
        array(result, "integer[]", 1007, {});
        result = evaluate("q", rows.at(1));
        scalar(result, "integer[]", std::nullopt);
        need(result.typeMetadata && result.typeMetadata->actualOid == 1007 &&
             result.typeMetadata->kind == DatumKind::Array, "whole NULL retains its array declaration");
        result = evaluate("ra", rows.at(2));
        array(result, "regtype[]", 2211, {});
        result = evaluate("array_lower(ia,1)+array_upper(ia,1)", rows.at(1));
        scalar(result, "integer", "2");
        need(calls.size() == 1 && calls["ia"] == 1, "repeated array reads share one physical input");
        result = evaluate("g+g+n", rows.at(1));
        scalar(result, "integer", "14");
        need(calls.size() == 1 && calls["n"] == 1, "generated targets bypass source reader and share only base input");
        result = evaluate("coalesce(g,bad)", rows.at(1));
        scalar(result, "integer", "5");
        need(calls.size() == 1 && calls["n"] == 1, "unused failing virtual input is not evaluated");
        result = evaluate("CASE WHEN false THEN bad ELSE n END", rows.at(1));
        scalar(result, "integer", "4");
        need(calls.size() == 1 && calls["n"] == 1, "CASE branch avoids failing generated target");
        result = evaluate("coalesce(g,7)", rows.at(2));
        scalar(result, "integer", "7");
        need(calls.size() == 1 && calls["n"] == 1, "generated NULL propagates from actual NULL base");
        result = evaluate("gr", rows.at(1));
        scalar(result, "regtype", "integer");
        need(value(result).objectOid == 23 && calls.size() == 1 && calls["r"] == 1,
             "generated reference retains typed identity without re-decoding its display");
        result = evaluate("ga", rows.at(1));
        array(result, "integer[]", 1007, {"7", {}, "9"}, 0);
        need(calls.size() == 1 && calls["ia"] == 1, "generated array retains the source's bounds and NULLs");
        bool generatedError = false;
        try { (void)evaluate("bad", rows.at(1)); }
        catch (const GeneratedColumnReadError& error) { generatedError = error.sqlState() == "22012"; }
        need(generatedError && calls.size() == 1 && calls["n"] == 1,
             "selected generated failure keeps its distinct exception type and base demand");
        result = evaluate("upper(missing)", rows.at(1));
        need(!result.ok && result.sqlState == "42703" && calls.empty(), "unknown names fail binding before any value demand");

        // The eager API retains its initial declaration/value construction
        // contract, while an ordinary row need not decode unrelated fields.
        need(!DdlExecutor{}.executeSql("CREATE TABLE eager_inputs(id integer,r regtype,z regtype,t text)", session),
             "actual eager source declaration without generated columns");
        need(!DdlExecutor{}.executeSql("CREATE SEQUENCE eager_metadata_calls", session), "actual volatile-effect sequence");
        need(g_engine.insertRow(database, "eager_inputs", {{"id", "1"}, {"r", "integer"}, {"z", "-"}, {"t", "7"}}) == DBStatus::OK,
             "actual eager source row");
        auto eagerMetadata = g_engine.prepareDatumMetadataContext(database, "eager_inputs");
        need(eagerMetadata && eagerMetadata->sourceColumns && !eagerMetadata->generatedSource &&
             eagerMetadata->storedColumns.count("r"), "real eager storage provenance");
        std::map<std::string, std::string> eagerRow;
        const auto eagerSchema = g_engine.getTableSchema(database, "eager_inputs");
        {
            auto cursor = g_engine.beginHeapScan(database, "eager_inputs", eagerSchema);
            HeapScanRow item;
            need(cursor->next(item) && item.nulls.size() == eagerSchema.len, "actual eager heap row and bitmap");
            detail::ScopedStoredRowNullBitmap bitmap(eagerSchema, item.nulls);
            for (size_t i = 0; i < eagerSchema.len; ++i) {
                need(!item.nulls[i], "eager fixture contains actual non-NULL stored fields");
                eagerRow.emplace(eagerSchema.cols[i].dataName,
                    g_engine.extractColumnValue(item.data, eagerSchema, i, database, false));
            }
        }
        const std::map<std::string, std::string> originalHints = {
            {"id", "INT4"}, {"r", "REGTYPE"}, {"z", "regtype"}, {"t", "TEXT"}};
        auto eagerHints = originalHints;
        const auto eagerEvaluate = [&](const std::string& expression,
                const std::map<std::string, std::string>& input,
                const std::map<std::string, ExprValue>* owning = nullptr) {
            return ExprHelper::evalStringWithNulls(expression, input, {}, eagerHints,
                database, "admin", &g_engine, {}, true, owning, eagerMetadata.get());
        };
        result = eagerEvaluate("pg_typeof(t)::text", eagerRow);
        scalar(result, "text", "text");
        result = eagerEvaluate("r", eagerRow);
        scalar(result, "regtype", "integer");
        need(value(result).objectOid == 23 && result.typeMetadata && result.typeMetadata->actualOid == 2206,
             "original type-hint spelling and stored provenance retain reference role");
        result = eagerEvaluate("z", eagerRow);
        scalar(result, "regtype", "-");
        need(value(result).objectOid.has_value() && *value(result).objectOid == 0,
             "eager source preserves real OID zero");
        auto malformed = eagerRow;
        malformed["r"] = "not-a-persisted-oid";
        result = eagerEvaluate("upper(t)", malformed);
        scalar(result, "text", "7");
        result = eagerEvaluate("pg_typeof(t)::text", malformed);
        scalar(result, "text", "text");
        need(eagerHints == originalHints, "elision does not normalize or rewrite the caller's type-hint map");
        bool storedError = false;
        try { (void)eagerEvaluate("nextval('eager_metadata_calls')::text || r::text", malformed); }
        catch (const DbError& error) { storedError = error.sqlState() == "XX001"; }
        need(storedError, "selected malformed stored reference still fails during eager construction");
        need(g_engine.nextval(database, "eager_metadata_calls") == 1,
             "selected stored conversion fails before an earlier volatile expression can run");
        result = eagerEvaluate("nextval('eager_metadata_calls')::text || t", malformed);
        scalar(result, "text", "27");
        need(g_engine.nextval(database, "eager_metadata_calls") == 3,
             "unselected conversion elision does not skip or duplicate the selected volatile call");

        const auto textNames = [&](const std::string& expression,
                const std::map<std::string, std::string>& input) {
            std::map<std::string, std::string> hints;
            for (const auto& entry : input) hints.emplace(entry.first, "TeXt");
            return ExprHelper::evalStringWithNulls(expression, input, {}, hints,
                "", "", nullptr, {}, true);
        };
        scalar(textNames("r || ':' || t.r || ':' || s.t.r",
            {{"r", "bare"}, {"t.r", "table"}, {"s.t.r", "schema"}}), "text", "bare:table:schema");
        scalar(textNames("s.t.r", {{"r", "bare"}, {"t.r", "table"}}), "text", "table");
        scalar(textNames("s.t.r", {{"r", "bare"}}), "text", "bare");
        scalar(textNames("upper(\"R\") || upper(r)", {{"R", "UP"}, {"r", "down"}}), "text", "UPDOWN");
        scalar(textNames("upper(R) || upper(\"R\")", {{"R", "UP"}, {"r", "down"}}), "text", "DOWNUP");

        // These are genuine aliases of a REGTYPE declaration from the actual
        // catalog. Only the first existing resolution key should be decoded.
        auto aliasHints = eagerHints;
        aliasHints.emplace("eager_inputs.r", "REGTYPE");
        aliasHints.emplace("public.eager_inputs.r", "regtype");
        auto aliases = malformed;
        aliases.emplace("eager_inputs.r", "16");
        aliases.emplace("public.eager_inputs.r", "0");
        const auto aliasEvaluate = [&](const std::string& expression) {
            return ExprHelper::evalStringWithNulls(expression, aliases, {}, aliasHints,
                database, "admin", &g_engine, {}, true, nullptr, eagerMetadata.get());
        };
        result = aliasEvaluate("public.eager_inputs.r");
        scalar(result, "regtype", "-");
        need(value(result).objectOid == 0, "full schema key wins without decoding malformed bare fallback");
        result = aliasEvaluate("eager_inputs.r");
        scalar(result, "regtype", "boolean");
        need(value(result).objectOid == 16, "table key wins over malformed bare fallback");
        aliases.erase("public.eager_inputs.r");
        result = aliasEvaluate("public.eager_inputs.r");
        scalar(result, "regtype", "boolean");
        aliases.erase("eager_inputs.r");
        aliases["r"] = "23";
        result = aliasEvaluate("public.eager_inputs.r");
        scalar(result, "regtype", "integer");

        std::map<std::string, std::string> richBadRow;
        std::set<std::string> richNulls;
        for (const auto& [name, entry] : rows.at(1)) {
            richBadRow.emplace(name, entry.value_or(""));
            if (!entry) richNulls.insert(name);
        }
        richBadRow["r"] = "not-a-persisted-oid";
        storedError = false;
        try { (void)ExprHelper::evalStringWithNulls("n+1", richBadRow, richNulls, types,
                database, "admin", &g_engine, {}, true, nullptr, metadata.get()); }
        catch (const DbError& error) { storedError = error.sqlState() == "XX001"; }
        need(storedError, "any generated source retains the complete eager conversion path");
        const std::map<std::string, ExprValue> emptyOwners;
        storedError = false;
        try { (void)eagerEvaluate("upper(t)", malformed, &emptyOwners); }
        catch (const DbError& error) { storedError = error.sqlState() == "XX001"; }
        need(storedError, "even an empty output-coercion pointer disables elision");
        const std::map<std::string, ExprValue> unrelatedOwner{{"t", ExprValue("text", "7", false)}};
        storedError = false;
        try { (void)eagerEvaluate("upper(t)", malformed, &unrelatedOwner); }
        catch (const DbError& error) { storedError = error.sqlState() == "XX001"; }
        need(storedError, "an unrelated owning override does not suppress legacy input conversion");
        const std::map<std::string, ExprValue> zeroOwner{{"r", value(evaluate("z", rows.at(1)))}};
        result = eagerEvaluate("r", malformed, &zeroOwner);
        scalar(result, "regtype", "-");
        need(value(result).objectOid.has_value() && *value(result).objectOid == 0,
             "existing output coercion keeps an actual typed zero datum rather than decoding its display");

        ExprEvalResult detachedLazy, detachedEager;
        std::weak_ptr<const Frozen> sourceLifetime;
        {
            auto source = std::make_shared<const Frozen>(rows.at(1));
            sourceLifetime = source;
            const auto reader = [source](const std::string& name) -> Cell { return source->at(name); };
            std::map<std::string, std::string> eager;
            std::set<std::string> nulls;
            for (const auto& [name, entry] : *source) {
                eager.emplace(name, entry.value_or(""));
                if (!entry) nulls.insert(name);
            }
            const std::string expression = "ROW(r,z,ra,ia,ta,e,q,gr,ga,g)";
            detachedLazy = ExprHelper::evalStringWithLazyColumns(expression, types, reader, database, "admin", &g_engine,
                                                                {}, true, metadata.get());
            detachedEager = ExprHelper::evalStringWithNulls(expression, eager, nulls, types, database, "admin", &g_engine,
                                                          {}, true, nullptr, metadata.get());
        }
        need(sourceLifetime.expired(), "helper retains no callback/source ownership after returning");
        need(sameValue(value(detachedLazy), value(detachedEager)) &&
             sameType(detachedLazy.typeMetadata.get(), detachedEager.typeMetadata.get()),
             "eager/lazy owned records agree after source maps are destroyed");
        std::weak_ptr<const DatumMetadataContext> metadataLifetime = metadata;
        metadata.reset();
        need(metadataLifetime.expired(), "result retains pure declarations rather than source context");
        need(g_engine.dropDatabase(database) == DBStatus::OK, "cleanup only owned database");
        need(sameValue(value(detachedLazy), value(detachedEager)), "detached owned data survives database removal");
        std::cout << "LAZY_SCALAR_METADATA_PASS checks=" << checked << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "LAZY_SCALAR_METADATA_FAIL checks=" << checked << " " << error.what() << '\n';
        return 1;
    }
}

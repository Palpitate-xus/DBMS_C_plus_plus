// Run in a fresh private CWD: test_stubs constructs its engine before main.
#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "expression/CompoundValueOps.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
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
unsigned checked = 0;

void need(bool good, const std::string& reason) {
    ++checked;
    if (!good) throw std::runtime_error(reason);
}

struct CatalogIdentity {
    std::vector<std::string> relations, attributes;
    bool operator==(const CatalogIdentity& other) const {
        return relations == other.relations && attributes == other.attributes;
    }
};

CatalogIdentity catalogIdentity(const std::string& database) {
    const auto snapshot = g_engine.catalogService().metadataSnapshot(database, false);
    CatalogIdentity result;
    for (const auto& relation : snapshot.relations) result.relations.push_back(relation.toString());
    for (const auto& attribute : snapshot.attributes) result.attributes.push_back(attribute.toString());
    std::sort(result.relations.begin(), result.relations.end());
    std::sort(result.attributes.begin(), result.attributes.end());
    return result;
}

Column referenceColumn(const std::string& name, const std::string& type, bool array = false) {
    auto result = makeVarCharColumn(name, true, 65535);
    result.dataType = type;
    result.typeMod = -1; // This builtin source is not a VARCHAR declaration.
    result.isArray = array;
    return result;
}

TableSchema sourceSchema(const std::string& physical, bool temporary = true) {
    TableSchema result;
    result.tablename = physical;
    result.isTemporary = temporary;
    result.append(makeIntColumn("id", false, 2));
    result.append(referenceColumn("r", "regtype"));
    result.append(referenceColumn("ra", "regtype", true));
    result.append(referenceColumn("c", "regclass"));
    result.append(referenceColumn("ca", "regclass", true));
    result.append(makeVarCharColumn("real_text", true, 65535));
    return result;
}

void noStoredDeclarations(const DatumMetadataContextPtr& metadata, const std::string& reason) {
    need(metadata && metadata->sourceColumns && metadata->columns.empty() && metadata->storedColumns.empty(), reason);
}

void scalarDeclaration(const DatumTypePtr& declaration, uint32_t oid) {
    need(declaration && declaration->actualOid == oid && declaration->codecOid == oid &&
         declaration->kind == DatumKind::Scalar && declaration->codecSize == 4 &&
         declaration->effectiveTypeMod == -1 && declaration->hasBinaryOutput,
         "actual internal scalar declaration and codec");
}

void arrayDeclaration(const DatumTypePtr& declaration, uint32_t oid, uint32_t element) {
    need(declaration && declaration->actualOid == oid && declaration->codecOid == oid &&
         declaration->kind == DatumKind::Array && declaration->codecSize == -1 &&
         declaration->effectiveTypeMod == -1 && declaration->hasBinaryOutput,
         "actual internal array declaration and codec");
    scalarDeclaration(declaration->arrayElement, element);
}

struct RawRow {
    std::map<std::string, std::string> values;
    std::set<std::string> nulls;
};

std::map<int, RawRow> actualRows(const std::string& database, const TableSchema& schema) {
    std::map<int, RawRow> result;
    auto cursor = g_engine.beginHeapScan(database, schema.tablename, schema);
    HeapScanRow row;
    while (cursor->next(row)) {
        need(row.rid >= 0 && row.nulls.size() == schema.len, "actual heap RID and NULL bitmap");
        detail::ScopedStoredRowNullBitmap bitmap(schema, row.nulls);
        RawRow raw;
        for (size_t index = 0; index < schema.len; ++index) {
            const auto& name = schema.cols[index].dataName;
            if (row.nulls[index]) {
                raw.nulls.insert(name);
                raw.values.emplace(name, "");
            } else {
                raw.values.emplace(name, g_engine.extractColumnValue(row.data, schema, index, database, false));
            }
        }
        need(result.emplace(std::stoi(raw.values.at("id")), std::move(raw)).second, "unique physical source row");
    }
    return result;
}

const ExprValue& owning(const ExprEvalResult& result) {
    need(result.ok && result.owningValue.has_value(), "actual helper owning result: " + result.error);
    return *result.owningValue;
}

void expectArray(const ExprEvalResult& result, const std::string& type, uint32_t oid,
                 uint32_t element, const Cell& display,
                 const std::vector<std::optional<uint32_t>>& identities,
                 int lower = 1) {
    const auto& value = owning(result);
    need(value.typeName == type && value.isNull == !display &&
         (!display || value.value == *display), "array output and whole NULL remain separate");
    arrayDeclaration(result.typeMetadata, oid, element);
    if (!display) {
        need(!value.objectOid && value.elementOids.empty(), "whole NULL does not invent element identities");
        return;
    }
    need(value.compound && value.compound->arrayElements && !value.objectOid &&
         value.elementOids == identities, "owning reference array keeps every original element OID");
    arrayDeclaration(value.compound->type, oid, element);
    const auto& children = *value.compound->arrayElements;
    need(children.size() == identities.size(), "actual array cardinality");
    if (identities.empty()) {
        need(value.compound->dimensions.empty() && value.compound->arrayNullBitmap == false,
             "empty array retains its zero-dimensional shape");
    } else {
        need(value.compound->dimensions.size() == 1 && value.compound->dimensions[0].lower == lower &&
             value.compound->dimensions[0].length == static_cast<int32_t>(identities.size()),
             "actual non-default lower bound and length");
    }
    for (size_t index = 0; index < identities.size(); ++index)
        need(children[index].isNull == !identities[index] && children[index].objectOid == identities[index],
             "owning element SQL NULL and OID zero remain distinct");
}

// These are the same storage calls made by main's ordinary outer SELECT
// coordinator. No SQL BEGIN is issued and no explicit Session block starts.
struct OrdinaryCommand {
    Session& session;
    bool active = false;
    explicit OrdinaryCommand(Session& value) : session(value) {
        need(!g_engine.inTransaction() && !session.searchPathTransaction.active &&
             !session.failedTransactionBlock, "ordinary autocommit command starts outside a user transaction block");
        need(g_engine.beginTransaction(session.currentDB) == DBStatus::OK, "ordinary statement-owned storage transaction");
        active = true;
        need(g_engine.beginSqlCommand(), "ordinary SQL command visibility owner");
        g_engine.noteQuerySnapshot();
        need(!session.searchPathTransaction.active, "internal statement transaction does not enter SQL BEGIN state");
    }
    void finish() {
        need(g_engine.finishSqlCommand(), "ordinary command finishes");
        need(g_engine.commitTransaction() == DBStatus::OK, "ordinary autocommit owner commits");
        active = false;
        need(!g_engine.inTransaction() && !session.searchPathTransaction.active,
             "ordinary command leaves no explicit or internal transaction");
    }
    ~OrdinaryCommand() { if (active) (void)g_engine.rollbackTransaction(); }
};
} // namespace

int main() {
    try {
        using namespace dbms;
        TypeRegistry::instance().bootstrap();
        const std::string name = "internal_reference_metadata_" + std::to_string(::getpid());
        const auto database = testDbPath(name), other = testDbPath(name + "_other");
        need(!std::filesystem::exists(database) && !std::filesystem::exists(other), "fresh private databases");
        need(g_engine.createDatabase(database, "utf8") == DBStatus::OK &&
             g_engine.createDatabase(other, "utf8") == DBStatus::OK, "actual database owners");
        Session session;
        session.username = "admin"; session.permission = 1; session.currentDB = database; session.pid = ::getpid();
        struct Restore { Session* prior = currentSession(); ~Restore() { setCurrentSession(prior); } } restore;
        setCurrentSession(&session);
        need(!DdlExecutor{}.executeSql("CREATE TABLE referenced_object(id integer)", session), "actual REGCLASS referent");
        const auto referentSnapshot = g_engine.catalogService().metadataSnapshot(database, false);
        uint32_t referent = 0;
        for (const auto& relation : referentSnapshot.relations)
            if (relation.relname == "referenced_object") referent = relation.oid;
        need(referent != 0, "real referent OID captured from its actual catalog row");

        const std::string logical = "__cte_reference_source";
        const auto physical = tempTablePrefix(session, logical);
        auto schema = sourceSchema(physical);
        need(g_engine.createTable(database, schema) == DBStatus::OK, "actual uncatalogued temporary source heap");
        session.transientTempTables.insert(logical);
        need(g_engine.createTable(other, schema) == DBStatus::OK, "same physical name in an independent database");
        const auto unregistered = tempTablePrefix(session, "__cte_unregistered");
        need(g_engine.createTable(database, sourceSchema(unregistered)) == DBStatus::OK, "actual unregistered temporary heap");
        const auto noheap = tempTablePrefix(session, "__cte_missing_heap");
        need(g_engine.createTable(database, sourceSchema(noheap)) == DBStatus::OK, "actual heap created before removal");
        const auto missingHeapPath = g_engine.dbPath(database) / (noheap + ".dt");
        need(std::filesystem::is_regular_file(missingHeapPath) && std::filesystem::remove(missingHeapPath) &&
             g_engine.tableExists(database, noheap), "missing actual heap retains its real schema as a negative control");
        const std::string ordinary = "ordinary_uncatalogued";
        need(g_engine.createTable(database, sourceSchema(ordinary, false)) == DBStatus::OK,
             "ordinary physical heap without catalog attributes");
        const auto virtualName = tempTablePrefix(session, "__cte_virtual_reference");
        auto virtualSchema = sourceSchema(virtualName);
        auto generated = referenceColumn("gr", "regtype");
        generated.generatedKind = 'v'; generated.generatedExpr = "r";
        virtualSchema.append(generated);
        need(g_engine.createTable(database, virtualSchema) == DBStatus::OK, "actual internal schema with a virtual reference");

        const auto before = catalogIdentity(database), otherBefore = catalogIdentity(other);
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, physical), "outside a command cannot manufacture source declarations");
        OrdinaryCommand command(session);
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, physical), "unregistered physical source stays unavailable");
        g_engine.registerSqlCommandInternalRelation(physical);
        g_engine.registerSqlCommandInternalRelation(noheap);
        g_engine.registerSqlCommandInternalRelation(ordinary);
        g_engine.registerSqlCommandInternalRelation(virtualName);
        const auto absent = tempTablePrefix(session, "__cte_never_created");
        g_engine.registerSqlCommandInternalRelation(absent);
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, absent), "registration cannot create an absent schema or heap");
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, noheap), "schema alone cannot prove a live internal heap");
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, unregistered), "unregistered temporary heap remains unmarked");
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, ordinary), "registration cannot turn an ordinary missing-attribute table into a query temporary");
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(other, physical), "same registered name cannot confer provenance in another database");
        const auto virtualMetadata = g_engine.prepareDatumMetadataContext(database, virtualName);
        need(virtualMetadata && virtualMetadata->generatedSource && !virtualMetadata->storedColumns.count("gr") &&
             !virtualMetadata->columns.count("gr"), "actual virtual reference is never marked as stored input");
        scalarDeclaration(virtualMetadata->columns.at("r"), 2206);

        const auto metadata = g_engine.prepareDatumMetadataContext(database, physical);
        need(metadata && metadata->sourceColumns && !metadata->generatedSource &&
             metadata->columns.size() == 12 && metadata->storedColumns.size() == 12,
             "producer publishes only four actual reference fields and their three name bindings");
        const auto tempSchema = sessionTempSchemaName(session);
        for (const auto& column : {std::string("r"), std::string("ra"), std::string("c"), std::string("ca")}) {
            const auto& declaration = metadata->columns.at(column);
            for (const auto& key : {column, logical + "." + column, tempSchema + "." + logical + "." + column})
                need(metadata->storedColumns.count(key) && metadata->columns.at(key) == declaration,
                     "qualified and bare bindings retain the same owned current-source declaration");
        }
        scalarDeclaration(metadata->columns.at("r"), 2206);
        scalarDeclaration(metadata->columns.at("c"), 2205);
        arrayDeclaration(metadata->columns.at("ra"), 2211, 2206);
        arrayDeclaration(metadata->columns.at("ca"), 2210, 2205);
        need(!metadata->columns.count("real_text") && !metadata->storedColumns.count("real_text"),
             "physical text never acquires a stored reference role");

        need(g_engine.insertRow(database, physical, {{"id","1"},{"r","integer"},{"ra","[0:2]={integer,NULL,-}"},
            {"c","referenced_object"},{"ca","[-1:1]={referenced_object,NULL,-}"},{"real_text","23"}}) == DBStatus::OK,
            "actual raw reference row, source NULL elements and non-default bounds");
        need(g_engine.insertRow(database, physical, {{"id","2"},{"r","-"},{"ra","{}"},{"c","-"},{"ca","{}"},
            {"real_text","NULL"}}) == DBStatus::OK, "actual OID zero and empty arrays");
        need(g_engine.insertRow(database, physical, {{"id","3"},{"r",std::nullopt},{"ra",std::nullopt},
            {"c",std::nullopt},{"ca",std::nullopt},{"real_text",""}}) == DBStatus::OK, "actual whole NULL references");
        need(g_engine.insertRow(database, physical, {{"id","4"},{"r","4294967295"},{"ra","{4294967295,NULL}"},
            {"c","4294967295"},{"ca","{4294967295,NULL}"},{"real_text",std::nullopt}}) == DBStatus::OK,
            "actual unknown maximum unsigned OIDs");
        schema = g_engine.getTableSchema(database, physical);
        std::map<std::string,std::string> hints;
        for (size_t index = 0; index < schema.len; ++index)
            hints.emplace(schema.cols[index].dataName, schema.cols[index].dataType + (schema.cols[index].isArray ? "[]" : ""));
        const auto rows = actualRows(database, schema);
        need(rows.size() == 4 && rows.at(1).values.at("r") == "23" && rows.at(1).values.at("ra") == "[0:2]={23,NULL,0}" &&
             rows.at(1).values.at("c") == std::to_string(referent) &&
             rows.at(1).values.at("ca") == "[-1:1]={" + std::to_string(referent) + ",NULL,0}",
             "physical producer retains real identities before helper evaluation");
        const auto evaluate = [&](const std::string& expression, int id) {
            const auto& row = rows.at(id);
            return ExprHelper::evalStringWithNulls(expression, row.values, row.nulls, hints, database,
                session.username, &g_engine, {}, true, nullptr, metadata.get());
        };
        const std::vector<Cell> scalarTypes{"integer","-",{},"4294967295"};
        const std::vector<Cell> scalarClasses{"referenced_object","-",{},"4294967295"};
        const std::vector<std::optional<uint32_t>> typeOids{23,0,{},UINT32_MAX}, classOids{referent,0,{},UINT32_MAX};
        for (int id = 1; id <= 4; ++id) {
            for (bool relation : {false,true}) {
                const auto result = evaluate(relation ? "c" : "r", id);
                const auto& value = owning(result);
                const auto& display = (relation ? scalarClasses : scalarTypes).at(id-1);
                need(value.isNull == !display && (!display || value.value == *display) &&
                     value.objectOid == (relation ? classOids : typeOids).at(id-1),
                     "scalar owning display, SQL NULL and original OID remain separate");
                scalarDeclaration(result.typeMetadata, relation ? 2205 : 2206);
                const auto cast = evaluate(relation ? "c::oid" : "r::oid", id);
                const auto& castValue = owning(cast);
                need(castValue.isNull == !display &&
                     (!display || castValue.value == std::to_string(*(relation ? classOids : typeOids).at(id-1))),
                     "selected scalar OID cast preserves original identity");
            }
        }
        expectArray(evaluate("ra",1),"regtype[]",2211,2206,"[0:2]={integer,NULL,-}",{23,{},0},0);
        expectArray(evaluate("ca",1),"regclass[]",2210,2205,"[-1:1]={referenced_object,NULL,-}",{referent,{},0},-1);
        expectArray(evaluate("ra",2),"regtype[]",2211,2206,"{}",{});
        expectArray(evaluate("ca",2),"regclass[]",2210,2205,"{}",{});
        expectArray(evaluate("ra",3),"regtype[]",2211,2206,{},{});
        expectArray(evaluate("ca",3),"regclass[]",2210,2205,{},{});
        expectArray(evaluate("ra",4),"regtype[]",2211,2206,"{4294967295,NULL}",{UINT32_MAX,{}});
        expectArray(evaluate("ca",4),"regclass[]",2210,2205,"{4294967295,NULL}",{UINT32_MAX,{}});
        need(evaluate("ra::text",1).value == "[0:2]={integer,NULL,-}" &&
             evaluate("ca::text",1).value == "[-1:1]={referenced_object,NULL,-}",
             "selected array text casts see decoded inputs rather than stored numeric tokens");

        auto misleading = hints;
        misleading["real_text"] = "regtype";
        const auto falseHint = ExprHelper::evalStringWithNulls("real_text", rows.at(1).values,
            rows.at(1).nulls, misleading, database, session.username, &g_engine, {}, true, nullptr, metadata.get());
        need(owning(falseHint).value == "23" && !owning(falseHint).objectOid &&
             !metadata->columns.count("real_text") && !metadata->storedColumns.count("real_text"),
             "a caller hint cannot create physical reference provenance or an object identity for real text");
        need(catalogIdentity(database) == before && catalogIdentity(other) == otherBefore,
             "metadata and actual evaluation allocate no catalog relation or attribute identities");
        need(g_engine.advanceSqlCommandCounter(), "internal command counter may advance without losing the source registration");
        scalarDeclaration(g_engine.prepareDatumMetadataContext(database, physical)->columns.at("r"),2206);
        command.finish();
        noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, physical), "finished command cannot lend internal provenance to later callers");
        {
            OrdinaryCommand next(session);
            noStoredDeclarations(g_engine.prepareDatumMetadataContext(database, physical), "a new ordinary command starts with no stale internal registration");
            next.finish();
        }
        need(catalogIdentity(database) == before && catalogIdentity(other) == otherBefore,
             "ordinary command lifetime does not publish fake catalog identities");
        need(g_engine.dropDatabase(database) == DBStatus::OK && g_engine.dropDatabase(other) == DBStatus::OK,
             "private source databases are removed after the owners finish");
        cleanupTestDb(name); cleanupTestDb(name + "_other");
        std::cout << "[INTERNAL REFERENCE METADATA] " << checked << " checks passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[INTERNAL REFERENCE METADATA] check " << checked << ": " << error.what() << '\n';
        return 1;
    }
}

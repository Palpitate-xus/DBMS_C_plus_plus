// Run in a fresh private CWD when linked with test_stubs (its engine is global).
#include "Session.h"
#include "catalog/type_registry.h"
#include "expression/StoredReferenceOutput.h"
#include "interfaces/table_schema.h"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using namespace dbms;
using Role = StoredReferenceOutputRole;
using Kind = Role::Kind;
unsigned checked = 0;
void need(bool pass, const std::string& reason) {
    ++checked;
    if (!pass) throw std::runtime_error(reason);
}
Role role(Kind kind, const std::string& source = "physical_reference") {
    return {kind, kind == Kind::None ? "" : source};
}
void row(StoredReferenceOutputRenderer& renderer, std::vector<std::string> cells,
         const std::vector<bool>& nulls, const std::vector<Role>& roles,
         const std::vector<std::string>& expected, const std::string& label) {
    const auto originalNulls = nulls;
    const auto originalRoles = roles;
    renderer.render(cells, nulls, roles);
    need(cells == expected, label);
    need(nulls == originalNulls && roles == originalRoles, label + ": provenance and NULLs unchanged");
}
void rejected(StoredReferenceOutputRenderer& renderer, std::vector<std::string> cells,
              const std::vector<bool>& nulls, const std::vector<Role>& roles,
              const std::string& state, const std::string& label) {
    const auto originalCells = cells;
    const auto originalNulls = nulls;
    const auto originalRoles = roles;
    bool failed = false;
    try { renderer.render(cells, nulls, roles); }
    catch (const DbError& error) { failed = error.sqlState() == state; }
    need(failed, label + ": exact SQLSTATE " + state);
    need(cells == originalCells && nulls == originalNulls && roles == originalRoles,
         label + ": no partial output or provenance mutation");
}

// Copied pg_type/pg_class rows are inputs to the real output functions. This
// fixture neither creates catalog objects nor replaces name/visibility logic.
CatalogManager::MetadataSnapshot catalog() {
    CatalogManager::MetadataSnapshot value;
    value.types = CatalogManager::builtinTypeRows();
    value.namespaces = CatalogManager::builtinNamespaceRows();
    value.namespaces.push_back({20000, "review_schema", 10});
    PgTypeRow custom;
    custom.oid = 50000; custom.typname = "Mixed Type"; custom.typnamespace = 20000;
    custom.typowner = 10; custom.typtype = 'e'; custom.typcategory = 'E';
    custom.typlen = 4; custom.typbyval = true; custom.typarray = 50001;
    value.types.push_back(custom);
    PgTypeRow array;
    array.oid = 50001; array.typname = "_mixed_type"; array.typnamespace = 20000;
    array.typcategory = 'A'; array.typelem = 50000; array.typstorage = 'x';
    value.types.push_back(array);
    PgTypeRow quoted = custom;
    quoted.oid = 50002; quoted.typname = "type\"quote"; quoted.typarray = 0;
    value.types.push_back(quoted);
    PgClassRow relation;
    relation.oid = 50000; relation.relname = "Mixed Table"; relation.relnamespace = 20000;
    value.relations.push_back(relation);
    relation.oid = 23; relation.relname = "same_oid_table"; relation.relnamespace = 2200;
    value.relations.push_back(relation);
    relation.oid = 50002; relation.relname = "table\"quote"; relation.relnamespace = 20000;
    value.relations.push_back(relation);
    return value;
}

void physicalRoles() {
    Column column;
    column.dataName = "R"; column.dataType = "regtype";
    need(storedReferenceOutputRoleForColumn(column) == role(Kind::Regtype, "R"),
         "actual scalar REGTYPE declaration creates its physical role");
    column.isArray = true;
    need(storedReferenceOutputRoleForColumn(column) == role(Kind::RegtypeArray, "R"),
         "actual array REGTYPE declaration creates its physical role");
    column.dataType = "regclass";
    need(storedReferenceOutputRoleForColumn(column) == role(Kind::RegclassArray, "R"),
         "actual array REGCLASS declaration creates its physical role");
    column.isArray = false;
    need(storedReferenceOutputRoleForColumn(column) == role(Kind::Regclass, "R"),
         "actual scalar REGCLASS declaration creates its physical role");
    column.generatedKind = 'v';
    need(storedReferenceOutputRoleForColumn(column) == Role{},
         "virtual reference output is already presented, never stored OID text");
    column.generatedKind = 's';
    need(storedReferenceOutputRoleForColumn(column) == role(Kind::Regclass, "R"),
         "stored generated reference retains its physical role");
    column.dataType = "regtype"; column.domainName = "review_schema.reference_domain";
    need(storedReferenceOutputRoleForColumn(column) == role(Kind::Regtype, "R"),
         "resolved domain declaration uses its actual physical base role");
    column.generatedKind = 0; column.domainName.clear();
    for (const std::string type : {"text", "integer", "oid", "REGTYPE", "regtype[]",
                                   "review_schema.reference_domain", ""}) {
        column.dataType = type;
        need(storedReferenceOutputRoleForColumn(column) == Role{},
             "no inferred reference role from unresolved/other type: " + type);
    }
}
} // namespace

int main() {
    try {
        using namespace dbms;
        TypeRegistry::instance().bootstrap();
        physicalRoles();
        const auto snapshot = catalog();
        Session hidden;
        hidden.username = "testuser"; hidden.pid = 54321; hidden.searchPath = "public";
        Session visible;
        visible.username = "testuser"; visible.pid = 54322;
        visible.searchPath = "review_schema, pg_catalog, public";
        StoredReferenceOutputRenderer renderer(snapshot, &hidden);
        const auto rt = role(Kind::Regtype), rc = role(Kind::Regclass);
        const auto rta = role(Kind::RegtypeArray), rca = role(Kind::RegclassArray);
        const auto none = Role{};

        row(renderer, {"23", "25", "0", "4294967295", "2206", "1007"},
            {false, false, false, false, false, false}, {rt, rt, rt, rt, rt, rt},
            {"integer", "text", "-", "4294967295", "regtype", "integer[]"},
            "real reference identities include OID zero and unknown uint32 OID");
        row(renderer, {"23", "0", "4294967295"}, {false, false, false}, {rc, rc, rc},
            {"same_oid_table", "-", "4294967295"}, "REGCLASS output uses pg_class identities");
        row(renderer, {"23", "integer", "0", "{23,NULL,0}", "{integer,NULL,-}", "-", "not an OID"},
            {false, false, false, false, false, false, false},
            {none, none, none, none, none, none, none},
            {"23", "integer", "0", "{23,NULL,0}", "{integer,NULL,-}", "-", "not an OID"},
            "None preserves numeric text and already presented scalar/array spellings exactly");
        row(renderer, {"bad persisted scalar", "{malformed", "integer", "not an array"},
            {true, true, true, true}, {rt, rta, rc, rca},
            {"bad persisted scalar", "{malformed", "integer", "not an array"},
            "whole SQL NULL skips every reference decoder including malformed bytes");
        row(renderer, {"23", "not an array", "25", "23"},
            {false, true, false, false}, {rt, rta, rt, none},
            {"integer", "not an array", "text", "23"},
            "mixed raw, whole NULL, and presented roles remain independent");
        row(renderer, {}, {}, {}, {}, "empty row is valid");

        row(renderer, {"[0:2]={23,NULL,0}", "{}", "{NULL,NULL}", "{4294967295,25,23,23}"},
            {false, false, false, false}, {rta, rta, rta, rta},
            {"[0:2]={integer,NULL,-}", "{}", "{NULL,NULL}", "{4294967295,text,integer,integer}"},
            "reference arrays preserve empty, NULL, zero, lower bounds and repeated OIDs");
        row(renderer, {"[0:1][3:4]={{23,NULL},{0,4294967295}}",
                       "[-2:-1][0:1]={{23,NULL},{0,4294967295}}"},
            {false, false}, {rta, rca},
            {"[0:1][3:4]={{integer,NULL},{-,4294967295}}",
             "[-2:-1][0:1]={{same_oid_table,NULL},{-,4294967295}}"},
            "both codecs preserve multidimensional ordering and signed lower bounds");
        row(renderer, {"50000", "50001", "50000", "50002", "50002"},
            {false, false, false, false, false}, {rt, rt, rc, rt, rc},
            {"review_schema.\"Mixed Type\"", "review_schema.\"Mixed Type\"[]",
             "review_schema.\"Mixed Table\"", "review_schema.\"type\"\"quote\"",
             "review_schema.\"table\"\"quote\""},
            "hidden custom references use real namespace and identifier quoting");
        row(renderer, {"{50000,NULL,50002}", "{50000,NULL,50002}"},
            {false, false}, {rta, rca},
            {R"ARR({"review_schema.\"Mixed Type\"",NULL,"review_schema.\"type\"\"quote\""})ARR",
             R"ARR({"review_schema.\"Mixed Table\"",NULL,"review_schema.\"table\"\"quote\""})ARR"},
            "array output escapes the actual qualified quoted identifier once");

        StoredReferenceOutputRenderer visibleRenderer(snapshot, &visible);
        row(visibleRenderer, {"50000", "50001", "50000", "50002", "50002"},
            {false, false, false, false, false}, {rt, rt, rc, rt, rc},
            {"\"Mixed Type\"", "\"Mixed Type\"[]", "\"Mixed Table\"",
             "\"type\"\"quote\"", "\"table\"\"quote\""},
            "a new renderer uses its fixed visible search path without reusing other query names");
        row(renderer, {"50000", "50000", "50000", "{50000,50000}", "{50000,50000}"},
            {false, false, false, false, false}, {rt, rc, rt, rta, rca},
            {"review_schema.\"Mixed Type\"", "review_schema.\"Mixed Table\"",
             "review_schema.\"Mixed Type\"",
             R"ARR({"review_schema.\"Mixed Type\"","review_schema.\"Mixed Type\""})ARR",
             R"ARR({"review_schema.\"Mixed Table\"","review_schema.\"Mixed Table\""})ARR"},
            "query cache distinguishes reference codecs and shares scalar/array identities");

        for (const std::string raw : {"", "-1", "+23", "23 ", " 23", "4294967296", "integer", "-", "23x"})
            rejected(renderer, {"25", raw}, {false, false}, {rt, rt}, "XX001",
                     "malformed stored OID never becomes name input: " + raw);
        rejected(renderer, {"23", "integer"}, {false, false}, {rt, rt}, "XX001",
                 "an already displayed name cannot be decoded as an actual raw reference");
        rejected(renderer, {"23", "{23,bad}"}, {false, false}, {rt, rta}, "XX001",
                 "invalid later array leaf cannot publish a converted earlier field");
        rejected(renderer, {"23", "{23,\"NULL\"}"}, {false, false}, {rc, rca}, "XX001",
                 "quoted NULL is a real malformed OID rather than SQL NULL");
        rejected(renderer, {"23", "{23"}, {false, false}, {rt, rta}, "22P02",
                 "array syntax errors retain their decoder SQLSTATE and atomic output");
        rejected(renderer, {"23"}, {}, {rt}, "XX000", "short NULL bitmap rejected before conversion");
        rejected(renderer, {"23"}, {false}, {}, "XX000", "short role vector rejected before conversion");
        rejected(renderer, {"23"}, {false, true}, {rt}, "XX000", "long NULL bitmap rejected before conversion");
        rejected(renderer, {"23"}, {false}, {rt, none}, "XX000", "long role vector rejected before conversion");
        row(renderer, {"23", "{23,NULL,0}"}, {false, false}, {rt, rta},
            {"integer", "{integer,NULL,-}"}, "valid cached output remains usable after failed staging");

        auto renamed = snapshot;
        for (auto& type : renamed.types) if (type.oid == 50000) type.typname = "Renamed Type";
        for (auto& relation : renamed.relations) if (relation.oid == 50000) relation.relname = "Renamed Table";
        StoredReferenceOutputRenderer renamedRenderer(renamed, &hidden);
        row(renamedRenderer, {"50000", "50001", "50000"}, {false, false, false}, {rt, rt, rc},
            {"review_schema.\"Renamed Type\"", "review_schema.\"Renamed Type\"[]",
             "review_schema.\"Renamed Table\""}, "a new catalog generation never inherits old cached names");
        row(renderer, {"50000", "50000"}, {false, false}, {rt, rc},
            {"review_schema.\"Mixed Type\"", "review_schema.\"Mixed Table\""},
            "the original immutable catalog generation retains its own names");
        auto dropped = renamed;
        dropped.types.erase(std::remove_if(dropped.types.begin(), dropped.types.end(),
            [](const auto& type) { return type.oid == 50000 || type.oid == 50001; }), dropped.types.end());
        dropped.relations.erase(std::remove_if(dropped.relations.begin(), dropped.relations.end(),
            [](const auto& relation) { return relation.oid == 50000; }), dropped.relations.end());
        StoredReferenceOutputRenderer droppedRenderer(dropped, &hidden);
        row(droppedRenderer, {"50000", "50001", "50000", "{50000,NULL,0}"},
            {false, false, false, false}, {rt, rt, rc, rta},
            {"50000", "50001", "50000", "{50000,NULL,-}"},
            "new queries render dropped identities numerically instead of keeping stale names");

        auto broken = snapshot;
        broken.namespaces.erase(std::remove_if(broken.namespaces.begin(), broken.namespaces.end(),
            [](const auto& space) { return space.oid == 20000; }), broken.namespaces.end());
        StoredReferenceOutputRenderer brokenRenderer(broken, &hidden);
        rejected(brokenRenderer, {"23", "50000"}, {false, false}, {rt, rt}, "XX001",
                 "missing type namespace metadata cannot partially publish output");
        rejected(brokenRenderer, {"23", "50000"}, {false, false}, {rc, rc}, "XX001",
                 "missing relation namespace metadata cannot partially publish output");

        StoredReferenceOutputRenderer noSessionRenderer(snapshot, nullptr);
        row(noSessionRenderer, {"23", "50000", "50000"}, {false, false, false}, {rt, rt, rc},
            {"integer", "review_schema.\"Mixed Type\"", "review_schema.\"Mixed Table\""},
            "no-session output uses the existing default namespace semantics");
        const auto declaration = std::find_if(snapshot.types.begin(), snapshot.types.end(),
            [](const auto& type) { return type.oid == 2206; });
        need(declaration != snapshot.types.end() && declaration->typname == "regtype",
             "REGTYPE declaration OID 2206 stays distinct from the referred OID 23");
        need(hidden.searchPath == "public" && visible.searchPath == "review_schema, pg_catalog, public",
             "rendering never mutates caller namespace state");
        std::cout << "STORED_REFERENCE_OUTPUT_CHECKED=" << checked << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "STORED_REFERENCE_OUTPUT_CHECKED=" << checked << " FAIL=" << error.what() << '\n';
        return 1;
    }
}

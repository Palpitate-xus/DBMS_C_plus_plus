#include "commands/ColumnDefaultBinding.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>

using namespace dbms;
namespace {
unsigned checks = 0;
void need(bool value, const std::string& reason) {
    ++checks; if (!value) throw std::runtime_error(reason);
}
}
int main() {
    try {
        cleanupTestDb("column_default_binding");
        CatalogManager catalog(testDbPath("column_default_binding"), false);
        catalog.bootstrapSystemNamespaces(); catalog.bootstrapSystemTypes();
        PgClassRow target; target.oid = 16384; target.relname = "shape_target"; target.relnamespace = 2200;
        need(catalog.createClass(target) == target.oid, "actual referenced catalog relation");
        PgClassRow quoted = target; quoted.oid = 16385; quoted.relname = "Mixed ' Name";
        need(catalog.createClass(quoted) == quoted.oid, "actual quoted relation");
        Session session; session.searchPath = "public";
        const auto bind = [&](const std::string& sql, Oid type = 0) {
            return column_default::bind(sql, type, catalog.metadataSnapshot(), &session);
        };
        const auto explicitValue = bind(" 'shape_target'::regclass ", 2205);
        need(explicitValue.expression == " CAST('16384' AS pg_catalog.regclass) " &&
             explicitValue.references == std::vector<std::pair<Oid, Oid>>{{PgClassOid_Class, 16384}},
             "explicit object input binds actual identity and preserves surrounding SQL bytes");
        need(bind("'shape_target'", 2205).expression == "CAST('16384' AS pg_catalog.regclass)",
             "assignment of unknown literal binds the declared reference type");
        need(bind("$$shape_target$$::regclass").references == explicitValue.references &&
             bind("E'shape_target'::regclass").references == explicitValue.references,
             "dollar and escape literal source spans bind the actual parsed token");
        need(bind("'\"Mixed '' Name\"'::regclass").expression == "CAST('16385' AS pg_catalog.regclass)",
             "quoted object identifier and doubled SQL quote retain identity");
        const std::string late = "('shape_target'::text)::regclass";
        need(bind(late, 2205).expression == late && bind(late, 2205).references.empty(),
             "runtime TEXT conversion remains late bound");
        const auto literalArray = bind("'{shape_target,NULL,0}'", 2210);
        need(literalArray.expression == "CAST('{16384,NULL,0}' AS pg_catalog.regclass[])" && literalArray.references.empty(),
             "array Const has bound elements but no scalar object dependency");
        const auto constructor = bind("ARRAY['shape_target'::regclass,NULL::regclass]", 2210);
        need(constructor.references == explicitValue.references && constructor.expression.find("'16384'") != std::string::npos,
             "scalar constants inside ARRAY retain their genuine dependencies");
        const std::string lateArray = "ARRAY['shape_target']";
        need(bind(lateArray, 2210).expression == lateArray && bind(lateArray, 2210).references.empty(),
             "ARRAY of unknown strings is not changed into an early-bound array Const");
        need(bind("'0'::regclass").references.empty() && bind("'4294967295'::regclass").references.empty(),
             "numeric constants with no actual target have no object dependency");
        need(bind("'integer'::regtype").expression == "CAST('23' AS pg_catalog.regtype)",
             "REGTYPE input uses its actual catalog identity");
        target.relname = "renamed";
        need(catalog.updateClass(target.oid, target), "rename actual relation");
        PgClassRow replacement = target; replacement.oid = 16386; replacement.relname = "shape_target";
        need(catalog.createClass(replacement) == replacement.oid &&
             bind("'shape_target'::regclass").expression == "CAST('16386' AS pg_catalog.regclass)" &&
             explicitValue.expression == " CAST('16384' AS pg_catalog.regclass) ",
             "stored bound SQL retains the original object across rename and same-name replacement");
        bool missing = false;
        try { (void)bind("'no_such_default_relation'::regclass"); }
        catch (const DbError& error) { missing = error.sqlState() == "42P01"; }
        need(missing, "missing object rejected during binding with actual SQLSTATE");
        unsigned resolved = 0;
        const auto sequence = column_default::bind("nextval('shape_target'::regclass)", 23,
            catalog.metadataSnapshot(), &session, [&](const FunctionCallExpr*) { ++resolved; return true; });
        need(resolved == 1 && sequence.expression == "pg_catalog.nextval(CAST('16386' AS pg_catalog.regclass))" &&
             sequence.references == std::vector<std::pair<Oid, Oid>>{{PgClassOid_Class, 16386}},
             "resolved builtin call composes its nested bound argument without evaluation");
        const std::string shadow = "nextval('shape_target')";
        need(column_default::bind(shadow, 23, catalog.metadataSnapshot(), &session,
             [](const FunctionCallExpr*) { return false; }).expression == shadow,
             "metadata callback can preserve a shadowing stored routine");
        const auto lateSequence = column_default::bind("nextval('shape_target'::text)", 20,
            catalog.metadataSnapshot(), &session, [](const FunctionCallExpr*) { return true; });
        need(lateSequence.expression == "pg_catalog.nextval(CAST('shape_target'::text AS pg_catalog.regclass))" &&
             lateSequence.references.empty(), "runtime TEXT argument receives the actual builtin REGCLASS signature");
        need(bind("NULL", 23).nullConstant && bind("NULL::regclass", 2205).nullConstant &&
             !bind("'NULL'", 25).nullConstant, "only a proven SQL NULL is marked for default omission");

        // These complete node strings were captured from PostgreSQL 18.6,
        // not computed by this implementation (raw protocol evidence is in
        // identity-object-default-constants-wire-PG-v1).
        using pg_object_constant::node;
        need(node({2205, "0"}) == "{CONST :consttype 2205 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}", "PG zero Datum encoding");
        need(node({2205, "4294967295"}) == "{CONST :consttype 2205 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 [ -1 -1 -1 -1 0 0 0 0 ]}", "PG unsigned OID Datum encoding");
        need(node({2205, std::nullopt}) == "{CONST :consttype 2205 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull true :location -1 :constvalue <>}", "PG NULL node encoding");
        need(node({2210, "{16384,NULL,0}"}) == "{CONST :consttype 2210 :consttypmod -1 :constcollid 0 :constlen -1 :constbyval false :constisnull false :location -1 :constvalue 40 [ -96 0 0 0 1 0 0 0 32 0 0 0 -99 8 0 0 3 0 0 0 1 0 0 0 5 0 0 0 0 0 0 0 0 64 0 0 0 0 0 0 ]}", "PG array bitmap and native varlena encoding");
        need(node({2210, "[0:1][-2:-1]={{16384,NULL},{0,4294967295}}"}) == "{CONST :consttype 2210 :consttypmod -1 :constcollid 0 :constlen -1 :constbyval false :constisnull false :location -1 :constvalue 52 [ -48 0 0 0 2 0 0 0 40 0 0 0 -99 8 0 0 2 0 0 0 2 0 0 0 0 0 0 0 -2 -1 -1 -1 13 0 0 0 0 0 0 0 0 64 0 0 0 0 0 0 -1 -1 -1 -1 ]}", "PG multidimensional lower bounds and non-NULL packing");
        need(node({2210, "{}"}) == "{CONST :consttype 2210 :consttypmod -1 :constcollid 0 :constlen -1 :constbyval false :constisnull false :location -1 :constvalue 16 [ 64 0 0 0 0 0 0 0 0 0 0 0 -99 8 0 0 ]}", "PG empty array has zero dimensions");
        need(node({2211, "{23,NULL}"}) == "{CONST :consttype 2211 :consttypmod -1 :constcollid 0 :constlen -1 :constbyval false :constisnull false :location -1 :constvalue 36 [ -112 0 0 0 1 0 0 0 32 0 0 0 -98 8 0 0 2 0 0 0 1 0 0 0 1 0 0 0 0 0 0 0 23 0 0 0 ]}", "PG REGTYPE array uses actual element identity");
        std::cout << "COLUMN_DEFAULT_BINDING_CHECKED=" << checks << " FAILED=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "COLUMN_DEFAULT_BINDING_CHECKED=" << checks << " FAIL=" << error.what() << '\n';
        return 1;
    }
}

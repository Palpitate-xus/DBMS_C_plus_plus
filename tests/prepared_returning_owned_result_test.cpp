#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
using Row = std::vector<ExprValue>;
using Cell = std::optional<std::string>;

struct Outcome {
    DmlResult result;
    std::string state;
    bool handled = false;
    bool failed = false;
    bool restored = false;
};

const ExprValue* cell(const DmlResult& result, size_t row = 0, size_t column = 0) {
    if (!result.typedRows || row >= result.typedRows->size() ||
        column >= result.typedRows->at(row).size()) return nullptr;
    return &result.typedRows->at(row).at(column);
}

const ExprValue* field(const ExprValue* value, size_t index) {
    return value && value->recordFields && index < value->recordFields->size()
        ? &value->recordFields->at(index) : nullptr;
}

bool scalar(const ExprValue* value, const std::string& type, const Cell& expected) {
    return value && value->typeName == type && value->isNull == !expected &&
        (!expected || value->value == *expected);
}

bool reference(const ExprValue* value, uint32_t oid) {
    return value && value->typeName == "regtype" && !value->isNull && value->objectOid == oid;
}

bool record(const ExprValue* value, const std::vector<uint32_t>& fields) {
    if (!value || value->isNull || value->typeName != "record" ||
        !value->recordFields || value->recordFields->size() != fields.size() ||
        !value->compound || !value->compound->type) return false;
    const auto& metadata = *value->compound->type;
    if (metadata.actualOid != 2249 || metadata.codecOid != 2249 ||
        metadata.kind != DatumKind::Record || metadata.recordFields.size() != fields.size()) return false;
    for (size_t i = 0; i < fields.size(); ++i)
        if (metadata.recordFields[i].actualOid != fields[i]) return false;
    return true;
}

bool array(const ExprValue* value, const std::string& type,
           const std::vector<Cell>& expected, int32_t lower = 1,
           std::optional<bool> bitmap = {}) {
    if (!value || value->isNull || value->typeName != type || !value->compound ||
        !value->compound->arrayElements || value->compound->arrayElements->size() != expected.size()) return false;
    const auto& data = *value->compound;
    if (expected.empty()) {
        if (!data.dimensions.empty()) return false;
    } else if (data.dimensions.size() != 1 || data.dimensions[0].lower != lower ||
               data.dimensions[0].length != static_cast<int32_t>(expected.size())) return false;
    if (bitmap && data.arrayNullBitmap != bitmap) return false;
    for (size_t i = 0; i < expected.size(); ++i) {
        const auto& actual = data.arrayElements->at(i);
        if (actual.isNull != !expected[i] || (expected[i] && actual.value != *expected[i])) return false;
    }
    return true;
}

bool referenceArray(const ExprValue* value) {
    if (!value || !array(value, "regtype[]", {"integer", {}}, 1, true)) return false;
    const auto& elements = *value->compound->arrayElements;
    return reference(&elements[0], 23) && elements[1].isNull && !elements[1].objectOid;
}

bool captured(const Outcome& output, const std::string& command, size_t rows, size_t width) {
    const auto& result = output.result;
    if (!output.handled || output.failed || !output.state.empty() || !output.restored ||
        !result.available || result.metadataOnly || result.commandTag != command ||
        !result.typedRows || !result.typedRowDescriptor || result.rows.size() != rows ||
        result.nulls.size() != rows || result.typedRows->size() != rows ||
        result.typedRowDescriptor->size() != width) return false;
    for (size_t r = 0; r < rows; ++r) {
        if (result.rows[r].size() != width || result.nulls[r].size() != width ||
            result.typedRows->at(r).size() != width) return false;
        for (size_t c = 0; c < width; ++c)
            if (result.nulls[r][c] != result.typedRows->at(r)[c].isNull) return false;
    }
    return true;
}

bool declared(const DmlResult& result, size_t column, uint32_t oid, DatumKind kind) {
    if (!result.typedRowDescriptor || column >= result.typedRowDescriptor->size()) return false;
    const auto& meta = result.typedRowDescriptor->at(column).typeMetadata;
    return meta && meta->actualOid == oid && meta->codecOid == oid && meta->kind == kind;
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "prepared_returning_owned_result", database = testDbPath(name);
    cleanupTestDb(name);
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK)
        throw std::runtime_error("cannot create actual test database");
    Session session;
    session.username = "admin"; session.permission = 1; session.currentDB = database;
    DdlExecutor ddl;
    const auto create = [&](const std::string& sql) {
        if (ddl.executeSql(sql, session)) throw std::runtime_error("fixture DDL failed: " + sql);
    };
    create("CREATE TABLE own_rows(id integer PRIMARY KEY,t text,a integer[],r regtype,ra regtype[])");
    create("CREATE TABLE own_generated(id integer,ga integer[] GENERATED ALWAYS AS(ARRAY[id,NULL]) STORED,"
           "v integer GENERATED ALWAYS AS(id+1) VIRTUAL)");
    create("CREATE SEQUENCE own_effects START 1");

    // The public bridge builds its genuine prepared mutation/RETURNING owner.
    // No callback, source row, statement frame, AST, or RID is manufactured.
    const auto run = [&](const std::string& sql, bool capture) {
        Outcome output;
        const bool previous = typedResultCaptureRequested();
        const bool previousExplicit = typedResultCaptureExplicitReceiver();
        auto* previousSession = currentSession();
        clearLastDmlResult();
        {
            ScopedTypedResultCapture request(capture);
            try {
                output.failed = tryDmlBridge(sql, SQLParser::classify(sql), session, output.handled, sql);
            } catch (const DbError& error) {
                output.state = error.sqlState(); output.handled = true;
            }
            output.result = takeLastDmlResult();
        }
        output.restored = typedResultCaptureRequested() == previous &&
            typedResultCaptureExplicitReceiver() == previousExplicit && currentSession() == previousSession;
        return output;
    };
    // Sequence builtins consume the same genuine Session as the mutation.
    // Reading outside that Session would inspect a different native fallback
    // currval map even when the actual DML evaluated each site correctly.
    const auto sequence = [&](bool reset) {
        struct Restore {
            Session* previous;
            ~Restore() { setCurrentSession(previous); }
        } restore{currentSession()};
        setCurrentSession(&session);
        return reset ? g_engine.setval(database,"own_effects",1)
                     : g_engine.currval(database,"own_effects");
    };
    const auto reset = [&] {
        if (g_engine.truncateTable(database, "own_rows") != DBStatus::OK ||
            g_engine.truncateTable(database, "own_generated") != DBStatus::OK)
            throw std::runtime_error("cannot reset actual physical table");
        const auto rows = run("INSERT INTO own_rows VALUES"
            "(1,'','[0:2]={1,NULL,3}'::integer[],'boolean'::regtype,ARRAY['integer'::regtype,NULL]),"
            "(2,'NULL',NULL,NULL,NULL),(3,'雪','{}'::integer[],'0'::regtype,'{}'::regtype[]),"
            "(4,'tail','[0:1]={4,5}'::integer[],'bigint'::regtype,ARRAY['bigint'::regtype])", false);
        const auto generated = run("INSERT INTO own_generated(id) VALUES(5)", false);
        if (!rows.handled || rows.failed || !rows.state.empty() || !generated.handled ||
            generated.failed || !generated.state.empty()) throw std::runtime_error("cannot seed actual rows");
        if (sequence(true) != 1)
            throw std::runtime_error("cannot reset actual sequence");
    };
    size_t controls = 0;
    std::vector<std::string> failures;
    const auto check = [&](const std::string& label, bool good) {
        ++controls;
        std::cout << "RETURNING_OWNED_CONTROL " << controls << ' ' << label << ' ' << (good ? "PASS" : "FAIL") << '\n';
        if (!good) failures.push_back(label);
    };

    reset();
    auto output = run("UPDATE own_rows SET t=t WHERE id=1 RETURNING ROW(id,t,a)", false);
    check("default-off keeps optional owning result absent", output.handled && !output.failed && output.state.empty() &&
        output.restored && output.result.available && output.result.rows.size() == 1 &&
        !output.result.typedRows && !output.result.typedRowDescriptor);

    output = run("UPDATE own_rows SET t=t WHERE id=1 RETURNING ROW(id,t,a,r,ra)", true);
    auto* value = cell(output.result);
    check("actual stored record preserves declarations, bounds, NULL and reference identities",
        captured(output, "UPDATE 1", 1, 1) && declared(output.result, 0, 2249, DatumKind::Record) &&
        record(value, {23,25,1007,2206,2211}) && scalar(field(value,0), "integer", "1") &&
        scalar(field(value,1), "text", "") && array(field(value,2), "integer[]", {"1",{},"3"}, 0, true) &&
        reference(field(value,3),16) && referenceArray(field(value,4)));

    output = run("UPDATE own_rows SET t=t WHERE id=2 RETURNING ROW(a,r,ra)", true);
    value = cell(output.result);
    check("NULL fields retain actual array and reference declarations",
        captured(output,"UPDATE 1",1,1) && record(value,{1007,2206,2211}) &&
        scalar(field(value,0),"integer[]",{}) && scalar(field(value,1),"regtype",{}) &&
        scalar(field(value,2),"regtype[]",{}));

    output = run("UPDATE own_rows SET t=t WHERE id=3 RETURNING ROW(a,ra)",true);
    value = cell(output.result);
    check("genuine empty arrays own empty payload and zero dimensions",
        captured(output,"UPDATE 1",1,1) && record(value,{1007,2211}) &&
        array(field(value,0),"integer[]",{},1,false) && array(field(value,1),"regtype[]",{},1,false));

    output = run("UPDATE own_rows SET t=t WHERE id=4 RETURNING ROW(a)",true);
    check("NULL-free stored array retains nondefault lower bound without inventing bitmap history",
        captured(output,"UPDATE 1",1,1) && record(cell(output.result),{1007}) &&
        array(field(cell(output.result),0),"integer[]",{"4","5"},0));

    output = run("UPDATE own_rows SET t=t WHERE id=1 RETURNING ARRAY[ROW(id,t),ROW(id+10,NULL::text)]",true);
    value = cell(output.result);
    const auto* elements = value && value->compound && value->compound->arrayElements
        ? &*value->compound->arrayElements : nullptr;
    check("record array retains per-record fields including actual NULL declaration",
        captured(output,"UPDATE 1",1,1) && declared(output.result,0,2287,DatumKind::Array) &&
        value && !value->isNull && elements && elements->size()==2 &&
        record(&elements->at(0),{23,25}) && scalar(field(&elements->at(0),0),"integer","1") &&
        scalar(field(&elements->at(0),1),"text","") && record(&elements->at(1),{23,25}) &&
        scalar(field(&elements->at(1),0),"integer","11") && scalar(field(&elements->at(1),1),"text",{}));

    output = run("INSERT INTO own_rows(id,t,a) VALUES(9,'new',ARRAY[9,NULL]) RETURNING WITH(OLD AS o,NEW AS n) ROW(o.id,o.a,n.id,n.a)",true);
    value = cell(output.result);
    check("INSERT actual OLD NULL image and NEW stored array",
        captured(output,"INSERT 0 1",1,1) && record(value,{23,1007,23,1007}) &&
        scalar(field(value,0),"integer",{}) && scalar(field(value,1),"integer[]",{}) &&
        scalar(field(value,2),"integer","9") && array(field(value,3),"integer[]",{"9",{}},1,true));

    output = run("UPDATE own_rows SET a=ARRAY[id+10,NULL] WHERE id=1 RETURNING WITH(OLD AS o,NEW AS n) ROW(a,o.a,n.a)",true);
    value = cell(output.result);
    check("UPDATE target, OLD and NEW keep distinct genuine owning arrays",
        captured(output,"UPDATE 1",1,1) && record(value,{1007,1007,1007}) &&
        array(field(value,0),"integer[]",{"11",{}},1,true) &&
        array(field(value,1),"integer[]",{"1",{},"3"},0,true) &&
        array(field(value,2),"integer[]",{"11",{}},1,true));

    reset();
    output = run("DELETE FROM own_rows WHERE id=1 RETURNING WITH(OLD AS o,NEW AS n) ROW(o.id,o.a,n.id,n.a)",true);
    value = cell(output.result);
    check("DELETE public bridge materializes real OLD physical array and NULL NEW",
        captured(output,"DELETE 1",1,1) && record(value,{23,1007,23,1007}) &&
        scalar(field(value,0),"integer","1") && array(field(value,1),"integer[]",{"1",{},"3"},0,true) &&
        scalar(field(value,2),"integer",{}) && scalar(field(value,3),"integer[]",{}) &&
        g_engine.query(database,"own_rows",{}, {"id"}).size()==3);

    reset();
    DmlResult detached;
    std::weak_ptr<PreparedQuery> formerOwner;
    {
        auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,
            "DELETE FROM own_rows WHERE id=1 RETURNING ROW(old.a,old.r,old.ra)"));
        formerOwner = query;
        ScopedTypedResultCapture request(true);
        prepareBoundDml(query->ast.get(),session,query,{}, {},true);
        detached = executeAtomicDmlUnit(session,[&] {
            // This statement has no source/subquery child, so no child reader
            // exists. The real query's own bound source is consumed directly.
            return executeBoundDml(query->ast.get(),session,query,{}, {}, {},true);
        });
    }
    value = cell(detached);
    check("published record survives destruction of its real PreparedQuery and execution owner",
        formerOwner.expired() && detached.available && detached.commandTag=="DELETE 1" &&
        record(value,{1007,2206,2211}) && array(field(value,0),"integer[]",{"1",{},"3"},0,true) &&
        reference(field(value,1),16) && referenceArray(field(value,2)) && !g_engine.inTransaction());

    output = run("UPDATE own_generated SET id=id WHERE id=5 RETURNING ROW(ga,v)",true);
    value = cell(output.result);
    check("stored generated array and virtual scalar retain the actual returned values",
        captured(output,"UPDATE 1",1,1) && record(value,{1007,23}) &&
        array(field(value,0),"integer[]",{"5",{}},1,true) && scalar(field(value,1),"integer","6"));

    output = run("UPDATE own_rows SET t=t WHERE false RETURNING ROW(nextval('own_effects'),a)",true);
    check("zero UPDATE owns declaration without evaluating RETURNING",
        captured(output,"UPDATE 0",0,1) && declared(output.result,0,2249,DatumKind::Record) &&
        sequence(false)==1);

    output = run("UPDATE own_rows SET t=t WHERE id=2 RETURNING ROW(nextval('own_effects'),nextval('own_effects'))",true);
    value = cell(output.result);
    check("each original volatile RETURNING site runs once",
        captured(output,"UPDATE 1",1,1) && record(value,{20,20}) &&
        scalar(field(value,0),"bigint","2") && scalar(field(value,1),"bigint","3") &&
        sequence(false)==3);

    reset();
    output = run("UPDATE own_rows SET t='changed' WHERE id<=2 RETURNING ROW(nextval('own_effects'),10/(id-1),a)",true);
    const auto stored = g_engine.query(database,"own_rows",{}, {"t"},{{"id",true}});
    check("primary projection error publishes no partial owning result and rolls back heap writes",
        output.handled && !output.failed && output.state=="22012" && output.restored &&
        !output.result.available && !output.result.typedRows && !g_engine.inTransaction() &&
        sequence(false)==2 && stored==std::vector<std::string>({" ","NULL ","雪 ","tail "}));

    Outcome outerAfter, innerOff;
    bool nestedRestored = false;
    {
        ScopedTypedResultCapture outer(true,true);
        innerOff = run("UPDATE own_rows SET t=t WHERE id=1 RETURNING ROW(a)",false);
        outerAfter = run("UPDATE own_rows SET t=t WHERE id=1 RETURNING ROW(a)",true);
        nestedRestored = typedResultCaptureRequested() && typedResultCaptureExplicitReceiver();
    }
    check("nested receiver scopes restore actual request while default-off execution stays off",
        innerOff.restored && innerOff.result.available && !innerOff.result.typedRows &&
        captured(outerAfter,"UPDATE 1",1,1) && nestedRestored &&
        !typedResultCaptureRequested() && !typedResultCaptureExplicitReceiver());

    output = run("UPDATE own_rows SET t=t WHERE id=3 RETURNING ROW(t,NULL::text,'NULL'::text,''::text)",true);
    value = cell(output.result);
    check("UTF8, literal NULL, SQL NULL and empty text stay distinct owning fields",
        captured(output,"UPDATE 1",1,1) && record(value,{25,25,25,25}) &&
        scalar(field(value,0),"text","雪") && scalar(field(value,1),"text",{}) &&
        scalar(field(value,2),"text","NULL") && scalar(field(value,3),"text",""));

    output = run("UPDATE own_rows SET t=t WHERE id=1 RETURNING *",true);
    check("original star slots retain physical array and object-reference values",
        captured(output,"UPDATE 1",1,5) && array(cell(output.result,0,2),"integer[]",{"1",{},"3"},0,true) &&
        reference(cell(output.result,0,3),16) && referenceArray(cell(output.result,0,4)));

    output = run("UPDATE own_rows SET t=t WHERE id<=3 RETURNING ROW(id,a)",true);
    bool independent = captured(output,"UPDATE 3",3,1);
    std::map<std::string,const ExprValue*> perId;
    if (independent) for (size_t row=0;row<3;++row) {
        const auto* datum = cell(output.result,row);
        const auto* id = field(datum,0);
        if (!record(datum,{23,1007}) || !id || id->isNull || !perId.emplace(id->value,datum).second)
            independent=false;
    }
    independent = independent && perId.size()==3 && perId.count("1") && perId.count("2") && perId.count("3") &&
        array(field(perId["1"],1),"integer[]",{"1",{},"3"},0,true) &&
        scalar(field(perId["2"],1),"integer[]",{}) && array(field(perId["3"],1),"integer[]",{},1,false);
    check("multiple physical rows retain independent record/array payload after input advancement",independent);

    output = run("DELETE FROM own_rows WHERE false RETURNING ROW(a,r,ra)",true);
    check("zero DELETE retains pure output declaration and no rows",
        captured(output,"DELETE 0",0,1) && declared(output.result,0,2249,DatumKind::Record) &&
        g_engine.query(database,"own_rows",{}, {"id"}).size()==4);

    if (g_engine.dropDatabase(database)!=DBStatus::OK) throw std::runtime_error("cannot drop actual test database");
    value = cell(detached);
    check("owning output remains readable after physical relation/database destruction",
        record(value,{1007,2206,2211}) && array(field(value,0),"integer[]",{"1",{},"3"},0,true) &&
        reference(field(value,1),16) && referenceArray(field(value,2)));
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "RETURNING_OWNED_RESULT_TERMINAL controls=" << controls
              << " expected=20 failures=" << failures.size() << '\n';
    if (controls!=20 || !failures.empty()) return 1;
    return 0;
}

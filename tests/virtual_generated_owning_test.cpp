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
    const std::string name="virtual_generated_owning",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)throw std::runtime_error("create actual database failed");
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    const auto create=[&](const std::string& sql){if(ddl.executeSql(sql,session))throw std::runtime_error("DDL fixture failed: "+sql);};
    create("CREATE TABLE vg(id integer DEFAULT 1, v integer GENERATED ALWAYS AS(id+1) VIRTUAL,"
        "a integer[] GENERATED ALWAYS AS(ARRAY[id,NULL]) VIRTUAL,"
        "n integer[] GENERATED ALWAYS AS(CASE WHEN id<0 THEN NULL::integer[] ELSE ARRAY[id,NULL] END) VIRTUAL,"
        "e integer[] GENERATED ALWAYS AS('{}'::integer[]) VIRTUAL,"
        "l integer[] GENERATED ALWAYS AS('[0:1]={9,NULL}'::integer[]) VIRTUAL,"
        "s integer[] GENERATED ALWAYS AS((ARRAY[NULL,id,id+1])[2:3]) VIRTUAL,"
        "b bigint[] GENERATED ALWAYS AS(ARRAY[id::bigint,NULL::bigint]) VIRTUAL,"
        "different bigint[] GENERATED ALWAYS AS(ARRAY[id,NULL]) VIRTUAL,"
        "wide bigint GENERATED ALWAYS AS(id) VIRTUAL,"
        "narrow smallint GENERATED ALWAYS AS(id) VIRTUAL)");
    create("CREATE TABLE vg_source(id integer)");
    create("CREATE SEQUENCE vg_source_calls START 1");
    create("CREATE SEQUENCE vg_output_calls START 1");
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

    std::size_t controls=0;std::vector<std::string> failures;
    const auto check=[&](const std::string& label,bool ok){++controls;std::cout<<"VIRTUAL_OWNING_CONTROL "<<controls<<" "<<label<<" "<<(ok?"PASS":"FAIL")<<"\n";if(!ok)failures.push_back(label);};
    const auto success=[&](const Outcome& o){return o.handled&&!o.failed&&o.state.empty()&&o.restored;};
    const auto seed=[&](int id){if(g_engine.truncateTable(database,"vg")!=DBStatus::OK)throw std::runtime_error("truncate fixture failed");auto o=run("INSERT INTO vg(id) VALUES("+std::to_string(id)+")",false);if(!success(o))throw std::runtime_error("actual seed failed");};
    auto o=run("INSERT INTO vg(id) VALUES(1) RETURNING ROW(id,a)",false);
    check("default off retains no optional owned rows",success(o)&&o.result.available&&!o.result.typedRows&&!o.result.typedRowDescriptor);
    seed(1);o=run("INSERT INTO vg(id) VALUES(2) RETURNING ROW(id,a)",true);
    check("INSERT exact owned array and real record metadata",captured(o,"INSERT 0 1",1,1)&&record(cell(o.result),{23,1007})&&scalar(field(cell(o.result),0),"integer","2")&&array(field(cell(o.result),1),"integer[]",{"2",{}},1,true));
    o=run("INSERT INTO vg(id) VALUES(3) RETURNING ROW(OLD.id,OLD.a,NEW.id,NEW.a)",true);
    check("INSERT OLD whole NULL and actual NEW",captured(o,"INSERT 0 1",1,1)&&record(cell(o.result),{23,1007,23,1007})&&scalar(field(cell(o.result),0),"integer",{})&&scalar(field(cell(o.result),1),"integer[]",{})&&array(field(cell(o.result),3),"integer[]",{"3",{}},1,true));
    seed(1);o=run("UPDATE vg SET id=2 RETURNING ROW(OLD.id,OLD.a,NEW.id,NEW.a)",true);
    check("UPDATE keeps two actual virtual row versions",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{23,1007,23,1007})&&array(field(cell(o.result),1),"integer[]",{"1",{}},1,true)&&array(field(cell(o.result),3),"integer[]",{"2",{}},1,true));
    seed(2);o=run("DELETE FROM vg RETURNING ROW(id,a)",true);
    check("DELETE default is actual OLD owned array",captured(o,"DELETE 1",1,1)&&array(field(cell(o.result),1),"integer[]",{"2",{}},1,true));
    seed(2);o=run("DELETE FROM vg RETURNING ROW(OLD.id,OLD.a,NEW.id,NEW.a)",true);
    check("DELETE NEW remains whole typed NULL",captured(o,"DELETE 1",1,1)&&array(field(cell(o.result),1),"integer[]",{"2",{}},1,true)&&scalar(field(cell(o.result),2),"integer",{})&&scalar(field(cell(o.result),3),"integer[]",{}));
    seed(9);o=run("INSERT INTO vg DEFAULT VALUES RETURNING ROW(id,a)",true);
    check("DEFAULT VALUES captures at original input boundary",captured(o,"INSERT 0 1",1,1)&&array(field(cell(o.result),1),"integer[]",{"1",{}},1,true));
    seed(4);o=run("UPDATE vg SET id=id RETURNING ROW(id,v)",true);
    check("same-type virtual scalar preserved",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{23,23})&&scalar(field(cell(o.result),1),"integer","5"));
    seed(-1);o=run("UPDATE vg SET id=id RETURNING ROW(id,n)",true);
    check("virtual array SQL NULL stays distinct from empty",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{23,1007})&&scalar(field(cell(o.result),1),"integer[]",{}));
    seed(4);o=run("UPDATE vg SET id=id RETURNING ROW(e)",true);
    check("genuine empty virtual array has zero dimensions",captured(o,"UPDATE 1",1,1)&&array(field(cell(o.result),0),"integer[]",{},1,false));
    o=run("UPDATE vg SET id=id RETURNING ROW(l)",true);
    check("nondefault lower bound survives original virtual value",captured(o,"UPDATE 1",1,1)&&array(field(cell(o.result),0),"integer[]",{"9",{}},0,true));
    o=run("UPDATE vg SET id=id RETURNING ROW(s)",true);
    check("NULL-free virtual slice retains actual bitmap",captured(o,"UPDATE 1",1,1)&&array(field(cell(o.result),0),"integer[]",{"4","5"},1,true));
    o=run("UPDATE vg SET id=id RETURNING ROW(ROW(id,a),a)",true);
    check("nested record uses true owned children",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{2249,1007})&&record(field(cell(o.result),0),{23,1007})&&array(field(field(cell(o.result),0),1),"integer[]",{"4",{}},1,true));
    o=run("UPDATE vg SET id=id RETURNING ARRAY[ROW(id,a),ROW(id+1,a)]",true);
    const auto* rows=cell(o.result);
    check("record array keeps each real body declaration",captured(o,"UPDATE 1",1,1)&&rows&&rows->compound&&rows->compound->arrayElements&&rows->compound->arrayElements->size()==2&&record(&rows->compound->arrayElements->at(0),{23,1007})&&array(field(&rows->compound->arrayElements->at(1),1),"integer[]",{"4",{}},1,true));
    seed(40000);o=run("UPDATE vg SET id=id RETURNING ROW(id)",true);
    check("unused narrowing declaration causes no XX000 or premature overflow",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{23})&&scalar(field(cell(o.result),0),"integer","40000"));
    o=run("UPDATE vg SET id=id RETURNING ROW(CASE WHEN false THEN narrow ELSE id END)",true);
    check("unselected narrow source does not change CASE demand",captured(o,"UPDATE 1",1,1)&&scalar(field(cell(o.result),0),"integer","40000"));
    seed(-1);o=run("UPDATE vg SET id=id RETURNING ROW(n)",true);
    check("NULL value enters source only with its real canonical type",captured(o,"UPDATE 1",1,1)&&scalar(field(cell(o.result),0),"integer[]",{}));
    seed(4);o=run("UPDATE vg SET id=id RETURNING ROW(wide)",true);
    check("different scalar type retains existing genuine legacy slot",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{20})&&scalar(field(cell(o.result),0),"bigint","4"));
    o=run("UPDATE vg SET id=id RETURNING ROW(b)",true);
    check("same-type bigint array preserves owned data",captured(o,"UPDATE 1",1,1)&&record(cell(o.result),{1016})&&array(field(cell(o.result),0),"bigint[]",{"4",{}},1,true));
    o=run("UPDATE vg SET id=id RETURNING ROW(CASE WHEN false THEN different ELSE b END)",true);
    check("unselected different array source cannot introduce XX000",captured(o,"UPDATE 1",1,1)&&array(field(cell(o.result),0),"bigint[]",{"4",{}},1,true));
    seed(1);run("INSERT INTO vg_source VALUES(1)",false);
    o=run("MERGE INTO vg AS t USING vg_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET id=t.id+1 RETURNING ROW(a)",true);
    check("MERGE UPDATE keeps original action owned row",captured(o,"MERGE 1",1,1)&&array(field(cell(o.result),0),"integer[]",{"2",{}},1,true));
    o=run("MERGE INTO vg AS t USING vg_source AS s ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id) VALUES(s.id) RETURNING ROW(a)",true);
    check("MERGE INSERT transports actual NEW owned row",captured(o,"MERGE 1",1,1)&&array(field(cell(o.result),0),"integer[]",{"1",{}},1,true));
    o=run("MERGE INTO vg AS t USING vg_source AS s ON t.id=s.id WHEN MATCHED THEN DELETE RETURNING ROW(a)",true);
    check("MERGE DELETE transports actual OLD owned row",captured(o,"MERGE 1",1,1)&&array(field(cell(o.result),0),"integer[]",{"1",{}},1,true));
    seed(5);o=run("UPDATE vg SET id=id WHERE false RETURNING ROW(a)",true);
    check("zero UPDATE retains pure declaration no values",captured(o,"UPDATE 0",0,1)&&declared(o.result,0,2249,DatumKind::Record));
    o=run("DELETE FROM vg WHERE false RETURNING ROW(a)",true);
    check("zero DELETE has no hidden owner or output values",captured(o,"DELETE 0",0,1)&&declared(o.result,0,2249,DatumKind::Record));
    o=run("UPDATE vg SET id=id+1 RETURNING ROW(a,1/(id-6))",true);
    auto after=run("UPDATE vg SET id=id RETURNING ROW(id)",true);
    check("RETURNING error preserves state and atomic rollback",o.handled&&o.state=="22012"&&o.restored&&!o.result.available&&captured(after,"UPDATE 1",1,1)&&scalar(field(cell(after.result),0),"integer","5"));
    o=run("UPDATE vg SET id=id+nextval('vg_source_calls') RETURNING ROW(a),nextval('vg_output_calls')",true);
    struct SessionRestore{Session* previous;~SessionRestore(){setCurrentSession(previous);}} restore{currentSession()};setCurrentSession(&session);
    const bool counters=g_engine.currval(database,"vg_source_calls")==1&&g_engine.currval(database,"vg_output_calls")==1;
    setCurrentSession(restore.previous);
    check("real DML and output volatile sites execute once",captured(o,"UPDATE 1",1,2)&&array(field(cell(o.result),0),"integer[]",{"6",{}},1,true)&&scalar(cell(o.result,0,1),"bigint","1")&&counters);
    ExprValue detached=*cell(o.result);const auto* child=field(&detached,0);std::weak_ptr<const CompoundDatum> payload=child?child->compound:std::shared_ptr<const CompoundDatum>{};
    o={};after={};
    if(g_engine.dropDatabase(database)!=DBStatus::OK)throw std::runtime_error("drop actual fixture failed");
    const bool detachedGood=record(&detached,{1007})&&array(field(&detached,0),"integer[]",{"6",{}},1,true)&&!payload.expired();
    detached=ExprValue{};
    check("real owning array survives original owner and relation destruction then releases",detachedGood&&payload.expired());
    cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"VIRTUAL_GENERATED_OWNING_TERMINAL controls="<<controls<<" expected=28 failures="<<failures.size()<<"\n";
    return controls==28&&failures.empty()?0:1;
}

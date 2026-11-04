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
    const std::string name="merge_returning_source_owning",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)
        throw std::runtime_error("create actual database failed");
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    DdlExecutor ddl;
    const auto create=[&](const std::string& sql) {
        if(ddl.executeSql(sql,session))throw std::runtime_error("DDL fixture failed: "+sql);
    };
    create("CREATE TABLE ms_target(id integer PRIMARY KEY,n integer,"
           "v integer[] GENERATED ALWAYS AS(ARRAY[id,NULL]) VIRTUAL)");
    create("CREATE TABLE ms_source(id integer,x integer,r regtype,a integer[],ra regtype[],"
           "g smallint GENERATED ALWAYS AS(x) VIRTUAL)");
    create("CREATE TABLE ms_aux(id integer,z integer)");
    create("CREATE SEQUENCE ms_changes START 1");
    create("CREATE SEQUENCE ms_outputs START 1");
    const auto run=[&](const std::string& sql,bool capture) {
        Outcome output;
        const bool previous=typedResultCaptureRequested();
        const bool previousExplicit=typedResultCaptureExplicitReceiver();
        auto* previousSession=currentSession();clearLastDmlResult();
        {
            ScopedTypedResultCapture request(capture);
            try {output.failed=tryDmlBridge(sql,SQLParser::classify(sql),session,output.handled,sql);}
            catch(const DbError& error){output.state=error.sqlState();output.handled=true;}
            output.result=takeLastDmlResult();
        }
        output.restored=typedResultCaptureRequested()==previous &&
            typedResultCaptureExplicitReceiver()==previousExplicit && currentSession()==previousSession;
        return output;
    };
    const auto success=[](const Outcome& o){return o.handled&&!o.failed&&o.state.empty()&&o.restored;};
    const auto observeSequence=[&](const std::string& sequence,bool reset) {
        struct Restore{Session* previous;~Restore(){setCurrentSession(previous);}} restore{currentSession()};
        setCurrentSession(&session);
        return reset?g_engine.setval(database,sequence,1):g_engine.currval(database,sequence);
    };
    const auto reset=[&](int target=1,int source=1,int x=7) {
        for(const auto& relation:{"ms_target","ms_source","ms_aux"})
            if(g_engine.truncateTable(database,relation)!=DBStatus::OK)throw std::runtime_error("truncate fixture failed");
        if(target>0&&!success(run("INSERT INTO ms_target(id,n) VALUES("+std::to_string(target)+",10)",false)))
            throw std::runtime_error("target seed failed");
        if(source>0&&!success(run("INSERT INTO ms_source(id,x,r,a,ra) VALUES("+std::to_string(source)+","+
            std::to_string(x)+",'boolean'::regtype,'[0:2]={1,NULL,3}'::integer[],ARRAY['integer'::regtype,NULL])",false)))
            throw std::runtime_error("source seed failed");
        if(!success(run("INSERT INTO ms_aux VALUES(1,90)",false)))throw std::runtime_error("aux seed failed");
        if(observeSequence("ms_changes",true)!=1||observeSequence("ms_outputs",true)!=1)
            throw std::runtime_error("sequence reset failed");
    };
    const std::string matched="MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.x RETURNING ";
    size_t controls=0;std::vector<std::string> failures;
    const auto check=[&](const std::string& label,bool ok){++controls;std::cout<<"MERGE_SOURCE_CONTROL "<<controls<<" "<<label<<" "<<(ok?"PASS":"FAIL")<<"\n";if(!ok)failures.push_back(label);};
    reset();auto o=run("MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.x RETURNING ROW(id,n)",false);
    check("target-only default-off result keeps optional rows absent",success(o)&&o.result.commandTag=="MERGE 1"&&o.result.rows==std::vector<std::vector<std::string>>{{"(1,7)"}}&&!o.result.typedRows&&!o.result.typedRowDescriptor);
    reset();o=run(matched+"s.x",true);
    check("actual source scalar uses true source declaration",captured(o,"MERGE 1",1,1)&&scalar(cell(o.result),"integer","7")&&declared(o.result,0,23,DatumKind::Scalar));
    reset();o=run("MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.x RETURNING WITH(OLD AS o,NEW AS n) ROW(o.id,o.n,n.n,s.x)",true);
    check("source and distinct OLD NEW share original returning context",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{23,23,23,23})&&scalar(field(cell(o.result),0),"integer","1")&&scalar(field(cell(o.result),1),"integer","10")&&scalar(field(cell(o.result),2),"integer","7")&&scalar(field(cell(o.result),3),"integer","7"));
    reset();o=run(matched+"ROW(s.a)",true);
    check("requested stored array owns original lower bound and NULL",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{1007})&&array(field(cell(o.result),0),"integer[]",{"1",{},"3"},0,true));
    reset();if(!success(run("UPDATE ms_source SET a=NULL,r=NULL,ra=NULL",false)))throw std::runtime_error("NULL fixture failed");
    o=run(matched+"ROW(s.a,s.r,s.ra)",true);
    check("source SQL NULL fields preserve actual declared identities",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{1007,2206,2211})&&scalar(field(cell(o.result),0),"integer[]",{})&&scalar(field(cell(o.result),1),"regtype",{})&&scalar(field(cell(o.result),2),"regtype[]",{}));
    reset();o=run(matched+"ROW(s.r)",true);
    check("source REGTYPE owns numeric identity before display",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{2206})&&reference(field(cell(o.result),0),16));
    reset();o=run(matched+"ROW(s.r)",false);
    check("all-text source REGTYPE uses boolean display rather than raw 16",success(o)&&o.result.available&&o.result.commandTag=="MERGE 1"&&o.result.rows==std::vector<std::vector<std::string>>{{"(boolean)"}}&&!o.result.typedRows&&!o.result.typedRowDescriptor);
    reset();o=run(matched+"cardinality(s.a)",false);
    check("all-text SQL expression receives requested true array",success(o)&&o.result.commandTag=="MERGE 1"&&o.result.rows==std::vector<std::vector<std::string>>{{"3"}}&&!o.result.typedRows);
    reset();o=run(matched+"ARRAY[ROW(s.x,s.a),ROW(s.x+1,s.a)]",true);
    const auto* value=cell(o.result);const auto* elements=value&&value->compound&&value->compound->arrayElements?&*value->compound->arrayElements:nullptr;
    check("source nested record array retains independent full body metadata",captured(o,"MERGE 1",1,1)&&declared(o.result,0,2287,DatumKind::Array)&&elements&&elements->size()==2&&record(&elements->at(0),{23,1007})&&record(&elements->at(1),{23,1007})&&scalar(field(&elements->at(1),0),"integer","8")&&array(field(&elements->at(1),1),"integer[]",{"1",{},"3"},0,true));
    reset();o=run(matched+"ROW(s.ra)",true);
    check("late source ordinal is sparse and retains actual REGTYPE array children",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{2211})&&referenceArray(field(cell(o.result),0)));
    reset();o=run("MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN MATCHED THEN DELETE RETURNING WITH(OLD AS o,NEW AS n) ROW(o.id,n.id,s.x,s.a)",true);
    check("matched DELETE keeps selected source and typed NULL NEW",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{23,23,23,1007})&&scalar(field(cell(o.result),0),"integer","1")&&scalar(field(cell(o.result),1),"integer",{})&&scalar(field(cell(o.result),2),"integer","7")&&array(field(cell(o.result),3),"integer[]",{"1",{},"3"},0,true));
    reset(0);o=run("MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id,n) VALUES(s.id,s.x) RETURNING WITH(OLD AS o,NEW AS n) ROW(o.id,n.id,s.x,s.a)",true);
    check("not matched INSERT pairs real source tuple with actual NEW",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{23,23,23,1007})&&scalar(field(cell(o.result),0),"integer",{})&&scalar(field(cell(o.result),1),"integer","1")&&scalar(field(cell(o.result),2),"integer","7")&&array(field(cell(o.result),3),"integer[]",{"1",{},"3"},0,true));
    reset(1,2);o=run("MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET n=11 RETURNING ROW(t.id,s.x,s.a)",true);
    check("BY SOURCE absence creates typed whole NULL source image",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{23,23,1007})&&scalar(field(cell(o.result),0),"integer","1")&&scalar(field(cell(o.result),1),"integer",{})&&scalar(field(cell(o.result),2),"integer[]",{}));
    reset();o=run("MERGE INTO ms_target AS t USING ms_source AS s JOIN ms_aux AS q ON s.id=q.id ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.x RETURNING ROW(s.x,q.z)",true);
    check("two genuine source occurrences keep distinct real column ordinals",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{23,23})&&scalar(field(cell(o.result),0),"integer","7")&&scalar(field(cell(o.result),1),"integer","90"));
    reset(1,1,40000);o=run(matched+"ROW(s.x,CASE WHEN false THEN s.g ELSE s.x END)",true);
    check("unselected narrowing source does not cause retagging or premature error",captured(o,"MERGE 1",1,1)&&record(cell(o.result),{23,23})&&scalar(field(cell(o.result),0),"integer","40000")&&scalar(field(cell(o.result),1),"integer","40000"));
    reset(1,2);o=run(matched+"ROW(s.x,s.a)",true);
    check("zero output retains pure source declarations and no values",captured(o,"MERGE 0",0,1)&&declared(o.result,0,2249,DatumKind::Record));
    reset();o=run(matched+"ROW(s.a,1/(s.x-7))",true);
    auto after=run("UPDATE ms_target SET n=n RETURNING ROW(n)",true);
    check("actual RETURNING error preserves primary state and rolls mutation back",o.handled&&o.state=="22012"&&o.restored&&!o.result.available&&captured(after,"UPDATE 1",1,1)&&scalar(field(cell(after.result),0),"integer","10"));
    reset();o=run("MERGE INTO ms_target AS t USING ms_source AS s ON t.id=s.id WHEN MATCHED THEN UPDATE SET n=s.x+nextval('ms_changes') RETURNING ROW(s.x,s.a),nextval('ms_outputs')",true);
    check("original action and output volatile sites each execute once",captured(o,"MERGE 1",1,2)&&record(cell(o.result),{23,1007})&&scalar(field(cell(o.result),0),"integer","7")&&scalar(cell(o.result,0,1),"bigint","2")&&observeSequence("ms_changes",false)==2&&observeSequence("ms_outputs",false)==2);
    ExprValue detached=cell(o.result)?*cell(o.result):ExprValue{};
    const auto* a=field(&detached,1);std::weak_ptr<const CompoundDatum> weak=a?a->compound:std::shared_ptr<const CompoundDatum>{};
    o={};after={};clearLastDmlResult();
    if(g_engine.dropDatabase(database)!=DBStatus::OK)throw std::runtime_error("drop actual fixture failed");
    const bool detachedGood=record(&detached,{23,1007})&&array(field(&detached,1),"integer[]",{"1",{},"3"},0,true)&&!weak.expired();
    detached=ExprValue{};
    check("source payload owns values beyond statement relation and releases last owner",detachedGood&&weak.expired());
    cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"MERGE_SOURCE_OWNING_TERMINAL controls="<<controls<<" expected=19 failures="<<failures.size()<<"\n";
    return controls==19&&failures.empty()?0:1;
}

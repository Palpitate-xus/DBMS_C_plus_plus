// Covers real generated-column demands, assignment, presence and spill ownership.
// test_link_flags: -Wl,--wrap=mkstemp
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>
extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
using Expected=std::vector<std::vector<std::optional<std::string>>>;
size_t sortCreates=0;
bool actualSortFile(int fd) {
    char link[64],target[4096];std::snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
    const auto n=::readlink(link,target,sizeof(target)-1);if(n<0)return false;target[n]='\0';
    return std::strstr(target,"/.dbms-sort-")!=nullptr;
}
struct Fixture {
    std::shared_ptr<PreparedQuery> query;
    std::shared_ptr<PreparedQueryExecution> owner;
    std::unique_ptr<PreparedQueryCursor> cursor;
};
Fixture prepare(const std::string& db,const std::string& sql,const std::vector<QueryBindingDatum>& inputs={}) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql,inputs));
    auto owner=std::make_shared<PreparedQueryExecution>(query,&g_engine,db);
    owner->planStatementConstants(query->ast.get());
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get(),{},[owner]{return owner;});
    auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),query->output);
    return {std::move(query),std::move(owner),std::move(cursor)};
}
Rows consume(PreparedQueryCursor& cursor) {
    Rows rows;std::vector<ExprValue> row;while(cursor.next(row))rows.push_back(row);cursor.close();cursor.close();return rows;
}
bool values(const Rows& actual,const Expected& expected) {
    if(actual.size()!=expected.size())return false;
    for(size_t r=0;r<expected.size();++r){if(actual[r].size()!=expected[r].size())return false;
        for(size_t c=0;c<expected[r].size();++c)if(actual[r][c].isNull!=!expected[r][c] ||
            (expected[r][c] && actual[r][c].value!=*expected[r][c]))return false;}
    return true;
}
std::string error(const std::function<void()>& action) {
    try{action();}catch(const DbError& e){return e.sqlState();}return {};
}
struct Mutation {DmlResult result;std::string state,diagnostic;bool good=false;};
Mutation mutate(const std::string& sql,Session& session,bool capture=true) {
    Mutation result;bool handled=false;clearLastDmlResult();
    const auto before=typedResultCaptureRequested();auto* previous=currentSession();
    std::ostringstream diagnostic;
    {struct Restore {std::streambuf* old;~Restore(){std::cout.rdbuf(old);}} output{std::cout.rdbuf(diagnostic.rdbuf())};
     ScopedTypedResultCapture request(capture);
        try{result.good=!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql)&&handled;}
        catch(const DbError& e){result.state=e.sqlState();}
        result.result=takeLastDmlResult();}
    result.diagnostic=diagnostic.str();std::cout<<result.diagnostic;
    // The embedded bridge also reports legacy DBStatus errors on its actual
    // stdout channel. Preserve the complete text while checking that channel.
    if(result.state.empty()) {
        const auto at=result.diagnostic.find("SQLSTATE ");
        if(at!=std::string::npos && at+14<=result.diagnostic.size())result.state=result.diagnostic.substr(at+9,5);
    }
    result.good=result.good && result.state.empty() && typedResultCaptureRequested()==before && currentSession()==previous;
    return result;
}

const ExprValue* field(const Mutation& result,size_t index) {
    if(!result.result.typedRows || result.result.typedRows->size()!=1 || result.result.typedRows->at(0).size()!=1)return nullptr;
    const auto& record=result.result.typedRows->at(0).at(0);
    return record.recordFields && index<record.recordFields->size()?&record.recordFields->at(index):nullptr;
}
bool fieldValue(const Mutation& result,size_t index,const std::optional<std::string>& expected) {
    const auto* actual=field(result,index);return actual && actual->isNull==!expected && (!expected || actual->value==*expected);
}
bool integerArray(const ExprValue& value,const std::string& first) {
    return !value.isNull && value.typeName=="integer[]" && value.compound && value.compound->arrayElements &&
        value.compound->arrayElements->size()==2 && value.compound->arrayElements->at(0).value==first &&
        !value.compound->arrayElements->at(0).isNull && value.compound->arrayElements->at(1).isNull &&
        value.compound->dimensions.size()==1 && value.compound->dimensions[0].length==2 && value.compound->dimensions[0].lower==1;
}
// Actual heap bytes/actual RID NULL bitmap and original bound occurrence.
// This is a public native value consumer, not a replacement source or frame.
RowContext actualFirstInput(Fixture& f,const std::string& db,const std::string& table) {
    const auto source=std::find_if(f.query->sourceRanges.begin(),f.query->sourceRanges.end(),[&](const auto& range){return range.relationName==table;});
    if(source==f.query->sourceRanges.end())throw DbError("XX000","native fixture lost its real bound heap occurrence");
    const auto schema=g_engine.getTableSchema(db,table);RowContext context=f.owner->context();bool found=false;
    if(!g_engine.forEachVisibleRow(db,table,"SELECT",[&](uint32_t page,uint16_t slot,const char* data,size_t size){
        if(found)return;found=true;std::vector<ExprValue> cells;const auto rid=StorageEngine::encodeRid(page,slot);
        const std::string raw(data,size);
        for(size_t i=0;i<schema.len;++i){if(schema.cols[i].generatedKind=='v'){cells.emplace_back();continue;}
            const auto null=g_engine.isColumnNullByRid(db,table,rid,i);
            cells.emplace_back(source->columns.at(i).type,null?std::string{}:g_engine.extractColumnValue(raw,schema,i,db,false),null);}
        f.owner->setSourceInputs(context,source->ordinal,cells,true);
    }) || !found)throw DbError("XX000","native fixture has no actual stored row");
    return context;
}
}
extern "C" int __real_mkstemp(char*);
extern "C" int __wrap_mkstemp(char* name) {const auto fd=__real_mkstemp(name);if(fd>=0 && actualSortFile(fd))++sortCreates;return fd;}
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();cleanupTestDb("generated_demand");
    const auto db=testDbPath("generated_demand");if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;session.workMem.assign(64,false);
    auto* previous=currentSession();setCurrentSession(&session);DdlExecutor ddl;
    const auto create=[&](const std::string& sql){if(ddl.executeSql(sql,session))throw DbError("XX000","actual DDL fixture failed: "+sql);};
    create("CREATE TABLE gd_over(id integer,v smallint GENERATED ALWAYS AS(id) VIRTUAL)");
    create("CREATE TABLE gd_char(id integer,v varchar(3) GENERATED ALWAYS AS('abcdef') VIRTUAL)");
    create("CREATE TABLE gd_num(id integer,v numeric(4,1) GENERATED ALWAYS AS(id::numeric/3) VIRTUAL)");
    create("CREATE TABLE gd_error(id integer,v integer GENERATED ALWAYS AS(1/(id-id)) VIRTUAL)");
    create("CREATE TABLE gd_const(x integer,v integer GENERATED ALWAYS AS(7) VIRTUAL)");
    create("CREATE TABLE gd_native(id integer,v integer GENERATED ALWAYS AS(id+1) VIRTUAL)");
    create("CREATE TABLE gd_spill(id integer,padding text,a integer[] GENERATED ALWAYS AS(ARRAY[id,NULL]) VIRTUAL)");
    create("CREATE TABLE gd_notnull(id integer,v smallint GENERATED ALWAYS AS(id) VIRTUAL NOT NULL)");
    create("CREATE TABLE gd_check(id integer,v smallint GENERATED ALWAYS AS(id) VIRTUAL,CHECK(CASE WHEN false THEN v>0 ELSE true END))");
    create("CREATE TABLE gd_nullcheck(id integer,t text,v text GENERATED ALWAYS AS(CASE WHEN t IS NULL THEN 'null' ELSE 'non-null' END) VIRTUAL,CHECK(v='non-null'))");
    create("CREATE TABLE gd_fk(id integer PRIMARY KEY)");
    for(const auto& sql:{"INSERT INTO gd_over VALUES(40000)","INSERT INTO gd_char VALUES(1)","INSERT INTO gd_num VALUES(3)","INSERT INTO gd_error VALUES(1)","INSERT INTO gd_native VALUES(1)"})
        if(!mutate(sql,session,false).good)throw DbError("XX000","actual seed failed");
    for(size_t id=1;id<=128;++id)if(g_engine.insertRow(db,"gd_spill",{{"id",std::to_string(id)},{"padding",std::string(1536,'x')}})!=DBStatus::OK)return 2;
    TableSchema callback;callback.tablename="gd_callback";callback.cols[callback.len++]=makeIntColumn("id",true,4);
    callback.cols[callback.len]=makeIntColumn("v",true,4);callback.cols[callback.len].generatedKind='v';
    callback.cols[callback.len++].generatedExpr="gd_outer(gd_inner(id))";
    callback.cols[callback.len]=makeIntColumn("w",true,4);callback.cols[callback.len].generatedKind='v';
    callback.cols[callback.len++].generatedExpr="gd_outer(gd_inner(id))";
    if(g_engine.createTable(db,callback)!=DBStatus::OK || g_engine.insertRow(db,"gd_callback",{{"id","5"}})!=DBStatus::OK)return 2;
    size_t count=0,failed=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action){++count;bool good=false;
        try{good=action();}catch(const DbError& e){std::cerr<<"SQLSTATE="<<e.sqlState()<<" message="<<e.message()<<'\n';}
        catch(const std::exception& e){std::cerr<<e.what()<<'\n';}
        good=good && StorageEngine::captureNullRowBinding().engine==nullptr;failed+=!good;
        std::cout<<"GENERATED_DEMAND "<<count<<' '<<label<<" good="<<good<<'\n';};
    const auto query=[&](const std::string& sql){auto f=prepare(db,sql);return consume(*f.cursor);};
    check("unused invalid smallint has no assignment demand",[&]{return values(query("SELECT id FROM gd_over"),{{"40000"}});});
    check("selected smallint enforces actual assignment range",[&]{return error([&]{query("SELECT v FROM gd_over");})=="22003";});
    check("CASE false does not read virtual input",[&]{return values(query("SELECT CASE WHEN false THEN v ELSE id END FROM gd_over"),{{"40000"}});});
    check("zero LIMIT does not open invalid virtual source",[&]{auto f=prepare(db,"SELECT v FROM gd_error LIMIT 0");return consume(*f.cursor).empty();});
    check("WHERE false leaves invalid virtual projection unused",[&]{return query("SELECT v FROM gd_error WHERE false").empty();});
    check("varchar assignment does not use explicit-cast truncation",[&]{return error([&]{query("SELECT v FROM gd_char");})=="22001";});
    check("varchar unselected target remains harmless",[&]{return values(query("SELECT CASE WHEN false THEN v ELSE 'ok' END FROM gd_char"),{{"ok"}});});
    check("numeric virtual assignment retains declared scale",[&]{const auto rows=query("SELECT v FROM gd_num");return values(rows,{{"1.0"}}) && rows[0][0].typeName=="numeric";});
    check("unused division column does not become an input demand",[&]{return values(query("SELECT id FROM gd_error"),{{"1"}});});
    check("selected virtual division preserves primary SQLSTATE",[&]{return error([&]{query("SELECT v FROM gd_error");})=="22012";});
    check("COALESCE first known nonNULL does not read later virtual",[&]{return values(query("SELECT COALESCE(7,v) FROM gd_error"),{{"7"}});});
    check("real all-NULL NEW row differs from absent OLD",[&]{auto r=mutate("INSERT INTO gd_const VALUES(NULL) RETURNING ROW(OLD.v,NEW.v)",session);return r.good&&fieldValue(r,0,{})&&fieldValue(r,1,"7");});
    check("DELETE absent NEW suppresses constant generation",[&]{auto r=mutate("DELETE FROM gd_const RETURNING ROW(OLD.v,NEW.v)",session);return r.good&&fieldValue(r,0,"7")&&fieldValue(r,1,{});});
    check("UPDATE old and new actual presence survive SQL NULL bases",[&]{mutate("INSERT INTO gd_const VALUES(NULL)",session,false);auto r=mutate("UPDATE gd_const SET x=x RETURNING ROW(OLD.v,NEW.v)",session);return r.good&&fieldValue(r,0,"7")&&fieldValue(r,1,"7");});
    check("OLD/NEW stars use actual value reads",[&]{auto r=mutate("UPDATE gd_const SET x=x RETURNING OLD.*,NEW.*",session);return r.good&&r.result.typedRows&&values(*r.result.typedRows,{{{},"7",{},"7"}});});
    check("zero RETURNING preserves descriptor without values",[&]{auto r=mutate("UPDATE gd_error SET id=id WHERE false RETURNING ROW(v)",session);return r.good&&r.result.typedRows&&r.result.typedRows->empty()&&r.result.typedRowDescriptor&&r.result.typedRowDescriptor->size()==1;});
    check("RETURNING generated error preserves mutation rollback",[&]{auto r=mutate("UPDATE gd_error SET id=2 RETURNING v",session);return r.state=="22012"&&!r.result.available&&values(query("SELECT id FROM gd_error"),{{"1"}});});
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder})check("same graph actual origin/restart/error "+std::to_string(static_cast<unsigned>(origin)),[&]{
        QueryBindingDatum input{"generated-branch","branch","boolean",{},false,"false",1};input.origin=origin;
        auto f=prepare(db,"SELECT CASE WHEN $1 THEN v ELSE id END FROM gd_over",{input});auto* graph=f.cursor->plan();
        std::optional<size_t> slot;for(const auto& use:f.query->uses)if(f.query->source.substr(use.begin,use.end-use.begin)=="$1")slot=use.slot;
        if(!slot || *slot>=f.query->parameters.size() || !f.cursor->supportsRestart())return false;
        const auto original=f.query->parameters[*slot];bool good=values(consume(*f.cursor),{{"40000"}});
        auto parameters=f.query->parameters;parameters[*slot]=ExprValue("boolean","true",false);RowContext caller;caller.setParameters(parameters);
        f.cursor->restart(caller);const auto state=error([&]{consume(*f.cursor);});f.cursor->close();
        // StatementInput deliberately retains its immutable declaration; real
        // RuntimeCell/MetadataPlaceholder origins use the existing caller path.
        good=good && (origin==ParameterOrigin::StatementInput?state.empty():state=="22003");
        parameters[*slot]=ExprValue("boolean","false",false);caller.setParameters(parameters);f.cursor->restart(caller);
        return good&&values(consume(*f.cursor),{{"40000"}})&&f.cursor->plan()==graph&&f.query->parameters[*slot].value==original.value;
    });
    ExprValue detached;std::weak_ptr<PreparedQuery> released;
    check("real 64k spill preserves generated program/base tuple association",[&]{
        sortCreates=0;auto f=prepare(db,"SELECT ROW(id,a) FROM gd_spill ORDER BY id DESC");released=f.query;auto result=consume(*f.cursor);
        bool good=sortCreates>0&&result.size()==128;
        for(size_t i=0;i<result.size();++i){const auto& r=result[i][0];const auto id=std::to_string(128-i);
            good=good&&r.recordFields&&r.recordFields->size()==2&&r.recordFields->at(0).value==id&&integerArray(r.recordFields->at(1),id);}
        if(!result.empty())detached=result.front().front();return good;
    });
    check("detached compound metadata does not retain parent query",[&]{return released.expired()&&detached.recordFields&&integerArray(detached.recordFields->at(1),"128");});
    check("actual native callback runs on the caller evaluator with no value memo",[&]{
        auto f=prepare(db,"SELECT v FROM gd_callback");auto row=actualFirstInput(f,db,"gd_callback");
        auto* select=dynamic_cast<SelectStmt*>(f.query->ast.get());if(!select)return false;size_t calls=0;ExprEvaluator evaluator;evaluator.setCurrentDB(db);
        evaluator.registerFunction("gd_outer",[](const auto& args){return args.at(0);});
        evaluator.registerFunction("gd_inner",[&](const auto& args){++calls;evaluator.registerFunction("gd_outer",[](const auto&){return ExprValue("integer","99",false);});return args.at(0);});
        const auto first=evaluator.eval(select->selectList[0].expr.get(),row),second=evaluator.eval(select->selectList[0].expr.get(),row);
        return first.value=="99"&&second.value=="99"&&calls==2;
    });
    check("public evaluator copy keeps registered callback identity",[&]{
        auto f=prepare(db,"SELECT v,w FROM gd_callback");auto row=actualFirstInput(f,db,"gd_callback");auto* select=dynamic_cast<SelectStmt*>(f.query->ast.get());
        if(!select)return false;ExprEvaluator evaluator;evaluator.setCurrentDB(db);size_t calls=0;
        evaluator.registerFunction("gd_inner",[&](const auto& args){++calls;return args.at(0);});
        evaluator.registerFunction("gd_outer",[](const auto& args){return args.at(0);});
        auto first=evaluator.eval(select->selectList[0].expr.get(),row);auto copied=evaluator;
        auto originalNewSite=evaluator.eval(select->selectList[1].expr.get(),row);
        auto second=copied.eval(select->selectList[0].expr.get(),row);
        auto copiedNewSite=copied.eval(select->selectList[1].expr.get(),row);
        return first.value=="5"&&originalNewSite.value=="5"&&second.value=="5"&&copiedNewSite.value=="5"&&calls==4;
    });
    check("actual source alias binds generation dependencies to original occurrence",[&]{return values(query("SELECT a.v FROM gd_native AS a"),{{"2"}});});
    check("closed scalar child with no rows never reads virtual projection",[&]{return values(query("VALUES((SELECT v FROM gd_error WHERE false))"),{{{}}});});
    check("virtual NOT NULL enforces assignment before notnull",[&]{return mutate("INSERT INTO gd_notnull VALUES(40000)",session,false).state=="22003";});
    check("CHECK conditional does not materialize unused invalid virtual",[&]{return mutate("INSERT INTO gd_check VALUES(40000) RETURNING id",session).good;});
    check("CHECK preserves genuine NULL separate from empty text",[&]{auto valid=mutate("INSERT INTO gd_nullcheck VALUES(1,'')",session,false);auto invalid=mutate("INSERT INTO gd_nullcheck VALUES(2,NULL)",session,false);return valid.good&&invalid.state=="23514";});
    check("SQL direct virtual index keeps PG unsupported boundary",[&]{return error([&]{ddl.executeSql("CREATE INDEX gd_v ON gd_native(v)",session);})=="0A000";});
    check("SQL virtual index predicate is analyzed structurally only",[&]{return error([&]{ddl.executeSql("CREATE INDEX gd_p ON gd_native(id) WHERE v>0",session);})=="0A000";});
    check("SQL UNIQUE on virtual column remains unsupported",[&]{return error([&]{ddl.executeSql("ALTER TABLE gd_native ADD UNIQUE(v)",session);})=="0A000";});
    check("SQL FK on virtual column remains unsupported",[&]{return error([&]{ddl.executeSql("ALTER TABLE gd_native ADD FOREIGN KEY(v) REFERENCES gd_fk(id)",session);})=="0A000";});
    check("existing embedded matcher remains a genuine full-row consumer",[&]{
        bool observed=false;StorageEngine::SqlUpdateMatcher matcher=[&](const StorageEngine::SqlRow& row){const auto v=row.find("v");observed=row.size()==2&&v!=row.end()&&v->second&&*v->second=="2";return false;};
        return g_engine.updateRows(db,"gd_native",{{"id","3"}},{},nullptr,{},matcher)==DBStatus::OK&&observed;
    });
    check("early close/restart does not evaluate unopened generated suffix",[&]{auto f=prepare(db,"SELECT id FROM gd_error");std::vector<ExprValue> row;
        bool good=f.cursor->next(row)&&row.size()==1&&row[0].value=="1";f.cursor->close();f.cursor->close();f.cursor->restart(f.owner->context());return good&&values(consume(*f.cursor),{{"1"}});});
    check("selected generated exception clears actual scan binding",[&]{auto f=prepare(db,"SELECT v FROM gd_error ORDER BY id");const auto state=error([&]{consume(*f.cursor);});f.cursor->close();f.cursor->close();return state=="22012"&&StorageEngine::captureNullRowBinding().engine==nullptr;});
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;setCurrentSession(previous);cleanupTestDb("generated_demand");finalCleanupTestData();
    std::cout<<"GENERATED_DEMAND_TERMINAL controls="<<count<<" expected=36 failures="<<failed<<" dropped="<<dropped<<'\n';
    return count==36&&failed==0&&dropped?0:1;
}

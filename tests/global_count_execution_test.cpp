// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite
// Ordinary global counts through public operators; no private AST/header hooks.
#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "expression/prepared_query_execution.h"
#include "Config.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <numeric>
#include <unistd.h>
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
struct Io {size_t creates=0,reads=0,writes=0,maxFds=0;bool failRead=false,triggered=false;} io;
bool sortFile(int fd) {
    char path[64],name[4096];std::snprintf(path,sizeof(path),"/proc/self/fd/%d",fd);
    const auto n=::readlink(path,name,sizeof(name)-1);if(n<0)return false;name[n]='\0';
    return std::strstr(name,"/.dbms-sort-")!=nullptr;
}
size_t descriptors() {size_t n=0;for(const auto& p:std::filesystem::directory_iterator("/proc/self/fd"))n+=sortFile(std::stoi(p.path().filename().string()));return n;}
bool clean() {if(descriptors())return false;for(const auto& p:std::filesystem::directory_iterator("."))if(p.path().filename().string().rfind(".dbms-sort-",0)==0)return false;return true;}
bool equal(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields){if(a.recordFields->size()!=b.recordFields->size())return false;for(size_t i=0;i<a.recordFields->size();++i)if(!equal(a.recordFields->at(i),b.recordFields->at(i)))return false;}return true;
}
bool equal(const Rows& a,const Rows& b) {if(a.size()!=b.size())return false;for(size_t i=0;i<a.size();++i){if(a[i].size()!=b[i].size())return false;for(size_t j=0;j<a[i].size();++j)if(!equal(a[i][j],b[i][j]))return false;}return true;}
bool integers(const Rows& rows,const std::vector<std::vector<int64_t>>& wanted) {
    if(rows.size()!=wanted.size())return false;
    for(size_t i=0;i<rows.size();++i){if(rows[i].size()!=wanted[i].size())return false;for(size_t j=0;j<rows[i].size();++j)if(rows[i][j].isNull || rows[i][j].value!=std::to_string(wanted[i][j]))return false;}return true;
}
Rows consume(PreparedQueryCursor& cursor) {Rows rows;std::vector<ExprValue> row;while(cursor.next(row))rows.push_back(row);cursor.close();cursor.close();return rows;}
struct Fixture {std::shared_ptr<PreparedQuery> query;std::unique_ptr<PreparedQueryCursor> cursor;};
Fixture fixture(const std::string& database,const std::string& sql,const std::vector<QueryBindingDatum>& args={},const RowContext& caller={}) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql,args));
    return {query,QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output)};
}
Rows execute(const std::string& database,const std::string& sql) {auto f=fixture(database,sql);return consume(*f.cursor);}
size_t slot(const PreparedQuery& query,const std::string& spelling) {
    std::optional<size_t> found;for(const auto& use:query.uses)if(query.source.substr(use.begin,use.end-use.begin)==spelling){if(found && *found!=use.slot)throw DbError("XX000","one SQL input owns multiple slots");found=use.slot;}
    if(!found || *found>=query.parameters.size())throw DbError("XX000","missing actual input slot");
    return *found;
}
QueryBindingDatum argument(const char* name,size_t number,const char* value,ParameterOrigin origin=ParameterOrigin::RuntimeCell) {
    QueryBindingDatum result{name,name,"bigint",{},false,value,number};result.origin=origin;return result;
}
ExprPtr parameter(size_t slot,const char* type) {auto p=std::make_unique<ParameterExpr>();p->slot=slot;p->origin=ParameterOrigin::RuntimeCell;p->declaredType=type;return p;}
ExprValue recordValue(size_t id) {
    RowExpr empty;empty.constructor=true;ExprEvaluator evaluator;auto zero=evaluator.eval(&empty,RowContext{});
    if(id%4==0)return zero;
    if(id%4==2)return ExprValue("record","",true);
    ExprValue identity("regtype",id%2?"bigint":"boolean");identity.objectOid=id%2?20:16;
    ExprValue text("text",id%4==1?"β":"");text.collation="c";
    RowExpr record;record.constructor=true;record.fieldTypes={"text","regtype","record"};
    record.elements.push_back(parameter(0,"text"));record.elements.push_back(parameter(1,"regtype"));record.elements.push_back(parameter(2,"record"));
    RowContext context;context.setParameters({text,identity,zero});return evaluator.eval(&record,context);
}
struct Probe {size_t opens=0,reads=0,eof=0,closes=0;const PreparedQueryExecution* owner=nullptr;bool sameOwner=true;};
struct Data {
    Rows rows[2];Probe probe[2];size_t count=64;bool sourceError=false;std::exception_ptr primary;
    void rebuild(size_t epoch=0) {
        probe[0]={};probe[1]={};rows[0].clear();rows[1].clear();
        for(size_t branch=0;branch<2;++branch)for(size_t position=0;position<count;++position){
            const auto id=branch*count+position;ExprValue key("bigint",std::to_string(9007199254740993LL+int64_t(id%13)),id%11==0);
            ExprValue text("text",std::to_string(epoch)+":"+std::to_string(id)+"β"+std::string(8192,'a'+id%23));text.collation="c";
            if(id%17==0){text.value.clear();text.isNull=true;}else if(id%17==1)text.value.clear();else if(id%17==2)text.value="NULL";
            ExprValue object("regtype",id%2?"bigint":"boolean");object.objectOid=id%2?20:16;
            if(id%7==0){object.value.clear();object.isNull=true;object.objectOid.reset();}
            ExprValue array("regtype[]","{boolean,NULL,bigint}");array.elementOids={16,std::nullopt,20};
            if(id%3==1){array.value="{}";array.elementOids.clear();}else if(id%3==2){array.value.clear();array.isNull=true;array.elementOids.clear();}
            rows[branch].push_back({ExprValue("integer",std::to_string(id)),std::move(key),std::move(text),recordValue(id),std::move(object),std::move(array)});
        }
    }
};
QueryRowDescriptor descriptor() {return {{"id","integer"},{"k","bigint"},{"payload","text"},{"rec","record",false,0,2249},{"o","regtype",false,0,2206},{"arr","regtype[]",false,0,2211}};}
// This is a genuine logical source; descriptor provenance is empty. The
// planner supplies its real carrier and owns all projection/append nodes.
class Source final:public Operator {
    std::shared_ptr<Data> data_;size_t branch_,position_=0;bool opened_=false;std::vector<ExprValue> row_;
    QueryPlanner::PreparedExecutionProvider provider_;
public:
    Source(std::shared_ptr<Data> data,size_t branch,QueryPlanner::PreparedExecutionProvider provider):data_(std::move(data)),branch_(branch),provider_(std::move(provider)){}
    bool open()override {clearError();position_=0;opened_=true;row_.clear();auto& p=data_->probe[branch_];++p.opens;p.owner=provider_().get();return p.owner!=nullptr;}
    bool next(std::string& display)override {auto& p=data_->probe[branch_];p.sameOwner=p.sameOwner && p.owner==provider_().get();if(!opened_)throw DbError("XX000","closed logical source was read");
        if(data_->sourceError && !branch_ && position_==24)std::rethrow_exception(data_->primary);
        if(position_==data_->count){++p.eof;return false;}row_=data_->rows[branch_].at(position_++);++p.reads;display.clear();return true;}
    void close()override {if(opened_){opened_=false;++data_->probe[branch_].closes;row_.clear();}}
    bool supportsStructuredRows()const override{return true;}
    bool lastStructuredValues(std::vector<ExprValue>& row)const override{row=row_;return !row.empty();}
    bool supportsPreparedOuterRow()const override{return true;}
    bool bindPreparedOuterRow(const RowContext&)override{close();return true;}
};
struct Synthetic {std::shared_ptr<Data> data;std::shared_ptr<PreparedQuery> query;std::unique_ptr<PreparedQueryCursor> cursor;};
Synthetic synthetic(const std::string& database,const std::string& tail,const std::vector<QueryBindingDatum>& args={}) {
    auto data=std::make_shared<Data>();data->rebuild();QueryBindingMetadata metadata;
    metadata.relation=[](const std::string& name){return QueryRelationMetadata{"public",name,descriptor(),{}};};
    auto query=std::make_shared<PreparedQuery>(prepareQuery("SELECT id,k,payload,rec,o,arr FROM runtime_logical_left UNION ALL SELECT id,k,payload,rec,o,arr FROM runtime_logical_right "+tail,args,metadata));
    const auto branch=[data,query,database](SelectStmt* select,bool body,QueryPlanner::PreparedExecutionProvider provider){
        const size_t index=select->fromClause->tableName=="runtime_logical_left"?0:1;TableSchema schema;
        for(const auto& item:descriptor()){Column column;column.dataName=item.name;column.dataType=item.type;column.isVariableLength=true;schema.append(column);}
        auto source=std::make_unique<Source>(data,index,provider);
        return QueryPlanner::buildPreparedSelectPlan(&g_engine,database,query,select,schema,std::move(source),{},{},{},false,false,std::move(provider),body);
    };
    auto plan=QueryPlanner::buildPreparedUnionAllPlan(&g_engine,database,query,static_cast<SelectStmt*>(query->ast.get()),branch,{},{},false,false);
    return {data,query,QueryPlanner::makePreparedCursor(std::move(plan),query->output)};
}
Rows wanted(const Data& data,size_t offset=0,std::optional<size_t> cap={}) {
    Rows all=data.rows[0];all.insert(all.end(),data.rows[1].begin(),data.rows[1].end());
    std::sort(all.begin(),all.end(),[](const auto& a,const auto& b){if(a[1].isNull!=b[1].isNull)return !a[1].isNull;
        if(!a[1].isNull && a[1].value!=b[1].value)return std::stoll(a[1].value)<std::stoll(b[1].value);
        return std::stoll(a[0].value)<std::stoll(b[0].value);});
    const auto begin=std::min(offset,all.size()),end=begin+std::min(cap.value_or(all.size()),all.size()-begin);return {all.begin()+begin,all.begin()+end};
}
bool full(const Data& data) {return data.probe[0].reads==data.count && data.probe[1].reads==data.count && data.probe[0].eof==1 && data.probe[1].eof==1 && data.probe[0].closes==1 && data.probe[1].closes==1 && data.probe[0].sameOwner && data.probe[1].sameOwner && data.probe[0].owner && data.probe[0].owner==data.probe[1].owner;}
bool terminal(Operator& plan) {const auto reads=io.reads,writes=io.writes;std::string out;bool good=clean() && !plan.next(out) && !plan.next(out);plan.close();plan.close();return good && clean() && io.reads==reads && io.writes==writes;}
}
extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __wrap_mkstemp(char* name) {if(std::strncmp(name,".dbms-sort-",11))return __real_mkstemp(name);++io.creates;const auto fd=__real_mkstemp(name);io.maxFds=std::max(io.maxFds,descriptors());return fd;}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset) {if(sortFile(fd)){++io.reads;if(io.failRead && !io.triggered){io.triggered=true;errno=EIO;return -1;}}return __real_pread(fd,data,size,offset);}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset) {if(sortFile(fd))++io.writes;return __real_pwrite(fd,data,size,offset);}
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();const auto database=testDbPath("global_count_execution");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);const auto budget=g_config.workMemKb;g_config.workMemKb=64;
    DdlExecutor ddl;size_t checked=0,failures=0,sequenceId=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action){++checked;io={};session.workMem.assign(64,false);bool good=false;
        try{good=action();}catch(const DbError& error){std::cout<<"SQLSTATE="<<error.sqlState()<<" message="<<error.message()<<'\n';}catch(const std::exception& error){std::cout<<"exception="<<error.what()<<'\n';}
        good=good && clean();failures+=!good;std::cout<<"GLOBAL_COUNT_EXECUTION "<<checked<<' '<<label<<" good="<<good<<" creates="<<io.creates<<" reads="<<io.reads<<" writes="<<io.writes<<" maxfds="<<io.maxFds<<'\n';};
    const auto sequence=[&]{const auto name="global_native_sequence_"+std::to_string(sequenceId++);if(ddl.executeSql("CREATE SEQUENCE "+name,session) || g_engine.nextval(database,name)!=1)throw DbError("XX000","sequence fixture failed");return name;};
    const bool setup=!ddl.executeSql("CREATE TABLE global_left(v INTEGER)",session) && !ddl.executeSql("CREATE TABLE global_right(v INTEGER)",session) && !ddl.executeSql("CREATE TABLE global_parent(v INTEGER)",session);
    // Genuine stored inputs use the public storage insertion API.
    bool inserted=true;
    for(const auto& item:std::vector<std::pair<std::string,std::vector<int>>>{{"global_left",{6,2,4}},{"global_right",{5,1,3}},{"global_parent",{0,1,2}}})
        for(int value:item.second)inserted=(g_engine.insertRow(database,item.first,{{"v",std::to_string(value)}})==DBStatus::OK) && inserted;
    if(!setup || !inserted)++failures;
    const std::string base="(VALUES(6),(2),(4) UNION ALL VALUES(5),(1),(3)) ORDER BY column1";
    const std::vector<std::string> shapes={base,"VALUES(6),(2),(4) UNION ALL VALUES(5),(1),(3) ORDER BY column1",
        "SELECT v FROM global_left UNION ALL SELECT v FROM global_right ORDER BY v",
        "SELECT v FROM global_left UNION ALL VALUES(5),(1),(3) ORDER BY v",
        "((VALUES(6),(2) UNION ALL VALUES(4)) UNION ALL (VALUES(5),(1) UNION ALL VALUES(3))) ORDER BY column1",
        "WITH q AS(VALUES(6),(2),(4)) SELECT column1 AS v FROM q UNION ALL SELECT v FROM global_right ORDER BY v",
        "SELECT v FROM(VALUES(6),(2),(4)) AS a(v) UNION ALL SELECT v FROM(VALUES(5),(1),(3)) AS b(v) ORDER BY v"};
    for(size_t i=0;i<shapes.size();++i)check("real branch shape "+std::to_string(i),[&,i]{return integers(execute(database,shapes[i]+" LIMIT (1+1) OFFSET (1+0)"),{{2},{3}}) && integers(execute(database,shapes[i]+" LIMIT (SELECT 2) OFFSET (SELECT 3)"),{{4},{5}});});
    for(const auto origin:{ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder,ParameterOrigin::StatementInput})check("same graph real parameter owner "+std::to_string(static_cast<unsigned>(origin)),[&]{
        auto cap=argument("cap",1,"2",origin),skip=argument("skip",2,"1",origin);auto f=fixture(database,base+" LIMIT $1 OFFSET $2",{cap,skip});
        const auto capSlot=slot(*f.query,"$1"),skipSlot=slot(*f.query,"$2");const auto* graph=f.cursor->plan();bool good=f.cursor->supportsRestart() && integers(consume(*f.cursor),{{2},{3}});
        const auto bind=[&](const char* limit,const char* offset,bool isNull){auto values=f.query->parameters;values[capSlot]=ExprValue("bigint",limit,isNull);values[skipSlot]=ExprValue("bigint",offset,isNull);RowContext caller;caller.setParameters(std::move(values));f.cursor->restart(caller);return caller;};
        auto caller=bind("","",true);good=(origin==ParameterOrigin::StatementInput?integers(consume(*f.cursor),{{2},{3}}):integers(consume(*f.cursor),{{1},{2},{3},{4},{5},{6}})) && good;
        caller=bind("1","0",false);good=(origin==ParameterOrigin::StatementInput?integers(consume(*f.cursor),{{2},{3}}):integers(consume(*f.cursor),{{1}})) && good;
        caller=bind("0","0",false);good=(origin==ParameterOrigin::StatementInput?integers(consume(*f.cursor),{{2},{3}}):consume(*f.cursor).empty()) && good;
        caller=bind("-1","-1",false);std::string state;Rows rows;try{rows=consume(*f.cursor);}catch(const DbError& error){state=error.sqlState();}
        good=(origin==ParameterOrigin::StatementInput?state.empty() && integers(rows,{{2},{3}}):state=="2201X" && rows.empty()) && good;
        caller=bind("1","0",false);good=(origin==ParameterOrigin::StatementInput?integers(consume(*f.cursor),{{2},{3}}):integers(consume(*f.cursor),{{1}})) && good;
        return good && f.cursor->plan()==graph && f.query->parameters[capSlot].value=="2" && f.query->parameters[skipSlot].value=="1" && caller.parameter(capSlot).value=="1" && caller.parameter(skipSlot).value=="0";});
    check("fresh uncorrelated scalar memo on same actual graph",[&]{const auto name=sequence();auto f=fixture(database,base+" LIMIT (SELECT nextval('"+name+"'))");const auto* graph=f.cursor->plan();bool good=g_engine.currval(database,name)==1 && integers(consume(*f.cursor),{{1},{2}}) && g_engine.currval(database,name)==2;f.cursor->restart(RowContext{});return integers(consume(*f.cursor),{{1},{2},{3}}) && g_engine.currval(database,name)==3 && f.cursor->plan()==graph && good;});
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{{" LIMIT (0-1)","2201W"},{" LIMIT (1-1) OFFSET (0-1)","2201X"},{" LIMIT (0-1) OFFSET (0-1)","2201X"},{" LIMIT (VALUES(1),(2))","21000"},{" LIMIT (0-1) OFFSET (VALUES(1),(2))","21000"}})check("count error before genuine branch cells "+item.first,[&]{const auto name=sequence();const auto call="nextval('"+name+"')";auto f=fixture(database,"(VALUES("+call+") UNION ALL VALUES("+call+")) ORDER BY column1"+item.first);std::vector<ExprValue> row;std::string state;try{f.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}f.cursor->close();return state==item.second && row.empty() && g_engine.currval(database,name)==1 && io.creates==0;});
    check("offset then limit once and zero prevents both branches",[&]{const auto c=sequence(),s=sequence(),count="nextval('"+c+"')",cell="nextval('"+s+"')";auto f=fixture(database,"(VALUES("+cell+") UNION ALL VALUES("+cell+")) ORDER BY column1 LIMIT ("+count+"-3) OFFSET "+count);return g_engine.currval(database,c)==1 && g_engine.currval(database,s)==1 && consume(*f.cursor).empty() && g_engine.currval(database,c)==3 && g_engine.currval(database,s)==1 && io.creates==0;});
    check("empty scalar and typed NULL count remain unlimited",[&]{return integers(execute(database,base+" LIMIT (SELECT 1 WHERE false) OFFSET (NULL::bigint)"),{{1},{2},{3},{4},{5},{6}});});
    for(bool child:{false,true})check(child?"real parent scalar-child count":"real parent direct count",[&]{const auto count=child?"(SELECT p.v%2)":"p.v%2";const auto rows=execute(database,"SELECT p.v,(VALUES(p.v+10) UNION ALL VALUES(p.v+20) ORDER BY column1 DESC LIMIT "+std::string(count)+") FROM global_parent p ORDER BY p.v");return rows.size()==3 && rows[0][0].value=="0" && rows[0][1].isNull && integers(Rows{rows[1]},{{1,21}}) && rows[2][0].value=="2" && rows[2][1].isNull;});
    check("real parent OFFSET binding",[&]{return integers(execute(database,"SELECT p.v,(VALUES(p.v+10) UNION ALL VALUES(p.v+20) ORDER BY column1 DESC LIMIT 1 OFFSET (p.v%2)) FROM global_parent p ORDER BY p.v"),{{0,20},{1,11},{2,22}});});
    check("dynamic FETCH NULL TIES is unlimited on same graph",[&]{auto f=fixture(database,"(VALUES(1),(1) UNION ALL VALUES(2),(3)) ORDER BY column1 FETCH FIRST $1 ROWS WITH TIES",{argument("ties",1,"1")});const auto index=slot(*f.query,"$1");const auto* graph=f.cursor->plan();bool good=integers(consume(*f.cursor),{{1},{1}});RowContext caller;auto values=f.query->parameters;values[index]=ExprValue("bigint","",true);caller.setParameters(std::move(values));f.cursor->restart(caller);good=integers(consume(*f.cursor),{{1},{1},{2},{3}}) && good;values=f.query->parameters;values[index]=ExprValue("bigint","1");caller.setParameters(std::move(values));f.cursor->restart(caller);return integers(consume(*f.cursor),{{1},{1}}) && f.cursor->plan()==graph && good;});
    for(bool spill:{false,true})check(spill?"low budget full typed metadata and same actual branch provider":"high budget full typed metadata and same actual branch provider",[&]{session.workMem.assign(spill?64:16384,false);auto f=synthetic(database,"ORDER BY k,id LIMIT (10+0) OFFSET (3+0)");bool good=equal(consume(*f.cursor),wanted(*f.data,3,10)) && full(*f.data);return good && io.maxFds<=2 && (spill?io.creates>0 && io.reads>0 && io.writes>0:io.creates==0);});
    check("spilled finite NULL finite rebind has no stale bound",[&]{
        auto f=synthetic(database,"ORDER BY k,id LIMIT $1 OFFSET $2",{argument("cap",1,"2"),argument("skip",2,"1")});
        const auto cap=slot(*f.query,"$1"),skip=slot(*f.query,"$2");const auto* graph=f.cursor->plan();
        const bool firstValues=equal(consume(*f.cursor),wanted(*f.data,1,2)) && full(*f.data);
        const bool firstMemory=io.creates==0 && io.reads==0 && io.writes==0;
        size_t unlimitedFiles=0,unlimitedReads=0,unlimitedWrites=0;bool unlimitedValues=false,unlimitedSpill=false,finalValues=false,finalMemory=false;
        for(size_t epoch=1;epoch<=2;++epoch){
            f.data->rebuild(epoch);auto values=f.query->parameters;
            values[cap]=ExprValue("bigint",epoch==1?"":"1",epoch==1);values[skip]=ExprValue("bigint",epoch==1?"":"0",epoch==1);
            RowContext caller;caller.setParameters(std::move(values));f.cursor->restart(caller);
            const bool actualValues=equal(consume(*f.cursor),wanted(*f.data,0,epoch==1?std::optional<size_t>{}:std::optional<size_t>{1})) && full(*f.data);
            if(epoch==1){unlimitedValues=actualValues;unlimitedFiles=io.creates;unlimitedReads=io.reads;unlimitedWrites=io.writes;
                unlimitedSpill=unlimitedFiles>0 && unlimitedReads>0 && unlimitedWrites>0;}
            else{finalValues=actualValues;finalMemory=io.creates==unlimitedFiles && io.reads==unlimitedReads && io.writes==unlimitedWrites;}
        }
        std::cout<<"GLOBAL_COUNT_BOUND_PHASES first_values="<<firstValues<<" first_memory="<<firstMemory
                 <<" unlimited_values="<<unlimitedValues<<" unlimited_spill="<<unlimitedSpill
                 <<" final_values="<<finalValues<<" final_memory="<<finalMemory<<'\n';
        return firstValues && firstMemory && unlimitedValues && unlimitedSpill && finalValues && finalMemory &&
            f.cursor->plan()==graph && io.maxFds<=2 && f.query->parameters[cap].value=="2" && f.query->parameters[skip].value=="1";});
    check("zero has no branch open read or spool",[&]{auto f=synthetic(database,"ORDER BY k,id LIMIT (1-1) OFFSET (SELECT 3)");return consume(*f.cursor).empty() && f.data->probe[0].opens==0 && f.data->probe[1].opens==0 && f.data->probe[0].reads==0 && f.data->probe[1].reads==0 && io.creates==0;});
    check("unsorted dynamic prefix does not open right branch",[&]{auto f=synthetic(database,"LIMIT (1+0)");const auto actual=consume(*f.cursor);return equal(actual,Rows{f.data->rows[0][0]}) && f.data->probe[0].opens==1 && f.data->probe[0].reads==1 && f.data->probe[0].closes==1 && f.data->probe[1].opens==0 && io.creates==0;});
    check("early close after spilled first output avoids successor I/O",[&]{auto f=synthetic(database,"ORDER BY k,id LIMIT (NULL::bigint)");std::vector<ExprValue> first;bool good=f.cursor->next(first) && equal(Rows{first},Rows{wanted(*f.data).front()}) && io.creates>0 && descriptors()>0;const auto reads=io.reads,writes=io.writes;f.cursor->close();f.cursor->close();return good && clean() && io.reads==reads && io.writes==writes;});
    check("source exception preserves identity after spool and is terminal",[&]{auto f=synthetic(database,"ORDER BY k,id LIMIT (NULL::bigint)");f.data->sourceError=true;f.data->primary=std::make_exception_ptr(std::logic_error("primary global-count source exception"));auto* plan=f.cursor->plan();bool good=plan->open(),exact=false;std::string out;try{plan->next(out);}catch(...){exact=std::current_exception()==f.data->primary;}return good && exact && io.creates>0 && terminal(*plan);});
    check("one emitted row then exact next-demand EIO and terminal cleanup",[&]{auto f=synthetic(database,"ORDER BY k,id LIMIT (NULL::bigint)");auto* plan=f.cursor->plan();std::string out;size_t rows=0;bool good=plan->open() && plan->next(out);rows+=good;io.failRead=true;bool exact=false;try{if(plan->next(out))++rows;}catch(const DbError& error){exact=error.sqlState()=="58030" && error.message()==std::string("could not read sort temporary file: ")+std::strerror(EIO);}return good && exact && rows==1 && io.triggered && terminal(*plan);});
    for(bool timeout:{false,true})check(timeout?"live timeout directly closes count wrapper spool":"live cancel directly closes count wrapper spool",[&]{const auto previousToken=currentQueryInterruptState();auto token=session.interruptState;token->queryActive.store(true);token->cancelRequested.store(false);token->timeoutRequested.store(false);setCurrentQueryInterruptState(token);
        struct Restore {std::shared_ptr<SessionInterruptState> old,token;~Restore(){token->cancelRequested.store(false);token->timeoutRequested.store(false);token->queryActive.store(false);setCurrentQueryInterruptState(std::move(old));}} restore{previousToken,token};
        auto f=synthetic(database,"ORDER BY k,id OFFSET (0+0) ROW FETCH FIRST (1+0) ROWS WITH TIES");auto* plan=f.cursor->plan();std::string out;size_t rows=0;bool good=plan->open() && plan->next(out);rows+=good;good=good && io.creates>0 && descriptors()>0;const auto reads=io.reads,writes=io.writes;token->cancelRequested.store(true);token->timeoutRequested.store(timeout);bool exact=false;try{if(plan->next(out))++rows;}catch(const DbError& error){exact=error.sqlState()=="57014" && error.message()==(timeout?"canceling statement due to statement timeout":"canceling statement due to user request");}const bool immediate=clean() && io.reads==reads && io.writes==writes;token->cancelRequested.store(false);token->timeoutRequested.store(false);return good && exact && immediate && rows==1 && terminal(*plan);});
    check("sorted K1 still evaluates discarded input error without spool",[&]{
        auto f=synthetic(database,"ORDER BY k,id LIMIT (1+0)");f.data->sourceError=true;
        f.data->primary=std::make_exception_ptr(std::logic_error("primary discarded global-count source exception"));
        auto* plan=f.cursor->plan();bool good=plan->open(),exact=false;std::string out;size_t rows=0;
        try{if(plan->next(out))++rows;}catch(...){exact=std::current_exception()==f.data->primary;}
        good=good && exact && rows==0 && io.creates==0 && f.data->probe[0].opens==1 &&
            f.data->probe[0].reads==24 && f.data->probe[0].closes==1 && f.data->probe[1].opens==0;
        return terminal(*plan) && good && f.data->probe[0].reads==24 && f.data->probe[1].opens==0;});
    const bool dropped=g_engine.dropDatabase(database)==DBStatus::OK;g_config.workMemKb=budget;setCurrentSession(previous);if(!dropped)++failures;
    constexpr size_t expectedControls=33;if(checked!=expectedControls)++failures;
    std::cout<<"GLOBAL_COUNT_EXECUTION_COMPLETE "<<checked<<" FAILURES "<<failures<<'\n';return failures?1:0;
}

#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "catalog/CatalogService.h"
#include "expression/expr_helper.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

// Cumulative requests made by the actual executor thread, not live bytes,
// malloc usable sizes, process RSS, or the private comparator's call count.
namespace allocation_requests {
thread_local bool enabled=false;
thread_local size_t calls=0,bytes=0;
void record(size_t size) noexcept {if(enabled){++calls;bytes+=size;}}
void* allocate(size_t size) {
    void* result=std::malloc(size?size:1);
    if(!result)throw std::bad_alloc();
    record(size);return result;
}
void* aligned(size_t size,size_t alignment) {
    void* result=nullptr;
    if(posix_memalign(&result,alignment,size?size:1))throw std::bad_alloc();
    record(size);return result;
}
struct Counts {size_t calls=0,bytes=0;};
class Region {
    bool active_=true;
public:
    Region(){calls=bytes=0;enabled=true;}
    ~Region(){if(active_)enabled=false;}
    Counts finish(){enabled=false;active_=false;return {calls,bytes};}
};
}
void* operator new(size_t size){return allocation_requests::allocate(size);}
void* operator new[](size_t size){return allocation_requests::allocate(size);}
void operator delete(void* value)noexcept{std::free(value);}
void operator delete[](void* value)noexcept{std::free(value);}
void operator delete(void* value,size_t)noexcept{std::free(value);}
void operator delete[](void* value,size_t)noexcept{std::free(value);}
void* operator new(size_t size,std::align_val_t alignment){return allocation_requests::aligned(size,static_cast<size_t>(alignment));}
void* operator new[](size_t size,std::align_val_t alignment){return allocation_requests::aligned(size,static_cast<size_t>(alignment));}
void operator delete(void* value,std::align_val_t)noexcept{std::free(value);}
void operator delete[](void* value,std::align_val_t)noexcept{std::free(value);}
void operator delete(void* value,size_t,std::align_val_t)noexcept{std::free(value);}
void operator delete[](void* value,size_t,std::align_val_t)noexcept{std::free(value);}

namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
using Expected=std::vector<std::vector<std::optional<std::string>>>;
using Integer=std::optional<int64_t>;
struct Fixture {std::shared_ptr<PreparedQuery> query;std::unique_ptr<PreparedQueryCursor> cursor;};
struct Probe {size_t reads=0,eof=0,closes=0,rebinds=0;};
struct Logical {Fixture fixture;std::shared_ptr<Probe> probe;};
bool same(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields){if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!same(a.recordFields->at(i),b.recordFields->at(i)))return false;}
    return true;
}
bool same(const Rows& a,const Rows& b) {
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i){if(a[i].size()!=b[i].size())return false;
        for(size_t j=0;j<a[i].size();++j)if(!same(a[i][j],b[i][j]))return false;}
    return true;
}
bool values(const Rows& actual,const Expected& expected) {
    if(actual.size()!=expected.size())return false;
    for(size_t i=0;i<actual.size();++i){if(actual[i].size()!=expected[i].size())return false;
        for(size_t j=0;j<actual[i].size();++j)if(expected[i][j] ? actual[i][j].isNull || actual[i][j].value!=*expected[i][j] : !actual[i][j].isNull)return false;}
    return true;
}
Rows consume(PreparedQueryCursor& cursor) {
    Rows rows;std::vector<ExprValue> row;
    while(cursor.next(row))rows.push_back(row);
    cursor.close();cursor.close();return rows;
}
Fixture physical(const std::string& db,const std::string& sql,const std::vector<QueryBindingDatum>& inputs={}) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql,inputs));
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
    return {query,QueryPlanner::makePreparedCursor(std::move(plan),query->output)};
}
size_t windowNodes(Operator* root) {
    std::vector<Operator*> pending{root};std::set<Operator*> visited;size_t result=0;
    while(!pending.empty()){auto* node=pending.back();pending.pop_back();if(!node || !visited.insert(node).second)continue;
        result+=node->preparedPlanNodeName()=="WindowAgg";
        for(auto* child:node->preparedPlanChildren())pending.push_back(child);
        if(auto* limit=dynamic_cast<LimitOp*>(node))pending.push_back(limit->child());
        if(auto* offset=dynamic_cast<OffsetOp*>(node))pending.push_back(offset->child());}
    return result;
}
size_t inputSlot(const PreparedQuery& query) {
    std::optional<size_t> result;
    for(const auto& use:query.uses)if(query.source.substr(use.begin,use.end-use.begin)=="$1"){
        if(result && *result!=use.slot)throw DbError("XX000","parameter sites do not share their actual slot");result=use.slot;}
    if(!result || *result>=query.parameters.size())throw DbError("XX000","actual parameter slot missing");return *result;
}
std::shared_ptr<PreparedQuery> logicalQuery(const std::string& db,const std::string& sql,const QueryRowDescriptor& descriptor) {
    QueryBindingMetadata metadata;
    metadata.relation=[descriptor](const std::string& name){return QueryRelationMetadata{"public",name,descriptor,{}};};
    metadata.functionType=[&](const FunctionCallExpr* call){return ExprHelper::inferParsedResultType(call,{},db,&g_engine);};
    return std::make_shared<PreparedQuery>(prepareQuery(sql,{},metadata));
}
Logical fromContexts(const std::string& db,std::shared_ptr<PreparedQuery> query,Rows data,
                     std::optional<size_t> failAt={},bool throwingClose=false) {
    auto* select=dynamic_cast<SelectStmt*>(query->ast.get());
    if(!select || query->sourceRanges.size()!=1)throw DbError("XX000","fixture requires one actual bound source");
    const auto occurrence=query->sourceRanges.front().ordinal;
    auto probe=std::make_shared<Probe>();auto rows=std::make_shared<const Rows>(std::move(data));
    auto source=std::make_unique<PreparedSourceContextsOp>(
        [rows,probe,occurrence,failAt](size_t index,RowContext& row){
            if(failAt && index==*failAt)throw DbError("58030","primary integer-key source failure");
            if(index>=rows->size()){++probe->eof;return false;}
            row=RowContext{};for(size_t column=0;column<rows->at(index).size();++column)
                row.setBoundColumn(occurrence,column,rows->at(index)[column]);
            ++probe->reads;return true;
        },[probe,throwingClose]{++probe->closes;if(throwingClose)throw DbError("XX000","secondary close failure");},
        [probe](const RowContext&){++probe->rebinds;});
    auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,db,query,select,TableSchema{},std::move(source));
    return {{query,QueryPlanner::makePreparedCursor(std::move(plan),query->output)},probe};
}
Logical logical(const std::string& db,const std::string& sql,const QueryRowDescriptor& descriptor,Rows data,
                std::optional<size_t> failAt={},bool throwingClose=false) {
    return fromContexts(db,logicalQuery(db,sql,descriptor),std::move(data),failAt,throwingClose);
}
QueryRowDescriptor basic(uint32_t oid=23) {return {{"id","integer",false,0,23},{"k","integer",false,0,oid}};}
std::vector<size_t> ordered(const std::vector<Integer>& keys,bool asc,bool nullsFirst) {
    std::vector<size_t> result(keys.size());std::iota(result.begin(),result.end(),size_t{0});
    std::sort(result.begin(),result.end(),[&](size_t a,size_t b){
        if(!keys[a] || !keys[b]){if(bool(keys[a])!=bool(keys[b]))return !keys[a]==nullsFirst;return a<b;}
        if(*keys[a]!=*keys[b])return asc?*keys[a]<*keys[b]:*keys[a]>*keys[b];return a<b;});
    return result;
}
Expected ranks(const std::vector<Integer>& keys,bool asc,bool nullsFirst) {
    const auto index=ordered(keys,asc,nullsFirst);std::vector<size_t> rank(keys.size()),dense(keys.size()),rn(keys.size());
    size_t group=0,first=0;
    for(size_t position=0;position<index.size();++position){if(!position || keys[index[position]]!=keys[index[position-1]]){++group;first=position;}
        rank[index[position]]=first+1;dense[index[position]]=group;rn[index[position]]=position+1;}
    Expected result;for(size_t i=0;i<keys.size();++i)result.push_back({std::to_string(i+1),keys[i]?std::optional<std::string>{std::to_string(*keys[i])}:std::nullopt,
        std::to_string(rank[i]),std::to_string(dense[i]),std::to_string(rn[i])});return result;
}
Rows costRows(size_t count) {
    Rows result;result.reserve(count);
    for(size_t i=0;i<count;++i){const auto key=(i*109+37)%count;
        result.push_back({ExprValue("integer",std::to_string(i+1)),ExprValue("integer",std::to_string(key))});}
    return result;
}
Expected costExpected(size_t count) {
    Expected result(count);
    for(size_t i=0;i<count;++i){const auto key=(i*109+37)%count;
        result[key]={std::to_string(i+1),std::to_string(key),std::to_string(key+1)};}
    return result;
}
struct Measurement {Rows rows;allocation_requests::Counts requests;};
Measurement measure(PreparedQueryCursor& cursor) {
    allocation_requests::Region region;
    cursor.restart(RowContext{});auto rows=consume(cursor);auto requests=region.finish();
    return {std::move(rows),requests};
}
void printCost(const char* name,const Measurement& value) {
    std::cout<<"WINDOW_KEY_ALLOCATION "<<name<<" calls="<<value.requests.calls<<" requested_bytes="<<value.requests.bytes<<'\n';
}
ExprPtr cell(size_t index,const char* type) {auto result=std::make_unique<ParameterExpr>();result->slot=index;
    result->declaredType=type;result->origin=ParameterOrigin::RuntimeCell;return result;}
ExprValue record(const ExprValue& text) {
    RowExpr empty;empty.constructor=true;ExprEvaluator evaluator;auto nested=evaluator.eval(&empty,RowContext{});
    RowExpr outer;outer.constructor=true;outer.fieldTypes={"text","record"};outer.elements.push_back(cell(0,"text"));outer.elements.push_back(cell(1,"record"));
    RowContext row;row.setParameters({text,nested});return evaluator.eval(&outer,row);
}
}

int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("prepared_window_integer_key_cache");cleanupTestDb("prepared_window_integer_key_cache");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);DdlExecutor ddl;
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action){++controls;bool good=false;
        try{good=action();}catch(const DbError& error){std::cerr<<"SQLSTATE="<<error.sqlState()<<" message="<<error.message()<<'\n';}
        catch(const std::exception& error){std::cerr<<"exception="<<error.what()<<'\n';}
        good=good && StorageEngine::captureNullRowBinding().engine==nullptr;failures+=!good;
        std::cout<<"PREPARED_WINDOW_INTEGER_KEY "<<controls<<' '<<label<<" good="<<good<<'\n';};
    struct Integers {std::string table,type;uint32_t oid;std::vector<Integer> keys;};
    const std::vector<Integers> fixtures{
        {"wik_i2","smallint",21,{32767,-32768,0,-1,1,-32768,std::nullopt,32767}},
        {"wik_i4","integer",23,{2147483647,-2147483648LL,0,-1,1,-2147483648LL,std::nullopt,2147483647}},
        {"wik_i8","bigint",20,{INT64_MAX,INT64_MIN,9007199254740993LL,9007199254740994LL,-9007199254740993LL,-9007199254740994LL,
            0,-1,1,INT64_MIN,std::nullopt,INT64_MAX}}};
    bool setup=true;
    for(const auto& fixture:fixtures){setup=!ddl.executeSql("CREATE TABLE "+fixture.table+"(id INTEGER,k "+fixture.type+")",session) && setup;
        for(size_t i=0;i<fixture.keys.size();++i)setup=(g_engine.insertRow(db,fixture.table,{{"id",std::to_string(i+1)},
            {"k",fixture.keys[i]?std::optional<std::string>{std::to_string(*fixture.keys[i])}:std::nullopt}})==DBStatus::OK) && setup;}
    setup=!ddl.executeSql("CREATE DOMAIN wik_domain AS INTEGER",session) && setup;
    setup=!ddl.executeSql("CREATE TABLE wik_domain_rows(id INTEGER,k wik_domain)",session) && setup;
    if(!setup){setCurrentSession(previous);return 2;}

    // Independent integer ordering over original mathematical constants; no
    // parser/cache helper is reproduced in this oracle.
    for(const auto& input:fixtures)for(bool asc:{true,false})for(bool first:{true,false})
        check(input.type+" endpoints peers "+(asc?"ASC":"DESC")+(first?" NULLS FIRST":" NULLS LAST"),[&]{
            const std::string key="k "+std::string(asc?"ASC":"DESC")+(first?" NULLS FIRST":" NULLS LAST");
            auto f=physical(db,"SELECT id,k,rank() OVER(ORDER BY "+key+"),dense_rank() OVER(ORDER BY "+key+"),row_number() OVER(ORDER BY "+key+",id) FROM "+input.table+" ORDER BY id");
            const auto descriptor=f.cursor->descriptor();const auto rows=consume(*f.cursor);
            bool good=descriptor.size()==5 && descriptor[0].typeOid==23 && descriptor[1].typeOid==input.oid && windowNodes(f.cursor->plan())>=1;
            for(size_t i=2;i<descriptor.size();++i)good=descriptor[i].typeOid==20 && good;
            for(const auto& row:rows)good=row[1].typeName==input.type && good;
            return good && values(rows,ranks(input.keys,asc,first));});
    for(const auto& input:fixtures)check(input.type+" repeated partition/order key index and GROUPS peers",[&]{
        auto f=physical(db,"SELECT id,k,row_number() OVER(PARTITION BY k ORDER BY k,id),count(*) OVER(PARTITION BY k ORDER BY k GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) FROM "+input.table+" ORDER BY id");
        Expected wanted;for(size_t i=0;i<input.keys.size();++i){size_t total=0,position=0;
            for(size_t j=0;j<input.keys.size();++j)if(input.keys[j]==input.keys[i]){++total;position+=j<=i;}
            wanted.push_back({std::to_string(i+1),input.keys[i]?std::optional<std::string>{std::to_string(*input.keys[i])}:std::nullopt,std::to_string(position),std::to_string(total)});}
        return values(consume(*f.cursor),wanted) && windowNodes(f.cursor->plan())>=1;});
    check("same keys distinct ROWS frame stages preserve exact aggregate state",[&]{
        const auto& input=fixtures.back();auto f=physical(db,"SELECT id,sum(id) OVER(ORDER BY k NULLS LAST,id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),count(*) OVER(ORDER BY k NULLS LAST,id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM wik_i8 ORDER BY id");
        const auto index=ordered(input.keys,true,false);std::vector<int64_t> sums(index.size());int64_t total=0;
        for(const auto i:index){total+=static_cast<int64_t>(i+1);sums[i]=total;}
        Expected wanted;for(size_t i=0;i<sums.size();++i)wanted.push_back({std::to_string(i+1),std::to_string(sums[i]),"1"});
        return windowNodes(f.cursor->plan())>=2 && values(consume(*f.cursor),wanted);});
    check("multiple opposite stages retain NULL lag values and original key associations",[&]{
        const auto& input=fixtures.back();auto f=physical(db,"SELECT id,rank() OVER(ORDER BY k NULLS LAST),lag(k) OVER(ORDER BY id DESC),row_number() OVER(ORDER BY k NULLS LAST,id) FROM wik_i8 ORDER BY id");
        const auto ranking=ranks(input.keys,true,false);Expected wanted;
        for(size_t i=0;i<input.keys.size();++i)wanted.push_back({std::to_string(i+1),ranking[i][2],
            i+1<input.keys.size() && input.keys[i+1]?std::optional<std::string>{std::to_string(*input.keys[i+1])}:std::nullopt,ranking[i][4]});
        return windowNodes(f.cursor->plan())>=2 && values(consume(*f.cursor),wanted);});
    for(const auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder})
        check("same graph three executions preserve parameter origin "+std::to_string(static_cast<unsigned>(origin)),[&]{
            QueryBindingDatum parameter{"window-key-input","v","bigint",{},false,"7",1};parameter.origin=origin;
            auto f=physical(db,"SELECT id,row_number() OVER(ORDER BY k NULLS LAST,id),lag($1,0) OVER(ORDER BY k NULLS LAST,id) FROM wik_i8 ORDER BY k NULLS LAST,id LIMIT 4",{parameter});
            const auto* graph=f.cursor->plan();const auto slot=inputSlot(*f.query);const auto original=f.query->parameters[slot];
            const auto index=ordered(fixtures.back().keys,true,false);
            const auto expected=[&](std::optional<std::string> parameterValue){Expected rows;for(size_t i=0;i<4;++i)rows.push_back({std::to_string(index[i]+1),std::to_string(i+1),parameterValue});return rows;};
            bool good=f.cursor->supportsRestart() && values(consume(*f.cursor),expected("7"));
            auto cells=f.query->parameters;cells[slot]=ExprValue("bigint","20");RowContext caller;caller.setParameters(std::move(cells));f.cursor->restart(caller);
            good=values(consume(*f.cursor),expected(origin==ParameterOrigin::StatementInput?"7":"20")) && good;
            auto nulls=f.query->parameters;nulls[slot]=ExprValue("bigint","",true);RowContext nullCaller;nullCaller.setParameters(std::move(nulls));f.cursor->restart(nullCaller);
            good=values(consume(*f.cursor),expected(origin==ParameterOrigin::StatementInput?std::optional<std::string>{"7"}:std::nullopt)) && good;
            return good && f.cursor->plan()==graph && same(f.query->parameters[slot],original) &&
                same(caller.parameter(slot),ExprValue("bigint","20")) && same(nullCaller.parameter(slot),ExprValue("bigint","",true));});
    check("early close twice then restart preserves the actual owning graph",[&]{
        auto f=physical(db,"SELECT id,k,row_number() OVER(ORDER BY k NULLS LAST,id) FROM wik_i8 ORDER BY k NULLS LAST,id");
        auto* graph=f.cursor->plan();std::vector<ExprValue> row;bool good=f.cursor->next(row);
        f.cursor->close();f.cursor->close();f.cursor->restart(RowContext{});
        const auto index=ordered(fixtures.back().keys,true,false);Expected wanted;
        for(size_t i=0;i<index.size();++i){const auto id=index[i];wanted.push_back({std::to_string(id+1),fixtures.back().keys[id]?std::optional<std::string>{std::to_string(*fixtures.back().keys[id])}:std::nullopt,std::to_string(i+1)});}
        return good && f.cursor->supportsRestart() && f.cursor->plan()==graph && values(consume(*f.cursor),wanted);});
    check("source primary exception survives a throwing close with one terminal cleanup",[&]{
        auto s=logical(db,"SELECT id,row_number() OVER(ORDER BY k) FROM wik_logical",basic(),costRows(8),3,true);
        std::string state,message;try{consume(*s.fixture.cursor);}catch(const DbError& error){state=error.sqlState();message=error.message();}
        s.fixture.cursor->close();s.fixture.cursor->close();
        return state=="58030" && message=="primary integer-key source failure" && s.probe->reads==3 && s.probe->closes==1;});
    check("reached record fallback error remains primary after integer comparisons",[&]{
        const QueryRowDescriptor descriptor{{"id","integer",false,0,23},{"p","integer",false,0,23},{"r","record",false,0,2249}};
        Rows data;for(size_t i=0;i<8;++i)data.push_back({ExprValue("integer",std::to_string(i+1)),ExprValue("integer","0"),ExprValue("record","(1)")});
        auto s=logical(db,"SELECT id,row_number() OVER(ORDER BY p,r) FROM wik_logical",descriptor,std::move(data));
        std::string state,message;try{consume(*s.fixture.cursor);}catch(const DbError& error){state=error.sqlState();message=error.message();}
        s.fixture.cursor->close();s.fixture.cursor->close();
        return state=="0A000" && message.find("record datum has no typed field descriptor")!=std::string::npos && s.probe->closes==1;});
    for(const auto& raw:std::vector<std::string>{"","+1"," 1","1 ","1x","2147483648"})
        check("uncompared single runtime integer datum remains lazy raw="+raw,[&]{
            auto s=logical(db,"SELECT id,row_number() OVER(ORDER BY k) FROM wik_logical",basic(),{{ExprValue("integer","1"),ExprValue("integer",raw)}});
            return values(consume(*s.fixture.cursor),{{"1","1"}}) && s.probe->reads==1 && s.probe->closes==1;});
    check("unique first key never demands malformed secondary integer comparisons",[&]{
        Rows data;for(int id:{5,1,4,2,3})data.push_back({ExprValue("integer",std::to_string(id)),ExprValue("integer","bad-later-key")});
        auto s=logical(db,"SELECT id,row_number() OVER(ORDER BY id,k) FROM wik_logical ORDER BY id",basic(),std::move(data));
        return values(consume(*s.fixture.cursor),{{"1","1"},{"2","2"},{"3","3"},{"4","4"},{"5","5"}});});
    check("integer-key sort preserves complete owning text record and OID datums",[&]{
        const QueryRowDescriptor descriptor{{"id","integer",false,0,23},{"k","integer",false,0,23},{"t","text",false,0,25},
            {"r","record",false,0,2249},{"a","regtype[]",false,0,2211},{"o","regtype",false,0,2206}};
        Rows data;for(size_t i=0;i<5;++i){ExprValue text("text",i==0?"":i==1?"NULL":"β row "+std::to_string(i));text.collation="c";if(i==2){text.value.clear();text.isNull=true;}
            ExprValue array("regtype[]",i==0?"{}":"{boolean,NULL,bigint}");if(i)array.elementOids={16,std::nullopt,20};if(i==2){array.value.clear();array.isNull=true;array.elementOids.clear();}
            ExprValue object("regtype",i%2?"bigint":"boolean");object.objectOid=i%2?20:16;if(i==2){object.value.clear();object.isNull=true;object.objectOid.reset();}
            data.push_back({ExprValue("integer",std::to_string(i+1)),ExprValue("integer",std::to_string(5-i)),text,record(text),array,object});}
        Rows wanted;for(size_t i=data.size();i>0;--i){auto row=data[i-1];row.push_back(ExprValue("bigint",std::to_string(data.size()-i+1)));wanted.push_back(std::move(row));}
        auto s=logical(db,"SELECT id,k,t,r,a,o,row_number() OVER(ORDER BY k,id) FROM wik_logical ORDER BY k,id",descriptor,data);
        const auto actual=consume(*s.fixture.cursor);s.fixture.cursor->restart(RowContext{});const auto restarted=consume(*s.fixture.cursor);
        return same(actual,wanted) && same(restarted,wanted) && s.probe->reads==10 && s.probe->closes==2;});

    // Compare actual public cursor requests, with identical source/receiver
    // rows and one untimed warm execution each. No private cache statistics,
    // allocation-size constants, or exact allocation counts form the oracle.
    constexpr size_t count=2048;const auto data=costRows(count);const auto wanted=costExpected(count);
    const std::string costSql="SELECT id,k,row_number() OVER(ORDER BY k,id) FROM wik_logical ORDER BY k,id";
    std::optional<Measurement> known,unknown,stale,domain;
    check("known builtin key reduces real cursor requests against unknown range OID",[&]{
        auto builtin=logical(db,costSql,basic(),data),unspecified=logical(db,costSql,basic(0),data);
        bool good=values(consume(*builtin.fixture.cursor),wanted) && values(consume(*unspecified.fixture.cursor),wanted);
        known=measure(*builtin.fixture.cursor);unknown=measure(*unspecified.fixture.cursor);printCost("builtin",*known);printCost("unknown",*unknown);
        return good && values(known->rows,wanted) && same(known->rows,unknown->rows) &&
            known->requests.calls+count<unknown->requests.calls && known->requests.bytes<unknown->requests.bytes;});
    check("authoritative range OID zero cannot be rescued by stale binding or output",[&]{
        auto query=logicalQuery(db,costSql,basic());
        bool good=query->sourceRanges.size()==1 && query->sourceRanges[0].columns[1].typeOid==23 && query->output[1].typeOid==23;
        const auto* select=static_cast<SelectStmt*>(query->ast.get());const auto projection=query->projectionBindings.find(select);
        good=projection!=query->projectionBindings.end() && projection->second.size()==3 && projection->second[1].column && projection->second[1].column->typeOid==23 && good;
        // SourceRange's optional type identity is authoritative even when a
        // public logical-source caller conservatively drops it after binding.
        // Original expression sites, bindings, output, and parameter owners
        // remain the real whole-bound graph, not replacement AST nodes.
        query->sourceRanges[0].columns[1].typeOid=0;
        auto s=fromContexts(db,query,data);good=values(consume(*s.fixture.cursor),wanted) && good;stale=measure(*s.fixture.cursor);printCost("range-zero-stale-output",*stale);
        return good && known && same(known->rows,stale->rows) && known->requests.calls+count<stale->requests.calls && known->requests.bytes<stale->requests.bytes;});
    check("actual catalog domain OID preserves fallback despite flattened integer name",[&]{
        const auto sql="SELECT id,k,row_number() OVER(ORDER BY k,id) FROM wik_domain_rows ORDER BY k,id";
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        if(query->sourceRanges.size()!=1 || query->sourceRanges[0].columns.size()!=2)return false;
        const auto column=query->sourceRanges[0].columns[1];const auto snapshot=g_engine.catalogService().metadataSnapshot(db);bool actualDomain=false;
        for(const auto& type:snapshot.types)if(type.oid==column.typeOid && type.typtype=='d' && type.typname=="wik_domain")actualDomain=true;
        auto s=fromContexts(db,query,data);bool good=actualDomain && column.type=="integer" && column.typeOid!=23 && column.typeOid!=21 && column.typeOid!=20 && values(consume(*s.fixture.cursor),wanted);
        domain=measure(*s.fixture.cursor);printCost("catalog-domain",*domain);
        return good && known && same(known->rows,domain->rows) && known->requests.calls+count<domain->requests.calls && known->requests.bytes<domain->requests.bytes;});
    check("missing column is rejected by whole binding rather than a fabricated unbound plan",[&]{
        std::string state;try{auto f=physical(db,"SELECT row_number() OVER(ORDER BY missing_window_key) FROM wik_i4");(void)f;}
        catch(const DbError& error){state=error.sqlState();}return state=="42703";});
    check("zero LIMIT does not read source or evaluate invalid window arguments",[&]{
        auto s=logical(db,"SELECT ntile(0) OVER(ORDER BY k) FROM wik_logical LIMIT 0",basic(),data);
        return consume(*s.fixture.cursor).empty() && s.probe->reads==0 && s.probe->eof==0;});
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;setCurrentSession(previous);
    if(controls!=36 || !dropped)++failures;
    std::cout<<"PREPARED_WINDOW_INTEGER_KEY_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}

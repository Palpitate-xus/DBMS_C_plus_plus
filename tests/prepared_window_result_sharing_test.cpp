#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "catalog/CatalogService.h"
#include "Session.h"
#include "test_utils.h"
#include <cstdlib>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

// Cumulative C++ new requests in a real warm cursor invocation. This is not
// live/peak storage, RSS, or a private producer/comparator callback counter.
namespace sharing_allocations {
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
void* operator new(size_t size){return sharing_allocations::allocate(size);}
void* operator new[](size_t size){return sharing_allocations::allocate(size);}
void operator delete(void* value)noexcept{std::free(value);}
void operator delete[](void* value)noexcept{std::free(value);}
void operator delete(void* value,size_t)noexcept{std::free(value);}
void operator delete[](void* value,size_t)noexcept{std::free(value);}
void* operator new(size_t size,std::align_val_t alignment){return sharing_allocations::aligned(size,static_cast<size_t>(alignment));}
void* operator new[](size_t size,std::align_val_t alignment){return sharing_allocations::aligned(size,static_cast<size_t>(alignment));}
void operator delete(void* value,std::align_val_t)noexcept{std::free(value);}
void operator delete[](void* value,std::align_val_t)noexcept{std::free(value);}
void operator delete(void* value,size_t,std::align_val_t)noexcept{std::free(value);}
void operator delete[](void* value,size_t,std::align_val_t)noexcept{std::free(value);}

namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
struct Fixture {std::shared_ptr<PreparedQuery> query;std::unique_ptr<PreparedQueryCursor> cursor;};
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
Fixture physical(const std::string& db,const std::string& sql) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
    return {query,QueryPlanner::makePreparedCursor(std::move(plan),query->output)};
}
Rows consume(PreparedQueryCursor& cursor) {
    Rows rows;std::vector<ExprValue> row;
    while(cursor.next(row))rows.push_back(row);
    cursor.close();cursor.close();return rows;
}
size_t nodes(Operator* root,const std::string& name) {
    std::vector<Operator*> pending{root};std::set<Operator*> visited;size_t result=0;
    while(!pending.empty()){auto* node=pending.back();pending.pop_back();if(!node || !visited.insert(node).second)continue;
        result+=node->preparedPlanNodeName()==name;
        for(auto* child:node->preparedPlanChildren())pending.push_back(child);
        if(auto* limit=dynamic_cast<LimitOp*>(node))pending.push_back(limit->child());
        if(auto* offset=dynamic_cast<OffsetOp*>(node))pending.push_back(offset->child());}
    return result;
}
constexpr size_t count=512;
std::optional<std::string> text(size_t id) {
    if(id%11==0)return std::nullopt;
    return id%13==0?std::string():id%17==0?std::string("NULL"):"payload-"+std::to_string(id);
}
bool ids(const Rows& rows,bool descending=false) {
    if(rows.size()!=count)return false;
    for(size_t i=0;i<count;++i)if(rows[i].empty() || rows[i][0].isNull ||
        rows[i][0].typeName!="integer" || rows[i][0].value!=std::to_string(descending?count-i:i+1))return false;
    return true;
}
bool duplicateCells(const Rows& rows,size_t columns) {
    if(!ids(rows))return false;
    for(const auto& row:rows){if(row.size()!=columns)return false;
        for(size_t i=2;i<columns;++i)if(!same(row[1],row[i]))return false;}
    return true;
}
void cost(const std::string& label,Fixture& fixture,const Rows& expected,bool& good) {
    sharing_allocations::Region region;
    fixture.cursor->restart(RowContext{});auto rows=consume(*fixture.cursor);const auto requests=region.finish();
    good=same(rows,expected) && good;
    std::cout<<"WINDOW_SHARING_ALLOCATION "<<label<<" calls="<<requests.calls<<" requested_bytes="<<requests.bytes<<'\n';
}
}

int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("prepared_window_result_sharing");cleanupTestDb("prepared_window_result_sharing");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);DdlExecutor ddl;
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action){++controls;bool good=false;
        try{good=action();}catch(const DbError& error){std::cerr<<"SQLSTATE="<<error.sqlState()<<" message="<<error.message()<<'\n';}
        catch(const std::exception& error){std::cerr<<"exception="<<error.what()<<'\n';}
        good=good && StorageEngine::captureNullRowBinding().engine==nullptr;failures+=!good;
        std::cout<<"PREPARED_WINDOW_RESULT_SHARING "<<controls<<' '<<label<<" good="<<good<<'\n';};
    bool setup=!ddl.executeSql("CREATE TABLE wps_rows(id INTEGER,k INTEGER,t TEXT COLLATE \"C\")",session);
    for(size_t id=1;id<=count;++id)setup=(g_engine.insertRow(db,"wps_rows",{{"id",std::to_string(id)},
        {"k",std::to_string(id%7)},{"t",text(id)}})==DBStatus::OK) && setup;
    if(!setup){setCurrentSession(previous);return 2;}
    for(size_t copies:{size_t{1},size_t{2},size_t{4}})check("pure LAG copies "+std::to_string(copies)+" retain values and warm cursor costs",[&]{
        std::string sql="SELECT id";for(size_t i=0;i<copies;++i)sql+=",lag(length(t),0) OVER(ORDER BY id) AS v"+std::to_string(i);
        sql+=" FROM wps_rows ORDER BY id";auto f=physical(db,sql);const auto rows=consume(*f.cursor);
        bool good=duplicateCells(rows,copies+1) && nodes(f.cursor->plan(),"WindowAgg")==1;
        for(size_t i=0;i<rows.size();++i){const auto value=text(i+1);good=(rows[i][1].isNull==!value &&
            (!value || rows[i][1].value==std::to_string(value->size()))) && good;}
        cost("lag-"+std::to_string(copies),f,rows,good);return good;});
    check("aggregate aliases preserve NULLs and cumulative frame results",[&]{
        auto f=physical(db,"SELECT id,sum(k) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) AS a,sum(k) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) AS b FROM wps_rows ORDER BY id");
        const auto rows=consume(*f.cursor);bool good=duplicateCells(rows,3);int64_t sum=0;
        for(size_t i=0;i<rows.size();++i){sum+=(i+1)%7;good=rows[i][1].value==std::to_string(sum) && good;}
        cost("sum-2",f,rows,good);return good;});
    check("broadcast preserves complete nested record and referenced object identity",[&]{
        const auto call="lag(ROW('boolean'::regtype,ROW(t,k)),0) OVER(ORDER BY id)";
        auto f=physical(db,std::string("SELECT id,")+call+" AS a,"+call+" AS b FROM wps_rows ORDER BY id");
        const auto rows=consume(*f.cursor);if(!duplicateCells(rows,3))return false;
        for(size_t i=0;i<rows.size();++i){const auto& record=rows[i][1];if(!record.recordFields || record.recordFields->size()!=2)return false;
            const auto& object=record.recordFields->at(0);const auto& nested=record.recordFields->at(1);
            if(object.objectOid!=16 || !nested.recordFields || nested.recordFields->size()!=2)return false;
            const auto& actual=nested.recordFields->at(0);const auto wanted=text(i+1);
            if(actual.isNull!=!wanted || (wanted && actual.value!=*wanted) || nested.recordFields->at(1).value!=std::to_string((i+1)%7))return false;}
        return true;});
    check("broadcast preserves array element OIDs including true NULL",[&]{
        const auto call="lag(ARRAY['boolean'::regtype,NULL::regtype,'bigint'::regtype],0) OVER(ORDER BY id)";
        auto f=physical(db,std::string("SELECT id,")+call+" AS a,"+call+" AS b FROM wps_rows ORDER BY id");
        const auto rows=consume(*f.cursor);if(!duplicateCells(rows,3))return false;
        for(const auto& row:rows)if(row[1].elementOids!=std::vector<std::optional<uint32_t>>{16,std::nullopt,20})return false;
        return true;});
    check("real graph restart and early explicit close do not retain row values",[&]{
        auto f=physical(db,"SELECT id,lag(t,0) OVER(ORDER BY id),lag(t,0) OVER(ORDER BY id) FROM wps_rows ORDER BY id");
        auto* graph=f.cursor->plan();const auto first=consume(*f.cursor);if(!duplicateCells(first,3))return false;
        f.cursor->restart(RowContext{});std::vector<ExprValue> row;
        for(size_t i=0;i<19;++i)if(!f.cursor->next(row))return false;
        f.cursor->close();f.cursor->close();f.cursor->restart(RowContext{});
        return f.cursor->plan()==graph && same(first,consume(*f.cursor));});
    check("distinct frame/window declarations retain independent results",[&]{
        auto f=physical(db,"SELECT id,lag(k,0) OVER wa,lag(k,1,-1) OVER wb FROM wps_rows WINDOW wa AS (ORDER BY id),wb AS (ORDER BY id) ORDER BY id");
        const auto rows=consume(*f.cursor);if(!ids(rows) || nodes(f.cursor->plan(),"WindowAgg")!=2)return false;
        for(size_t i=0;i<rows.size();++i)if(rows[i].size()!=3 || rows[i][1].value!=std::to_string((i+1)%7) ||
            rows[i][2].value!=(i?std::to_string(i%7):"-1"))return false;return true;});
    check("pruned zero LIMIT avoids all bad window argument demand",[&]{
        auto f=physical(db,"SELECT ntile(0) OVER(ORDER BY id),ntile(0) OVER(ORDER BY id) FROM wps_rows LIMIT 0");
        return consume(*f.cursor).empty();});
    check("runtime argument error retains exact preceding rows on restart",[&]{
        auto f=physical(db,"SELECT id,lag(1/(id-3),0) OVER(ORDER BY id),lag(1/(id-3),0) OVER(ORDER BY id) FROM wps_rows ORDER BY id");
        auto* graph=f.cursor->plan();bool good=f.cursor->descriptor().size()==3;Rows saved;
        for(const auto& column:f.cursor->descriptor())good=column.typeOid==23 && good;
        const Rows expected{{ExprValue("integer","1"),ExprValue("integer","0"),ExprValue("integer","0")},
                            {ExprValue("integer","2"),ExprValue("integer","-1"),ExprValue("integer","-1")}};
        for(size_t attempt=0;attempt<2;++attempt){if(attempt)f.cursor->restart(RowContext{});Rows rows;std::vector<ExprValue> row;std::string state;
            try{while(f.cursor->next(row))rows.push_back(row);}catch(const DbError& error){state=error.sqlState();}
            f.cursor->close();good=state=="22012" && same(rows,expected) && good;
            if(!attempt)saved=rows;else good=same(saved,rows) && good;}
        return good && f.cursor->plan()==graph;});
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;setCurrentSession(previous);
    if(controls!=10 || !dropped)++failures;
    std::cout<<"PREPARED_WINDOW_RESULT_SHARING_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}

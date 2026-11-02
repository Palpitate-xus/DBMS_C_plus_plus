// test_link_flags: -Wl,--wrap=mkstemp
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "Config.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <numeric>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace allocation_probe {
struct Counts { size_t calls=0,bytes=0,deletes=0,payload=0,identities=0; };
struct Tracker {
    Counts total,beforeEof;
    size_t payloadRequest=0,identityRequest=0;
    bool sawEof=false,overflow=false;
};
thread_local Tracker* active=nullptr;
void account(size_t bytes) noexcept {
    auto* trace=active;if(!trace)return;
    auto& total=trace->total;++total.calls;
    if(bytes>std::numeric_limits<size_t>::max()-total.bytes)trace->overflow=true;
    else total.bytes+=bytes;
    total.payload+=trace->payloadRequest && bytes==trace->payloadRequest;
    total.identities+=trace->identityRequest && bytes==trace->identityRequest;
}
void* allocate(size_t bytes,size_t alignment=alignof(std::max_align_t)) {
    void* result=nullptr;
    if(alignment<=alignof(std::max_align_t))result=std::malloc(bytes?bytes:1);
    else if(::posix_memalign(&result,alignment,bytes?bytes:1))result=nullptr;
    if(!result)throw std::bad_alloc();
    account(bytes);return result;
}
void release(void* value) noexcept {
    if(value && active)++active->total.deletes;
    std::free(value);
}
Counts difference(const Counts& a,const Counts& b) {
    return {a.calls-b.calls,a.bytes-b.bytes,a.deletes-b.deletes,a.payload-b.payload,a.identities-b.identities};
}
}
void* operator new(size_t n){return allocation_probe::allocate(n);}
void* operator new[](size_t n){return allocation_probe::allocate(n);}
void operator delete(void* p) noexcept{allocation_probe::release(p);}
void operator delete[](void* p) noexcept{allocation_probe::release(p);}
void operator delete(void* p,size_t) noexcept{allocation_probe::release(p);}
void operator delete[](void* p,size_t) noexcept{allocation_probe::release(p);}
void* operator new(size_t n,const std::nothrow_t&) noexcept{try{return ::operator new(n);}catch(...){return nullptr;}}
void* operator new[](size_t n,const std::nothrow_t&) noexcept{try{return ::operator new[](n);}catch(...){return nullptr;}}
void operator delete(void* p,const std::nothrow_t&) noexcept{allocation_probe::release(p);}
void operator delete[](void* p,const std::nothrow_t&) noexcept{allocation_probe::release(p);}
void* operator new(size_t n,std::align_val_t a){return allocation_probe::allocate(n,static_cast<size_t>(a));}
void* operator new[](size_t n,std::align_val_t a){return allocation_probe::allocate(n,static_cast<size_t>(a));}
void operator delete(void* p,std::align_val_t) noexcept{allocation_probe::release(p);}
void operator delete[](void* p,std::align_val_t) noexcept{allocation_probe::release(p);}
void operator delete(void* p,size_t,std::align_val_t) noexcept{allocation_probe::release(p);}
void operator delete[](void* p,size_t,std::align_val_t) noexcept{allocation_probe::release(p);}
void* operator new(size_t n,std::align_val_t a,const std::nothrow_t&) noexcept{try{return ::operator new(n,a);}catch(...){return nullptr;}}
void* operator new[](size_t n,std::align_val_t a,const std::nothrow_t&) noexcept{try{return ::operator new[](n,a);}catch(...){return nullptr;}}
void operator delete(void* p,std::align_val_t,const std::nothrow_t&) noexcept{allocation_probe::release(p);}
void operator delete[](void* p,std::align_val_t,const std::nothrow_t&) noexcept{allocation_probe::release(p);}

namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
size_t sortCreates=0;
struct Case {const char* name;size_t count,width,oids;bool integer=false,offset=false,spill=false;};
bool equal(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields) {
        if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!equal(a.recordFields->at(i),b.recordFields->at(i)))return false;
    }
    return true;
}
ExprPtr parameter(size_t slot,const char* type) {
    auto value=std::make_unique<ParameterExpr>();value->slot=slot;value->declaredType=type;
    value->origin=ParameterOrigin::RuntimeCell;return value;
}
ExprValue nested(size_t width,size_t oidCount,size_t epoch) {
    ExprValue payload("text",std::string(width,static_cast<char>('a'+epoch)));payload.collation="c";
    ExprValue array("regtype[]","{");array.elementOids.reserve(oidCount);
    for(size_t i=0;i<oidCount;++i) {
        if(i)array.value+=',';
        if(i%7==0){array.value+="NULL";array.elementOids.push_back(std::nullopt);}
        else if((i+epoch)%2){array.value+="boolean";array.elementOids.push_back(16);}
        else {array.value+="bigint";array.elementOids.push_back(20);}
    }
    array.value+='}';
    RowExpr expression;expression.constructor=true;expression.fieldTypes={"record","text","regtype[]"};
    auto empty=std::make_unique<RowExpr>();empty->constructor=true;
    expression.elements.push_back(std::move(empty));expression.elements.push_back(parameter(0,"text"));
    expression.elements.push_back(parameter(1,"regtype[]"));
    std::vector<ExprValue> values;values.push_back(std::move(payload));values.push_back(std::move(array));
    RowContext context;context.setParameters(std::move(values));ExprEvaluator evaluator;
    return evaluator.eval(&expression,context);
}
struct Data {
    Case config;
    Rows rows[2];
    size_t epoch=0,read=0,eof=0;
    allocation_probe::Tracker* trace=nullptr;
    bool corruptWidth=false;
    int rank(size_t id) const {const auto raw=static_cast<int>((id*73)%131);return epoch?131-raw:raw;}
    void rebuild(size_t nextEpoch) {
        epoch=nextEpoch;read=eof=0;rows[0].clear();rows[1].clear();
        const auto tail=nested(config.width,config.oids,epoch);
        RowExpr record;record.constructor=true;record.fieldTypes={"integer","record"};
        record.elements.push_back(parameter(0,"integer"));record.elements.push_back(parameter(1,"record"));
        ExprEvaluator evaluator;
        for(size_t id=0;id<config.count;++id) {
            ExprValue key;
            if(config.integer)key=ExprValue("bigint",std::to_string(rank(id)));
            else {
                std::vector<ExprValue> values;values.emplace_back("integer",std::to_string(rank(id)));values.push_back(tail);
                RowContext context;context.setParameters(std::move(values));key=evaluator.eval(&record,context);
            }
            ExprValue payload("text",config.integer?std::string(config.width,static_cast<char>('p'+epoch)):
                "row_"+std::to_string(id));payload.collation="c";
            rows[id%2].push_back({ExprValue("integer",std::to_string(id)),std::move(key),std::move(payload)});
        }
    }
    const std::vector<ExprValue>& row(size_t id) const{return rows[id%2].at(id/2);}
};
QueryRowDescriptor descriptor(bool integer) {
    return {{"id","integer",false,0,23},{"k",integer?"bigint":"record",false,0,integer?20U:2249U},
        {"payload","text",false,0,25}};
}
struct Fixture {std::shared_ptr<Data> data;std::shared_ptr<PreparedQuery> query;OpPtr plan;};
Fixture fixture(const std::string& db,Case config) {
    auto data=std::make_shared<Data>();data->config=config;data->rebuild(0);
    const auto output=descriptor(config.integer);
    QueryBindingMetadata metadata;
    metadata.relation=[output](const std::string& name){return QueryRelationMetadata{"",name,output,{}};};
    const auto sql=std::string("SELECT id,k,payload FROM provided_left UNION ALL SELECT id,k,payload FROM provided_right ORDER BY k")+
        (config.offset?" LIMIT 7 OFFSET 31":"");
    auto query=std::make_shared<PreparedQuery>(prepareQuery(sql,{},metadata));
    auto* root=static_cast<SelectStmt*>(query->ast.get());
    const auto branch=[data,query,output,db](SelectStmt* select,bool body,QueryPlanner::PreparedExecutionProvider owner) {
        const size_t side=select->fromClause->tableName=="provided_left"?0:1;
        // A real public typed provider owns these cells. There is no heap,
        // engine/RID provenance, temporary relation or replacement SQL.
        auto source=std::make_unique<PreparedSourceRowsOp>(output,
            [data,side](size_t index,std::vector<ExprValue>& row) {
                if(index==data->rows[side].size()) {
                    ++data->eof;
                    if(data->eof==2 && data->trace) {
                        data->trace->beforeEof=data->trace->total;data->trace->sawEof=true;
                    }
                    return false;
                }
                ++data->read;row=data->rows[side].at(index);
                if(data->corruptWidth && side==0 && index==24)row.pop_back();
                return true;
            });
        TableSchema schema;
        for(const auto& field:output){Column column;column.dataName=field.name;column.dataType=field.type;
            column.isVariableLength=field.type=="record" || field.type=="text";schema.append(column);}
        return QueryPlanner::buildPreparedSelectPlan(&g_engine,db,query,select,schema,std::move(source),{},{},{},false,false,std::move(owner),body);
    };
    auto plan=QueryPlanner::buildPreparedUnionAllPlan(&g_engine,db,query,root,branch,{},{},false,false);
    return {std::move(data),std::move(query),std::move(plan)};
}
bool noSortDescriptors() {
    for(const auto& item:std::filesystem::directory_iterator("/proc/self/fd")) {
        char name[4096];const auto length=::readlink(item.path().c_str(),name,sizeof(name)-1);
        if(length<0)continue;
        name[length]='\0';if(std::strstr(name,"/.dbms-sort-"))return false;
    }
    return true;
}
void counters(const char* label,const allocation_probe::Counts& counts) {
    std::cout<<' '<<label<<"_calls="<<counts.calls<<' '<<label<<"_bytes="<<counts.bytes
        <<' '<<label<<"_deletes="<<counts.deletes<<' '<<label<<"_payload_requests="<<counts.payload
        <<' '<<label<<"_oid_requests="<<counts.identities;
}
bool run(Fixture& item,size_t epoch) {
    using namespace allocation_probe;
    item.data->rebuild(epoch);std::vector<size_t> ids(item.data->config.count);std::iota(ids.begin(),ids.end(),0);
    std::sort(ids.begin(),ids.end(),[&](size_t a,size_t b){return item.data->rank(a)<item.data->rank(b);});
    const auto begin=item.data->config.offset?std::min(size_t(31),ids.size()):0;
    const auto end=item.data->config.offset?std::min(begin+7,ids.size()):ids.size();
    Tracker trace;trace.payloadRequest=item.data->config.width?item.data->config.width+1:0;
    trace.identityRequest=item.data->config.oids*sizeof(std::optional<uint32_t>);
    item.data->trace=&trace;sortCreates=0;
    const auto* graph=item.plan.get();std::string display;bool opened=false,present=false;
    active=&trace;
    try {opened=item.plan->open();if(opened)present=item.plan->next(display);}
    catch(...){active=nullptr;item.data->trace=nullptr;throw;}
    active=nullptr;item.data->trace=nullptr;
    size_t position=begin;
    bool good=opened && trace.sawEof && !trace.overflow && item.data->read==ids.size() && item.data->eof==2 && (item.data->config.spill?sortCreates>0:sortCreates==0);
    while(present) {
        std::vector<ExprValue> row;
        const bool typed=item.plan->lastStructuredValues(row);
        if(position>=end){good=false;break;}
        const auto& wanted=item.data->row(ids[position++]);
        if(!typed || row.size()!=wanted.size())good=false;
        else for(size_t i=0;i<row.size();++i)good=equal(row[i],wanted[i]) && good;
        present=item.plan->next(display);
    }
    good=good && position==end && !item.plan->hasError();
    item.plan->close();item.plan->close();good=good && item.plan.get()==graph && noSortDescriptors();
    const bool metadataGood=good;
    // Fixed real-operator resource budgets, supported by before/after
    // allocation receipts. They include producer/startup/display costs,
    // allow substantial headroom, and make no elapsed-time assertion.
    bool costGood=true;
    if(item.data->config.integer && !item.data->config.spill)
        costGood=trace.total.calls<=14000 && trace.total.bytes<=768*1024;
    if(!std::strcmp(item.data->config.name,"record_wider_oids"))
        costGood=trace.total.bytes<=200*1024*1024;
    good=metadataGood && costGood;
    std::cout<<"SET_COMPARISON_ALLOCATION case="<<item.data->config.name<<" epoch="<<epoch
        <<" input="<<ids.size()<<" output="<<position-begin<<" width="<<item.data->config.width
        <<" oid_elements="<<item.data->config.oids<<" metadata_good="<<metadataGood
        <<" cost_good="<<costGood<<" sort_files="<<sortCreates;
    counters("whole",trace.total);counters("before_eof",trace.beforeEof);counters("after_eof",difference(trace.total,trace.beforeEof));
    std::cout<<'\n';return good;
}
}
extern "C" int __real_mkstemp(char*);
extern "C" int __wrap_mkstemp(char* name) {
    if(!std::strncmp(name,".dbms-sort-",11))++sortCreates;
    return __real_mkstemp(name);
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();const auto db=testDbPath("set_sort_key_cache");
    if(g_engine.createDatabase(db)!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);session.workMem.assign(65536,false);
    const std::vector<Case> cases{
        {"int_full",128,0,0,true,false},{"int_offset",128,0,0,true,true},
        {"record_single",1,4093,257,false,false},{"record_text",128,4093,0,false,false},
        {"record_text_oids",128,4093,257,false,false},{"record_wider_oids",128,8191,257,false,false},
        {"record_offset",128,4093,257,false,true},{"record_single_offset",1,4093,257,false,true},
        {"integer_spill",128,1531,0,true,false,true}};
    size_t checked=0,failures=0;
    for(const auto& config:cases) {
        try {session.workMem.assign(config.spill?64:65536,false);auto item=fixture(db,config);
            for(size_t epoch=0;epoch<2;++epoch){++checked;failures+=!run(item,epoch);}}
        catch(const DbError& error){++failures;std::cout<<"SET_COMPARISON_ALLOCATION_ERROR case="<<config.name<<" state="<<error.sqlState()<<" message="<<error.what()<<'\n';}
        catch(const std::exception& error){++failures;std::cout<<"SET_COMPARISON_ALLOCATION_ERROR case="<<config.name<<" message="<<error.what()<<'\n';}
    }
    ++checked;
    try {
        session.workMem.assign(64,false);
        auto item=fixture(db,{"descriptor_error",128,8191,0,true,false,true});
        item.data->corruptWidth=true;sortCreates=0;
        bool good=item.plan->open();std::string display,state;
        try{if(item.plan->next(display))good=false;}
        catch(const DbError& error){state=error.sqlState();}
        // A real provider violates its declared row width only after enough
        // input has caused actual spill. This checks public descriptor/error
        // cleanup; it does not reconstruct or imitate a private tape header.
        good=good && state=="XX000" && sortCreates>0 && noSortDescriptors();
        good=!item.plan->next(display) && good;
        item.plan->close();item.plan->close();good=good && noSortDescriptors();
        failures+=!good;
        std::cout<<"SET_KEY_CACHE_DESCRIPTOR metadata_good="<<good<<" state="<<state
            <<" sort_files="<<sortCreates<<'\n';
    } catch(const DbError& error){++failures;std::cout<<"SET_KEY_CACHE_DESCRIPTOR_ERROR state="<<error.sqlState()<<'\n';}
    setCurrentSession(previous);if(g_engine.dropDatabase(db)!=DBStatus::OK)++failures;
    if(checked!=19)++failures;
    std::cout<<"SET_SORT_KEY_CACHE_COMPLETE "<<checked<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}

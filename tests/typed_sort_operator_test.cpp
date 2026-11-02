// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite
#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "expression/prepared_query_execution.h"
#include "catalog/systables.h"
#include "Config.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <malloc.h>
#include <numeric>
#include <set>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
constexpr size_t streamCount=192, width=8192;
constexpr int64_t keyBase=9007199254740993LL;
size_t liveBytes(){const auto memory=mallinfo2();return memory.uordblks+memory.hblkhd;}
struct Observation {
    size_t creates=0,maxFds=0,reads=0,writes=0,baseline=0,peak=0;
    void sample(){const auto now=liveBytes();peak=std::max(peak,now>baseline?now-baseline:0);}
} observed;
bool sortFile(int fd){
    char path[64],name[4096];std::snprintf(path,sizeof(path),"/proc/self/fd/%d",fd);
    const auto size=::readlink(path,name,sizeof(name)-1);if(size<0)return false;
    name[size]='\0';return std::strstr(name,"/.dbms-sort-")!=nullptr;
}
size_t descriptors(){
    size_t count=0;for(const auto& entry:std::filesystem::directory_iterator("/proc/self/fd"))
        count+=sortFile(std::stoi(entry.path().filename().string()));
    return count;
}
bool cleaned(){
    if(descriptors())return false;
    for(const auto& entry:std::filesystem::directory_iterator("."))
        if(entry.path().filename().string().rfind(".dbms-sort-",0)==0)return false;
    return true;
}
bool equal(const ExprValue& a,const ExprValue& b){
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields){
        if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!equal(a.recordFields->at(i),b.recordFields->at(i)))return false;
    }
    return true;
}
bool equal(const std::vector<ExprValue>& a,const std::vector<ExprValue>& b){
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i)if(!equal(a[i],b[i]))return false;
    return true;
}
ExprValue identity(size_t id){
    ExprValue value("regtype",id%2?"bigint":"integer");value.objectOid=id%2?20:23;return value;
}
std::vector<ExprValue> cells(size_t id,const std::string& keyType="bigint",bool right=false){
    const auto key=keyBase+int64_t(id%17);
    ExprValue text("text",id%11==0?"":id%11==1?"":id%11==2?"NULL":
        "row "+std::to_string(id)+"\nβ "+std::string(width,'a'+id%23),id%11==0);
    if(right){text.value="rhs "+std::to_string(id)+" "+std::string(width/2,'A'+id%23);text.isNull=false;}
    text.collation="C";
    auto oid=identity(id);if(id%19==0){oid.value.clear();oid.isNull=true;oid.objectOid.reset();}
    ExprValue array("regtype[]","{integer,NULL,text}");array.elementOids={23,std::nullopt,25};
    if(id%13==0){array.value.clear();array.isNull=true;array.elementOids.clear();}
    ExprValue record("record","("+std::to_string(id)+",integer,(x))");
    if(id%4==0){record.value="()";record.recordFields.emplace();}
    else if(id%4==1){
        ExprValue nested("record","(x,())");
        ExprValue empty("record","()");empty.recordFields.emplace();
        ExprValue leaf("text","x");leaf.collation="C";
        nested.recordFields=std::vector<ExprValue>{leaf,empty};
        record.recordFields=std::vector<ExprValue>{ExprValue("integer",std::to_string(id)),identity(id),nested};
    }else if(id%4==2){record.value.clear();record.isNull=true;}
    // id%4==3 retains an absent structural record snapshot, independently of
    // the engaged empty ROW() snapshot above. No text reparse may invent one.
    return {ExprValue("integer",std::to_string(id+(right?10000:0))),
        ExprValue(keyType,std::to_string(key)+(keyType=="numeric"?".000":""),id%7==0),
        std::move(text),std::move(oid),std::move(array),std::move(record)};
}
QueryRowDescriptor descriptor(const std::string& keyType){
    return {{"id","integer",false,0,23},{"k",keyType,false,0,keyType=="numeric"?1700U:20U},
        {"payload","text",false,0,25},{"o","regtype",false,0,2206},
        {"arr","regtype[]",false,0,2211},{"rec","record",false,0,2249}};
}
Operator* sorted(Operator* plan){
    if(!plan)return nullptr;
    if(plan->preparedPlanNodeName()=="TypedSort")return plan;
    // These public physical wrappers expose child(), while prepared nodes
    // expose preparedPlanChildren(). Neither interface may be assumed to
    // substitute for the other when observing the actual operator graph.
    if(auto* limit=dynamic_cast<LimitOp*>(plan))return sorted(limit->child());
    if(auto* offset=dynamic_cast<OffsetOp*>(plan))return sorted(offset->child());
    for(auto* child:plan->preparedPlanChildren())if(auto* result=sorted(child))return result;
    return nullptr;
}
struct Probe {size_t reads=0,eof=0,closed=0,rebound=0,retained=0;};
struct Fixture {
    std::shared_ptr<PreparedQuery> query;
    OpPtr plan;
    std::shared_ptr<Probe> probe;
    size_t a=0,b=0;
    bool contexts=false;
    std::string keyType;
};
Fixture stream(const std::string& db,const std::string& suffix,bool contexts=false,const std::string& keyType="bigint"){
    QueryBindingMetadata metadata;const auto columns=descriptor(keyType);
    metadata.relation=[columns](const std::string& name){return QueryRelationMetadata{"public",name,columns,{}};};
    std::vector<QueryBindingDatum> parameters;
    if(contexts){QueryBindingDatum parameter{"typed-sort-runtime-p","p","regtype",{},true,"integer",1};
        parameter.origin=ParameterOrigin::RuntimeCell;parameters.push_back(std::move(parameter));}
    const auto sql=contexts?
        "SELECT a.id,a.k,a.payload,a.o,a.arr,a.rec,b.id,b.payload,p FROM source_rows a "
        "JOIN source_rows b ON a.id+10000=b.id "+suffix:
        "SELECT id,k,payload,o,arr,rec FROM source_rows "+suffix;
    auto query=std::make_shared<PreparedQuery>(prepareQuery(sql,parameters,metadata));
    auto* select=static_cast<SelectStmt*>(query->ast.get());
    auto probe=std::make_shared<Probe>();
    auto binding=std::make_shared<PreparedQueryExecution>(query,&g_engine,db);
    size_t a=query->sourceRanges.front().ordinal,b=a;
    if(contexts)for(const auto& range:query->sourceRanges){if(range.name=="a")a=range.ordinal;if(range.name=="b")b=range.ordinal;}
    OpPtr source;TableSchema schema;
    const auto before=[probe](size_t position){
        if(position==0){observed.baseline=liveBytes();observed.peak=0;}
        observed.sample();
        if(position==streamCount){++probe->eof;const auto now=liveBytes();probe->retained=now>observed.baseline?now-observed.baseline:0;return false;}
        ++probe->reads;return true;
    };
    if(contexts){
        source=std::make_unique<PreparedSourceContextsOp>(
            [before,binding,a,b,keyType](size_t position,RowContext& row){
                if(!before(position))return false;
                row=RowContext{};
                binding->setSourceRow(row,a,cells(position,keyType));
                binding->setSourceRow(row,b,cells(position,keyType,true));
                row.set("legacy",identity(position));
                row.setSqlValue(FunctionCallExpr::SqlValue::CurrentUser,ExprValue("name","caller_"+std::to_string(position)));
                row.setParameters({identity(position)});return true;
            },[probe]{++probe->closed;},[probe](const RowContext&){++probe->rebound;});
    }else{
        for(const auto& output:columns){Column column;column.dataName=output.name;column.dataType=output.type;
            column.isVariableLength=true;column.collation=output.name=="payload"?"C":"";schema.append(column);}
        // This genuine ordinal source owns typed cells; schema is its copied
        // descriptor, not a fabricated heap layout or stored engine/RID.
        source=std::make_unique<PreparedSourceRowsOp>(columns,
            [before,keyType](size_t position,std::vector<ExprValue>& row){
                if(!before(position))return false;
                row=cells(position,keyType);return true;
            });
    }
    auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,db,query,select,schema,std::move(source));
    return {std::move(query),std::move(plan),std::move(probe),a,b,contexts,keyType};
}
std::vector<size_t> expected(bool ascending,bool first,size_t offset=0,size_t limit=streamCount){
    std::vector<size_t> result(streamCount);std::iota(result.begin(),result.end(),0);
    std::sort(result.begin(),result.end(),[&](size_t a,size_t b){
        const bool an=a%7==0,bn=b%7==0;if(an!=bn)return an==first;
        if(!an && a%17!=b%17)return ascending?a%17<b%17:a%17>b%17;
        return a>b; // Every SQL control has id DESC as its genuine second key.
    });
    const auto begin=std::min(offset,result.size()),end=begin+std::min(limit,result.size()-begin);
    return {result.begin()+begin,result.begin()+end};
}
bool consume(Fixture& fixture,const std::vector<size_t>& ids,bool requireSpill,bool boundMemory=true,bool opened=false){
    auto* sort=sorted(fixture.plan.get());bool good=sort && (opened || fixture.plan->open());
    if(requireSpill)good=good && observed.creates>0 && descriptors()==1;
    else good=good && observed.creates==0 && descriptors()==0;
    std::string display;size_t position=0;
    while(fixture.plan->next(display)){
        if(position>=ids.size()){good=false;break;}
        const auto id=ids[position++];auto wanted=cells(id,fixture.keyType);
        if(fixture.contexts){const auto rhs=cells(id,fixture.keyType,true);wanted.push_back(rhs[0]);wanted.push_back(rhs[2]);wanted.push_back(identity(id));}
        std::vector<ExprValue> actual;good=fixture.plan->lastStructuredValues(actual) && equal(actual,wanted) && good;
        if(fixture.contexts){
            RowContext row;good=sort->lastPreparedContext(row) && good;
            const auto legacy=row.get("legacy"),user=row.sqlValue(FunctionCallExpr::SqlValue::CurrentUser);
            good=good && legacy && equal(*legacy,identity(id)) && user && user->value=="caller_"+std::to_string(id) &&
                row.parameterCount()==1 && equal(row.parameter(0),identity(id));
            const auto lhs=cells(id,fixture.keyType),rhs=cells(id,fixture.keyType,true);
            for(size_t column=0;column<lhs.size();++column)
                good=equal(row.boundColumn(fixture.a,column),lhs[column]) &&
                    equal(row.boundColumn(fixture.b,column),rhs[column]) && good;
        }
    }
    good=good && position==ids.size() && !fixture.plan->hasError() && observed.maxFds<=2;
    if(boundMemory && requireSpill)good=good && fixture.probe->retained<1024*1024 && observed.peak<1024*1024;
    fixture.plan->close();fixture.plan->close();return good && cleaned();
}
std::string values(size_t count){
    std::string result="VALUES ";
    for(size_t id=0;id<count;++id){if(id)result+=',';
        result+='('+std::to_string(id)+','+std::to_string(id%5)+",'"+std::string(4096,'a'+id%23)+"'::text)";}
    return result;
}
OpPtr actualPlan(const std::string& db,const std::string& sql,std::shared_ptr<PreparedQuery>& query){
    query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
    return QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
}
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __wrap_mkstemp(char* name){
    if(std::strncmp(name,".dbms-sort-",11))return __real_mkstemp(name);
    ++observed.creates;const auto fd=__real_mkstemp(name);observed.maxFds=std::max(observed.maxFds,descriptors());return fd;
}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset){
    if(sortFile(fd)){++observed.reads;observed.sample();}return __real_pread(fd,data,size,offset);
}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset){
    if(sortFile(fd)){++observed.writes;observed.sample();}return __real_pwrite(fd,data,size,offset);
}

int main(){
    TypeRegistry::instance().bootstrap();const auto db=testDbPath("typed_sort_operator");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    const auto originalBudget=g_config.workMemKb;g_config.workMemKb=64;
    size_t checked=0,failures=0;
    const auto check=[&](const char* label,const auto& action){
        ++checked;observed={};observed.baseline=liveBytes();bool good=false;
        try{good=action();}catch(const std::exception& error){std::cerr<<"TYPED_SORT_OPERATOR_EXCEPTION "<<label<<" : "<<error.what()<<'\n';}
        failures+=!good;std::cout<<"TYPED_SORT_OPERATOR "<<label<<" passed="<<good<<" creates="<<observed.creates
            <<" maxfds="<<observed.maxFds<<" reads="<<observed.reads<<" writes="<<observed.writes<<" peak="<<observed.peak<<'\n';
    };
    for(bool asc:{false,true})for(bool first:{false,true})
        check("SourceRows multikey bigint/NULL/identity/record spill",[&]{
            session.workMem.assign(64,false);
            auto fixture=stream(db,"ORDER BY k "+std::string(asc?"ASC":"DESC")+" NULLS "+(first?"FIRST":"LAST")+",id DESC");
            const bool good=consume(fixture,expected(asc,first),true);
            return good && fixture.probe->reads==streamCount && fixture.probe->eof==1;
        });
    check("SourceRows high budget is a genuine no-spill control",[&]{
        session.workMem.assign(65536,false);auto fixture=stream(db,"ORDER BY k ASC NULLS LAST,id DESC");
        return consume(fixture,expected(true,false),false,false);
    });
    check("SourceRows large LIMIT still obeys byte budget",[&]{
        session.workMem.assign(64,false);auto fixture=stream(db,"ORDER BY k ASC NULLS LAST,id DESC LIMIT 1000");
        return consume(fixture,expected(true,false),true) && fixture.probe->reads==streamCount;
    });
    check("SourceRows TopN/OFFSET full metadata and full key demand",[&]{
        session.workMem.assign(64,false);auto fixture=stream(db,"ORDER BY k DESC NULLS FIRST,id DESC LIMIT 7 OFFSET 3");
        return consume(fixture,expected(false,true,3,7),true) && fixture.probe->reads==streamCount;
    });
    check("LIMIT zero opens no source or tape",[&]{
        auto fixture=stream(db,"ORDER BY k ASC NULLS LAST,id DESC LIMIT 0");
        return consume(fixture,{},false) && fixture.probe->reads==0 && fixture.probe->eof==0;
    });
    check("SourceContexts real JOIN bindings preserve all four context maps",[&]{
        auto fixture=stream(db,"ORDER BY a.k DESC NULLS FIRST,a.id DESC",true);
        return consume(fixture,expected(false,true),true) && fixture.probe->closed==1;
    });
    check("SourceContexts TopN preserves runtime typed parameter snapshots",[&]{
        auto fixture=stream(db,"ORDER BY a.k ASC NULLS LAST,a.id DESC LIMIT 7 OFFSET 3",true);
        return consume(fixture,expected(true,false,3,7),true) && fixture.probe->reads==streamCount && fixture.probe->closed==1;
    });
    check("SourceContexts existing-range NUMERIC sorts exactly beyond 2^53",[&]{
        auto fixture=stream(db,"ORDER BY a.k ASC NULLS LAST,a.id DESC",true,"numeric");
        return consume(fixture,expected(true,false),true) && fixture.probe->closed==1;
    });
    check("SourceContexts early close and genuine same-graph rebind",[&]{
        auto fixture=stream(db,"ORDER BY a.k ASC NULLS LAST,a.id DESC",true);
        bool good=fixture.plan->open() && observed.creates>0;std::string raw;
        good=fixture.plan->next(raw) && good;const auto reads=observed.reads,writes=observed.writes;
        fixture.plan->close();fixture.plan->close();good=good && cleaned() && observed.reads==reads && observed.writes==writes;
        good=fixture.plan->bindPreparedOuterRow(RowContext{}) && good;
        good=consume(fixture,expected(true,false),true) && good;
        return good && fixture.probe->closed==2 && fixture.probe->rebound==1 && fixture.probe->reads==2*streamCount;
    });
    check("actual buildPreparedQueryPlan CTE/VALUES SourceRows",[&]{
        const auto sql="WITH q(id,k,payload) AS("+values(32)+") SELECT id,k,payload FROM q ORDER BY k DESC,id ASC";
        std::shared_ptr<PreparedQuery> query;auto plan=actualPlan(db,sql,query);
        bool good=sorted(plan.get()) && plan->open() && observed.creates>0;std::string raw;size_t count=0;
        for(int key=4;key>=0;--key)for(size_t id=0;id<32;++id)if(id%5==size_t(key)){
            std::vector<ExprValue> actual;good=plan->next(raw) && plan->lastStructuredValues(actual) && actual.size()==3 && good;
            if(actual.size()==3)good=good && actual[0].value==std::to_string(id) && actual[1].value==std::to_string(key) &&
                actual[2].value==std::string(4096,'a'+id%23);++count;
        }
        good=good && count==32 && !plan->next(raw);plan->close();return good && cleaned();
    });
    check("actual derived VALUES structural ROW and regtype identities",[&]{
        const auto sql="SELECT id,payload,'integer'::regtype,ROW(id,ROW('x'::text,ROW())) "
            "FROM("+values(24)+") AS q(id,k,payload) ORDER BY id DESC";
        std::shared_ptr<PreparedQuery> query;auto plan=actualPlan(db,sql,query);
        bool good=sorted(plan.get()) && plan->open() && observed.creates>0;std::string raw;
        for(size_t n=24;n>0;--n){std::vector<ExprValue> actual;
            good=plan->next(raw) && plan->lastStructuredValues(actual) && actual.size()==4 && good;
            if(actual.size()!=4)continue;
            const auto id=n-1;
            good=good && actual[0].value==std::to_string(id) && actual[1].value==std::string(4096,'a'+id%23) &&
                actual[2].objectOid==23 && actual[3].recordFields && actual[3].recordFields->size()==2;
            if(actual[3].recordFields && actual[3].recordFields->size()==2){const auto& fields=*actual[3].recordFields;
                good=good && fields[0].value==std::to_string(id) && fields[1].recordFields && fields[1].recordFields->size()==2;
                if(fields[1].recordFields && fields[1].recordFields->size()==2){const auto& inner=*fields[1].recordFields;
                    good=good && inner[0].typeName=="text" && inner[0].value=="x" && inner[1].recordFields && inner[1].recordFields->empty();}
            }
        }
        good=good && !plan->next(raw);plan->close();return good && cleaned();
    });

    TableSchema table;table.tablename="physical_rows";table.append(makeIntColumn("id",false,2));table.append(makeTextColumn("payload",false));
    if(g_engine.createTable(db,table)!=DBStatus::OK || g_engine.beginTransaction(db)!=DBStatus::OK)return 2;
    // The first source row is individually wider than the row workspace;
    // it forces a real spill even with LIMIT 1, before a later key error.
    for(size_t id=0;id<64;++id)if(g_engine.insertRow(db,table.tablename,{{"id",std::to_string(id)},
        {"payload",std::string(id==0?32768:8192,'a'+id%23)}})!=DBStatus::OK)return 2;
    if(g_engine.commitTransaction()!=DBStatus::OK)return 2;
    // SQL nextval arguments bind genuine regclass catalog identities. The
    // legacy engine createSequence API alone creates no pg_class object.
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE SEQUENCE key_calls",session) ||
       ddl.executeSql("CREATE SEQUENCE projection_calls",session))return 2;
    check("actual heap volatile alias key executes once per original row",[&]{
        bool good=g_engine.nextval(db,"key_calls")==1;std::shared_ptr<PreparedQuery> query;
        auto plan=actualPlan(db,"SELECT id,nextval('key_calls') AS n FROM physical_rows ORDER BY n DESC LIMIT 3",query);
        good=good && g_engine.nextval(db,"key_calls")==2 && plan->open() && observed.creates>0;std::string raw;
        for(size_t i=0;i<3;++i){std::vector<ExprValue> actual;good=plan->next(raw) && plan->lastStructuredValues(actual) && actual.size()==2 && good;
            if(actual.size()==2)good=good && actual[0].value==std::to_string(63-i) && actual[1].value==std::to_string(66-i);}
        good=good && !plan->next(raw) && g_engine.nextval(db,"key_calls")==67;plan->close();return good && cleaned();
    });
    check("actual heap volatile projection follows skipped and demanded rows",[&]{
        std::shared_ptr<PreparedQuery> query;auto plan=actualPlan(db,
            "SELECT id,nextval('projection_calls') FROM physical_rows ORDER BY id DESC LIMIT 3 OFFSET 4",query);
        // Probe metadata purity before open: OffsetOp::open itself consumes
        // the four skipped projection rows, so its exact visible count is 5
        // including this one observer allocation, rather than still being 1.
        bool good=g_engine.nextval(db,"projection_calls")==1 && plan->open() && observed.creates>0 &&
            g_engine.currval(db,"projection_calls")==5;std::string raw;
        for(size_t i=0;i<3;++i){std::vector<ExprValue> actual;good=plan->next(raw) && plan->lastStructuredValues(actual) && actual.size()==2 && good;
            if(actual.size()==2)good=good && actual[0].value==std::to_string(59-i) && actual[1].value==std::to_string(6+i);}
        good=good && !plan->next(raw) && g_engine.nextval(db,"projection_calls")==9;plan->close();return good && cleaned();
    });
    check("actual heap discarded TopN key error remains observable",[&]{
        std::shared_ptr<PreparedQuery> query;auto plan=actualPlan(db,
            "SELECT id FROM physical_rows ORDER BY 1/(id-37),id LIMIT 1",query);
        bool exact=false;try{plan->open();}catch(const DbError& error){exact=error.sqlState()=="22012";}
        plan->close();return exact && observed.creates>0 && cleaned();
    });
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;
    g_config.workMemKb=originalBudget;setCurrentSession(previous);
    assert(checked==17);
    std::cout<<"TYPED_SORT_OPERATOR_COMPLETE "<<checked<<" FAILURES "<<failures+!dropped<<'\n';
    return failures || !dropped?1:0;
}

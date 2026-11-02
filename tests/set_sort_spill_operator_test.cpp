// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "Config.h"
#include "Session.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <malloc.h>
#include <numeric>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
constexpr int64_t keyBase=9007199254740993LL;
enum class Fault { None, Read, Write, Create, Partial, Cancel };
size_t liveBytes(){const auto info=mallinfo2();return info.uordblks+info.hblkhd;}
struct Observation {
    size_t creates=0,reads=0,writes=0,maxFds=0,baseline=0,peak=0;
    int outputGood=-1,resourceGood=-1,leftBody=-1,rootHasLhs=-1;
    size_t outputRows=0,leftOrders=0,leftOffset=0,leftLimit=0,rightRetained=0;
    std::string outputIds;
    bool triggered=false;
    Fault fault=Fault::None;
    SessionInterruptState* interrupt=nullptr;
    void sample(){const auto now=liveBytes();peak=std::max(peak,now>baseline?now-baseline:0);}
} observed;
bool sortFile(int fd){
    char path[64],name[4096];std::snprintf(path,sizeof(path),"/proc/self/fd/%d",fd);
    const auto size=::readlink(path,name,sizeof(name)-1);if(size<0)return false;
    name[size]='\0';return std::strstr(name,"/.dbms-sort-")!=nullptr;
}
size_t descriptors(){
    size_t count=0;
    for(const auto& entry:std::filesystem::directory_iterator("/proc/self/fd"))
        count+=sortFile(std::stoi(entry.path().filename().string()));
    return count;
}
bool cleaned(){
    if(descriptors())return false;
    for(const auto& entry:std::filesystem::directory_iterator("."))
        if(entry.path().filename().string().rfind(".dbms-sort-",0)==0)return false;
    return true;
}
struct Probe {size_t opened=0,next=0,rows=0,eof=0,closed=0,retained=0;};
struct State {
    size_t count=128,width=8192,epoch=0;
    bool promotion=false,forceWide=false,leftFailure=false,rightOpenFailure=false,closeFailure=false;
    std::shared_ptr<Probe> probes[2]={std::make_shared<Probe>(),std::make_shared<Probe>()};
    std::exception_ptr primary;
};
ExprValue identity(size_t id){ExprValue result("regtype",id%2?"bigint":"integer");result.objectOid=id%2?20:23;return result;}
std::vector<ExprValue> row(size_t branch,size_t position,const State& state,bool common=false){
    const auto id=branch*state.count+position+state.epoch*2*state.count;
    const bool narrow=state.promotion && branch==0;
    const auto type=narrow && !common?"integer":"bigint";
    const auto key=narrow?int64_t(id%13):keyBase+int64_t(id%13);
    ExprValue keyCell(type,std::to_string(key),id%7==0);
    if(narrow && common && keyCell.isNull)keyCell.value.clear();
    ExprValue payload("text", "row "+std::to_string(id)+"\nβ "+std::string(state.width,'a'+id%23));payload.collation="C";
    if(!state.forceWide){if(id%11==0){payload.value.clear();payload.isNull=true;}else if(id%11==1)payload.value.clear();else if(id%11==2)payload.value="NULL";}
    auto oid=identity(id);if(id%19==0){oid.value.clear();oid.isNull=true;oid.objectOid.reset();}
    ExprValue array("regtype[]","{integer,NULL,text}");array.elementOids={23,std::nullopt,25};
    if(id%17==0){array.value.clear();array.isNull=true;array.elementOids.clear();}
    ExprValue record("record","("+std::to_string(id)+",(x,()))");
    if(id%4==0){record.value="()";record.recordFields.emplace();}
    else if(id%4==1){ExprValue nested("record","(x,())"),empty("record","()"),text("text","x");
        text.collation="C";empty.recordFields.emplace();nested.recordFields=std::vector<ExprValue>{text,empty};
        record.recordFields=std::vector<ExprValue>{ExprValue("integer",std::to_string(id)),identity(id),nested};}
    else if(id%4==2){record.value.clear();record.isNull=true;}
    return {ExprValue("integer",std::to_string(id)),std::move(keyCell),
        std::move(payload),std::move(oid),std::move(array),std::move(record)};
}
bool equal(const ExprValue& a,const ExprValue& b){
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields){if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!equal(a.recordFields->at(i),b.recordFields->at(i)))return false;}
    return true;
}
bool equal(const std::vector<ExprValue>& a,const std::vector<ExprValue>& b){
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i)if(!equal(a[i],b[i]))return false;
    return true;
}
QueryRowDescriptor descriptor(bool narrow){
    return {{"id","integer"},{"k",narrow?"integer":"bigint"},{"payload","text"},
        {"o","regtype",false,0,2206},{"arr","regtype[]",false,0,2211},{"rec","record",false,0,2249}};
}
// These sources own ordinal typed values and have no engine/RID provenance.
// Actual Append/project/provider nodes are constructed by the public planner.
class BranchSource final:public Operator {
    std::shared_ptr<State> state_;
    size_t branch_,position_=0;
    bool opened_=false;
    std::vector<ExprValue> current_;
public:
    BranchSource(std::shared_ptr<State> state,size_t branch):state_(std::move(state)),branch_(branch){}
    bool open()override{
        clearError();opened_=true;position_=0;current_.clear();++state_->probes[branch_]->opened;
        if(branch_==1 && state_->rightOpenFailure)std::rethrow_exception(state_->primary);
        return true;
    }
    bool next(std::string& display)override{
        auto& probe=*state_->probes[branch_];++probe.next;
        if(!opened_)throw DbError("XX000","closed native branch was read");
        if(branch_==0 && position_==0){observed.baseline=liveBytes();observed.peak=0;}
        observed.sample();
        if(branch_==0 && state_->leftFailure && position_==17)std::rethrow_exception(state_->primary);
        if(position_==state_->count){++probe.eof;const auto now=liveBytes();probe.retained=now>observed.baseline?now-observed.baseline:0;return false;}
        current_=row(branch_,position_++,*state_);++probe.rows;display.clear();return true;
    }
    void close()override{
        if(!opened_)return;
        opened_=false;current_.clear();++state_->probes[branch_]->closed;
        if(branch_==0 && state_->closeFailure)throw DbError("22023","secondary native close failure");
    }
    bool supportsStructuredRows()const override{return true;}
    bool lastStructuredValues(std::vector<ExprValue>& values)const override{values=current_;return !current_.empty();}
    bool lastStructuredRow(std::vector<std::string>& values,std::vector<bool>& nulls)const override{
        values.clear();nulls.clear();for(const auto& cell:current_){values.push_back(cell.value);nulls.push_back(cell.isNull);}return !current_.empty();
    }
};
struct Fixture {std::shared_ptr<State> state;std::shared_ptr<PreparedQuery> query;OpPtr plan;};
Fixture fixture(const std::string& tail,std::shared_ptr<State> state=std::make_shared<State>(),bool inlineLeft=false){
    QueryBindingMetadata metadata;
    metadata.relation=[state](const std::string& name){return QueryRelationMetadata{"public",name,descriptor(state->promotion && name=="left_rows"),{}};};
    const std::string left="SELECT id,k,payload,o,arr,rec FROM left_rows";
    const std::string sql=(inlineLeft?left:"("+left+")")+" UNION ALL SELECT id,k,payload,o,arr,rec FROM right_rows "+tail;
    auto query=std::make_shared<PreparedQuery>(prepareQuery(sql,{},metadata));
    auto* root=static_cast<SelectStmt*>(query->ast.get());
    observed.rootHasLhs=bool(root->setOpLhs);
    const auto branch=[state,query](SelectStmt* select,bool body,QueryPlanner::PreparedExecutionProvider owner){
        const size_t index=select->fromClause->tableName=="left_rows"?0:1;
        if(index==0){observed.leftBody=body;observed.leftOrders=select->orderBy.size();
            observed.leftOffset=select->offset.value_or(0);observed.leftLimit=select->limit.value_or(0);}
        TableSchema schema;
        for(const auto& output:descriptor(state->promotion && index==0)){Column column;column.dataName=output.name;
            column.dataType=output.type;column.isVariableLength=true;schema.append(column);}
        return QueryPlanner::buildPreparedSelectPlan(&g_engine,"",query,select,schema,
            std::make_unique<BranchSource>(state,index),{},{},{},false,false,std::move(owner),body);
    };
    auto plan=QueryPlanner::buildPreparedUnionAllPlan(&g_engine,"",query,root,branch,{},{},false,false);
    return {std::move(state),std::move(query),std::move(plan)};
}
using Position=std::pair<size_t,size_t>;
std::vector<Position> expected(const State& state,bool ascending=true,bool first=false,bool stableOnly=false,
                               size_t offset=0,size_t limit=std::numeric_limits<size_t>::max(),bool ties=false,bool idOnly=false){
    std::vector<Position> result;
    for(size_t branch=0;branch<2;++branch)for(size_t position=0;position<state.count;++position)result.emplace_back(branch,position);
    const auto id=[&](Position value){return value.first*state.count+value.second+state.epoch*2*state.count;};
    const auto key=[&](Position value){return state.promotion && !value.first?int64_t(id(value)%13):keyBase+int64_t(id(value)%13);};
    std::sort(result.begin(),result.end(),[&](Position a,Position b){
        if(idOnly)return ascending?id(a)<id(b):id(a)>id(b);
        const bool an=id(a)%7==0,bn=id(b)%7==0;if(an!=bn)return an==first;
        if(!an && key(a)!=key(b))return ascending?key(a)<key(b):key(a)>key(b);
        // Stable peers retain the actual Append ordinal. The explicit id ASC
        // secondary key in other controls happens to produce the same order.
        (void)stableOnly;return id(a)<id(b);
    });
    const auto begin=std::min(offset,result.size());size_t end=begin+std::min(limit,result.size()-begin);
    if(ties && end>begin)while(end<result.size()){
        const auto a=result[end-1],b=result[end];const bool an=id(a)%7==0,bn=id(b)%7==0;
        if(an!=bn || (!an && key(a)!=key(b)))break;
        ++end;
    }
    return {result.begin()+begin,result.begin()+end};
}
bool fullRead(const State& state){
    for(const auto& probe:state.probes)if(probe->rows!=state.count || probe->eof!=1 || probe->closed!=1)return false;
    return true;
}
bool consume(Fixture& item,const std::vector<Position>& wanted,bool spill,bool memory=true,bool open=true){
    bool good=!open || item.plan->open();std::string display;size_t count=0;
    while(item.plan->next(display)){
        if(count>=wanted.size()){good=false;break;}
        std::vector<ExprValue> actual;
        const auto where=wanted[count++];
        good=item.plan->lastStructuredValues(actual) && equal(actual,row(where.first,where.second,*item.state,true)) && good;
        if(count<=8 && !actual.empty())observed.outputIds+=actual[0].value+",";
    }
    observed.outputRows=count;
    observed.rightRetained=item.state->probes[1]->retained;
    observed.outputGood=good && count==wanted.size() && !item.plan->hasError();
    observed.resourceGood=observed.maxFds<=2 && (spill?observed.creates>0 && observed.reads>0 && observed.writes>0:observed.creates==0);
    if(spill && memory)observed.resourceGood=observed.resourceGood && observed.peak<1024*1024 && item.state->probes[1]->retained<1024*1024;
    good=observed.outputGood && observed.resourceGood;
    item.plan->close();item.plan->close();return good && cleaned();
}
bool terminal(Fixture& item){
    bool good=cleaned();const auto reads=observed.reads,writes=observed.writes;
    const auto a=item.state->probes[0]->next,b=item.state->probes[1]->next;
    try{std::string output;good=!item.plan->next(output) && !item.plan->next(output) && good;}catch(...){good=false;}
    try{item.plan->close();item.plan->close();}catch(...){good=false;}
    return good && cleaned() && observed.reads==reads && observed.writes==writes &&
        item.state->probes[0]->next==a && item.state->probes[1]->next==b;
}
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __wrap_mkstemp(char* name){
    if(std::strncmp(name,".dbms-sort-",11))return __real_mkstemp(name);
    ++observed.creates;
    if(observed.fault==Fault::Create){observed.triggered=true;errno=EMFILE;return -1;}
    const auto fd=__real_mkstemp(name);observed.maxFds=std::max(observed.maxFds,descriptors());return fd;
}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset){
    if(!sortFile(fd))return __real_pread(fd,data,size,offset);
    ++observed.reads;observed.sample();
    if(observed.fault==Fault::Read && !observed.triggered){observed.triggered=true;errno=EIO;return -1;}
    if(observed.fault==Fault::Partial && observed.reads==2){errno=EINTR;return -1;}
    return __real_pread(fd,data,observed.fault==Fault::Partial && observed.reads==1?std::min<size_t>(3,size):size,offset);
}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset){
    if(!sortFile(fd))return __real_pwrite(fd,data,size,offset);
    ++observed.writes;observed.sample();
    if(observed.fault==Fault::Write){observed.triggered=true;errno=ENOSPC;return -1;}
    if(observed.fault==Fault::Partial && observed.writes==2){errno=EINTR;return -1;}
    const auto result=__real_pwrite(fd,data,observed.fault==Fault::Partial && observed.writes==1?std::min<size_t>(7,size):size,offset);
    if(observed.fault==Fault::Cancel && !observed.triggered){observed.triggered=true;observed.interrupt->cancelRequested.store(true);}
    return result;
}

int main(){
    TypeRegistry::instance().bootstrap();auto* prior=currentSession();Session session;setCurrentSession(&session);
    const auto budget=g_config.workMemKb;g_config.workMemKb=64;
    size_t checked=0,failures=0;
    const auto check=[&](const char* label,const auto& action){
        ++checked;observed={};observed.baseline=liveBytes();session.workMem.assign(64,false);bool good=false;
        try{good=action();}catch(const std::exception& error){std::cerr<<"SET_SORT_SPILL_EXCEPTION "<<label<<" : "<<error.what()<<'\n';}
        failures+=!good;std::cout<<"SET_SORT_SPILL "<<label<<" passed="<<good<<" creates="<<observed.creates
            <<" maxfds="<<observed.maxFds<<" reads="<<observed.reads<<" writes="<<observed.writes<<" peak="<<observed.peak
            <<" output_ok="<<observed.outputGood<<" resource_ok="<<observed.resourceGood<<" rows="<<observed.outputRows
            <<" right_retained="<<observed.rightRetained
            <<" root_lhs="<<observed.rootHasLhs<<" left_body="<<observed.leftBody<<" left_orders="<<observed.leftOrders
            <<" left_offset="<<observed.leftOffset<<" left_limit="<<observed.leftLimit<<" ids="<<observed.outputIds<<'\n';
    };
    check("low budget ascending complete typed metadata",[&]{auto item=fixture("ORDER BY k ASC NULLS LAST,id ASC");
        return consume(item,expected(*item.state),true) && fullRead(*item.state);});
    check("low budget descending NULLS FIRST",[&]{auto item=fixture("ORDER BY k DESC NULLS FIRST,id ASC");
        return consume(item,expected(*item.state,false,true),true) && fullRead(*item.state);});
    check("high budget no-spill resource control",[&]{session.workMem.assign(65536,false);auto item=fixture("ORDER BY k,id");
        return consume(item,expected(*item.state),false,false) && fullRead(*item.state);});
    check("stable Append ordinal peers cross temporary runs",[&]{auto item=fixture("ORDER BY k ASC NULLS LAST");
        return consume(item,expected(*item.state,true,false,true),true) && fullRead(*item.state);});
    check("Append common integer to bigint conversion precedes spill",[&]{auto state=std::make_shared<State>();state->promotion=true;
        auto item=fixture("ORDER BY k,id",state);return consume(item,expected(*state),true) && fullRead(*state);});
    check("high budget TopN OFFSET retains bound without spool",[&]{session.workMem.assign(65536,false);
        auto item=fixture("ORDER BY k,id LIMIT 5 OFFSET 3");const bool good=consume(item,expected(*item.state,true,false,false,3,5),false,false);
        observed.resourceGood=observed.resourceGood && item.state->probes[1]->retained<256*1024;
        return good && fullRead(*item.state) && observed.resourceGood;});
    check("low budget wide TopN OFFSET switches to spill",[&]{auto item=fixture("ORDER BY k,id LIMIT 5 OFFSET 7");
        return consume(item,expected(*item.state,true,false,false,7,5),true) && fullRead(*item.state);});
    check("WITH TIES never truncates rows at ordinary bound",[&]{auto item=fixture("ORDER BY k ASC NULLS LAST FETCH FIRST 1 ROW WITH TIES");
        const auto wanted=expected(*item.state,true,false,true,0,1,true);
        return wanted.size()>1 && consume(item,wanted,true) && fullRead(*item.state);});
    check("LIMIT zero opens neither branch nor tape",[&]{auto item=fixture("ORDER BY k LIMIT 0");
        const bool good=consume(item,{},false);return good && item.state->probes[0]->opened==0 && item.state->probes[1]->opened==0 &&
            item.state->probes[0]->next==0 && item.state->probes[1]->next==0;});
    check("open stays lazy and first next consumes both branches",[&]{auto item=fixture("ORDER BY k,id");
        bool good=item.plan->open() && observed.creates==0 && item.state->probes[0]->opened==1 && item.state->probes[1]->opened==0 &&
            item.state->probes[0]->next==0 && item.state->probes[1]->next==0;
        std::string output;std::vector<ExprValue> actual;const auto wanted=expected(*item.state);
        good=item.plan->next(output) && item.plan->lastStructuredValues(actual) && equal(actual,row(wanted[0].first,wanted[0].second,*item.state,true)) &&
            fullRead(*item.state) && observed.creates>0 && good;
        item.plan->close();return good && cleaned();});
    check("same genuine graph reopens with new branch values",[&]{session.workMem.assign(65536,false);auto item=fixture("ORDER BY k,id");
        const auto* graph=item.plan.get();const auto* ast=item.query->ast.get();const auto children=item.plan->preparedPlanChildren();
        bool good=consume(item,expected(*item.state),false,false);item.state->epoch=1;
        good=consume(item,expected(*item.state),false,false) && good;
        return good && item.plan.get()==graph && item.query->ast.get()==ast && item.plan->preparedPlanChildren()==children &&
            item.state->probes[0]->opened==2 && item.state->probes[1]->opened==2 && item.state->probes[0]->closed==2 && item.state->probes[1]->closed==2;});
    check("early close skips winning-run refill",[&]{auto state=std::make_shared<State>();state->forceWide=true;auto item=fixture("ORDER BY k,id",state);
        std::string output;bool good=item.plan->open() && item.plan->next(output) && fullRead(*state) && observed.creates>0;
        const auto reads=observed.reads,writes=observed.writes;item.plan->close();item.plan->close();
        return good && cleaned() && observed.reads==reads && observed.writes==writes;});
    check("next-refill EIO returns exactly one row then terminal cleanup",[&]{auto state=std::make_shared<State>();state->forceWide=true;
        auto item=fixture("ORDER BY k,id",state);std::string output;size_t count=0;bool exact=false;
        if(!item.plan->open() || !item.plan->next(output))return false;
        ++count;
        const auto reads=observed.reads;observed.fault=Fault::Read;
        try{if(item.plan->next(output))++count;}catch(const DbError& error){exact=error.sqlState()=="58030" &&
            error.message()==std::string("could not read sort temporary file: ")+std::strerror(EIO);}
        return terminal(item) && exact && count==1 && observed.triggered && observed.reads==reads+1 && fullRead(*state);});
    check("merge setup EIO emits zero rows and closes owners",[&]{auto item=fixture("ORDER BY k,id");observed.fault=Fault::Read;
        bool exact=false;size_t count=0;try{if(item.plan->open()){std::string output;while(item.plan->next(output))++count;}}
        catch(const DbError& error){exact=error.sqlState()=="58030";}
        return terminal(item) && exact && count==0 && observed.triggered && fullRead(*item.state);});
    for(const auto fault:{Fault::Write,Fault::Create})check(fault==Fault::Write?"ENOSPC releases sort and active branch":"EMFILE releases active branch",[&]{
        auto item=fixture("ORDER BY k,id");observed.fault=fault;bool exact=false;
        try{if(item.plan->open()){std::string output;item.plan->next(output);}}
        catch(const DbError& error){exact=error.sqlState()==(fault==Fault::Write?"53100":"53000");}
        return terminal(item) && exact && observed.triggered && item.state->probes[0]->closed==1 && item.state->probes[1]->opened==0;});
    check("real partial temporary I/O and EINTR retries",[&]{auto item=fixture("ORDER BY k,id");observed.fault=Fault::Partial;
        return consume(item,expected(*item.state),true) && fullRead(*item.state) && observed.reads>2 && observed.writes>2;});
    check("source primary beats throwing close and is never retried",[&]{auto state=std::make_shared<State>();state->forceWide=true;
        state->leftFailure=state->closeFailure=true;state->primary=std::make_exception_ptr(DbError("22012","native branch primary"));
        auto item=fixture("ORDER BY k,id",state);bool identity=false;
        try{if(item.plan->open()){std::string output;item.plan->next(output);}}catch(...){identity=std::current_exception()==state->primary;}
        return terminal(item) && identity && state->probes[0]->closed==1 && state->probes[1]->opened==0;});
    check("right open failure closes already spooled left state",[&]{auto state=std::make_shared<State>();state->rightOpenFailure=true;
        state->primary=std::make_exception_ptr(DbError("22003","native right open primary"));auto item=fixture("ORDER BY k,id",state);bool identity=false;
        try{if(item.plan->open()){std::string output;item.plan->next(output);}}catch(...){identity=std::current_exception()==state->primary;}
        return terminal(item) && identity && state->probes[0]->closed==1 && state->probes[1]->closed==1;});
    check("real cancel during temporary output propagates and cleans",[&]{auto priorInterrupt=currentQueryInterruptState();
        auto interrupt=std::make_shared<SessionInterruptState>();interrupt->queryActive.store(true);setCurrentQueryInterruptState(interrupt);
        observed.interrupt=interrupt.get();observed.fault=Fault::Cancel;auto item=fixture("ORDER BY k,id");bool canceled=false;
        try{if(item.plan->open()){std::string output;item.plan->next(output);}}catch(const DbError& error){canceled=error.sqlState()=="57014";}
        interrupt->cancelRequested.store(false);const bool good=terminal(item) && canceled && observed.triggered;
        setCurrentQueryInterruptState(priorInterrupt);return good;});
    check("inline left global LIMIT OFFSET has one scope",[&]{auto item=fixture("ORDER BY id ASC LIMIT 3 OFFSET 4",std::make_shared<State>(),true);
        // The outer OffsetOp legitimately consumes its four rows in open().
        // This control checks exact output and complete branch demand, not
        // the separate direct-SetSort open/first-next observation above.
        bool good=item.plan->open();
        std::string output;size_t count=0;while(item.plan->next(output)){std::vector<ExprValue> actual;
            good=item.plan->lastStructuredValues(actual) && equal(actual,row(0,count+4,*item.state,true)) && good;
            if(count<8 && !actual.empty())observed.outputIds+=actual[0].value+",";
            ++count;}
        observed.outputRows=count;observed.outputGood=good && count==3;item.plan->close();
        std::cout<<"SET_SORT_SCOPE_COUNTS left_rows="<<item.state->probes[0]->rows<<" right_rows="<<item.state->probes[1]->rows<<'\n';
        return observed.outputGood && fullRead(*item.state) && cleaned();});
    check("inline left global WITH TIES is not a branch clause",[&]{auto item=fixture("ORDER BY k ASC NULLS LAST FETCH FIRST 1 ROW WITH TIES",std::make_shared<State>(),true);
        const auto wanted=expected(*item.state,true,false,true,0,1,true);return wanted.size()>1 && consume(item,wanted,true) && fullRead(*item.state);});
    check("inline left global ORDER remains lazy until first next",[&]{session.workMem.assign(65536,false);
        auto item=fixture("ORDER BY k,id",std::make_shared<State>(),true);bool good=item.plan->open() &&
            item.state->probes[0]->next==0 && item.state->probes[1]->next==0;
        return consume(item,expected(*item.state),false,false,false) && fullRead(*item.state) && good;});
    check("actual inline VALUES global clauses stay above Append",[&]{
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery("",
            "VALUES(3),(1),(2) UNION ALL VALUES(6),(4),(5) ORDER BY 1 LIMIT 2 OFFSET 1"));
        const auto* select=static_cast<const SelectStmt*>(query->ast.get());
        const auto* left=select->setOpLhs?static_cast<const SelectStmt*>(select->setOpLhs.get()):select;
        observed.rootHasLhs=bool(select->setOpLhs);observed.leftBody=!select->setOpLhs;
        observed.leftOrders=left->orderBy.size();observed.leftOffset=left->offset.value_or(0);observed.leftLimit=left->limit.value_or(0);
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,"",query,query->ast.get());std::string output;bool good=plan->open();
        for(const auto expected:{"2","3"}){std::vector<ExprValue> actual;good=plan->next(output) && plan->lastStructuredValues(actual) && actual.size()==1 && good;
            if(actual.size()==1)good=good && actual[0].typeName=="integer" && !actual[0].isNull && actual[0].value==expected;}
        good=good && !plan->next(output);plan->close();return good && cleaned();});
    for(bool timeout:{false,true})check(timeout?"TIES timeout at wrapper entry closes spilled child immediately":
        "TIES cancel at wrapper entry closes spilled child immediately",[&]{
        const auto previousInterrupt=currentQueryInterruptState();
        auto token=session.interruptState;
        token->queryActive.store(true);token->cancelRequested.store(false);token->timeoutRequested.store(false);
        setCurrentQueryInterruptState(token);
        struct RestoreInterrupt {
            std::shared_ptr<SessionInterruptState> previous,token;
            ~RestoreInterrupt(){token->cancelRequested.store(false);token->timeoutRequested.store(false);
                token->queryActive.store(false);setCurrentQueryInterruptState(std::move(previous));}
        } restoreInterrupt{previousInterrupt,token};
        auto state=std::make_shared<State>();state->forceWide=true;
        auto item=fixture("ORDER BY k ASC NULLS LAST FETCH FIRST 2 ROWS WITH TIES",state);
        bool first=false;size_t count=0;std::string display;std::vector<ExprValue> actual;
        if(item.plan->open() && item.plan->next(display)){
            ++count;const auto wanted=expected(*state);
            first=item.plan->lastStructuredValues(actual) && equal(actual,row(wanted[0].first,wanted[0].second,*state,true));
        }
        const bool spooled=observed.creates>0 && descriptors()==1 && fullRead(*state);
        const auto reads=observed.reads,writes=observed.writes;
        token->timeoutRequested.store(timeout);token->cancelRequested.store(true);
        bool exact=false;
        try{if(item.plan->next(display))++count;}
        catch(const DbError& error){exact=error.sqlState()=="57014" && error.message()==
            (timeout?"canceling statement due to statement timeout":"canceling statement due to user request");}
        // The interruption is raised by the outer TIES wrapper before it
        // calls Sort.next(). The wrapper must therefore release its child
        // immediately, rather than relying on Sort's own error handler.
        const bool immediate=cleaned() && observed.reads==reads && observed.writes==writes;
        token->cancelRequested.store(false);token->timeoutRequested.store(false);
        observed.outputRows=count;observed.outputGood=first && count==1;
        observed.resourceGood=spooled && immediate;
        return terminal(item) && exact && observed.outputGood && observed.resourceGood && fullRead(*state);
    });
    g_config.workMemKb=budget;setCurrentSession(prior);
    assert(checked==26);
    std::cout<<"SET_SORT_SPILL_OPERATOR_COMPLETE "<<checked<<" FAILURES "<<failures<<'\n';return failures?1:0;
}

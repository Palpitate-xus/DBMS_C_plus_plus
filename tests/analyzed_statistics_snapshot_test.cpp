#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "catalog/AnalyzedStatisticsStore.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <future>
#include <iostream>
#include <limits>
#include <spawn.h>
#include <stdexcept>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>
#include <csignal>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sstream>
extern char** environ;
namespace {
namespace stats=dbms::analyzed_statistics;
namespace fs=std::filesystem;
size_t controls=0;
void check(bool value,const char* label){++controls;if(!value)throw std::runtime_error(label);}
template<class F>void rejects(F action,const char* state="XX001") {
    bool rejected=false;try{action();}catch(const dbms::DbError& e){rejected=e.sqlState()==state;}
    check(rejected,"strict error/state, not bad_alloc or a partial decode");
}
std::string fileBytes(const fs::path& p){std::ifstream in(p,std::ios::binary);check(bool(in),"fixture exists");std::string s{std::istreambuf_iterator<char>(in),{}};check(!in.bad(),"fixture complete");return s;}
void writeBytes(const fs::path& p,const std::string& b){std::ofstream out(p,std::ios::binary|std::ios::trunc);out.write(b.data(),b.size());out.close();check(!out.fail(),"fixture complete write");}
dbms::DatumTypePtr scalar(uint32_t oid,int16_t size=-1){auto p=std::make_shared<dbms::DatumType>();p->actualOid=p->codecOid=oid;p->codecSize=size;p->hasBinaryOutput=true;return p;}
dbms::DatumTypePtr array(uint32_t oid,dbms::DatumTypePtr element){auto p=std::make_shared<dbms::DatumType>(*scalar(oid));p->kind=dbms::DatumKind::Array;p->arrayElement=std::move(element);return p;}
dbms::ExprValue arr(dbms::DatumTypePtr type,std::vector<dbms::ExprValue> values,std::vector<dbms::DatumArrayDimension> dimensions) {
    dbms::ExprValue v("integer[]","typed owning array, not reparsed");auto d=std::make_shared<dbms::CompoundDatum>();d->type=std::move(type);d->arrayElements=std::move(values);d->dimensions=std::move(dimensions);d->arrayNullBitmap=true;v.compound=d;return v;
}
stats::Slot slot(stats::Kind kind,std::optional<std::vector<float>> numbers,std::optional<stats::Values> values={}){stats::Slot s;s.kind=kind;s.numbers=std::move(numbers);s.values=std::move(values);return s;}
stats::Column column(dbms::DatumTypePtr type){stats::Column c;c.physicalRelation=9001;c.schemaGeneration=47;c.attributeType=std::move(type);c.sampleRows=20;c.nullRows=2;c.totalRows=20;c.nullFraction=.1f;c.averageWidth=16;c.distinctEstimate=-.5f;return c;}
stats::Snapshot fixture() {
    stats::Snapshot s;s.generation=9;auto integer=scalar(23,4),text=scalar(25),real8=scalar(701,8),range=scalar(3904),ints=array(1007,integer);
    auto a=arr(ints,{{"integer","1"},{"integer","",true},{"integer","-2147483648"},{"integer","2147483647"}},{{2,-2},{2,4}});
    auto empty=arr(ints,{},{});auto c=column(ints);
    c.slots={slot(stats::Kind::MostCommon,std::vector<float>{.25f,.25f},stats::Values{ints,{a,empty}}),slot(stats::Kind::Histogram,{},stats::Values{ints,{empty,a}}),slot(stats::Kind::Correlation,std::vector<float>{-.75f}),slot(stats::Kind::CommonElements,std::vector<float>{.25f,.5f,.25f,.5f,.125f},stats::Values{integer,{{"integer","1"},{"integer","2"}}}),slot(stats::Kind::ElementCounts,std::vector<float>{0,1,2,2,1.5f})};
    c.slots[0].comparisonOperator=1070;s.columns.emplace(stats::Key{4101,1,false},c);s.columns.emplace(stats::Key{4101,1,true},c);
    c=column(text);c.slots={slot(stats::Kind::MostCommon,std::vector<float>{.25f,.5f},stats::Values{text,{{"text","comma,semi;pipe|quote\"\n界"},{"text",""}}}),slot(stats::Kind::Histogram,{},stats::Values{text,{{"text","NULL"},{"text"," z "}}})};s.columns.emplace(stats::Key{4101,2,false},c);
    c=column(range);c.slots={slot(stats::Kind::RangeLength,std::vector<float>{.25f},stats::Values{real8,{{"double precision","0"},{"double precision","Infinity"}}}),slot(stats::Kind::RangeBounds,{},stats::Values{range,{{"int4range","[1,3)"},{"int4range","(,5]"}}})};s.columns.emplace(stats::Key{4101,3,false},c);
    c=column(range);c.slots={slot(stats::Kind::RangeLength,std::vector<float>{1},stats::Values{real8,{}})};s.columns.emplace(stats::Key{4101,4,false},c);
    auto oid=scalar(2206,4);auto oids=array(2211,oid);auto refs=arr(oids,{{"regtype","integer"},{"regtype","",true},{"regtype","\"type.with.dot\""}},{{3,-7}});refs.typeName="regtype[]";refs.elementOids={23,std::nullopt,80001};auto d=std::make_shared<dbms::CompoundDatum>(*refs.compound);(*d->arrayElements)[0].objectOid=23;(*d->arrayElements)[2].objectOid=80001;refs.compound=d;
    auto record=std::make_shared<dbms::DatumType>(*scalar(80010));record->kind=dbms::DatumKind::Record;record->recordFields={*integer,*oids};dbms::ExprValue r("record","owning fields");r.recordFields=std::vector<dbms::ExprValue>{{"integer","",true},refs};auto rm=std::make_shared<dbms::CompoundDatum>();rm->type=record;r.compound=rm;
    c=column(record);c.slots={slot(stats::Kind::MostCommon,std::vector<float>{.5f},stats::Values{record,{r}})};s.columns.emplace(stats::Key{4101,5,false},c);
    auto domain=std::make_shared<dbms::DatumType>(*scalar(1043));domain->actualOid=80002;domain->effectiveTypeMod=36;c=column(domain);dbms::ExprValue v("\"schema.with.dot\".\"D |;,\"","literal NULL");v.collation="C";auto vm=std::make_shared<dbms::CompoundDatum>();vm->type=domain;v.compound=vm;c.slots={slot(stats::Kind::MostCommon,std::vector<float>{-0.0f},stats::Values{domain,{v}})};c.collation=950;s.columns.emplace(stats::Key{4101,6,false},c);
    auto emptyType=std::make_shared<dbms::DatumType>(*scalar(80011));emptyType->kind=dbms::DatumKind::Record;dbms::ExprValue emptyRecord("empty_named_record","()");emptyRecord.recordFields.emplace();auto ed=std::make_shared<dbms::CompoundDatum>();ed->type=emptyType;emptyRecord.compound=ed;c=column(emptyType);c.slots={slot(stats::Kind::MostCommon,std::vector<float>{1},stats::Values{emptyType,{emptyRecord}})};s.columns.emplace(stats::Key{4101,7,false},c);
    return s;
}
bool typeEqual(const dbms::DatumType& a,const dbms::DatumType& b);
bool typeEqual(dbms::DatumTypePtr a,dbms::DatumTypePtr b){return bool(a)==bool(b)&&(!a||typeEqual(*a,*b));}
bool typeEqual(const dbms::DatumType& a,const dbms::DatumType& b){if(a.actualOid!=b.actualOid||a.codecOid!=b.codecOid||a.codecSize!=b.codecSize||a.effectiveTypeMod!=b.effectiveTypeMod||a.kind!=b.kind||a.enumType!=b.enumType||a.hasBinaryOutput!=b.hasBinaryOutput||!typeEqual(a.arrayElement,b.arrayElement)||a.recordFields.size()!=b.recordFields.size())return false;for(size_t i=0;i<a.recordFields.size();++i)if(!typeEqual(a.recordFields[i],b.recordFields[i]))return false;return true;}
bool valueEqual(const dbms::ExprValue& a,const dbms::ExprValue& b);
bool cellsEqual(const std::optional<std::vector<dbms::ExprValue>>& a,const std::optional<std::vector<dbms::ExprValue>>& b){if(a.has_value()!=b.has_value())return false;if(!a)return true;if(a->size()!=b->size())return false;for(size_t i=0;i<a->size();++i)if(!valueEqual((*a)[i],(*b)[i]))return false;return true;}
bool valueEqual(const dbms::ExprValue& a,const dbms::ExprValue& b){if(a.typeName!=b.typeName||a.value!=b.value||a.isNull!=b.isNull||a.collation!=b.collation||a.objectOid!=b.objectOid||a.elementOids!=b.elementOids||!cellsEqual(a.recordFields,b.recordFields)||bool(a.compound)!=bool(b.compound))return false;if(!a.compound)return true;const auto& x=*a.compound;const auto& y=*b.compound;if(!typeEqual(x.type,y.type)||x.arrayNullBitmap!=y.arrayNullBitmap||!cellsEqual(x.arrayElements,y.arrayElements)||x.dimensions.size()!=y.dimensions.size())return false;for(size_t i=0;i<x.dimensions.size();++i)if(x.dimensions[i].length!=y.dimensions[i].length||x.dimensions[i].lower!=y.dimensions[i].lower)return false;return true;}
bool numbersEqual(const std::optional<std::vector<float>>& a,const std::optional<std::vector<float>>& b){return a.has_value()==b.has_value()&&(!a||(a->size()==b->size()&&(a->empty()||std::memcmp(a->data(),b->data(),a->size()*4)==0)));}
bool columnEqual(const stats::Column& a,const stats::Column& b){if(a.physicalRelation!=b.physicalRelation||a.schemaGeneration!=b.schemaGeneration||!typeEqual(a.attributeType,b.attributeType)||a.collation!=b.collation||a.target!=b.target||a.sampleRows!=b.sampleRows||a.nullRows!=b.nullRows||std::memcmp(&a.totalRows,&b.totalRows,8)||std::memcmp(&a.nullFraction,&b.nullFraction,4)||a.averageWidth!=b.averageWidth||std::memcmp(&a.distinctEstimate,&b.distinctEstimate,4)||a.slots.size()!=b.slots.size())return false;for(size_t i=0;i<a.slots.size();++i){const auto& x=a.slots[i];const auto& y=b.slots[i];if(x.kind!=y.kind||x.comparisonOperator!=y.comparisonOperator||x.collation!=y.collation||!numbersEqual(x.numbers,y.numbers)||x.values.has_value()!=y.values.has_value())return false;if(x.values&&(!typeEqual(x.values->elementType,y.values->elementType)||!cellsEqual(x.values->cells,y.values->cells)))return false;}return true;}
bool equal(const stats::Snapshot& a,const stats::Snapshot& b){if(a.generation!=b.generation||a.columns.size()!=b.columns.size())return false;auto x=a.columns.begin(),y=b.columns.begin();for(;x!=a.columns.end();++x,++y)if(x->first<y->first||y->first<x->first||!columnEqual(x->second,y->second))return false;return true;}
std::string hexBytes(const std::string& hex){std::string b;for(size_t i=0;i<hex.size();i+=2)b.push_back(static_cast<char>(std::stoul(hex.substr(i,2),nullptr,16)));return b;}
const char* goldenHex="0044424d535f414e414c595a45445f53544154530001000000b3000000000000000900000000000000010000002a0000000200003702000000000000020000000000000017000000170000000400ffffffff000001000000000000000000640000000400000000000000010000000000000000000000000010400000803e04000000000000bf010000000100600000000000000001010000000000003f0117000000170000000400ffffffff00000100000000000100000007000000696e746567657201000000370000000000000000000000001d157f408e01174b";
void rehash(std::string& bytes){uint64_t h=14695981039346656037ULL;for(size_t i=0;i<bytes.size()-8;++i){h^=static_cast<unsigned char>(bytes[i]);h*=1099511628211ULL;}for(size_t i=0;i<8;++i)bytes[bytes.size()-8+i]=static_cast<char>(h>>(i*8));}
void u32(std::string& b,size_t at,uint32_t v){for(size_t i=0;i<4;++i)b[at+i]=static_cast<char>(v>>(i*8));}
void descriptorCases(){
    // Frozen from real PostgreSQL18.6 pg_attribute, not from codec output.
    constexpr stats::Field expected[]{
        {"schemaname",19,64},
        {"tablename",19,64},
        {"attname",19,64},
        {"inherited",16,1},
        {"null_frac",700,4},
        {"avg_width",23,4},
        {"n_distinct",700,4},
        {"most_common_vals",2277,-1},
        {"most_common_freqs",1021,-1},
        {"histogram_bounds",2277,-1},
        {"correlation",700,4},
        {"most_common_elems",2277,-1},
        {"most_common_elem_freqs",1021,-1},
        {"elem_count_histogram",1021,-1},
        {"range_length_histogram",2277,-1},
        {"range_empty_frac",700,4},
        {"range_bounds_histogram",2277,-1}};
    check(stats::fields.size()==std::size(expected),"full17 target declaration");
    for(size_t i=0;i<std::size(expected);++i)check(std::string(stats::fields[i].name)==expected[i].name&&stats::fields[i].typeOid==expected[i].typeOid&&stats::fields[i].size==expected[i].size,"real PG18.6 declaration name/OID/size");
}
void codecCases(){
    auto source=fixture();const auto bytes=stats::encode(source);check(equal(source,stats::decode(bytes)),"all owned metadata and values, including NULL versus empty");check(bytes==stats::encode(source),"deterministic complete frame");
    check(stats::decode(bytes).columns.at({4101,4,false}).slots[0].values->cells.empty(),"present empty typed values retained");
    auto presence=source;presence.columns.begin()->second.slots[0].numbers=std::vector<float>{};check(equal(presence,stats::decode(stats::encode(presence))),"present-empty numbers differs from absent");
    const auto golden=hexBytes(goldenHex);auto s=stats::decode(golden);check(s.generation==9&&s.columns.size()==1,"independent golden generation/key count");const auto& c=s.columns.at({42,2,false});check(c.physicalRelation==567&&c.schemaGeneration==2&&c.sampleRows==4&&c.nullRows==1&&c.averageWidth==4&&c.nullFraction==.25f&&c.distinctEstimate==-.5f,"independent fixed scalar fields");check(c.attributeType->actualOid==23&&c.attributeType->codecOid==23&&c.attributeType->effectiveTypeMod==-1,"independent type declaration");check(c.slots.size()==1&&(*c.slots[0].numbers)[0]==.5f&&c.slots[0].values->cells[0].value=="7","independent used slot");check(stats::encode(s)==golden,"exact independently encoded byte order and checksum");
    for(size_t i=0;i<golden.size();++i){auto corrupt=golden;corrupt[i]^=1;rejects([&]{(void)stats::decode(corrupt);});}
    for(size_t i=0;i<golden.size();++i)rejects([&]{(void)stats::decode(std::string_view(golden).substr(0,i));});
    rejects([&]{(void)stats::decode(golden+"x");});
    for(const auto& change:std::vector<std::pair<size_t,unsigned char>>{{21,2},{51,2},{82,255},{138,8},{204,2}}){auto invalid=golden;invalid[change.first]=static_cast<char>(change.second);rehash(invalid);rejects([&]{(void)stats::decode(invalid);});}
    auto invalid=golden;u32(invalid,41,UINT32_MAX);rehash(invalid);rejects([&]{(void)stats::decode(invalid);});
    const size_t h=33;auto duplicate=golden.substr(0,h)+golden.substr(h,golden.size()-h-8)+golden.substr(h+12,golden.size()-h-20)+std::string(8,'\0');u32(duplicate,h+8,2);uint64_t len=duplicate.size()-h-8;for(size_t i=0;i<8;++i)duplicate[sizeof(stats::codec::magic)+4+i]=static_cast<char>(len>>(8*i));rehash(duplicate);rejects([&]{(void)stats::decode(duplicate);});
    auto bad=source;bad.columns.begin()->second.slots.push_back(bad.columns.begin()->second.slots[0]);rejects([&]{(void)stats::encode(bad);});
    bad=source;bad.columns.begin()->second.nullRows=21;rejects([&]{(void)stats::encode(bad);});bad=source;bad.columns.begin()->second.nullFraction=std::numeric_limits<float>::quiet_NaN();rejects([&]{(void)stats::encode(bad);});
    auto cyclic=std::make_shared<dbms::DatumType>();cyclic->actualOid=1007;cyclic->kind=dbms::DatumKind::Array;cyclic->arrayElement=cyclic;bad=source;bad.columns.begin()->second.attributeType=cyclic;rejects([&]{(void)stats::encode(bad);});cyclic->arrayElement.reset();
    auto deep=scalar(23,4);for(size_t i=0;i<stats::codec::maxDepth+2;++i)deep=array(1007,deep);bad=source;bad.columns.begin()->second.attributeType=deep;rejects([&]{(void)stats::encode(bad);},"54000");
    auto d=std::make_shared<dbms::CompoundDatum>();d->dimensions={{1,1}};dbms::ExprValue v("integer[]","");v.compound=d;d->arrayElements=std::vector<dbms::ExprValue>{v};bad=source;bad.columns.begin()->second.slots[0].values->cells={v};rejects([&]{(void)stats::encode(bad);});d->arrayElements.reset();
}
std::map<stats::Key,stats::Column> replacement(uint32_t relation){return {{{relation,1,false},column(scalar(23,4))}};}
struct OwnedChild {
    pid_t pid=0;
    bool reaped=false;
    ~OwnedChild(){
        if(pid<=0||reaped)return;
        int status=0;pid_t result;
        do{result=waitpid(pid,&status,WNOHANG);}while(result<0&&errno==EINTR);
        if(result!=0)return; // Already reaped/not our child: never signal it.
        if(getpgid(pid)==pid)kill(-pid,SIGKILL);else kill(pid,SIGKILL);
        while(waitpid(pid,&status,0)<0&&errno==EINTR){}
    }
};
std::unique_ptr<OwnedChild> startChild(const fs::path& exe,const fs::path& root,const std::string& action,const fs::path& path,const std::string& arg="") {
    std::vector<std::string> envs;
    for(char** e=environ;*e;++e)if(std::string(*e).rfind("DBMS_",0)!=0)envs.emplace_back(*e);
    const auto cwd=root/((action=="--read"?"read":"write")+arg);fs::create_directory(cwd);
    envs.push_back("DBMS_DATA_DIR="+cwd.string());std::vector<char*> env;
    for(auto& e:envs)env.push_back(e.data());
    env.push_back(nullptr);
    std::vector<std::string> args={exe.string(),action,path.string(),arg};std::vector<char*> argv;
    for(auto& a:args)argv.push_back(a.data());
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;check(posix_spawn_file_actions_init(&actions)==0,"spawn actions");
    check(posix_spawn_file_actions_addchdir_np(&actions,cwd.c_str())==0,"fresh child data dir before globals");
    posix_spawnattr_t attrs;check(posix_spawnattr_init(&attrs)==0,"spawn attr");
    check(posix_spawnattr_setflags(&attrs,POSIX_SPAWN_SETPGROUP)==0&&posix_spawnattr_setpgroup(&attrs,0)==0,"owned child group");
    auto child=std::make_unique<OwnedChild>();
    pid_t actualPid=0;
    const int status=posix_spawn(&actualPid,exe.c_str(),&actions,&attrs,argv.data(),env.data());
    if(status==0)child->pid=actualPid;
    posix_spawn_file_actions_destroy(&actions);posix_spawnattr_destroy(&attrs);
    check(status==0&&child->pid>0,"fresh real child");
    std::printf("ANALYZED_STATS_CHILD pid=%d action=%s\n",int(child->pid),action.c_str());
    return child;
}
void waitChild(OwnedChild& child) {
    int result=0;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(10);
    for(;;){const auto w=waitpid(child.pid,&result,WNOHANG);if(w==child.pid){child.reaped=true;break;}if(w<0&&errno!=EINTR)throw std::runtime_error("waitpid");if(std::chrono::steady_clock::now()>=deadline)throw std::runtime_error("owned child deadline");std::this_thread::sleep_for(std::chrono::milliseconds(2));}
    check(WIFEXITED(result)&&WEXITSTATUS(result)==0,"child actual normal0");
    check(kill(child.pid,0)==-1&&errno==ESRCH,"reaped child absent");
    check(kill(-child.pid,0)==-1&&errno==ESRCH,"owned child group absent");
}
void child(const fs::path& exe,const fs::path& root,const std::string& action,const fs::path& path,const std::string& arg="") {
    auto process=startChild(exe,root,action,path,arg);waitChild(*process);
}
bool waitingOnSnapshot(pid_t pid,const fs::path& snapshot) {
    std::ifstream in("/proc/"+std::to_string(pid)+"/syscall");
    long call=-1;std::string descriptor,command;in>>call>>descriptor>>command;
    if(!in||call!=SYS_fcntl)return false;
    const auto fd=std::stoul(descriptor,nullptr,0),cmd=std::stoul(command,nullptr,0);
    if(cmd!=F_OFD_SETLKW)return false;
    std::error_code error;const auto actual=fs::read_symlink("/proc/"+std::to_string(pid)+"/fd/"+std::to_string(fd),error);
    return !error&&actual==fs::path(snapshot.string()+".lock");
}
void storeCases(const fs::path& root,const fs::path& exe){
    const auto p=root/".analyzed_statistics";const auto legacy=root/".stats";writeBytes(legacy,"legacy __rows__ 3||\n");check(!stats::read(p),"missing new snapshot is unavailable, not fabricated rows");auto original=fixture();check(stats::replace(p,original)==dbms::StatsSnapshotPublication::Durable,"full normal publication durable");check(equal(*stats::read(p),original),"complete disk read exact");const auto originalBytes=fileBytes(p);child(exe,root,"--read",p);check(fileBytes(p)==originalBytes,"cold reader leaves whole image unchanged");
    std::exception_ptr failures[2];std::promise<void> gate;auto ready=gate.get_future().share();std::thread a([&]{try{ready.wait();check(stats::updateRelation(p,5001,replacement(5001)).publication==dbms::StatsSnapshotPublication::Durable,"thread1 update");}catch(...){failures[0]=std::current_exception();}}),b([&]{try{ready.wait();auto r=stats::updateRelation(p,5002,replacement(5002));if(r.publication!=dbms::StatsSnapshotPublication::Durable)throw std::runtime_error("thread2 update");}catch(...){failures[1]=std::current_exception();}});gate.set_value();a.join();b.join();for(auto e:failures)if(e)std::rethrow_exception(e);auto s=*stats::read(p);check(s.generation==11&&s.columns.count({5001,1,false})&&s.columns.count({5002,1,false}),"concurrent merge keeps both relations");
    auto held=std::make_unique<dbms::StatsFileLock>(p);check(held->ok(),"parent owns snapshot lock");
    auto first=startChild(exe,root,"--update",p,"5003"),second=startChild(exe,root,"--update",p,"5004");
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(10);
    while((!waitingOnSnapshot(first->pid,p)||!waitingOnSnapshot(second->pid,p))&&std::chrono::steady_clock::now()<deadline)std::this_thread::sleep_for(std::chrono::milliseconds(2));
    check(waitingOnSnapshot(first->pid,p)&&waitingOnSnapshot(second->pid,p),"both actual fresh processes blocked on this OFD lock before merge");
    held.reset();waitChild(*first);waitChild(*second);s=*stats::read(p);check(s.generation==13&&s.columns.count({5003,1,false})&&s.columns.count({5004,1,false}),"fresh-process reload and merge");
    const auto c=column(scalar(23,4));check(stats::updateRelation(p,5001,{{{5001,2,false},c},{{5001,2,true},c}}).publication==dbms::StatsSnapshotPublication::Durable,"replace entire relation generation");s=*stats::read(p);check(s.generation==14&&!s.columns.count({5001,1,false})&&s.columns.count({5001,2,true})&&s.columns.count({5002,1,false}),"base/inherited distinction and deleted attributes");check(stats::updateRelation(p,5002,{}).publication==dbms::StatsSnapshotPublication::Durable,"remove relation statistics");s=*stats::read(p);check(s.generation==15&&!s.columns.count({5002,1,false}),"whole relation removal");const auto before=fileBytes(p);rejects([&]{(void)stats::updateRelation(p,5001,replacement(9999));});check(fileBytes(p)==before,"invalid replacement nondestructive");auto badRows=replacement(5001);badRows.begin()->second.nullRows=21;rejects([&]{(void)stats::updateRelation(p,5001,badRows);});check(fileBytes(p)==before,"invalid merged row leaves original generation unchanged");
    for(const auto& x:original.columns)check(columnEqual(s.columns.at(x.first),x.second),"all original typed fields survive other relation writers");
    check(fileBytes(legacy)=="legacy __rows__ 3||\n","native legacy format remains unchanged");rejects([&]{(void)stats::read(legacy);});
    const auto corrupt=root/"corrupt";auto broken=originalBytes;broken.back()^=1;writeBytes(corrupt,broken);rejects([&]{(void)stats::read(corrupt);});rejects([&]{(void)stats::updateRelation(corrupt,5001,replacement(5001));});check(fileBytes(corrupt)==broken,"corrupt image not silently replaced");
    auto exhausted=original;exhausted.generation=UINT64_MAX;check(stats::replace(p,exhausted)==dbms::StatsSnapshotPublication::Durable,"real exhausted persisted generation");const auto end=fileBytes(p);rejects([&]{(void)stats::updateRelation(p,5001,replacement(5001));},"54000");check(fileBytes(p)==end,"generation never wraps");
    for(const auto& x:fs::directory_iterator(root))check(x.path().filename().string().find(".tmp.")==std::string::npos,"normal publication leaves no temporary");
}
} // namespace
int main(int argc,char** argv){try{
    if(argc==4&&std::string(argv[1])=="--read"){const auto s=stats::read(argv[2]);check(s&&equal(*s,fixture()),"fresh process full model");std::printf("ANALYZED_STATS_COLD controls=%zu failures=0\n",controls);return 0;}
    if(argc==4&&std::string(argv[1])=="--update"){const auto id=static_cast<uint32_t>(std::stoul(argv[3]));check(stats::updateRelation(argv[2],id,replacement(id)).publication==dbms::StatsSnapshotPublication::Durable,"fresh peer actual durable update");std::printf("ANALYZED_STATS_PEER controls=%zu failures=0\n",controls);return 0;}
    check(argc==1,"fixed test protocol");descriptorCases();codecCases();std::string pattern=(fs::temp_directory_path()/"dbms-analyzed-statistics-XXXXXX").string();std::vector<char> name(pattern.begin(),pattern.end());name.push_back(0);check(mkdtemp(name.data())!=nullptr,"owned unique fixture root");const auto root=fs::canonical(name.data());std::cout<<"ANALYZED_STATS_OWNED_ROOT "<<root<<std::endl;storeCases(root,fs::canonical("/proc/self/exe"));std::printf("ANALYZED_STATS_COMPLETE controls=%zu failures=0\n",controls);return 0;
}catch(const std::exception& e){std::fprintf(stderr,"ANALYZED_STATS_FAIL controls=%zu %s\n",controls,e.what());return 1;}}

// Real early/later restore clusters and fresh-process allocator regressions.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "common/sha256.h"
#include "storage/DataFileHeader.h"
#include "storage/HeapTupleHeader.h"
#include "storage/PgPage.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"
#include "transaction/TxnIdGenerator.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <spawn.h>
#include <sstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
extern dbms::StorageEngine g_engine;
namespace {
namespace fs=std::filesystem;
using namespace dbms;
const std::string DB="restore_horizon";
size_t checks=0;
void check(bool ok,const std::string& message) {
    ++checks;if(!ok)throw std::runtime_error(message);
}
std::string bytes(const fs::path& p) {
    std::ifstream in(p,std::ios::binary);check(bool(in),"read existing "+p.string());
    std::string s((std::istreambuf_iterator<char>(in)),{});check(!in.bad(),"complete read "+p.string());return s;
}
void outputText(const fs::path& p,const std::string& s) {
    check(!fs::exists(p),"fresh result path "+p.string());std::ofstream out(p,std::ios::binary);
    out<<s;out.flush();check(out.good(),"complete result write "+p.string());
}
std::map<std::string,std::string> inventory(const fs::path& root) {
    std::map<std::string,std::string> result;
    for(const auto& item:fs::recursive_directory_iterator(root)) {
        const auto key=item.path().lexically_relative(root).generic_string();
        check(!item.is_symlink(),"owned fixture/image contains no symlink");
        if(item.is_directory())result["D/"+key]="";
        else {check(item.is_regular_file(),"regular owned fixture file");const auto s=bytes(item.path());result["F/"+key]=std::to_string(s.size())+":"+SHA256::hash(s);}
    }return result;
}
void cloneClosed(const fs::path& source,const fs::path& target) {
    check(!fs::exists(target),"new target only");const auto before=inventory(source);
    fs::copy(source,target,fs::copy_options::recursive);check(inventory(target)==before,"full closed cluster clone byteexact");
}
uint64_t relationNext() {
    const auto s=bytes(".physical_relation_ids");check(s.size()==24,"real relation counter extent");
    uint32_t magic=0,version=0;uint64_t next=0;std::memcpy(&magic,s.data(),4);std::memcpy(&version,s.data()+4,4);std::memcpy(&next,s.data()+8,8);
    check(magic==0x31444947u&&version==1&&next>0,"real relation counter identity");return next;
}
struct Row {int id=0;int64_t rid=0;std::vector<std::string> values;std::vector<bool> nulls;};
bool sameRow(const Row& a,const Row& b) {return a.id==b.id&&a.rid==b.rid&&a.values==b.values&&a.nulls==b.nulls;}
bool sameRows(const std::vector<Row>& a,const std::vector<Row>& b) {
    if(a.size()!=b.size())return false;for(size_t i=0;i<a.size();++i)if(!sameRow(a[i],b[i]))return false;return true;
}
void printRows(const std::string& phase,const std::string& table,const std::vector<Row>& rows) {
    for(const auto& row:rows) {std::cout<<"RESTORE_ALLOC_ROW "<<phase<<' '<<table<<' '<<row.id<<' '<<row.rid;
        for(size_t c=0;c<row.values.size();++c)std::cout<<' '<<row.nulls[c]<<' '<<std::quoted(row.values[c]);std::cout<<'\n';}
}
std::vector<Row> snapshot(const std::string& table) {
    const auto schema=g_engine.getTableSchema(DB,table);check(schema.tablename==table&&schema.len==3,"actual three-column schema");
    TableScanOp scan(&g_engine,DB,table);check(scan.open(),"real scan open");std::string raw;std::vector<Row> rows;std::set<int64_t> rids;
    while(scan.next(raw)) {const auto origin=scan.scanOrigin();const auto rid=scan.lastRid();
        check(origin.engine==&g_engine&&origin.dbname==DB&&origin.tablename==table&&origin.rid==rid&&rid>0&&rids.insert(rid).second,"real unique scan occurrence/RID");
        std::vector<bool> bits;check(scan.lastStoredNullBitmap(bits)&&bits.size()==schema.len,"actual full physical NULL bitmap");Row row;row.rid=rid;
        for(size_t c=0;c<schema.len;++c){const bool null=scan.lastColumnIsNull(c);check(null==bits[c],"actual NULL accessor agreement");row.nulls.push_back(null);row.values.push_back(null?std::string{}:StorageEngine::extractColumnValueStatic(raw,schema,c));}
        check(!row.nulls[0],"actual primary key nonNULL");size_t used=0;row.id=std::stoi(row.values[0],&used);check(used==row.values[0].size(),"complete actual key parse");rows.push_back(std::move(row));
    }check(!scan.hasError(),"scan complete without suppressed error");scan.close();std::sort(rows.begin(),rows.end(),[](const Row&a,const Row&b){return a.id<b.id;});return rows;
}
using Cells=std::vector<std::optional<std::string>>;
void expectedCells(const std::vector<Row>& actual,const std::vector<Cells>& expected) {
    check(actual.size()==expected.size(),"full independently expected row count");
    for(size_t r=0;r<expected.size();++r){check(actual[r].values.size()==expected[r].size(),"full expected row width");
        for(size_t c=0;c<expected[r].size();++c)check(actual[r].nulls[c]==!expected[r][c]&&(!expected[r][c]||actual[r].values[c]==*expected[r][c]),"independent full row value/NULL");}
}
std::vector<Cells> earlyValues(){return {{{"1"},{"11"},{"before-update"}},{{"2"},{"22"},{"before-delete"}},{{"3"},{},{}}};}
std::vector<Cells> sourceValues(bool after=false,bool cold=false){auto out=std::vector<Cells>{{{"1"},{"-2147483648"},{""}},{{"3"},{},{}},{{"4"},{"0"},{"NULL"}},{{"5"},{"5"},{"prepared-commit"}}};if(after)out.push_back({{"8"},{"8"},{"post-restore"}});if(cold)out.push_back({{"9"},{"9"},{"cold-insert"}});return out;}
std::vector<Cells> probeValues(){return {{{"1"},{},{}},{{"2"},{"0"},{""}},{{"3"},{"-2147483648"},{"NULL"}},{{"4"},{"2147483647"},{std::string(6000,'T')+"λ"}}};}
void createTable(const std::string& name) {
    TableSchema s;s.tablename=name;s.formatVersion=DATA_FILE_FORMAT_VERSION;s.append(makeIntColumn("id",false,4,true));s.append(makeIntColumn("n",true,4));s.append(makeTextColumn("t",true));
    check(g_engine.createTable(DB,s)==DBStatus::OK,"genuine CREATE TABLE "+name);
}
void insert(const std::string& table,int id,std::optional<std::string> n,std::optional<std::string> text){check(g_engine.insertRow(DB,table,{{"id",std::to_string(id)},{"n",std::move(n)},{"t",std::move(text)}})==DBStatus::OK,"real INSERT "+table);}
uint32_t xmin(const std::string& table,int64_t rid) {
    uint32_t block=0;uint16_t slot=0;StorageEngine::decodeRid(rid,block,slot);
    auto* allocator=g_engine.getPageAllocator(DB,table);check(allocator!=nullptr,"real owning allocator for existing RID");
    char* buffer=allocator->fetchPage(block);check(buffer!=nullptr,"pin actual RID page");
    struct Pin {PageAllocator* allocator;uint32_t block;~Pin(){allocator->unpinPage(block);}} pin{allocator,block};
    const auto schema=g_engine.getTableSchema(DB,table);PageWrapper page(buffer,allocator->pageSize(),schema.formatVersion);
    check(page.isValid(block),"actual owning heap page identity");const char* tuple=nullptr;size_t length=0;
    check(page.read(slot,tuple,length)&&length>=sizeof(HeapTupleHeaderData),"actual owning RID tuple header");return castHeapHeader(tuple)->t_fields.t_xmin;
}
struct State {uint64_t tx=0,relNext=0,newIdentity=0;std::vector<Row> base,probe;};
void writeRows(std::ostream& out,const std::vector<Row>& rows){out<<rows.size()<<'\n';for(const auto& row:rows){out<<row.id<<' '<<row.rid<<' '<<row.values.size();for(size_t c=0;c<row.values.size();++c)out<<' '<<row.nulls[c]<<' '<<std::quoted(row.values[c]);out<<'\n';}}
std::vector<Row> readRows(std::istream& in){size_t size=0;in>>size;check(size<=16,"small actual fixture count bound");std::vector<Row> rows;for(size_t i=0;i<size;++i){Row row;size_t width=0;in>>row.id>>row.rid>>width;check(width==3,"actual expectation width");for(size_t c=0;c<width;++c){bool null=false;std::string value;in>>null>>std::quoted(value);row.nulls.push_back(null);row.values.push_back(value);}rows.push_back(std::move(row));}return rows;}
void writeState(const fs::path& p,const State& s){std::ostringstream out;out<<s.tx<<' '<<s.relNext<<' '<<s.newIdentity<<'\n';writeRows(out,s.base);writeRows(out,s.probe);outputText(p,out.str());}
State readState(const fs::path& p){std::ifstream in(p);check(bool(in),"actual source expectation open");State s;in>>s.tx>>s.relNext>>s.newIdentity;s.base=readRows(in);s.probe=readRows(in);check(bool(in),"complete source expectation parse");return s;}
State state(){return {TxnIdGenerator::instance().maxCommittedTxId(),relationNext(),0,snapshot("base"),snapshot("added")};}
void checkSource(const State& actual,const State& expected){expectedCells(actual.base,sourceValues());expectedCells(actual.probe,probeValues());check(sameRows(actual.base,expected.base)&&sameRows(actual.probe,expected.probe),"all source values/RIDs/NULL preserved");}
void worker(const std::string& phase,const fs::path& root) {
    const auto cluster=fs::canonical(fs::current_path());const char* data=std::getenv("DBMS_DATA_DIR");check(data&&fs::equivalent(cluster,data),"env/cwd real owned cluster established before exec");check(cluster.parent_path()==root,"worker cluster direct owned child");TypeRegistry::instance().bootstrap();
    if(phase=="seed") {check(g_engine.createDatabase(DB,"utf8")==DBStatus::OK,"actual database");createTable("base");insert("base",1,"11","before-update");insert("base",2,"22","before-delete");insert("base",3,std::nullopt,std::nullopt);const auto rows=snapshot("base");expectedCells(rows,earlyValues());printRows(phase,"base",rows);return;}
    if(phase=="later") {
        expectedCells(snapshot("base"),earlyValues());
        check(g_engine.beginTransaction(DB)==DBStatus::OK,"committed mutation begin");
        std::vector<StorageEngine::SqlRow> updated,removed;
        check(g_engine.updateRows(DB,"base",{{"n","-2147483648"},{"t",""}},{"=id 1"},&updated)==DBStatus::OK&&updated.size()==1,"real later UPDATE one row");
        check(g_engine.removeRows(DB,"base",{"=id 2"},&removed)==DBStatus::OK&&removed.size()==1,"real later DELETE one row");insert("base",4,"0","NULL");check(g_engine.commitTransaction()==DBStatus::OK,"commit INSERT UPDATE DELETE");
        createTable("added");for(const auto& row:probeValues())insert("added",std::stoi(*row[0]),row[1],row[2]);
        check(g_engine.beginTransaction(DB)==DBStatus::OK,"prepared commit begin");insert("base",5,"5","prepared-commit");check(g_engine.prepareTransaction("restore_horizon_commit")==DBStatus::OK,"actual PREPARE");check(g_engine.commitPrepared("restore_horizon_commit")==DBStatus::OK,"actual COMMIT PREPARED");
        check(g_engine.beginTransaction(DB)==DBStatus::OK,"prepared rollback begin");insert("base",6,"6","prepared-aborted");check(g_engine.prepareTransaction("restore_horizon_abort")==DBStatus::OK,"actual PREPARE to abort");check(g_engine.rollbackPrepared("restore_horizon_abort")==DBStatus::OK,"actual ROLLBACK PREPARED");
        const uint64_t beforeGap=TxnIdGenerator::instance().maxCommittedTxId();check(g_engine.beginTransaction(DB)==DBStatus::OK,"ordinary aborted gap begin");insert("base",7,"7","aborted-gap");check(g_engine.rollbackTransaction()==DBStatus::OK,"actual ROLLBACK gap");
        State s=state();expectedCells(s.base,sourceValues());expectedCells(s.probe,probeValues());check(s.tx>beforeGap,"real abort advances durable allocation floor");
        for(const auto& row:s.base)check(xmin("base",row.rid)<s.tx,"aborted allocation above every actual live base xmin");for(const auto& row:s.probe)check(xmin("added",row.rid)<s.tx,"aborted allocation above every actual live probe xmin");
        check(g_engine.listPreparedTransactions().empty(),"no unresolved prepared worker lock on backup");check(g_engine.physicalBackup(DB,(root/"image").string()),"real immutable source physicalBackup");
        checkSource(state(),s);writeState(root/"source.expected",s);printRows(phase,"base",s.base);printRows(phase,"added",s.probe);std::cout<<"RESTORE_ALLOC_SOURCE allocated_floor="<<s.tx<<" relation_next="<<s.relNext<<'\n';return;
    }
    const State source=readState(root/"source.expected");
    if(phase=="advance") {
        expectedCells(snapshot("base"),earlyValues());int count=0;
        while(TxnIdGenerator::instance().maxCommittedTxId()<=source.tx+3){check(count<128,"bounded actual ahead transaction setup");insert("base",100+count,"1","ahead-target");++count;}
        count=0;while(relationNext()<=source.relNext+2){check(count<32,"bounded actual ahead relation setup");createTable("ahead_"+std::to_string(count++));}
        check(TxnIdGenerator::instance().maxCommittedTxId()>source.tx&&relationNext()>source.relNext,"genuine ahead target, no counter-file rewinds");return;
    }
    const bool low=phase=="restore-low"||phase=="cold-low";const auto result=root/(low?"low.after":"ahead.after");
    if(phase=="restore-low"||phase=="restore-ahead") {
        uint64_t beforeTx=TxnIdGenerator::instance().maxCommittedTxId(),beforeRel=relationNext();
        if(low){expectedCells(snapshot("base"),earlyValues());check(beforeTx<source.tx&&beforeRel<source.relNext,"genuine earlier transaction/relation target");}
        else check(beforeTx>source.tx&&beforeRel>source.relNext,"genuine later target remains monotonic");
        insert("base",2001,"1","restore-sentinel");beforeTx=TxnIdGenerator::instance().maxCommittedTxId();
        const auto imageBefore=inventory(root/"image");check(g_engine.physicalRestore(DB,(root/"image").string()),"real physicalRestore returns success");
        State s=state();printRows(phase,"base",s.base);printRows(phase,"added",s.probe);std::cout<<"RESTORE_ALLOC_RESULT visible_base="<<s.base.size()<<" visible_added="<<s.probe.size()<<" actual_highwater="<<s.tx<<" source_floor="<<source.tx<<" before_target="<<beforeTx<<'\n'<<std::flush;
        checkSource(s,source);check(s.tx>=source.tx&&s.tx>=beforeTx,"restored allocator includes aborted source floor, never lowers destination");
        check(!g_engine.tableExists(DB,"ahead_0"),"destination-only physical table absent after real restore");
        insert("base",8,"8","post-restore");auto after=snapshot("base");expectedCells(after,sourceValues(true));check(xmin("base",after.back().rid)>std::max(source.tx,beforeTx),"real new tuple xmin cannot reuse restored or destination XID");
        createTable("post_identity");const auto id=g_engine.getTableSchema(DB,"post_identity").physicalRelationId;
        check(id>=std::max(source.relNext,beforeRel)&&id!=g_engine.getTableSchema(DB,"base").physicalRelationId&&id!=g_engine.getTableSchema(DB,"added").physicalRelationId,"actual CREATE cannot reuse restored/destination physical relation ID");
        s=state();s.newIdentity=id;expectedCells(s.base,sourceValues(true));expectedCells(s.probe,probeValues());writeState(result,s);check(inventory(root/"image")==imageBefore,"all original manifest/image bytes immutable");return;
    }
    check(phase=="cold-low"||phase=="cold-ahead","only fixed worker phase");const State expected=readState(result);State cold=state();expectedCells(cold.base,sourceValues(true));expectedCells(cold.probe,probeValues());check(sameRows(cold.base,expected.base)&&sameRows(cold.probe,expected.probe),"fresh process cold reopen preserves complete real RID/NULL/data");check(cold.tx>=expected.tx&&cold.relNext>=expected.relNext,"cold allocator state never regresses");check(g_engine.getTableSchema(DB,"post_identity").physicalRelationId==expected.newIdentity,"cold real schema identity retained");
    insert("base",9,"9","cold-insert");cold=state();expectedCells(cold.base,sourceValues(true,true));expectedCells(cold.probe,probeValues());check(xmin("base",cold.base.back().rid)>expected.tx,"cold insert real xid exceeds pre-close allocation floor");printRows(phase,"base",cold.base);printRows(phase,"added",cold.probe);
}
std::string startTicks(pid_t pid){std::ifstream in("/proc/"+std::to_string(pid)+"/stat");std::string line;std::getline(in,line);const auto at=line.rfind(')');check(at!=std::string::npos,"actual owned /proc identity");std::istringstream fields(line.substr(at+2));std::string value;for(int i=0;i<20;++i)fields>>value;check(!value.empty(),"owned starttime field");return value;}
void spawnWorker(const fs::path& executable,const fs::path& root,const fs::path& cluster,const std::string& phase) {
    std::vector<std::string> envStrings;for(char** p=environ;*p;++p)if(std::string(*p).rfind("DBMS_",0)!=0)envStrings.emplace_back(*p);envStrings.push_back("DBMS_DATA_DIR="+cluster.string());std::vector<char*> env;for(auto& s:envStrings)env.push_back(s.data());env.push_back(nullptr);
    std::vector<std::string> args={executable.string(),"--worker",phase,root.string()};std::vector<char*> argv;for(auto& s:args)argv.push_back(s.data());argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;check(posix_spawn_file_actions_init(&actions)==0,"spawn actions init");
    check(posix_spawn_file_actions_addchdir_np(&actions,cluster.c_str())==0,"set real cwd before exec/global g_engine");
    const auto stdoutPath=root/(phase+".stdout"),stderrPath=root/(phase+".stderr");
    check(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,stdoutPath.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0,"fresh owned child stdout");check(posix_spawn_file_actions_addopen(&actions,STDERR_FILENO,stderrPath.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0,"fresh owned child stderr");
    posix_spawnattr_t attributes;check(posix_spawnattr_init(&attributes)==0,"spawn attributes init");check(posix_spawnattr_setflags(&attributes,POSIX_SPAWN_SETPGROUP)==0&&posix_spawnattr_setpgroup(&attributes,0)==0,"own child process group");pid_t pid=0;
    const int error=posix_spawn(&pid,executable.c_str(),&actions,&attributes,argv.data(),env.data());posix_spawn_file_actions_destroy(&actions);posix_spawnattr_destroy(&attributes);check(error==0&&pid>0,"actual fresh worker exec");
    struct OwnedChild {
        pid_t pid;bool reaped=false;
        ~OwnedChild(){
            if(reaped)return;
            // Cleanup is restricted to this actual posix_spawn child/group.
            // It is never counted as a normal worker success.
            const bool ownGroup=getpgid(pid)==pid;
            if(ownGroup)(void)kill(-pid,SIGTERM);else(void)kill(pid,SIGTERM);
            int status=0;pid_t waited=0;
            for(int i=0;i<100;++i){waited=waitpid(pid,&status,WNOHANG);if(waited==pid||(waited<0&&errno==ECHILD))return;std::this_thread::sleep_for(std::chrono::milliseconds(5));}
            if(ownGroup)(void)kill(-pid,SIGKILL);else(void)kill(pid,SIGKILL);
            while(waitpid(pid,&status,0)<0&&errno==EINTR){}
        }
    } owned{pid};
    const auto ticks=startTicks(pid);check(getpgid(pid)==pid,"actual owned worker group");std::cout<<"RESTORE_ALLOC_CHILD phase="<<phase<<" pid="<<pid<<" start_ticks="<<ticks<<'\n'<<std::flush;
    int status=0;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(180);pid_t waited=0;
    for(;;){waited=waitpid(pid,&status,WNOHANG);if(waited==pid)break;if(waited<0&&errno!=EINTR)break;if(std::chrono::steady_clock::now()>=deadline)throw std::runtime_error("owned worker timeout (not normal lifecycle pass): "+phase);std::this_thread::sleep_for(std::chrono::milliseconds(5));}
    owned.reaped=waited==pid;
    std::cout<<"RESTORE_ALLOC_CHILD_TERMINAL phase="<<phase<<" status="<<status<<" stdout="<<stdoutPath<<" stderr="<<stderrPath<<'\n'<<std::flush;
    check(waited==pid&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"worker real normal0: "+phase);check(kill(pid,0)==-1&&errno==ESRCH,"actual reaped worker PID absent");check(kill(-pid,0)==-1&&errno==ESRCH,"actual owned process group absent");
}
void parent(const fs::path& exe) {
    std::string pattern=(fs::temp_directory_path()/"dbms-restore-horizon-XXXXXX").string();std::vector<char> name(pattern.begin(),pattern.end());name.push_back(0);const char* made=mkdtemp(name.data());check(made!=nullptr,"fresh unique owned root");const fs::path root=fs::canonical(made);std::cout<<"RESTORE_ALLOC_OWNED_ROOT "<<root<<'\n'<<std::flush;
    const auto early=root/"early",later=root/"later",low=root/"low",ahead=root/"ahead";fs::create_directory(early);spawnWorker(exe,root,early,"seed");const auto earlyClosed=inventory(early);cloneClosed(early,later);cloneClosed(early,low);cloneClosed(early,ahead);
    spawnWorker(exe,root,later,"later");const auto image=inventory(root/"image");check(fs::is_regular_file(root/"image/.dbms_backup_manifest")&&fs::is_regular_file(root/"image/.dbms_physical_backup"),"real immutable manifest image exists");spawnWorker(exe,root,ahead,"advance");
    spawnWorker(exe,root,low,"restore-low");spawnWorker(exe,root,low,"cold-low");spawnWorker(exe,root,ahead,"restore-ahead");spawnWorker(exe,root,ahead,"cold-ahead");
    check(inventory(root/"image")==image,"whole original image unchanged through all restores");check(inventory(early)==earlyClosed,"original earlier cluster preserved byteexact");
    std::cout<<"RESTORE_ALLOC_REGRESSION_COMPLETE workers=7 artificial_counter_rewinds=0 default_engine_global_pre_main=exec_isolated full_value_RID_NULL=1 owned="<<root<<'\n';
    // Leave owned evidence for the normal native runner; no cleanup wildcard.
}
} // namespace
int main(int argc,char** argv) {
    try{if(argc==4&&std::string(argv[1])=="--worker"){worker(argv[2],fs::canonical(argv[3]));std::cout<<"RESTORE_ALLOC_WORKER_CHECKS "<<checks<<" failures=0\n";return 0;}check(argc==1,"fixed self-exec worker protocol only");parent(fs::canonical("/proc/self/exe"));std::cout<<"Tests run: "<<checks<<"\nTests failed: 0\n";return 0;}
    catch(const std::exception& e){std::cerr<<"RESTORE_ALLOC_FAILURE checks="<<checks<<" detail="<<e.what()<<'\n';std::cout<<"Tests run: "<<checks<<"\nTests failed: 1\n";return 1;}
}

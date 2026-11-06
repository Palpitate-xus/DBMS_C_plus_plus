// Native regression: fixed19 CHECK definition expressions and actual source-collation lifecycle.
// test_link_flags: -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
// Compile against matching before/final public headers, core61 and fresh stub.
// A runner must nm-prove separate-TU fetchPage references/definition before run.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/sha256.h"
#include "storage/PageAllocator.h"
#include "utils/Session.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <spawn.h>
#include <sstream>
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
using namespace dbms;
namespace fs = std::filesystem;
using Cell = std::optional<std::string>;
using Cells = std::vector<Cell>;
using RowInput = StorageEngine::SqlRow;
const std::string database = "constraint_definition_signature";
size_t checks = 0, failures = 0, groups = 0, fetchProbes = 0, zeroFetch = 0, positiveFetch = 0;
thread_local std::array<PageAllocator*, 2> watched{};
thread_local std::array<size_t, 2> watchedCounts{};
thread_local bool armed = false;
void check(bool condition, const std::string& label) {
    ++checks;
    if (!condition) { ++failures; std::cerr << "CONSTRAINT_LIFECYCLE_FAIL " << label << '\n'; }
}
void need(bool condition, const std::string& label) {
    check(condition, label);
    if (!condition) throw std::runtime_error("unsafe fixture continuation: " + label);
}
struct Watch {
    explicit Watch(PageAllocator* a, PageAllocator* b = nullptr) {
        need(!armed && a, "actual target allocator and nonnested watch");
        watched = {a, b}; watchedCounts = {}; armed = true;
    }
    ~Watch() { armed = false; watched = {}; }
};
struct ActualRow { int64_t rid = 0; std::vector<std::string> values; std::vector<bool> nulls; size_t rawBytes = 0; };
using Rows = std::map<int, ActualRow>;
bool sameRows(const Rows& a, const Rows& b) {
    if (a.size() != b.size()) return false;
    auto x = a.begin(), y = b.begin();
    for (; x != a.end(); ++x, ++y)
        if (x->first != y->first || x->second.rid != y->second.rid ||
            x->second.values != y->second.values || x->second.nulls != y->second.nulls) return false;
    return true;
}
std::string bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary); need(bool(in), "read actual file " + path.string());
    std::string value{std::istreambuf_iterator<char>(in), {}};
    need(!in.bad(), "complete actual file read"); return value;
}
Rows rows(const std::string& table) {
    const auto schema = g_engine.getTableSchema(database, table);
    auto scan = g_engine.beginHeapScan(database, table, schema);
    need(bool(scan), "real heap cursor " + table);
    Rows actual; HeapScanRow row;
    while (scan->next(row)) {
        need(row.nulls.size() == schema.len, "real schema-width NULL bitmap");
        ActualRow value; value.rid = row.rid; value.rawBytes = row.data.size();
        detail::decodeStoredRowValues(g_engine, database, row.data, schema, row.nulls, value.values, value.nulls);
        need(value.values.size() == schema.len && value.nulls == row.nulls && !value.nulls.front(),
             "physical decoder agrees with actual cursor NULL owner");
        size_t used = 0; const int id = std::stoi(value.values.front(), &used);
        need(used == value.values.front().size() && value.rid > 0 && actual.emplace(id, value).second,
             "complete genuine unique PK/RID");
        uint32_t page = 0; uint16_t slot = 0;
        StorageEngine::decodeRid(row.rid, page, slot);
        need(page > 0 && StorageEngine::encodeRid(page, slot) == row.rid, "actual physical RID roundtrip");
    }
    return actual;
}
void expected(const Rows& actual, const std::map<int, Cells>& wanted, const std::string& label) {
    check(actual.size() == wanted.size(), label + " complete row count");
    for (const auto& item : wanted) {
        const auto found = actual.find(item.first);
        need(found != actual.end(), label + " genuine expected PK");
        const auto& value = found->second;
        need(value.values.size() == item.second.size(), label + " actual width");
        for (size_t i = 0; i < item.second.size(); ++i)
            check(value.nulls[i] == !item.second[i] && (!item.second[i] || value.values[i] == *item.second[i]),
                  label + " full value/NULL PK=" + std::to_string(item.first) + " col=" + std::to_string(i));
        std::cout << "CONSTRAINT_LIFECYCLE_ROW " << label << " id=" << item.first << " rid=" << value.rid;
        for (size_t i = 0; i < value.values.size(); ++i)
            std::cout << ' ' << value.nulls[i] << ' ' << std::quoted(value.values[i]);
        std::cout << '\n';
    }
}
struct SqlResult { bool success = false; std::string state; };
Session session;
SqlResult execute(const std::string& sql) {
    SqlResult value;
    try { DdlExecutor ddl; value.success = !ddl.executeSql(sql, session); }
    catch (const DbError& error) { value.state = error.sqlState(); }
    std::cout << "CONSTRAINT_LIFECYCLE_SQL success=" << value.success << " state=" << std::quoted(value.state)
              << " sql=" << std::quoted(sql) << '\n';
    return value;
}
void ddl(const std::string& sql) { need(execute(sql).success, "real DDL succeeds: " + sql); }
void sqlState(const std::string& sql, const std::string& state) {
    const auto result = execute(sql);
    check(!result.success && result.state == state, "real DDL SQLSTATE " + state + ": " + sql);
}
void insert(const std::string& table, const RowInput& values, DBStatus status = DBStatus::OK) {
    const auto actual = g_engine.insertRow(database, table, values);
    std::cout << "CONSTRAINT_LIFECYCLE_INSERT table=" << table << " expected=" << sqlstateForDBStatus(status)
              << " actual=" << sqlstateForDBStatus(actual) << '\n';
    need(actual == status, "native INSERT exact status " + table);
}
void update(const std::string& table, const RowInput& values, int id, DBStatus status = DBStatus::OK) {
    const auto actual = g_engine.updateRows(database, table, values, {"=id " + std::to_string(id)});
    std::cout << "CONSTRAINT_LIFECYCLE_UPDATE table=" << table << " id=" << id
              << " expected=" << sqlstateForDBStatus(status) << " actual=" << sqlstateForDBStatus(actual) << '\n';
    need(actual == status, "native UPDATE exact status " + table);
}
void flags(const std::string& table, const std::string& constraint, bool validated) {
    const auto params = g_engine.getStorageParams(database, table);
    const auto prefix = "constraint." + constraint + ".";
    const auto a = params.find(prefix + "validated"), b = params.find(prefix + "not_valid");
    need(a != params.end() && b != params.end(), "both actual persisted validation flags exist");
    check(a->second == (validated ? "1" : "0") && b->second == (validated ? "0" : "1"),
          "actual flags " + table + "." + constraint);
    const auto raw = bytes(fs::path(database) / (table + ".params"));
    check(raw.find(prefix + "validated=" + (validated ? "1" : "0") + "\n") != std::string::npos &&
          raw.find(prefix + "not_valid=" + (validated ? "0" : "1") + "\n") != std::string::npos,
          "real params file contains same validated/not_valid pair");
}
PageAllocator* target(const std::string& table) {
    const auto schema = g_engine.getTableSchema(database, table);
    need(schema.len > 0, "warm actual schema");
    auto* allocator = g_engine.getPageAllocator(database, table);
    need(allocator != nullptr, "actual target allocator"); return allocator;
}
void positive(const std::string& table) {
    auto* allocator = target(table); size_t count = 0; bool ok = false;
    { Watch watch(allocator); ok = g_engine.forEachRow(database, table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }); }
    ++fetchProbes; ++positiveFetch;
    need(ok && count > 0 && watchedCounts[0] > 0, "external real heap fetch wrapper positive control: " + table);
    std::cout << "CONSTRAINT_LIFECYCLE_FETCH positive=" << table << " rows=" << count << " fetch=" << watchedCounts[0] << '\n';
}
SqlResult watchedSql(const std::string& sql, const std::string& a, const std::string& b,
    bool mustScan, const std::string& state = "") {
    auto* pa = target(a); auto* pb = b.empty() ? nullptr : target(b);
    SqlResult result;
    { Watch watch(pa, pb); result = execute(sql); }
    ++fetchProbes;
    std::cout << "CONSTRAINT_LIFECYCLE_FETCH sql=" << std::quoted(sql)
              << " a=" << watchedCounts[0] << " b=" << watchedCounts[1] << '\n';
    if (mustScan) {
        ++positiveFetch; check(watchedCounts[0] > 0, "actual validation scans first target");
        if (pb) check(watchedCounts[1] > 0, "actual FK validation scans second target");
    } else {
        ++zeroFetch; check(watchedCounts[0] == 0 && watchedCounts[1] == 0,
                          "NOT VALID/definition/already-valid performs zero target heap fetch");
    }
    if (state.empty()) need(result.success, "watched statement succeeds");
    else need(!result.success && result.state == state, "watched statement preserves actual SQLSTATE " + state);
    return result;
}
template<class F> void group(const std::string& label, F&& run) {
    ++groups; std::cout << "CONSTRAINT_LIFECYCLE_GROUP " << groups << ' ' << label << '\n';
    const size_t oldFailures = failures;
    try { run(); }
    catch (const std::exception& error) {
        if (failures == oldFailures) { ++checks; ++failures; }
        std::cerr << "CONSTRAINT_LIFECYCLE_GROUP_STOP " << label << ' ' << error.what() << '\n';
        if (g_engine.inTransaction())
            check(g_engine.rollbackTransaction() == DBStatus::OK, "failing independent group releases its actual transaction");
    }
}
std::map<std::string, std::map<int, Cells>> wantedCold;
struct DefinitionCase { const char* name; const char* expression; const char* state; bool nativeQuery; };
const std::vector<DefinitionCase> definitionCases = {
    {"bad-like-rhs","tag LIKE n","42883",false},
    {"bad-like-lhs","n LIKE tag","42883",false},
    {"good-like-text","tag LIKE other_tag","",false},
    {"good-like-unknown","tag LIKE 'a%'","",false},
    {"good-like-NULL","tag LIKE NULL","",false},
    {"good-like-bytea","bytes LIKE other_bytes","",false},
    {"bad-ilike-bytea","bytes ILIKE other_bytes","42883",false},
    {"bad-IN-types","n IN(tag)","42883",false},
    {"bad-NOT-IN-types","n NOT IN(tag)","42883",false},
    {"good-IN-integer","n IN(1,2,NULL)","",false},
    {"good-IN-text","tag IN('a',NULL)","",false},
    {"bad-distinct-types","n IS DISTINCT FROM tag","42883",false},
    {"bad-not-distinct-types","n IS NOT DISTINCT FROM tag","42883",false},
    {"good-distinct-NULL","n IS DISTINCT FROM NULL","",false},
    {"query-head-native","(SELECT true)","0A000",true},
    {"query-head-values","(VALUES(true))","0A000",true},
    {"query-head-table","EXISTS(TABLE other_table)","0A000",true},
    {"quoted-query-text-positive","tag <> '(SELECT true)'","",false},
    {"quoted-query-identifier-positive","\"select\" > 0","",false},
};
void lifecycle() {
    group("19-pure-definition-signatures", [&] {
        ddl("CREATE TABLE other_table(id INT)");
        ddl("CREATE TABLE cv_sig(id INT PRIMARY KEY,n INT,tag TEXT,other_tag TEXT,bytes BYTEA,other_bytes BYTEA,\"select\" INT)");
        need(definitionCases.size()==19,"fixed19 original expression plan");
        for(size_t i=0;i<definitionCases.size();++i) {
            const auto& item=definitionCases[i];
            const std::string name="cv_sig_"+std::to_string(i);
            const auto prior=g_engine.getStorageParams(database,"cv_sig");
            const std::string sql="ALTER TABLE cv_sig ADD CONSTRAINT "+name+" CHECK ("+item.expression+") NOT VALID";
            watchedSql(sql,"cv_sig","",false,item.state);
            if(std::string(item.state).empty()) flags("cv_sig",name,false);
            else {
                const auto actual=g_engine.getStorageParams(database,"cv_sig");
                check(actual==prior,"bad signature does not publish any params: "+std::string(item.name));
                const auto schema=g_engine.getTableSchema(database,"cv_sig");
                bool exists=false;
                for(size_t c=0;c<schema.len;++c)exists=exists||schema.cols[c].checkConstraintName==name;
                for(const auto& constraint:schema.additionalCheckConstraints)exists=exists||constraint.name==name;
                check(!exists,"bad signature does not publish a CHECK slot: "+std::string(item.name));
            }
            std::cout<<"CONSTRAINT_DEFINITION_CASE index="<<i<<" name="<<std::quoted(item.name)
                     <<" expression="<<std::quoted(item.expression)<<" expected_state="<<std::quoted(item.state)<<'\n';
            if(item.nativeQuery) {
                auto* allocator=target("cv_sig");std::string state;DBStatus legacy=DBStatus::OK;
                { Watch watch(allocator);
                    try {(void)g_engine.alterTableAddCheckConstraint(database,"cv_sig",name+"_rich",item.expression,false);}
                    catch(const DbError& error){state=error.sqlState();}
                }
                ++fetchProbes;++zeroFetch;
                check(state=="0A000"&&watchedCounts[0]==0,"rich native query definition refuses actual token role without heap fetch");
                { Watch watch(allocator);legacy=g_engine.alterTableAddCheckConstraint(database,"cv_sig",name+"_legacy",item.expression); }
                ++fetchProbes;++zeroFetch;
                check(legacy==DBStatus::INVALID_VALUE&&watchedCounts[0]==0,"old native overload maps user query definition to legacy INVALID without heap fetch");
                check(g_engine.getStorageParams(database,"cv_sig")==prior,"native query refusal leaves params unchanged");
                std::cout<<"CONSTRAINT_DEFINITION_NATIVE name="<<std::quoted(item.name)<<" rich_state="<<std::quoted(state)
                         <<" legacy_status="<<sqlstateForDBStatus(legacy)<<'\n';
            }
        }
        check(rows("cv_sig").empty(),"all metadata definitions kept actual target heap empty");
    });
    group("actual-nocase-source-collation", [&] {
        ddl("CREATE TABLE cv_collate(id INT PRIMARY KEY,tag VARCHAR(50) COLLATE nocase)");
        insert("cv_collate",{{"id","1"},{"tag","A"}});
        insert("cv_collate",{{"id","2"},{"tag",std::nullopt}});
        insert("cv_collate",{{"id","3"},{"tag","a"}});
        const auto before=rows("cv_collate");positive("cv_collate");
        watchedSql("ALTER TABLE cv_collate ADD CONSTRAINT cv_collate_ck CHECK (tag = 'a') NOT VALID","cv_collate","",false);
        flags("cv_collate","cv_collate_ck",false);
        check(sameRows(before,rows("cv_collate")),"collation definition changes no real old value/RID/NULL");
        watchedSql("ALTER TABLE cv_collate VALIDATE CONSTRAINT cv_collate_ck","cv_collate","",true);
        flags("cv_collate","cv_collate_ck",true);
        insert("cv_collate",{{"id","4"},{"tag","A"}});
        update("cv_collate",{{"tag","a"}},1);
        update("cv_collate",{{"tag","A"}},1);
        insert("cv_collate",{{"id","91"},{"tag","B"}},DBStatus::CHECK_VIOLATION);
        update("cv_collate",{{"tag","B"}},3,DBStatus::CHECK_VIOLATION);
        insert("cv_collate",{{"id","92"},{"tag",""}},DBStatus::CHECK_VIOLATION);
        insert("cv_collate",{{"id","93"},{"tag","NULL"}},DBStatus::CHECK_VIOLATION);
        insert("cv_collate",{{"id","5"},{"tag",std::nullopt}});
        watchedSql("ALTER TABLE cv_collate ADD CONSTRAINT cv_collate_qualified CHECK (cv_collate.tag = 'a') NOT VALID","cv_collate","",false);
        watchedSql("ALTER TABLE cv_collate VALIDATE CONSTRAINT cv_collate_qualified","cv_collate","",true);
        flags("cv_collate","cv_collate_qualified",true);
        watchedSql("ALTER TABLE cv_collate VALIDATE CONSTRAINT cv_collate_ck","cv_collate","",false);
        wantedCold["cv_collate"]={{1,{{"1"},{"A"}}},{2,{{"2"},std::nullopt}},
            {3,{{"3"},{"a"}}},{4,{{"4"},{"A"}}},{5,{{"5"},std::nullopt}}};
        expected(rows("cv_collate"),wantedCold.at("cv_collate"),"actual-nocase");
    });
    group("actual-C-source-collation-counterexample", [&] {
        ddl("CREATE TABLE cv_binary(id INT PRIMARY KEY,tag VARCHAR(50) COLLATE C)");
        insert("cv_binary",{{"id","1"},{"tag","A"}});positive("cv_binary");
        watchedSql("ALTER TABLE cv_binary ADD CONSTRAINT cv_binary_ck CHECK (tag = 'a') NOT VALID","cv_binary","",false);
        flags("cv_binary","cv_binary_ck",false);
        const auto before=rows("cv_binary");const auto params=bytes(database+"/cv_binary.params");
        watchedSql("ALTER TABLE cv_binary VALIDATE CONSTRAINT cv_binary_ck","cv_binary","",true,"23514");
        flags("cv_binary","cv_binary_ck",false);
        check(sameRows(before,rows("cv_binary"))&&bytes(database+"/cv_binary.params")==params,"binary comparison failure preserves raw params/heap");
        update("cv_binary",{{"tag","a"}},1);
        watchedSql("ALTER TABLE cv_binary VALIDATE CONSTRAINT cv_binary_ck","cv_binary","",true);
        flags("cv_binary","cv_binary_ck",true);
        insert("cv_binary",{{"id","91"},{"tag","A"}},DBStatus::CHECK_VIOLATION);
        insert("cv_binary",{{"id","2"},{"tag",std::nullopt}});
        wantedCold["cv_binary"]={{1,{{"1"},{"a"}}},{2,{{"2"},std::nullopt}}};
        expected(rows("cv_binary"),wantedCold.at("cv_binary"),"actual-C");
    });
}
void saveCold(const fs::path& path) {
    need(!fs::exists(path),"fresh cold reference file");std::ofstream out(path,std::ios::binary);
    for(const auto& table:wantedCold) {
        const auto actual=rows(table.first);expected(actual,table.second,"pre-cold-"+table.first);
        out<<"TABLE "<<std::quoted(table.first)<<' '<<actual.size()<<'\n';
        for(const auto& row:actual) {out<<row.first<<' '<<row.second.rid<<' '<<row.second.values.size();
            for(size_t i=0;i<row.second.values.size();++i)out<<' '<<row.second.nulls[i]<<' '<<std::quoted(row.second.values[i]);out<<'\n';}
        out<<"PARAMS "<<std::quoted(bytes(fs::path(database)/(table.first+".params")))<<'\n';
    }
    out.close();need(!out.fail(),"complete independent cold reference write");
}
void cold(const fs::path& path) {
    std::ifstream in(path,std::ios::binary);need(bool(in),"read actual closed prior worker reference");
    std::string word;
    while(in>>word) {
        need(word=="TABLE","strict cold reference record");std::string table;size_t count=0;in>>std::quoted(table)>>count;Rows expectedRows;
        for(size_t i=0;i<count;++i) {int id=0;ActualRow row;size_t width=0;in>>id>>row.rid>>width;
            for(size_t c=0;c<width;++c){bool null=false;std::string value;in>>null>>std::quoted(value);row.nulls.push_back(null);row.values.push_back(std::move(value));}
            need(bool(in)&&expectedRows.emplace(id,std::move(row)).second,"complete cold row source");}
        std::string raw;in>>word>>std::quoted(raw);need(bool(in)&&word=="PARAMS","actual params cold source");
        check(sameRows(rows(table),expectedRows),"fresh process exact full value/NULL/RID: "+table);
        check(bytes(fs::path(database)/(table+".params"))==raw,"fresh process raw validation metadata exact: "+table);
    }
    need(in.eof(),"complete prior worker reference consumed");
    flags("cv_collate","cv_collate_ck",true);flags("cv_collate","cv_collate_qualified",true);flags("cv_binary","cv_binary_ck",true);
    positive("cv_collate");positive("cv_binary");
    watchedSql("ALTER TABLE cv_collate VALIDATE CONSTRAINT cv_collate_ck","cv_collate","",false);
    watchedSql("ALTER TABLE cv_collate VALIDATE CONSTRAINT cv_collate_qualified","cv_collate","",false);
    watchedSql("ALTER TABLE cv_binary VALIDATE CONSTRAINT cv_binary_ck","cv_binary","",false);
    insert("cv_collate",{{"id","91"},{"tag","B"}},DBStatus::CHECK_VIOLATION);
    insert("cv_binary",{{"id","91"},{"tag","A"}},DBStatus::CHECK_VIOLATION);
    const auto before=rows("cv_collate");update("cv_collate",{{"tag","a"}},1);update("cv_collate",{{"tag","A"}},1);
    const auto after=rows("cv_collate");
    check(before.size()==after.size(),"cold case-cycle preserves full row membership");
    for(const auto& item:before) {
        const auto found=after.find(item.first);need(found!=after.end(),"cold actual updated PK remains present");
        check(item.second.values==found->second.values&&item.second.nulls==found->second.nulls,
              "cold case-cycle preserves complete actual values and NULLs");
        if(item.first!=1)check(item.second.rid==found->second.rid,"unmodified cold row keeps its physical RID");
    }
    // The actually updated row has a genuine positive, unique RID checked by
    // rows(); MVCC is allowed to replace that row's physical tuple identity.

}
void worker(const std::string& phase,const fs::path& root) {
    const auto cluster=fs::canonical(fs::current_path());const auto* env=std::getenv("DBMS_DATA_DIR");
    need(env&&fs::equivalent(cluster,env)&&cluster==root/"cluster","real cwd/env set before exec/global engine");
    TypeRegistry::instance().bootstrap();session.username="admin";session.permission=1;session.currentDB=database;
    struct RestoreSession {Session* old;~RestoreSession(){setCurrentSession(old);}} restore{currentSession()};setCurrentSession(&session);
    if(phase=="lifecycle") {
        need(!g_engine.databaseExists(database)&&g_engine.createDatabase(database,"utf8")==DBStatus::OK,"fresh private real database");
        lifecycle();
        if(!failures) {need(wantedCold.size()==2,"all independent cold fixtures present");saveCold(root/"cold-reference.txt");}
    } else {need(phase=="cold"&&g_engine.databaseExists(database),"fixed actual existing cold phase");cold(root/"cold-reference.txt");}
    if(g_engine.inTransaction()) {const auto status=g_engine.rollbackTransaction();check(status==DBStatus::OK,"unclosed failing fixture transaction explicitly rolled back");}
    std::cout<<"CONSTRAINT_LIFECYCLE_WORKER phase="<<phase<<" groups="<<groups<<" checks="<<checks<<" failures="<<failures
             <<" fetch_probes="<<fetchProbes<<" zero_fetch="<<zeroFetch<<" positive_fetch="<<positiveFetch<<'\n';
}
void spawn(const fs::path& exe,const fs::path& root,const std::string& phase) {
    const auto cluster=root/"cluster";
    std::vector<std::string> environment;for(char** p=environ;*p;++p)if(std::string(*p).rfind("DBMS_",0)!=0)environment.emplace_back(*p);
    environment.push_back("DBMS_DATA_DIR="+cluster.string());std::vector<char*> env;for(auto& x:environment)env.push_back(x.data());env.push_back(nullptr);
    std::vector<std::string> arguments={exe.string(),"--worker",phase,root.string()};std::vector<char*> args;for(auto& x:arguments)args.push_back(x.data());args.push_back(nullptr);
    posix_spawn_file_actions_t actions;need(posix_spawn_file_actions_init(&actions)==0,"actual spawn actions");
    need(posix_spawn_file_actions_addchdir_np(&actions,cluster.c_str())==0,"child real cwd before global engine initialization");
    const auto out=root/(phase+".stdout"),err=root/(phase+".stderr");
    need(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,out.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0&&
         posix_spawn_file_actions_addopen(&actions,STDERR_FILENO,err.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0,"new child raw output only");
    posix_spawnattr_t attributes;need(posix_spawnattr_init(&attributes)==0,"actual spawn attrs");
    need(posix_spawnattr_setflags(&attributes,POSIX_SPAWN_SETPGROUP)==0&&posix_spawnattr_setpgroup(&attributes,0)==0,"owned child process group");pid_t pid=0;
    const int error=posix_spawn(&pid,exe.c_str(),&actions,&attributes,args.data(),env.data());posix_spawn_file_actions_destroy(&actions);posix_spawnattr_destroy(&attributes);
    need(error==0&&pid>0,"actual fresh worker exec");
    struct Child {pid_t pid;bool reaped=false;~Child(){if(reaped)return;const bool own=getpgid(pid)==pid;if(own)(void)kill(-pid,SIGTERM);else(void)kill(pid,SIGTERM);
        int status=0;for(int i=0;i<100;++i){const auto w=waitpid(pid,&status,WNOHANG);if(w==pid||(w<0&&errno==ECHILD))return;std::this_thread::sleep_for(std::chrono::milliseconds(5));}
        if(own)(void)kill(-pid,SIGKILL);else(void)kill(pid,SIGKILL);while(waitpid(pid,&status,0)<0&&errno==EINTR){}}} child{pid};
    std::ifstream stat("/proc/"+std::to_string(pid)+"/stat");std::string line;std::getline(stat,line);const auto close=line.rfind(')');
    need(close!=std::string::npos,"real child start identity");std::istringstream fields(line.substr(close+2));std::string ticks;for(int i=0;i<20;++i)fields>>ticks;
    std::cout<<"CONSTRAINT_LIFECYCLE_CHILD phase="<<phase<<" pid="<<pid<<" start_ticks="<<ticks<<'\n'<<std::flush;
    int status=0;pid_t waited=0;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(180);
    for(;;){waited=waitpid(pid,&status,WNOHANG);if(waited==pid)break;if(waited<0&&errno!=EINTR)break;
        if(std::chrono::steady_clock::now()>=deadline)throw std::runtime_error("owned worker timeout (not normal): "+phase);std::this_thread::sleep_for(std::chrono::milliseconds(5));}
    child.reaped=waited==pid;std::cout<<"CONSTRAINT_LIFECYCLE_CHILD_TERMINAL phase="<<phase<<" status="<<status<<" stdout="<<out<<" stderr="<<err<<'\n'<<std::flush;
    need(waited==pid&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"strict fresh worker normal0: "+phase);
    need(kill(pid,0)==-1&&errno==ESRCH&&kill(-pid,0)==-1&&errno==ESRCH,"actual child/group gone");
}
// Parent-only evidence capture after the actual child has exited normally.
// A later cold close can never replace an earlier phase's captured bytes.
void captureClosedPhase(const fs::path& root, const std::string& phase) {
    need(!armed && (phase == "lifecycle" || phase == "cold"), "phase capture outside fetch watch");
    const auto source = root / "cluster", imageRoot = root / "images", destination = imageRoot / phase;
    need(fs::is_directory(source) && !fs::is_symlink(source), "actual closed phase cluster");
    if (!fs::exists(imageRoot)) need(fs::create_directory(imageRoot), "new parent-owned image root");
    need(fs::is_directory(imageRoot) && !fs::is_symlink(imageRoot) && !fs::exists(destination), "fresh phase-specific image");
    need(fs::create_directory(destination), "new exact phase image directory");
    std::vector<fs::path> directories, files;
    for (const auto& entry : fs::recursive_directory_iterator(source)) {
        const auto status = entry.symlink_status();
        need(!fs::is_symlink(status), "reject symlink in closed phase inventory");
        const auto relative = entry.path().lexically_relative(source);
        need(!relative.empty() && !relative.is_absolute(), "actual relative phase path");
        for (const auto& component : relative) need(component != "..", "phase path stays under owned cluster");
        if (fs::is_directory(status)) directories.push_back(relative);
        else { need(fs::is_regular_file(status), "closed evidence is a regular file or directory"); files.push_back(relative); }
    }
    std::sort(directories.begin(), directories.end()); std::sort(files.begin(), files.end());
    const auto manifest = root / (phase + "-image-inventory-v1.txt");
    need(!fs::exists(manifest), "new fixed phase inventory path");
    std::ofstream inventory(manifest, std::ios::binary);
    need(bool(inventory), "open real phase inventory");
    inventory << "CONSTRAINT_LIFECYCLE_PHASE_IMAGE_V1 " << std::quoted(phase) << ' '
              << files.size() << ' ' << directories.size() << '\n';
    for (const auto& relative : directories) {
        need(fs::create_directories(destination / relative) || fs::is_directory(destination / relative), "preserve actual empty/nonempty directory");
        inventory << "D " << std::quoted(relative.generic_string()) << '\n';
    }
    uintmax_t totalBytes = 0;
    std::array<char, 64 * 1024> buffer{};
    for (const auto& relative : files) {
        const auto inputPath = source / relative, outputPath = destination / relative;
        const uintmax_t originalLength = fs::file_size(inputPath);
        need(!fs::exists(outputPath), "never overwrite an existing phase file");
        std::ifstream input(inputPath, std::ios::binary);
        std::ofstream output(outputPath, std::ios::binary);
        need(bool(input) && bool(output), "open exact source and new phase destination");
        SHA256 digest; uintmax_t count = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const auto length = input.gcount();
            if (length > 0) { digest.append(buffer.data(), static_cast<size_t>(length)); output.write(buffer.data(), length); count += static_cast<uintmax_t>(length); }
        }
        need(input.eof() && !input.bad() && count == originalLength && fs::file_size(inputPath) == originalLength,
             "complete stable closed source bytes, including zero-length files");
        output.close(); need(!output.fail(), "complete actual phase copy write");
        const auto sha = digest.finishHex();
        std::ifstream copied(outputPath, std::ios::binary); need(bool(copied), "read back actual phase destination");
        SHA256 copiedDigest; uintmax_t copiedCount = 0;
        while (copied) {
            copied.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const auto length = copied.gcount();
            if (length > 0) { copiedDigest.append(buffer.data(), static_cast<size_t>(length)); copiedCount += static_cast<uintmax_t>(length); }
        }
        need(copied.eof() && !copied.bad() && copiedCount == count && copiedDigest.finishHex() == sha,
             "copied phase full bytes/SHA equal captured source");
        totalBytes += count;
        const auto imageRelative = outputPath.lexically_relative(root).generic_string();
        inventory << "F " << std::quoted(relative.generic_string()) << ' ' << count << ' ' << std::quoted(sha)
                  << ' ' << std::quoted(imageRelative) << '\n';
        std::cout << "CONSTRAINT_LIFECYCLE_PHASE_FILE phase=" << std::quoted(phase)
                  << " source_relative=" << std::quoted(relative.generic_string())
                  << " image_relative=" << std::quoted(imageRelative) << " bytes=" << count << " sha256=" << sha << '\n';
    }
    inventory.close(); need(!inventory.fail(), "complete machine-readable phase inventory");
    std::cout << "CONSTRAINT_LIFECYCLE_PHASE_IMAGE phase=" << std::quoted(phase)
              << " source_relative=\"cluster\" image_relative=" << std::quoted(destination.lexically_relative(root).generic_string())
              << " manifest_relative=" << std::quoted(manifest.lexically_relative(root).generic_string())
              << " files=" << files.size() << " directories=" << directories.size() << " raw_bytes=" << totalBytes << '\n' << std::flush;
}

} // namespace
extern "C" char* realConstraintFetch(dbms::PageAllocator*,uint32_t) asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapConstraintFetch(dbms::PageAllocator*,uint32_t) asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapConstraintFetch(dbms::PageAllocator* allocator,uint32_t page) {
    if(armed)for(size_t i=0;i<watched.size();++i)if(allocator==watched[i])++watchedCounts[i];
    return realConstraintFetch(allocator,page);
}
int main(int argc,char** argv) {
    try {
        if(argc==4&&std::string(argv[1])=="--worker") {
            worker(argv[2],fs::canonical(argv[3]));
            std::cout<<"Tests run: "<<checks<<"\nTests failed: "<<failures<<'\n';return failures?1:0;
        }
        need(argc==1,"fixed standalone self-exec interface");std::string pattern=(fs::temp_directory_path()/"dbms-constraint-definition-XXXXXX").string();
        std::vector<char> name(pattern.begin(),pattern.end());name.push_back(0);const auto* made=mkdtemp(name.data());need(made,"fresh unique owned experiment root");
        const auto root=fs::canonical(made);need(fs::create_directory(root/"cluster"),"fresh genuine worker cluster");
        std::cout<<"CONSTRAINT_LIFECYCLE_OWNED_ROOT "<<root<<'\n'<<std::flush;
        const auto exe=fs::canonical("/proc/self/exe");
        spawn(exe,root,"lifecycle");captureClosedPhase(root,"lifecycle");
        spawn(exe,root,"cold");captureClosedPhase(root,"cold");
        std::cout<<"CONSTRAINT_LIFECYCLE_COMPLETE workers=2 no_fixture_retry=1 no_SQL_reparse_owner=1 owned="<<root<<'\n';
        std::cout<<"Tests run: "<<checks<<"\nTests failed: "<<failures<<'\n';return failures?1:0;
    } catch(const std::exception& error) {
        if(!failures)++failures;std::cerr<<"CONSTRAINT_LIFECYCLE_FATAL "<<error.what()<<'\n';
        std::cout<<"Tests run: "<<checks<<"\nTests failed: "<<failures<<'\n';return 1;
    }
}

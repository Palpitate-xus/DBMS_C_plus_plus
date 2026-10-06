#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;

struct PinnedHeap {
    std::string path;int fd=-1;struct stat before{};
    explicit PinnedHeap(std::string file):path(std::move(file)) {
        fd=::open(path.c_str(),O_RDONLY);assert(fd>=0);assert(::fstat(fd,&before)==0);
    }
    ~PinnedHeap(){if(fd>=0)::close(fd);}
    void unchanged() const {
        struct stat after{},pinned{};assert(::stat(path.c_str(),&after)==0);assert(::fstat(fd,&pinned)==0);
        std::cout<<"PURE_ALTER_HEAP old="<<before.st_dev<<':'<<before.st_ino
                 <<" current="<<after.st_dev<<':'<<after.st_ino<<" old_links="<<pinned.st_nlink<<std::endl;
        // Pinning prevents inode reuse: a database restore cannot replace
        // the heap and accidentally make this assertion pass.
        assert(before.st_dev==after.st_dev && before.st_ino==after.st_ino && pinned.st_nlink==before.st_nlink);
    }
};

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="ddl_add_column_clean_rollback",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.currentDB=db;session.username="admin";session.permission=1;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(base INT)",session));
    assert(g_engine.insertRow(db,"target",{{"base","7"}})==DBStatus::OK);
    const auto invalid=[&](const std::string& sql) {
        std::ostringstream output;auto* saved=std::cout.rdbuf(output.rdbuf());
        const bool failed=ddl.executeSql(sql,session);std::cout.rdbuf(saved);
        assert(failed && output.str().find("SQLSTATE 42601")!=std::string::npos);
    };
    {PinnedHeap heap(db+"/target.dt");invalid("ALTER TABLE target ADD COLUMN id SERIAL NULL");heap.unchanged();}
    assert(!g_engine.inTransaction());assert(g_engine.getTableSchema(db,"target").len==1);
    assert(g_engine.query(db,"target",{}, {"base"})==std::vector<std::string>{"7 "});

    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(g_engine.insertRow(db,"target",{{"base","8"}})==DBStatus::OK);
    assert(!g_engine.hasTransactionBackup());
    {PinnedHeap heap(db+"/target.dt");invalid("ALTER TABLE target ADD COLUMN id BIGSERIAL NULL");heap.unchanged();}
    assert(g_engine.inTransaction() && !g_engine.hasTransactionBackup());
    assert(g_engine.query(db,"target",{}, {"base"}).size()==2);
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(g_engine.query(db,"target",{}, {"base"}).size()==1);

    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(!ddl.executeSql("ALTER TABLE target ADD COLUMN kept INT",session));
    assert(g_engine.hasTransactionBackup() && g_engine.transactionBackupDirty());
    {PinnedHeap heap(db+"/target.dt");invalid("ALTER TABLE target ADD COLUMN id SMALLSERIAL NULL");heap.unchanged();}
    assert(g_engine.inTransaction() && g_engine.hasTransactionBackup() && g_engine.transactionBackupDirty());
    assert(g_engine.getTableSchema(db,"target").len==2);
    invalid("ALTER TABLE target ADD COLUMN leaked INT, ADD COLUMN id SERIAL NULL");
    auto schema=g_engine.getTableSchema(db,"target");
    assert(schema.len==2 && schema.cols[1].dataName=="kept");
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    assert(g_engine.getTableSchema(db,"target").len==1);
    invalid("ALTER TABLE target ADD COLUMN leaked INT, ADD COLUMN id SERIAL NULL");
    assert(!g_engine.inTransaction() && g_engine.getTableSchema(db,"target").len==1);
    assert(!ddl.executeSql("ALTER TABLE target ADD COLUMN nullable INT NULL",session));
    schema=g_engine.getTableSchema(db,"target");assert(schema.len==2 && schema.cols[1].isNull);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[DDL CLEAN ADD] pure validation preserves heap identity and dirty outer/multiaction rollback\n";
}

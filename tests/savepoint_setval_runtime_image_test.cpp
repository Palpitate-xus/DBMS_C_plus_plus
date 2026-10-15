#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/stat.h>

namespace fs=std::filesystem;
using namespace dbms;
static size_t images(const std::string& database) {
    size_t count=0;
    for(const auto& entry:fs::directory_iterator("."))
        count+=entry.is_directory() && entry.path().filename().string().rfind(
            database+".ddl_statement_backup.",0)==0;
    return count;
}
static ino_t identity(const fs::path& path) {
    struct stat value{};assert(::stat(path.c_str(),&value)==0);
    return value.st_ino;
}

int main() {
    TypeRegistry::instance().bootstrap();
    const std::string database="__t_savepoint_setval_runtime_image";
    StorageEngine engine;
    assert(engine.createDatabase(database)==DBStatus::OK);
    assert(engine.createSequence(database,"effects")==DBStatus::OK);
    assert(engine.beginTransaction(database,true)==DBStatus::OK);
    assert(engine.createTransactionBackup());
    engine.preserveTransactionBackupOnRollback(true);
    engine.restoreTransactionBackupBeforeRowUndo(true);
    engine.markTransactionBackupDirty();
    TableSchema table;table.tablename="items";table.append(makeIntColumn("id",false,4));
    assert(engine.createTable(database,table)==DBStatus::OK);
    const auto schema=fs::path(database)/"items.stc";
    assert(engine.savepoint("outer")==DBStatus::OK);
    assert(engine.setval(database,"effects",50,false)==50);
    assert(engine.savepoint("after_setval")==DBStatus::OK);
    assert(engine.nextval(database,"effects")==50);
    assert(engine.setval(database,"effects",60,true)==60);
    const auto before=identity(schema);
    assert(engine.rollbackToSavepoint("after_setval")==DBStatus::OK);
    const bool calledKeptImage=identity(schema)==before;
    assert(engine.nextval(database,"effects")==61);
    // Backward setval is legal and still nontransactional. Reuse must not
    // silently impose monotone counter values or change is_called=false.
    assert(engine.setval(database,"effects",1,false)==1);
    assert(engine.savepoint("backwards")==DBStatus::OK);
    const auto aliasImages=images(database);
    assert(engine.nextval(database,"effects")==1);
    const auto beforeBackward=identity(schema);
    assert(engine.rollbackToSavepoint("backwards")==DBStatus::OK);
    const bool backwardKeptImage=identity(schema)==beforeBackward;
    assert(engine.nextval(database,"effects")==2);
    assert(engine.rollbackTransaction()==DBStatus::OK);
    assert(!engine.tableExists(database,"items") && images(database)==0);
    assert(engine.nextval(database,"effects")==3);
    std::cout<<"SAVEPOINT_SETVAL_RUNTIME_IMAGE aliases="<<aliasImages
             <<" called_kept_image="<<calledKeptImage
             <<" backward_kept_image="<<backwardKeptImage<<" final_nextval=3\n";
    return aliasImages==1 && calledKeptImage && backwardKeptImage?0:1;
}

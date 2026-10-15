#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
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
static std::string readBytes(const fs::path& path) {
    std::ifstream input(path,std::ios::binary);assert(input);
    return {std::istreambuf_iterator<char>(input),{}};
}
static void overwrite(const fs::path& path,const std::string& bytes) {
    std::ofstream output(path,std::ios::binary|std::ios::trunc);
    output<<bytes;output.close();assert(output);
}

int main() {
    TypeRegistry::instance().bootstrap();
    const std::string database="__t_savepoint_sequence_runtime_image";
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
    assert(engine.insert(database,"items",{{"id","7"}})==DBStatus::OK);
    const auto schema=fs::path(database)/"items.stc";
    for(int iteration=0;iteration<16;++iteration) {
        assert(engine.nextval(database,"effects")==iteration+1);
        assert(engine.savepoint("same")==DBStatus::OK);
        const auto before=identity(schema);
        assert(engine.rollbackToSavepoint("same")==DBStatus::OK);
        assert(identity(schema)==before);
    }
    const auto aliasImages=images(database);
    assert(engine.nextval(database,"effects")==17);
    const auto beforeSequenceRollback=identity(schema);
    assert(engine.rollbackToSavepoint("same")==DBStatus::OK);
    const bool sequenceOnlyKeptImage=identity(schema)==beforeSequenceRollback;
    assert(engine.nextval(database,"effects")==18);

    // Any actual non-runtime physical change still needs the full restore.
    const auto sidecar=fs::path(database)/"untracked_physical_change";
    overwrite(sidecar,"not recorded in a logical mutation epoch\n");
    assert(engine.nextval(database,"effects")==19);
    assert(engine.rollbackToSavepoint("same")==DBStatus::OK);
    assert(!fs::exists(sidecar));
    assert(engine.query(database,"items",{}, {"id"})==std::vector<std::string>{"7 "});
    assert(engine.nextval(database,"effects")==20);
    assert(engine.savepoint("after_restore")==DBStatus::OK);
    const auto restoredImages=images(database);
    assert(engine.nextval(database,"effects")==21);
    assert(engine.savepoint("after_runtime")==DBStatus::OK);
    const bool resealedReusable=images(database)==restoredImages;

    // A malformed active runtime fork must not be called "nontransactional"
    // and ignored. Restore it only after the real SAVEPOINT rejects it.
    const auto runtime=fs::path(database)/".sequence_runtime";
    const auto fork=fs::directory_iterator(runtime)->path();
    const auto valid=readBytes(fork);
    overwrite(fork,"invalid sequence runtime\n");
    assert(engine.savepoint("bad_runtime")==DBStatus::IO_ERROR);
    overwrite(fork,valid);
    // A parseable runtime record still has to satisfy its declaration's
    // bounds; checking only a UUID/header would license an invalid fork.
    overwrite(fork,"DBMSSEQR2 "+fork.filename().string()+" -1 -1 0 -1 1\n");
    assert(engine.savepoint("bad_runtime_bounds")==DBStatus::IO_ERROR);
    overwrite(fork,valid);
    assert(fs::remove(fork));
    assert(engine.savepoint("missing_runtime")==DBStatus::IO_ERROR);
    overwrite(fork,valid);
    assert(engine.rollbackToSavepoint("after_runtime")==DBStatus::OK);
    assert(engine.nextval(database,"effects")==22);
    assert(engine.rollbackTransaction()==DBStatus::OK);
    assert(!engine.tableExists(database,"items") && images(database)==0);
    assert(engine.nextval(database,"effects")==23);
    std::cout<<"SAVEPOINT_SEQUENCE_RUNTIME_IMAGE aliases="<<aliasImages
             <<" sequence_only_kept_image="<<sequenceOnlyKeptImage
             <<" resealed_reusable="<<resealedReusable<<" final_nextval=23\n";
    return aliasImages==1 && sequenceOnlyKeptImage && resealedReusable?0:1;
}

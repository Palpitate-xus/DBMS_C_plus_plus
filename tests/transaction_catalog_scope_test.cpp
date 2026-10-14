#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <sys/syscall.h>
#include <unistd.h>

namespace {
thread_local bool failPublication=false;
thread_local unsigned publicationFailures=0;
thread_local std::string publicationTarget;
}
extern "C" int rename(const char* source,const char* target) noexcept {
    if(failPublication && std::filesystem::path(target).lexically_normal()==
                        std::filesystem::path(publicationTarget).lexically_normal()) {
        failPublication=false;++publicationFailures;errno=EIO;return -1;
    }
    return static_cast<int>(::syscall(SYS_renameat,AT_FDCWD,source,AT_FDCWD,target));
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string locked=testDbPath("transaction_catalog_locked");
    const std::string target=testDbPath("transaction_catalog_target");
    StorageEngine writer;
    assert(writer.createDatabase(locked)==DBStatus::OK);
    assert(writer.createDatabase(target)==DBStatus::OK);
    // Constructor-owned bootstrap models are intentionally not yet durable.
    // A BEGIN for target must not try to publish locked's unrelated model.
    StorageEngine reader;
    assert(reader.catalogService().has(locked));
    assert(writer.beginTransaction(locked,true)==DBStatus::OK);
    const auto status=reader.beginTransaction(target);
    std::cout<<"TRANSACTION_CATALOG_SCOPE_BEGIN actual="<<static_cast<int>(status)
        <<" expected="<<static_cast<int>(DBStatus::OK)<<std::endl;
    assert(status==DBStatus::OK);
    assert(reader.transactionDatabase()==target);
    assert(reader.rollbackTransaction()==DBStatus::OK);
    assert(!reader.inTransaction());
    // Narrow publication must still detect a real failure in the target,
    // retain its pending metadata, and release staged BEGIN ownership.
    auto targetModel=reader.catalogService().acquire(target);
    targetModel->setDescription(50321,1259,0,"pending target metadata");
    // Fail the real atomic publication before rename. Replacing a catalog
    // with a directory and restoring it changes ctime and is a genuine
    // generation conflict, not a transient I/O retry without peer changes.
    publicationTarget=(reader.dbPath(target)/"pg_catalog"/"pg_description.cat").string();
    failPublication=true;
    assert(reader.beginTransaction(target)==DBStatus::IO_ERROR);
    assert(publicationFailures==1 && !failPublication);
    assert(!reader.inTransaction());
    assert(targetModel->getDescription(50321,1259,0)=="pending target metadata");
    assert(targetModel->persistAll());
    assert(reader.beginTransaction(target)==DBStatus::OK);
    assert(reader.rollbackTransaction()==DBStatus::OK);
    assert(reader.catalogService().acquire(target)->getDescription(50321,1259,0)=="pending target metadata");
    assert(writer.rollbackTransaction()==DBStatus::OK);
    assert(writer.dropDatabase(locked)==DBStatus::OK);
    assert(writer.dropDatabase(target)==DBStatus::OK);
    std::cout<<"TRANSACTION_CATALOG_SCOPE_COMPLETE\n";
}

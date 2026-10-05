#include "access/IndexFileUtil.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

using dbms::DBStatus;

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("sequence_namespace_durability");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == DBStatus::OK);
    assert(engine.createSequence(database, "before_rename", 1, 1) ==
           DBStatus::OK);

    dbms::index_file::failNextDirectorySyncForTesting();
    assert(engine.renameSequence(database, "before_rename", "after_rename") ==
           DBStatus::IO_ERROR);
    dbms::SequenceInfo info;
    assert(engine.getSequenceInfo(database, "before_rename", info) ==
           DBStatus::OK);
    assert(!engine.sequenceExists(database, "after_rename"));
    assert(engine.renameSequence(database, "before_rename", "after_rename") ==
           DBStatus::OK);

    dbms::index_file::failNextDirectorySyncForTesting();
    assert(engine.dropSequence(database, "after_rename") == DBStatus::IO_ERROR);
    assert(engine.getSequenceInfo(database, "after_rename", info) ==
           DBStatus::OK);
    assert(engine.dropSequence(database, "after_rename") == DBStatus::OK);
    assert(engine.dropDatabase(database) == DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[SEQUENCE NAMESPACE DURABILITY] passed\n";
    return 0;
}

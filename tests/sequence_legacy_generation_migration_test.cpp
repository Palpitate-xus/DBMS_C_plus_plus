#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "sequence_legacy_generation_migration", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "admin"; session.permission = 1; session.currentDB = db;
    DdlExecutor ddl;
    const auto sql = [&](const std::string& text) { assert(!ddl.executeSql(text, session)); };
    const std::vector<std::string> definitions{
        "5 1 1 100 1 0 7 6  \n",
        "DBMSSEQ2 5 1 1 100 1 0 7 6 1 1  \n",
        "DBMSSEQ3 5 1 1 100 1 0 7 6 1 1 0  \n",
        "DBMSSEQ4 5 1 1 100 1 0 7 6 1 1 0 - -\n",
    };
    for (size_t i = 0; i < definitions.size(); ++i) {
        const std::string seq = "old" + std::to_string(i);
        sql("CREATE SEQUENCE " + seq + " START 5 MINVALUE 1 MAXVALUE 100");
        std::ofstream output(db + "/" + seq + ".seq", std::ios::trunc);
        output << definitions[i];
        output.close();
        assert(output.good());
        SequenceInfo info;
        assert(g_engine.getSequenceInfo(db, seq, info) == DBStatus::OK);
        assert(info.start == 5 && info.minValue == 1 && info.maxValue == 100);
    }
    // Migration occurs before taking a physical rollback image, not after
    // allocations. BEGIN alone may legitimately defer creating that image.
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    sql("CREATE TABLE migration_marker(id INT)");
    sql("ALTER TABLE migration_marker ADD COLUMN extra INT");
    assert(g_engine.transactionBackupDirty());
    for (size_t i = 0; i < definitions.size(); ++i) {
        std::ifstream input(db + "/old" + std::to_string(i) + ".seq");
        std::string version;
        assert(input >> version);
        assert(version == "DBMSSEQ5");
    }
    assert(g_engine.savepoint("q") == DBStatus::OK);
    for (size_t i = 0; i < definitions.size(); ++i) {
        const std::string seq = "old" + std::to_string(i);
        assert(g_engine.nextval(db, seq) == 7);
        assert(g_engine.nextval(db, seq) == 8);
    }
    assert(g_engine.rollbackToSavepoint("q") == DBStatus::OK);
    for (size_t i = 0; i < definitions.size(); ++i)
        assert(g_engine.nextval(db, "old" + std::to_string(i)) == 9);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    for (size_t i = 0; i < definitions.size(); ++i)
        assert(g_engine.nextval(db, "old" + std::to_string(i)) == 10);

    const std::string backup = db + "_physical_backup";
    assert(g_engine.physicalBackup(db, backup));
    for (size_t i = 0; i < definitions.size(); ++i) {
        const std::string seq = "old" + std::to_string(i);
        assert(g_engine.nextval(db, seq) == 11);
        assert(g_engine.nextval(db, seq) == 12);
    }
    assert(g_engine.physicalRestore(db, backup));
    for (size_t i = 0; i < definitions.size(); ++i)
        assert(g_engine.nextval(db, "old" + std::to_string(i)) == 11);
    std::filesystem::remove_all(backup);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[SEQUENCE LEGACY GENERATION MIGRATION] passed\n";
}

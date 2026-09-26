#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "commands/SequenceStorageName.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "catalog/type_registry.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <unistd.h>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_sequence_basic() {
    std::string db = testDbPath("seq_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE SEQUENCE s1 START 10 INCREMENT 2", s);
    assert(!err);
    assert(g_engine.sequenceExists(db, "s1"));

    assert(g_engine.nextval(db, "s1") == 10);
    assert(g_engine.nextval(db, "s1") == 12);
    assert(g_engine.currval(db, "s1") == 12);

    cleanup(db);
    std::cout << "[SEQUENCE] basic OK" << std::endl;
}

static void test_read_only_transaction_rejects_sequence_writes() {
    const std::string db = testDbPath("seq_read_only");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SEQUENCE guarded START 10", s));
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.setReadOnly(true));

    bool nextvalRejected = false;
    try {
        (void)g_engine.nextval(db, "guarded");
    } catch (const dbms::DbError& error) {
        nextvalRejected = error.sqlState() == "25006";
    }
    assert(nextvalRejected);

    bool setvalRejected = false;
    try {
        (void)g_engine.setval(db, "guarded", 99);
    } catch (const dbms::DbError& error) {
        setvalRejected = error.sqlState() == "25006";
    }
    assert(setvalRejected);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);

    // Neither rejected call may have changed the durable allocation state.
    assert(g_engine.nextval(db, "guarded") == 10);
    cleanup(db);
    std::cout << "[SEQUENCE] read-only transaction guard OK" << std::endl;
}

static void test_sequence_min_max_cycle() {
    std::string db = testDbPath("seq_cycle");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE SEQUENCE s1 START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 3 CYCLE", s);
    assert(!err);

    assert(g_engine.nextval(db, "s1") == 1);
    assert(g_engine.nextval(db, "s1") == 2);
    assert(g_engine.nextval(db, "s1") == 3);
    assert(g_engine.nextval(db, "s1") == 1); // cycle back to min

    cleanup(db);
    std::cout << "[SEQUENCE] min/max/cycle OK" << std::endl;
}

static void test_sequence_cache() {
    std::string db = testDbPath("seq_cache");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE SEQUENCE s1 START 1 INCREMENT 1 CACHE 5", s);
    assert(!err);

    for (int i = 0; i < 10; ++i) {
        assert(g_engine.nextval(db, "s1") == 1 + i);
    }

    cleanup(db);
    std::cout << "[SEQUENCE] cache OK" << std::endl;
}

static void test_sequence_alter() {
    std::string db = testDbPath("seq_alter");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE SEQUENCE s1 START 1 INCREMENT 1", s);
    assert(!err);
    assert(g_engine.nextval(db, "s1") == 1);

    err = ddl.executeSql("ALTER SEQUENCE s1 RESTART WITH 100 INCREMENT BY 10", s);
    assert(!err);
    assert(g_engine.nextval(db, "s1") == 100);
    assert(g_engine.nextval(db, "s1") == 110);

    // START changes the value used by a later bare RESTART, but does not
    // itself move the current sequence position.
    assert(!ddl.executeSql("ALTER SEQUENCE s1 START WITH 25", s));
    assert(g_engine.nextval(db, "s1") == 120);
    assert(!ddl.executeSql("ALTER SEQUENCE s1 RESTART", s));
    assert(g_engine.nextval(db, "s1") == 25);

    // An explicit RESTART value is a one-off position change and must not
    // overwrite the recorded START value.
    assert(!ddl.executeSql("ALTER SEQUENCE s1 RESTART WITH 40", s));
    assert(g_engine.nextval(db, "s1") == 40);
    assert(!ddl.executeSql("ALTER SEQUENCE s1 RESTART", s));
    assert(g_engine.nextval(db, "s1") == 25);

    // Options in the same statement are applied together; bare RESTART uses
    // the newly recorded START value.
    assert(!ddl.executeSql("ALTER SEQUENCE s1 START 50 RESTART", s));
    assert(g_engine.nextval(db, "s1") == 50);
    assert(ddl.executeSql(
        "ALTER SEQUENCE s1 START 0 MINVALUE 1", s));

    dbms::StorageEngine restarted;
    assert(restarted.nextval(db, "s1") == 60);

    cleanup(db);
    std::cout << "[SEQUENCE] alter START/RESTART semantics OK" << std::endl;
}

static void test_sequence_rename() {
    std::string db = testDbPath("seq_rename");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SEQUENCE s_old", s));
    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT DEFAULT nextval('s_old'), name VARCHAR(20))", s));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace != nullptr);
    const auto* oldRelation = catalog.findClassByName("s_old", publicNamespace->oid);
    assert(oldRelation != nullptr);
    const dbms::Oid sequenceOid = oldRelation->oid;

    dbms::SQLParser parser;
    const auto parsedRename = parser.parse("ALTER SEQUENCE s_old RENAME TO s_new");
    assert(parsedRename.success && parsedRename.stmt);
    assert(parsedRename.stmt->command == dbms::SqlCommand::AlterSequence);
    const auto* renameAst = dynamic_cast<const dbms::AlterObjectStmt*>(
        parsedRename.stmt.get());
    assert(renameAst != nullptr);
    assert(renameAst->subCommand == "RENAME TO s_new");

    assert(!ddl.executeSql("ALTER SEQUENCE s_old RENAME TO s_new", s));
    assert(!g_engine.sequenceExists(db, "s_old"));
    assert(g_engine.sequenceExists(db, "s_new"));
    assert(catalog.findClassByName("s_old", publicNamespace->oid) == nullptr);
    const auto* newRelation = catalog.findClassByName("s_new", publicNamespace->oid);
    assert(newRelation != nullptr && newRelation->oid == sequenceOid);

    const auto schema = g_engine.getTableSchema(db, "t");
    assert(schema.cols[0].defaultValue.find("s_new") != std::string::npos);
    assert(g_engine.insert(db, "t", {{"name", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"name", "b"}}) == dbms::DBStatus::OK);

    dbms::StorageEngine restarted;
    assert(restarted.sequenceExists(db, "s_new"));
    assert(!restarted.sequenceExists(db, "s_old"));
    const auto& restartedCatalog = restarted.catalogService().get(db);
    const auto* restartedPublic = restartedCatalog.findNamespaceByName("public");
    assert(restartedPublic != nullptr);
    const auto* restartedRelation = restartedCatalog.findClassByName(
        "s_new", restartedPublic->oid);
    assert(restartedRelation != nullptr && restartedRelation->oid == sequenceOid);
    assert(restarted.getTableSchema(db, "t").cols[0].defaultValue.find("s_new") !=
           std::string::npos);
    assert(restarted.nextval(db, "s_new") == 3);

    assert(!ddl.executeSql("CREATE SEQUENCE s_taken", s));
    assert(ddl.executeSql("ALTER SEQUENCE s_new RENAME TO s_taken", s));
    assert(g_engine.sequenceExists(db, "s_new"));
    assert(g_engine.sequenceExists(db, "s_taken"));
    cleanup(db);
    std::cout << "[SEQUENCE] rename/catalog/default/restart OK" << std::endl;
}

static void test_sequence_owned_by_drop_table() {
    std::string db = testDbPath("seq_owned");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE TABLE t (drop_me INT, id INT PRIMARY KEY)", s);
    assert(!err);
    err = ddl.executeSql("CREATE SEQUENCE s1 OWNED BY t.id", s);
    assert(!err);
    err = ddl.executeSql("CREATE SEQUENCE legacy_owned OWNED BY t.id", s);
    assert(!err);
    assert(g_engine.sequenceExists(db, "s1"));
    assert(g_engine.sequenceExists(db, "legacy_owned"));

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* seqRel = cat.resolveRelation("s1", {"public"});
    const auto* legacySeqRel =
        cat.resolveRelation("legacy_owned", {"public"});
    assert(seqRel != nullptr);
    assert(legacySeqRel != nullptr);
    const auto* tableRel = cat.resolveRelation("t", {"public"});
    assert(tableRel != nullptr);
    const auto* ownerColumn = cat.findAttribute(tableRel->oid, "id");
    assert(ownerColumn != nullptr);
    const dbms::Oid tableOid = tableRel->oid;
    const dbms::Oid sequenceOid = seqRel->oid;
    const dbms::Oid legacySequenceOid = legacySeqRel->oid;
    const int32_t ownerColumnNumber = ownerColumn->attnum;
    const auto ownerships = cat.findDepends(
        dbms::PgClassOid_Class, sequenceOid, 0);
    assert(std::count_if(
               ownerships.begin(), ownerships.end(),
               [&](const dbms::PgDependRow& dependency) {
                   return dependency.deptype == 'a' &&
                          dependency.refobjid == tableOid &&
                          dependency.refobjsubid == ownerColumnNumber;
               }) == 1);

    // Simulate a genuine ownership row written before referenced column
    // numbers were persisted. Rename must use the sequence file to preserve
    // this legacy association without confusing it with an old DEFAULT row.
    assert(cat.removeDepend(
        dbms::PgClassOid_Class, legacySequenceOid, 0,
        dbms::PgClassOid_Class, tableOid, ownerColumnNumber));
    dbms::PgDependRow legacyOwnership;
    legacyOwnership.classid = dbms::PgClassOid_Class;
    legacyOwnership.objid = legacySequenceOid;
    legacyOwnership.objsubid = 0;
    legacyOwnership.refclassid = dbms::PgClassOid_Class;
    legacyOwnership.refobjid = tableOid;
    legacyOwnership.refobjsubid = 0;
    legacyOwnership.deptype = 'a';
    cat.addDepend(legacyOwnership);
    cat.setDescription(
        tableOid, dbms::PgClassOid_Class, 1, "removed column");
    cat.setDescription(
        tableOid, dbms::PgClassOid_Class, ownerColumnNumber,
        "owned column");
    assert(cat.persistAll());
    assert(g_engine.commentOnColumn(
               db, "t", "drop_me", "must not reappear") ==
           dbms::DBStatus::OK);

    assert(!ddl.executeSql("ALTER TABLE t DROP COLUMN drop_me", s));
    const auto* compactedOwner = cat.findAttribute(tableOid, "id");
    assert(compactedOwner && compactedOwner->attnum == 1);
    const auto compactedOwnerships = cat.findDepends(
        dbms::PgClassOid_Class, sequenceOid, 0);
    assert(std::count_if(
               compactedOwnerships.begin(), compactedOwnerships.end(),
               [&](const dbms::PgDependRow& dependency) {
                   return dependency.deptype == 'a' &&
                          dependency.refobjid == tableOid &&
                          dependency.refobjsubid == 1;
               }) == 1);
    assert(cat.getDescription(
               tableOid, dbms::PgClassOid_Class, 1) == "owned column");
    assert(cat.getDescription(
               tableOid, dbms::PgClassOid_Class, 2).empty());
    assert(!ddl.executeSql("ALTER TABLE t ADD COLUMN drop_me INT", s));
    assert(g_engine.getColumnComment(db, "t", "drop_me").empty());

    assert(!ddl.executeSql("ALTER TABLE t RENAME TO renamed_t", s));
    dbms::SequenceInfo currentInfo;
    dbms::SequenceInfo legacyInfo;
    assert(g_engine.getSequenceInfo(db, "s1", currentInfo) ==
           dbms::DBStatus::OK);
    assert(g_engine.getSequenceInfo(db, "legacy_owned", legacyInfo) ==
           dbms::DBStatus::OK);
    assert(currentInfo.ownedByTable == "renamed_t");
    assert(legacyInfo.ownedByTable == "renamed_t");

    assert(!ddl.executeSql(
        "ALTER TABLE renamed_t RENAME COLUMN id TO sequence_id", s));
    assert(g_engine.getSequenceInfo(db, "s1", currentInfo) ==
           dbms::DBStatus::OK);
    assert(g_engine.getSequenceInfo(db, "legacy_owned", legacyInfo) ==
           dbms::DBStatus::OK);
    assert(currentInfo.ownedByColumn == "sequence_id");
    assert(legacyInfo.ownedByColumn == "sequence_id");
    const auto* renamedTable =
        cat.resolveRelation("renamed_t", {"public"});
    const auto* renamedColumn = renamedTable
        ? cat.findAttribute(renamedTable->oid, "sequence_id") : nullptr;
    assert(renamedTable && renamedTable->oid == tableOid);
    assert(renamedColumn && renamedColumn->attnum == 1);

    s.sequenceLastValues["s1"] = 23;
    s.sequenceLastValues["public.s1"] = 23;
    s.sequenceLastValues["legacy_owned"] = 24;
    err = ddl.executeSql("DROP TABLE renamed_t", s);
    assert(!err);
    assert(!g_engine.sequenceExists(db, "s1"));
    assert(!g_engine.sequenceExists(db, "legacy_owned"));
    assert(s.sequenceLastValues.count("s1") == 0);
    assert(s.sequenceLastValues.count("public.s1") == 0);
    assert(s.sequenceLastValues.count("legacy_owned") == 0);

    // A same-name sequence is a new catalog object and must not inherit the
    // removed sequence's session-local currval state.
    assert(!ddl.executeSql("CREATE SEQUENCE s1", s));
    assert(s.sequenceLastValues.count("s1") == 0);
    assert(s.sequenceLastValues.count("public.s1") == 0);

    cleanup(db);
    std::cout << "[SEQUENCE] owned by rename/drop lifecycle OK" << std::endl;
}

static void test_sequence_owned_by_drop_column() {
    const std::string db = testDbPath("seq_owned_column");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // The owning column's own DEFAULT is part of the column being removed;
    // it must not turn the automatic ownership dependency into a blocker.
    assert(!ddl.executeSql(
        "CREATE TABLE owner_table (id INT, keep INT)", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE owned_seq OWNED BY owner_table.id", s));
    assert(!ddl.executeSql(
        "ALTER TABLE owner_table ALTER COLUMN id "
        "SET DEFAULT nextval('owned_seq')", s));
    s.sequenceLastValues["owned_seq"] = 9;
    s.sequenceLastValues["public.owned_seq"] = 9;
    assert(!ddl.executeSql(
        "ALTER TABLE owner_table DROP COLUMN id", s));
    assert(!g_engine.sequenceExists(db, "owned_seq"));
    const auto ownerStorage = g_engine.getTableSchema(db, "owner_table");
    assert(ownerStorage.len == 1);
    assert(ownerStorage.cols[0].dataName == "keep");
    assert(s.sequenceLastValues.count("owned_seq") == 0);
    assert(s.sequenceLastValues.count("public.owned_seq") == 0);

    // Exercise an upgraded zero-subobject ownership row and schema-qualified
    // storage naming at the same time.
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE TABLE app.legacy_owner (id INT, keep INT)", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE app.legacy_owned "
        "OWNED BY app.legacy_owner.id", s));
    {
        dbms::CatalogManager& catalog =
            g_engine.catalogService().get(db);
        const auto* table =
            catalog.resolveRelation("app.legacy_owner", {"public"});
        const auto* sequence =
            catalog.resolveRelation("app.legacy_owned", {"public"});
        const auto* column = table
            ? catalog.findAttribute(table->oid, "id") : nullptr;
        assert(table && sequence && column);
        assert(catalog.removeDepend(
            dbms::PgClassOid_Class, sequence->oid, 0,
            dbms::PgClassOid_Class, table->oid, column->attnum));
        dbms::PgDependRow legacyOwnership;
        legacyOwnership.classid = dbms::PgClassOid_Class;
        legacyOwnership.objid = sequence->oid;
        legacyOwnership.objsubid = 0;
        legacyOwnership.refclassid = dbms::PgClassOid_Class;
        legacyOwnership.refobjid = table->oid;
        legacyOwnership.refobjsubid = 0;
        legacyOwnership.deptype = 'a';
        catalog.addDepend(legacyOwnership);
        assert(catalog.persistAll());
    }
    s.sequenceLastValues["app.legacy_owned"] = 3;
    assert(!ddl.executeSql(
        "ALTER TABLE app.legacy_owner DROP COLUMN id", s));
    assert(!g_engine.sequenceExists(db, "app.legacy_owned"));
    assert(s.sequenceLastValues.count("app.legacy_owned") == 0);
    const auto legacyOwnerStorage =
        g_engine.getTableSchema(db, "app__legacy_owner");
    assert(legacyOwnerStorage.len == 1);
    assert(legacyOwnerStorage.cols[0].dataName == "keep");

    // A DEFAULT on another column is a normal dependency. RESTRICT must
    // roll back the already-rewritten owner table; CASCADE clears that
    // default and then removes the owned sequence.
    assert(!ddl.executeSql(
        "CREATE TABLE shared_owner (id INT, keep INT)", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE shared_seq OWNED BY shared_owner.id", s));
    assert(!ddl.executeSql(
        "ALTER TABLE shared_owner ALTER COLUMN id "
        "SET DEFAULT nextval('shared_seq')", s));
    assert(!ddl.executeSql(
        "ALTER TABLE shared_owner ALTER COLUMN keep "
        "SET DEFAULT nextval('shared_seq')", s));
    assert(!ddl.executeSql(
        "CREATE TABLE sequence_consumer "
        "(value INT DEFAULT nextval('shared_seq'))", s));
    s.sequenceLastValues["shared_seq"] = 17;
    assert(ddl.executeSql(
        "ALTER TABLE shared_owner DROP COLUMN id RESTRICT", s));
    assert(g_engine.sequenceExists(db, "shared_seq"));
    const auto restrictedOwner =
        g_engine.getTableSchema(db, "shared_owner");
    assert(restrictedOwner.len == 2);
    assert(restrictedOwner.cols[0].dataName == "id");
    assert(!restrictedOwner.cols[1].defaultValue.empty());
    assert(!g_engine.getTableSchema(
        db, "sequence_consumer").cols[0].defaultValue.empty());
    assert(s.sequenceLastValues["shared_seq"] == 17);

    assert(!ddl.executeSql(
        "ALTER TABLE shared_owner DROP COLUMN id CASCADE", s));
    assert(!g_engine.sequenceExists(db, "shared_seq"));
    assert(s.sequenceLastValues.count("shared_seq") == 0);
    const auto cascadedOwner =
        g_engine.getTableSchema(db, "shared_owner");
    assert(cascadedOwner.len == 1);
    assert(cascadedOwner.cols[0].dataName == "keep");
    assert(cascadedOwner.cols[0].defaultValue.empty());
    assert(g_engine.getTableSchema(
        db, "sequence_consumer").cols[0].defaultValue.empty());

    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* publicNamespace =
            durable.findNamespaceByName("public");
        const auto* appNamespace = durable.findNamespaceByName("app");
        assert(publicNamespace && appNamespace);
        assert(durable.findClassByName(
                   "owned_seq", publicNamespace->oid) == nullptr);
        assert(durable.findClassByName(
                   "shared_seq", publicNamespace->oid) == nullptr);
        assert(durable.findClassByName(
                   "legacy_owned", appNamespace->oid) == nullptr);
        const auto* consumer = durable.findClassByName(
            "sequence_consumer", publicNamespace->oid);
        const auto* consumerColumn = consumer
            ? durable.findAttribute(consumer->oid, "value") : nullptr;
        assert(consumerColumn && !consumerColumn->atthasdef);
        const auto* durableOwner = durable.findClassByName(
            "shared_owner", publicNamespace->oid);
        const auto* durableKeep = durableOwner
            ? durable.findAttribute(durableOwner->oid, "keep") : nullptr;
        assert(durableKeep && !durableKeep->atthasdef);
    }

    cleanup(db);
    std::cout << "[SEQUENCE] owned by drop column lifecycle OK"
              << std::endl;
}

static void test_default_sequence_is_not_table_owned() {
    const std::string db = testDbPath("seq_default_not_owned");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SEQUENCE standalone", s));
    assert(!ddl.executeSql(
        "CREATE TABLE uses_standalone "
        "(id INT DEFAULT nextval('standalone'))", s));

    dbms::CatalogManager& catalog =
        g_engine.catalogService().get(db);
    const auto* sequence = catalog.resolveRelation("standalone", {"public"});
    const auto* table =
        catalog.resolveRelation("uses_standalone", {"public"});
    assert(sequence && table);
    const dbms::Oid sequenceOid = sequence->oid;
    const dbms::Oid tableOid = table->oid;
    const auto dependencies = catalog.findDepends(
        dbms::PgClassOid_Class, sequenceOid, 0);
    assert(std::none_of(
        dependencies.begin(), dependencies.end(),
        [&](const dbms::PgDependRow& dependency) {
            return dependency.deptype == 'a' &&
                   dependency.refobjid == tableOid;
        }));

    assert(!ddl.executeSql("DROP TABLE uses_standalone", s));
    assert(g_engine.sequenceExists(db, "standalone"));
    assert(g_engine.nextval(db, "standalone") == 1);

    // Simulate the zero-subobject dependency written by older releases.
    assert(!ddl.executeSql("CREATE SEQUENCE legacy_standalone", s));
    assert(!ddl.executeSql(
        "CREATE TABLE legacy_uses "
        "(id INT DEFAULT nextval('legacy_standalone'))", s));
    const auto* legacySequence =
        catalog.resolveRelation("legacy_standalone", {"public"});
    const auto* legacyTable =
        catalog.resolveRelation("legacy_uses", {"public"});
    assert(legacySequence && legacyTable);
    const dbms::Oid legacySequenceOid = legacySequence->oid;
    dbms::PgDependRow legacyDependency;
    legacyDependency.classid = dbms::PgClassOid_Class;
    legacyDependency.objid = legacySequenceOid;
    legacyDependency.objsubid = 0;
    legacyDependency.refclassid = dbms::PgClassOid_Class;
    legacyDependency.refobjid = legacyTable->oid;
    legacyDependency.refobjsubid = 0;
    legacyDependency.deptype = 'a';
    catalog.addDepend(legacyDependency);
    assert(catalog.persistAll());

    assert(!ddl.executeSql("DROP TABLE legacy_uses", s));
    assert(g_engine.sequenceExists(db, "legacy_standalone"));
    assert(g_engine.nextval(db, "legacy_standalone") == 1);
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableSequence =
            durable.findClass(legacySequenceOid);
        assert(durableSequence && durableSequence->relkind == 'S');
        const auto durableDependencies = durable.findDepends(
            dbms::PgClassOid_Class, legacySequenceOid, 0);
        assert(std::none_of(
            durableDependencies.begin(), durableDependencies.end(),
            [](const dbms::PgDependRow& dependency) {
                return dependency.deptype == 'a';
            }));
    }

    cleanup(db);
    std::cout << "[SEQUENCE] defaults remain independent of tables OK"
              << std::endl;
}

static void test_sequence_identity_still_works() {
    std::string db = testDbPath("seq_identity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, msg VARCHAR(50))", s);
    assert(!err);
    assert(g_engine.insert(db, "t", {{"msg", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"msg", "b"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[SEQUENCE] identity still works OK" << std::endl;
}

static void test_sequence_numeric_input_fails_closed() {
    std::string db = testDbPath("seq_invalid_numeric");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    dbms::SQLParser parser;

    assert(!parser.parse("CREATE SEQUENCE bad START nope").success);
    assert(ddl.executeSql("CREATE SEQUENCE bad START nope", s));
    assert(!g_engine.sequenceExists(db, "bad"));

    assert(!ddl.executeSql("CREATE SEQUENCE good START 1", s));
    assert(ddl.executeSql("ALTER SEQUENCE good INCREMENT nope", s));
    assert(ddl.executeSql("ALTER SEQUENCE good CACHE", s));
    assert(g_engine.nextval(db, "good") == 1);

    const auto descendingParsed = parser.parse(
        "CREATE SEQUENCE descending START -1 INCREMENT BY -2 "
        "MINVALUE -9 MAXVALUE -1");
    assert(descendingParsed.success && descendingParsed.stmt);
    const auto* descendingCreate =
        dynamic_cast<const dbms::CreateObjectStmt*>(
            descendingParsed.stmt.get());
    assert(descendingCreate);
    assert(descendingCreate->options.at("start") == "-1");
    assert(descendingCreate->options.at("increment") == "-2");
    assert(descendingCreate->options.at("minvalue") == "-9");
    assert(descendingCreate->options.at("maxvalue") == "-1");
    assert(!ddl.execute(descendingParsed.stmt, s));
    assert(g_engine.nextval(db, "descending") == -1);
    assert(g_engine.nextval(db, "descending") == -3);
    assert(!ddl.executeSql(
        "ALTER SEQUENCE descending RESTART WITH -7 INCREMENT BY -1", s));
    assert(g_engine.nextval(db, "descending") == -7);
    assert(g_engine.nextval(db, "descending") == -8);
    assert(!ddl.executeSql(
        "ALTER SEQUENCE descending START WITH -5", s));
    assert(g_engine.nextval(db, "descending") == -9);
    assert(!ddl.executeSql("ALTER SEQUENCE descending RESTART", s));
    assert(g_engine.nextval(db, "descending") == -5);

    assert(!ddl.executeSql(
        "CREATE SEQUENCE signed_positive START +2 INCREMENT +3 MAXVALUE +20",
        s));
    assert(g_engine.nextval(db, "signed_positive") == 2);
    assert(g_engine.nextval(db, "signed_positive") == 5);
    assert(ddl.executeSql("ALTER SEQUENCE descending RESTART -", s));
    assert(!parser.parse(
        "CREATE SEQUENCE overflow START -9223372036854775809").success);

    dbms::StorageEngine restarted;
    assert(restarted.nextval(db, "good") == 2);
    assert(restarted.nextval(db, "descending") == -6);
    assert(restarted.nextval(db, "signed_positive") == 8);

    const auto corruptPath = std::filesystem::path(db) / "corrupt.seq";
    {
        std::ofstream out(corruptPath);
        out << "1 1 1 2 1 0 1 0 owned col trailing\n";
    }
    bool corruptRejected = false;
    try {
        (void)restarted.nextval(db, "corrupt");
    } catch (const dbms::DbError& error) {
        corruptRejected = error.sqlState() == "XX001";
    }
    assert(corruptRejected);

    cleanup(db);
    std::cout << "[SEQUENCE] invalid numeric options fail closed OK" << std::endl;
}

static void test_schema_qualified_sequence_create() {
    const std::string db = testDbPath("seq_schema_create");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE app.counter START 7 INCREMENT 3", s));
    assert(g_engine.sequenceExists(db, "app.counter"));
    assert(!g_engine.sequenceExists(db, "counter"));

    dbms::CatalogManager& catalog =
        g_engine.catalogService().get(db);
    const auto* appNamespace = catalog.findNamespaceByName("app");
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(appNamespace && publicNamespace);
    const auto* appSequence =
        catalog.findClassByName("counter", appNamespace->oid);
    assert(appSequence && appSequence->relkind == 'S');
    assert(catalog.findClassByName(
               "counter", publicNamespace->oid) == nullptr);
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableNamespace =
            durable.findNamespaceByName("app");
        assert(durableNamespace);
        const auto* durableSequence = durable.findClassByName(
            "counter", durableNamespace->oid);
        assert(durableSequence && durableSequence->relkind == 'S');
    }

    assert(g_engine.nextval(db, "app.counter") == 7);
    assert(!ddl.executeSql(
        "CREATE SEQUENCE IF NOT EXISTS app.counter START 100", s));
    assert(g_engine.nextval(db, "app.counter") == 10);

    assert(ddl.executeSql("CREATE SEQUENCE missing.counter", s));
    assert(!g_engine.sequenceExists(db, "missing.counter"));
    assert(ddl.executeSql(
        "CREATE SEQUENCE app.bad_owner OWNED BY missing.id", s));
    assert(!g_engine.sequenceExists(db, "app.bad_owner"));

    const fs::path blockedCatalogTemporary =
        fs::path(db) / "pg_catalog" /
        ("pg_class.cat.tmp." +
         std::to_string(static_cast<unsigned long long>(::getpid())));
    fs::create_directories(blockedCatalogTemporary);
    assert(ddl.executeSql("CREATE SEQUENCE app.must_rollback", s));
    assert(!g_engine.sequenceExists(db, "app.must_rollback"));
    fs::remove_all(blockedCatalogTemporary);
    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloadedCatalog =
        g_engine.catalogService().get(db);
    const auto* reloadedAppNamespace =
        reloadedCatalog.findNamespaceByName("app");
    assert(reloadedAppNamespace);
    assert(reloadedCatalog.findClassByName(
               "must_rollback", reloadedAppNamespace->oid) == nullptr);

    assert(!ddl.executeSql("CREATE SEQUENCE counter START 100", s));
    assert(g_engine.sequenceExists(db, "public.counter"));
    assert(g_engine.nextval(db, "public.counter") == 100);

    cleanup(db);
    std::cout << "[SEQUENCE] schema-qualified create/catalog OK"
              << std::endl;
}

static void test_schema_qualified_sequence_alter() {
    const std::string db = testDbPath("seq_schema_alter");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE app.counter START 3 INCREMENT 2", s));

    dbms::SQLParser parser;
    const auto parsed = parser.parse(
        "ALTER SEQUENCE IF EXISTS app.counter RESTART WITH 11");
    assert(parsed.success && parsed.stmt);
    const auto* alter = dynamic_cast<const dbms::AlterObjectStmt*>(
        parsed.stmt.get());
    assert(alter && alter->ifExists);
    assert(alter->schema == "app" && alter->objectName == "counter");

    assert(!ddl.executeSql(
        "ALTER SEQUENCE IF EXISTS app.absent RESTART WITH 1", s));
    assert(ddl.executeSql(
        "ALTER SEQUENCE app.absent RESTART WITH 1", s));
    assert(!ddl.executeSql(
        "ALTER SEQUENCE app.counter RESTART WITH 11 INCREMENT BY 4", s));
    assert(g_engine.nextval(db, "app.counter") == 11);
    assert(g_engine.nextval(db, "app.counter") == 15);

    assert(!ddl.executeSql("CREATE TABLE app.owner (id INT)", s));
    assert(!ddl.executeSql("CREATE TABLE public_owner (id INT)", s));
    assert(ddl.executeSql(
        "ALTER SEQUENCE app.counter OWNED BY public.public_owner.id", s));
    assert(ddl.executeSql(
        "ALTER SEQUENCE app.counter OWNED BY app.owner.missing", s));
    assert(!ddl.executeSql(
        "ALTER SEQUENCE app.counter OWNED BY app.owner.id", s));

    dbms::CatalogManager& catalog =
        g_engine.catalogService().get(db);
    const auto* appNamespace = catalog.findNamespaceByName("app");
    assert(appNamespace);
    const auto* sequence =
        catalog.findClassByName("counter", appNamespace->oid);
    const auto* owner =
        catalog.findClassByName("owner", appNamespace->oid);
    assert(sequence && sequence->relkind == 'S');
    assert(owner && owner->relkind == 'r');
    const dbms::Oid sequenceOid = sequence->oid;
    const dbms::Oid ownerOid = owner->oid;
    const auto* ownerColumn = catalog.findAttribute(ownerOid, "id");
    assert(ownerColumn);
    auto ownerships = catalog.findDepends(
        dbms::PgClassOid_Class, sequenceOid, 0);
    assert(std::count_if(
               ownerships.begin(), ownerships.end(),
               [&](const dbms::PgDependRow& dependency) {
                   return dependency.deptype == 'a' &&
                          dependency.refobjid == ownerOid &&
                          dependency.refobjsubid == ownerColumn->attnum;
               }) == 1);
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto durableOwnerships = durable.findDepends(
            dbms::PgClassOid_Class, sequenceOid, 0);
        assert(std::count_if(
                   durableOwnerships.begin(), durableOwnerships.end(),
                   [&](const dbms::PgDependRow& dependency) {
                       return dependency.deptype == 'a' &&
                              dependency.refobjid == ownerOid &&
                              dependency.refobjsubid == ownerColumn->attnum;
                   }) == 1);
    }

    assert(!ddl.executeSql(
        "ALTER SEQUENCE app.counter OWNED BY NONE", s));
    ownerships = catalog.findDepends(
        dbms::PgClassOid_Class, sequenceOid, 0);
    assert(std::none_of(
        ownerships.begin(), ownerships.end(),
        [](const dbms::PgDependRow& dependency) {
            return dependency.deptype == 'a';
        }));

    assert(!ddl.executeSql(
        "CREATE TABLE app.uses_seq "
        "(id INT DEFAULT nextval('app.counter'), name VARCHAR(20))", s));
    assert(!ddl.executeSql("CREATE SEQUENCE counter START 100", s));
    assert(!ddl.executeSql(
        "CREATE TABLE public_uses_seq "
        "(id INT DEFAULT nextval('counter'))", s));
    s.sequenceLastValues["app.counter"] = 15;

    assert(!ddl.executeSql(
        "ALTER SEQUENCE app.counter RENAME TO renamed", s));
    assert(!g_engine.sequenceExists(db, "app.counter"));
    assert(g_engine.sequenceExists(db, "app.renamed"));
    assert(!g_engine.sequenceExists(db, "renamed"));
    assert(s.sequenceLastValues.count("app.counter") == 0);
    assert(s.sequenceLastValues["app.renamed"] == 15);
    const auto appTable = g_engine.getTableSchema(db, "app__uses_seq");
    const auto publicTable = g_engine.getTableSchema(db, "public_uses_seq");
    assert(appTable.cols[0].defaultValue.find("app.renamed") !=
           std::string::npos);
    assert(publicTable.cols[0].defaultValue.find("counter") !=
           std::string::npos);
    assert(publicTable.cols[0].defaultValue.find("renamed") ==
           std::string::npos);
    assert(g_engine.insert(
               db, "app__uses_seq", {{"name", "x"}}) ==
           dbms::DBStatus::OK);
    const auto rows = g_engine.query(
        db, "app__uses_seq", {}, {"id", "name"});
    assert(rows.size() == 1 && rows[0].find("19 x") == 0);

    const auto* renamed =
        catalog.findClassByName("renamed", appNamespace->oid);
    assert(renamed && renamed->oid == sequenceOid);
    assert(catalog.findClassByName("counter", appNamespace->oid) == nullptr);
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableApp = durable.findNamespaceByName("app");
        assert(durableApp);
        const auto* durableRenamed =
            durable.findClassByName("renamed", durableApp->oid);
        assert(durableRenamed && durableRenamed->oid == sequenceOid);
        assert(durable.findClassByName(
                   "counter", durableApp->oid) == nullptr);
    }

    assert(!ddl.executeSql("CREATE TABLE app.taken (id INT)", s));
    assert(ddl.executeSql(
        "ALTER SEQUENCE app.renamed RENAME TO taken", s));
    assert(g_engine.sequenceExists(db, "app.renamed"));
    assert(!g_engine.sequenceExists(db, "app.taken"));

    cleanup(db);
    std::cout << "[SEQUENCE] schema-qualified alter/rename/catalog OK"
              << std::endl;
}

static void test_schema_qualified_sequence_drop() {
    const std::string db = testDbPath("seq_schema_drop");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::SQLParser parser;
    const auto parsed = parser.parse(
        "DROP SEQUENCE IF EXISTS app.one, public.two CASCADE");
    assert(parsed.success && parsed.stmt);
    const auto* drop = dynamic_cast<const dbms::DropStmt*>(
        parsed.stmt.get());
    assert(drop && drop->ifExists && drop->cascade);
    assert(drop->objectNames ==
           std::vector<std::string>({"app.one", "public.two"}));
    assert(!parser.parse("DROP SEQUENCE app.one public.two").success);
    assert(!parser.parse("DROP SEQUENCE app.one,").success);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql("CREATE SEQUENCE app.one START 10", s));
    assert(!ddl.executeSql("CREATE SEQUENCE app.two START 20", s));
    assert(!ddl.executeSql("CREATE SEQUENCE one START 100", s));
    assert(!ddl.executeSql(
        "CREATE TABLE app.uses_one "
        "(id INT DEFAULT nextval('app.one'))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE public_uses_one "
        "(id INT DEFAULT nextval('one'))", s));
    assert(!ddl.executeSql("CREATE TABLE app.not_sequence (id INT)", s));

    s.sequenceLastValues["app.one"] = 10;
    s.sequenceLastValues["one"] = 100;
    assert(ddl.executeSql("DROP SEQUENCE app.one RESTRICT", s));
    assert(g_engine.sequenceExists(db, "app.one"));
    assert(g_engine.sequenceExists(db, "one"));
    assert(ddl.executeSql(
        "DROP SEQUENCE app.two, app.absent", s));
    assert(g_engine.sequenceExists(db, "app.two"));
    assert(ddl.executeSql(
        "DROP SEQUENCE IF EXISTS app.not_sequence", s));
    assert(g_engine.tableExists(db, "app__not_sequence"));

    assert(!ddl.executeSql(
        "DROP SEQUENCE IF EXISTS app.one, app.absent, app.two, public.one CASCADE",
        s));
    assert(!g_engine.sequenceExists(db, "app.one"));
    assert(!g_engine.sequenceExists(db, "app.two"));
    assert(!g_engine.sequenceExists(db, "one"));
    assert(s.sequenceLastValues.count("app.one") == 0);
    assert(s.sequenceLastValues.count("one") == 0);
    assert(g_engine.getTableSchema(
               db, "app__uses_one").cols[0].defaultValue.empty());
    assert(g_engine.getTableSchema(
               db, "public_uses_one").cols[0].defaultValue.empty());

    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableApp = durable.findNamespaceByName("app");
        const auto* durablePublic = durable.findNamespaceByName("public");
        assert(durableApp && durablePublic);
        assert(durable.findClassByName("one", durableApp->oid) == nullptr);
        assert(durable.findClassByName("two", durableApp->oid) == nullptr);
        assert(durable.findClassByName("one", durablePublic->oid) == nullptr);
    }

    assert(!ddl.executeSql("CREATE TABLE app.owner (id INT)", s));
    assert(!ddl.executeSql("CREATE TABLE public_owner (id INT)", s));
    const auto parsedOwnedCreate = parser.parse(
        "CREATE SEQUENCE app.owned OWNED BY app.owner.id");
    assert(parsedOwnedCreate.success && parsedOwnedCreate.stmt);
    const auto* ownedCreate = dynamic_cast<const dbms::CreateObjectStmt*>(
        parsedOwnedCreate.stmt.get());
    assert(ownedCreate);
    assert(ownedCreate->options.at("ownedby") == "app.owner.id");
    assert(ddl.executeSql(
        "CREATE SEQUENCE app.cross_schema "
        "OWNED BY public.public_owner.id", s));
    assert(!g_engine.sequenceExists(db, "app.cross_schema"));
    assert(!ddl.execute(parsedOwnedCreate.stmt, s));
    dbms::SequenceInfo ownedInfo;
    assert(g_engine.getSequenceInfo(
               db, "app.owned", ownedInfo) == dbms::DBStatus::OK);
    assert(ownedInfo.ownedByTable == "app.owner");
    assert(ownedInfo.ownedByColumn == "id");
    assert(!ddl.executeSql(
        "ALTER TABLE app.owner RENAME TO renamed_owner", s));
    assert(g_engine.getSequenceInfo(
               db, "app.owned", ownedInfo) == dbms::DBStatus::OK);
    assert(ownedInfo.ownedByTable == "app.renamed_owner");
    assert(!ddl.executeSql(
        "ALTER TABLE app.renamed_owner RENAME COLUMN id TO owner_id", s));
    assert(g_engine.getSequenceInfo(
               db, "app.owned", ownedInfo) == dbms::DBStatus::OK);
    assert(ownedInfo.ownedByTable == "app.renamed_owner");
    assert(ownedInfo.ownedByColumn == "owner_id");
    assert(!ddl.executeSql("DROP TABLE app.renamed_owner", s));
    assert(!g_engine.sequenceExists(db, "app.owned"));
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableApp = durable.findNamespaceByName("app");
        assert(durableApp);
        assert(durable.findClassByName(
                   "owned", durableApp->oid) == nullptr);
    }

    dbms::SequenceInfo orphanInfo;
    assert(g_engine.createSequence(
               db, "app.orphan", orphanInfo) == dbms::DBStatus::OK);
    assert(ddl.executeSql(
        "DROP SEQUENCE IF EXISTS app.orphan", s));
    assert(g_engine.sequenceExists(db, "app.orphan"));

    assert(!ddl.executeSql("CREATE SEQUENCE app.catalog_only", s));
    assert(g_engine.dropSequence(
               db, "app.catalog_only") == dbms::DBStatus::OK);
    assert(ddl.executeSql(
        "DROP SEQUENCE IF EXISTS app.catalog_only", s));
    dbms::CatalogManager& catalog =
        g_engine.catalogService().get(db);
    const auto* appNamespace = catalog.findNamespaceByName("app");
    assert(appNamespace);
    assert(catalog.findClassByName(
               "catalog_only", appNamespace->oid) != nullptr);

    const fs::path nonSequencePath = fs::path(db) / "app.not_a_file.seq";
    fs::create_directory(nonSequencePath);
    assert(g_engine.dropSequence(db, "app.not_a_file") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(fs::is_directory(nonSequencePath));

    cleanup(db);
    std::cout << "[SEQUENCE] schema-qualified/multi-target drop OK"
              << std::endl;
}

static void test_sequence_bound_defaults_and_file_upgrade() {
    const std::string db = testDbPath("seq_bound_metadata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE SEQUENCE descending_default "
        "INCREMENT -2 NO MINVALUE NO MAXVALUE", s));
    assert(g_engine.nextval(db, "descending_default") == -1);
    assert(g_engine.nextval(db, "descending_default") == -3);
    assert(!ddl.executeSql(
        "ALTER SEQUENCE descending_default "
        "INCREMENT 2 START 1 RESTART", s));
    assert(g_engine.nextval(db, "descending_default") == 1);
    assert(g_engine.nextval(db, "descending_default") == 3);

    assert(!ddl.executeSql(
        "CREATE SEQUENCE bounded START 10 INCREMENT 2 "
        "MINVALUE 10 MAXVALUE 30", s));
    assert(g_engine.nextval(db, "bounded") == 10);
    assert(!ddl.executeSql("ALTER SEQUENCE bounded INCREMENT -2", s));
    assert(g_engine.nextval(db, "bounded") == 12);
    assert(g_engine.nextval(db, "bounded") == 10);

    const fs::path boundedPath = fs::path(db) / "bounded.seq";
    {
        std::ifstream input(boundedPath);
        std::string magic;
        assert(input >> magic);
        assert(magic == "DBMSSEQ3");
    }

    assert(!ddl.executeSql(
        "ALTER SEQUENCE bounded NO MINVALUE NO MAXVALUE "
        "START -1 RESTART", s));
    assert(g_engine.nextval(db, "bounded") == -1);
    assert(g_engine.nextval(db, "bounded") == -3);

    assert(!ddl.executeSql(
        "CREATE SEQUENCE custom_min MINVALUE 5", s));
    assert(g_engine.nextval(db, "custom_min") == 5);

    const fs::path legacyPath = fs::path(db) / "legacy.seq";
    {
        std::ofstream output(legacyPath);
        output << "5 1 1 10 1 0 5 4  \n";
    }
    assert(g_engine.nextval(db, "legacy") == 5);
    {
        std::ifstream input(legacyPath);
        std::string magic;
        assert(input >> magic);
        assert(magic == "DBMSSEQ3");
    }
    dbms::SequenceInfo legacyAlter;
    legacyAlter.increment = -1;
    legacyAlter.incrementSpecified = true;
    assert(g_engine.alterSequence(db, "legacy", legacyAlter) ==
           dbms::DBStatus::OK);
    assert(g_engine.nextval(db, "legacy") == 6);

    dbms::StorageEngine restarted;
    assert(restarted.nextval(db, "bounded") == -5);
    assert(restarted.nextval(db, "custom_min") == 6);

    cleanup(db);
    std::cout << "[SEQUENCE] bound defaults/file upgrade OK" << std::endl;
}

static void test_sequence_integer_boundaries() {
    std::string db = testDbPath("seq_boundaries");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::SequenceInfo upper;
    upper.start = std::numeric_limits<int64_t>::max();
    upper.increment = 1;
    upper.maxValue = std::numeric_limits<int64_t>::max();
    assert(g_engine.createSequence(db, "upper", upper) == dbms::DBStatus::OK);
    assert(g_engine.nextval(db, "upper") == std::numeric_limits<int64_t>::max());
    bool upperExhausted = false;
    try {
        (void)g_engine.nextval(db, "upper");
    } catch (const dbms::DbError& error) {
        upperExhausted = error.sqlState() == "2200H";
    }
    assert(upperExhausted);
    assert(g_engine.currval(db, "upper") == std::numeric_limits<int64_t>::max());
    assert(g_engine.setval(db, "upper", std::numeric_limits<int64_t>::max()) ==
           std::numeric_limits<int64_t>::max());

    dbms::SequenceInfo lower;
    lower.start = -std::numeric_limits<int64_t>::max();
    lower.increment = -1;
    lower.minValue = -std::numeric_limits<int64_t>::max();
    lower.maxValue = -1;
    assert(g_engine.createSequence(db, "lower", lower) == dbms::DBStatus::OK);
    assert(g_engine.nextval(db, "lower") == -std::numeric_limits<int64_t>::max());
    bool lowerExhausted = false;
    try {
        (void)g_engine.nextval(db, "lower");
    } catch (const dbms::DbError& error) {
        lowerExhausted = error.sqlState() == "2200H";
    }
    assert(lowerExhausted);
    assert(g_engine.setval(db, "lower", -std::numeric_limits<int64_t>::max()) ==
           -std::numeric_limits<int64_t>::max());
    lowerExhausted = false;
    try {
        (void)g_engine.nextval(db, "lower");
    } catch (const dbms::DbError& error) {
        lowerExhausted = error.sqlState() == "2200H";
    }
    assert(lowerExhausted);

    dbms::SequenceInfo expandable;
    expandable.start = 2;
    expandable.minValue = 1;
    expandable.maxValue = 2;
    expandable.startSpecified = true;
    expandable.hasMinValue = true;
    expandable.hasMaxValue = true;
    assert(g_engine.createSequence(db, "expandable", expandable) ==
           dbms::DBStatus::OK);
    assert(g_engine.nextval(db, "expandable") == 2);
    try {
        (void)g_engine.nextval(db, "expandable");
        assert(false);
    } catch (const dbms::DbError& error) {
        assert(error.sqlState() == "2200H");
    }
    dbms::SequenceInfo expandedBound;
    expandedBound.maxValue = 3;
    expandedBound.hasMaxValue = true;
    assert(g_engine.alterSequence(db, "expandable", expandedBound) ==
           dbms::DBStatus::OK);
    assert(g_engine.nextval(db, "expandable") == 3);

    cleanup(db);
    std::cout << "[SEQUENCE] integer boundaries OK" << std::endl;
}

static void test_public_dotted_sequence_storage_migration() {
    const std::string db = testDbPath("seq_legacy_collision");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE SEQUENCE public.\"diff_legacy_collision.seq\" START 7", s));
    const std::string encoded = dbms::sequenceStorageName(
        "public", "diff_legacy_collision.seq");
    const fs::path upgradedPath = fs::path(db) / (encoded + ".seqv2");
    const fs::path legacyPath =
        fs::path(db) / "diff_legacy_collision.seq.seq";
    assert(fs::exists(upgradedPath));
    assert(g_engine.nextval(
        db, "public.\"diff_legacy_collision.seq\"") == 7);

    // Emulate a pre-upgrade directory. Reopening the engine must use the
    // catalog owner to move the old file without resetting its value.
    fs::rename(upgradedPath, legacyPath);
    {
        dbms::StorageEngine restarted;
        assert(fs::exists(upgradedPath));
        assert(!fs::exists(legacyPath));
        assert(restarted.nextval(
            db, "public.\"diff_legacy_collision.seq\"") == 8);
    }

    assert(!ddl.executeSql("CREATE SCHEMA diff_legacy_collision", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE diff_legacy_collision.seq START 11", s));
    assert(g_engine.nextval(db, "diff_legacy_collision.seq") == 11);
    assert(g_engine.nextval(
        db, "public.\"diff_legacy_collision.seq\"") == 9);

    assert(!ddl.executeSql(
        "CREATE SEQUENCE public.\"public.special\" START 31", s));
    const fs::path prefixedUpgradedPath = fs::path(db) /
        (dbms::sequenceStorageName("public", "public.special") +
         ".seqv2");
    const fs::path prefixedLegacyPath = fs::path(db) / "special.seq";
    assert(g_engine.nextval(db, "public.\"public.special\"") == 31);
    fs::rename(prefixedUpgradedPath, prefixedLegacyPath);
    {
        dbms::StorageEngine restarted;
        assert(fs::exists(prefixedUpgradedPath));
        assert(!fs::exists(prefixedLegacyPath));
        assert(restarted.nextval(
            db, "public.\"public.special\"") == 32);
    }
    assert(!ddl.executeSql("CREATE SEQUENCE public.special START 41", s));
    assert(g_engine.nextval(db, "public.special") == 41);
    assert(g_engine.nextval(db, "public.\"public.special\"") == 33);

    cleanup(db);
    std::cout << "[SEQUENCE] public dotted-name migration/collision OK"
              << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_sequence_basic();
    test_read_only_transaction_rejects_sequence_writes();
    test_sequence_min_max_cycle();
    test_sequence_cache();
    test_sequence_alter();
    test_sequence_rename();
    test_sequence_owned_by_drop_table();
    test_sequence_owned_by_drop_column();
    test_default_sequence_is_not_table_owned();
    test_sequence_identity_still_works();
    test_sequence_numeric_input_fails_closed();
    test_schema_qualified_sequence_create();
    test_schema_qualified_sequence_alter();
    test_schema_qualified_sequence_drop();
    test_sequence_bound_defaults_and_file_upgrade();
    test_sequence_integer_boundaries();
    test_public_dotted_sequence_storage_migration();
    std::cout << "[SEQUENCE_FULL] all passed" << std::endl;
    return 0;
}

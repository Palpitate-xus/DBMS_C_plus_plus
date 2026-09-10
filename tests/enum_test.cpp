#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string readBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static void test_enum_basic() {
    std::string db = testDbPath("enum_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')", s);
    assert(!err);

    dbms::Oid moodTypeOid = dbms::INVALID_OID;
    {
        auto& catalog = g_engine.catalogService().get(db);
        const auto* publicNamespace = catalog.findNamespaceByName("public");
        assert(publicNamespace);
        const auto* moodType =
            catalog.findTypeByName("mood", publicNamespace->oid);
        assert(moodType);
        moodTypeOid = moodType->oid;
        assert(moodType->typtype == 'e');
        assert(moodType->typcategory == 'E');
        const auto labels = catalog.findEnumLabels(moodType->oid);
        assert(labels.size() == 3);
        assert(labels[0].enumlabel == "happy");
        assert(labels[1].enumlabel == "sad");
        assert(labels[2].enumlabel == "neutral");
    }

    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, m mood)", s);
    assert(!err);

    {
        auto& catalog = g_engine.catalogService().get(db);
        const auto* publicNamespace = catalog.findNamespaceByName("public");
        assert(publicNamespace);
        const auto* table = catalog.findClassByName("t", publicNamespace->oid);
        assert(table);
        const auto* enumAttribute = catalog.findAttribute(table->oid, "m");
        assert(enumAttribute);
        assert(enumAttribute->atttypid == moodTypeOid);
    }

    // The named enum identity must survive schema serialization.  It is not
    // interchangeable with another enum merely because the labels match.
    {
        dbms::StorageEngine reopened;
        const auto schema = reopened.getTableSchema(db, "t");
        assert(schema.len == 2);
        assert(schema.cols[1].dataType == "mood");
        assert((schema.cols[1].enumValues ==
                std::vector<std::string>{"happy", "sad", "neutral"}));
    }
    assert(!ddl.executeSql(
        "CREATE TYPE other_mood AS ENUM ('happy', 'sad', 'neutral')", s));
    assert(ddl.executeSql(
        "CREATE TABLE enum_parent (m mood PRIMARY KEY)", s) == false);
    assert(ddl.executeSql(
        "CREATE TABLE enum_child_bad (id INT PRIMARY KEY, m other_mood "
        "REFERENCES enum_parent(m))", s));
    assert(ddl.executeSql(
        "CREATE TABLE enum_child_ok (id INT PRIMARY KEY, m mood "
        "REFERENCES enum_parent(m))", s) == false);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"m", "happy"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"m", "sad"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"m", "angry"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id", "m"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[ENUM] basic OK" << std::endl;
}

static void test_enum_update() {
    std::string db = testDbPath("enum_update");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TYPE color AS ENUM ('red', 'green', 'blue')", s);
    assert(!err);
    err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, c color)", s);
    assert(!err);

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"c", "red"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"c", "green"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"c", "yellow"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ENUM] update OK" << std::endl;
}

static void test_named_enum_alter_updates_dependent_schemas() {
    std::string db = testDbPath("enum_dependent_schema");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TYPE priority AS ENUM ('low', 'high', 'obsolete')", s));
    assert(!ddl.executeSql(
        "CREATE TABLE tasks (id INT PRIMARY KEY, p priority)", s));
    assert(g_engine.insert(db, "tasks", {{"id", "1"}, {"p", "low"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "tasks", {{"id", "2"}, {"p", "high"}}) ==
           dbms::DBStatus::OK);

    auto priority = g_engine.getEnumType(db, "priority");
    priority.labels.insert(priority.labels.begin() + 1, "medium");
    assert(g_engine.updateEnumType(db, priority) == dbms::DBStatus::OK);
    {
        auto& catalog = g_engine.catalogService().get(db);
        const auto* publicNamespace = catalog.findNamespaceByName("public");
        assert(publicNamespace);
        const auto* catalogType =
            catalog.findTypeByName("priority", publicNamespace->oid);
        assert(catalogType && catalogType->typtype == 'e');
        const auto catalogLabels = catalog.findEnumLabels(catalogType->oid);
        assert(catalogLabels.size() == priority.labels.size());
        for (size_t index = 0; index < priority.labels.size(); ++index)
            assert(catalogLabels[index].enumlabel == priority.labels[index]);

        dbms::StorageEngine reopened;
        const auto schema = reopened.getTableSchema(db, "tasks");
        assert(schema.cols[1].dataType == "priority");
        assert(schema.cols[1].enumValues == priority.labels);
    }
    assert(g_engine.insert(db, "tasks", {{"id", "3"}, {"p", "medium"}}) ==
           dbms::DBStatus::OK);

    // Renaming updates every dependent table and rewrites rows that carry the
    // old text-backed value as one engine transaction.
    priority.labels.back() = "urgent";
    assert(g_engine.updateEnumType(db, priority) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "tasks", {{"id", "4"}, {"p", "urgent"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "tasks", {{"id", "5"}, {"p", "obsolete"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    dbms::StorageEngine::OrderBySpec byPriority;
    byPriority.colName = "p";
    assert((g_engine.query(db, "tasks", {}, {"id"}, {byPriority}) ==
            std::vector<std::string>{"1 ", "3 ", "2 ", "4 "}));
    dbms::StorageEngine::OrderBySpec byId;
    byId.colName = "id";
    assert((g_engine.query(db, "tasks", {">p medium"}, {"id"}, {byId}) ==
            std::vector<std::string>{"2 ", "4 "}));

    const auto taskSchema = g_engine.getTableSchema(db, "tasks");
    dbms::SortOp volcanoSort(
        std::make_unique<dbms::TableScanOp>(&g_engine, db, "tasks"),
        taskSchema, "p", true);
    assert(volcanoSort.open());
    std::vector<std::string> volcanoOrder;
    std::string sortedRow;
    while (volcanoSort.next(sortedRow)) {
        volcanoOrder.push_back(dbms::StorageEngine::extractColumnValueStatic(
            sortedRow, taskSchema, 0));
    }
    volcanoSort.close();
    assert((volcanoOrder == std::vector<std::string>{"1", "3", "2", "4"}));

    priority.labels.front() = "minor";
    assert(g_engine.updateEnumType(db, priority) == dbms::DBStatus::OK);
    assert(g_engine.getEnumType(db, "priority").labels == priority.labels);
    const auto renamedSchema = g_engine.getTableSchema(db, "tasks");
    assert(renamedSchema.cols[1].enumValues == priority.labels);
    assert(g_engine.insert(db, "tasks", {{"id", "5"}, {"p", "low"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    const auto renamedRows = g_engine.query(
        db, "tasks", {"=id 1"}, {"p"});
    assert((renamedRows == std::vector<std::string>{"minor "}));
    {
        auto& catalog = g_engine.catalogService().get(db);
        const auto* publicNamespace = catalog.findNamespaceByName("public");
        assert(publicNamespace);
        const auto* catalogType =
            catalog.findTypeByName("priority", publicNamespace->oid);
        assert(catalogType);
        const auto catalogLabels = catalog.findEnumLabels(catalogType->oid);
        assert(catalogLabels.size() == priority.labels.size());
        for (size_t index = 0; index < priority.labels.size(); ++index)
            assert(catalogLabels[index].enumlabel == priority.labels[index]);
    }

    cleanup(db);
    std::cout << "[ENUM] dependent schema updates OK" << std::endl;
}

static void test_enum_labels_round_trip_losslessly() {
    std::string db = testDbPath("enum_lossless_labels");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TYPE punctuation AS ENUM "
        "('comma,label', 'pipe|label', '', 'line\nlabel', 'it''s')", s));

    const std::vector<std::string> expected = {
        "comma,label", "pipe|label", "", "line\nlabel", "it's"};
    dbms::StorageEngine reopened;
    const auto stored = reopened.getEnumType(db, "punctuation");
    assert(stored.name == "punctuation");
    assert(stored.labels == expected);

    auto& catalog = g_engine.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace);
    const auto* punctuationType =
        catalog.findTypeByName("punctuation", publicNamespace->oid);
    assert(punctuationType && punctuationType->typtype == 'e');
    const dbms::Oid punctuationTypeOid = punctuationType->oid;
    assert(catalog.persistAll());
    dbms::CatalogManager reopenedCatalog(
        (std::filesystem::path(db) / "pg_catalog").string());
    const auto persistedLabels =
        reopenedCatalog.findEnumLabels(punctuationTypeOid);
    assert(persistedLabels.size() == expected.size());
    for (size_t index = 0; index < expected.size(); ++index)
        assert(persistedLabels[index].enumlabel == expected[index]);

    const std::string raw = readBytes(std::filesystem::path(db) / ".enums");
    assert(raw.rfind("DBMS_ENUM_V2:", 0) == 0);
    assert(raw.find("pipe|label") == std::string::npos);
    assert(raw.find("line\nlabel") == std::string::npos);

    cleanup(db);
    std::cout << "[ENUM] lossless labels OK" << std::endl;
}

static void test_enum_catalog_label_oid_stability() {
    std::string db = testDbPath("enum_catalog_oids");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TYPE priority AS ENUM ('low', 'high', 'obsolete')", s));

    auto& catalog = g_engine.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace);
    const auto* type =
        catalog.findTypeByName("priority", publicNamespace->oid);
    assert(type && type->typtype == 'e');
    const dbms::Oid typeOid = type->oid;
    const auto original = catalog.findEnumLabels(typeOid);
    assert(original.size() == 3);

    assert(catalog.replaceEnumLabels(
        typeOid, {"low", "medium", "high", "obsolete"}));
    const auto added = catalog.findEnumLabels(typeOid);
    assert(added.size() == 4);
    assert(added[0].oid == original[0].oid);
    assert(added[2].oid == original[1].oid);
    assert(added[3].oid == original[2].oid);
    assert(added[1].oid != dbms::INVALID_OID);

    assert(catalog.replaceEnumLabels(
        typeOid, {"low", "medium", "high", "urgent"}));
    const auto renamed = catalog.findEnumLabels(typeOid);
    assert(renamed.size() == 4);
    assert(renamed[3].enumlabel == "urgent");
    assert(renamed[3].oid == added[3].oid);
    assert(catalog.persistAll());

    dbms::CatalogManager reopened(
        (std::filesystem::path(db) / "pg_catalog").string());
    const auto persisted = reopened.findEnumLabels(typeOid);
    assert(persisted.size() == renamed.size());
    for (size_t index = 0; index < renamed.size(); ++index) {
        assert(persisted[index].oid == renamed[index].oid);
        assert(persisted[index].enumlabel == renamed[index].enumlabel);
        assert(persisted[index].enumsortorder ==
               renamed[index].enumsortorder);
    }

    cleanup(db);
    std::cout << "[ENUM] catalog label OIDs OK" << std::endl;
}

static void test_enum_drop_dependencies_and_catalog_cleanup() {
    std::string db = testDbPath("enum_drop_dependencies");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TYPE lifecycle AS ENUM ('new', 'done')", s));
    assert(!ddl.executeSql(
        "CREATE TABLE jobs (id INT PRIMARY KEY, state lifecycle)", s));
    assert(g_engine.insert(
               db, "jobs", {{"id", "1"}, {"state", "new"}}) ==
           dbms::DBStatus::OK);

    auto& catalog = g_engine.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace);
    const dbms::Oid publicNamespaceOid = publicNamespace->oid;
    const auto* lifecycleType =
        catalog.findTypeByName("lifecycle", publicNamespaceOid);
    assert(lifecycleType);
    const dbms::Oid lifecycleTypeOid = lifecycleType->oid;

    // RESTRICT must leave all three representations untouched.
    assert(ddl.executeSql("DROP TYPE lifecycle", s));
    assert(!g_engine.getEnumType(db, "lifecycle").name.empty());
    assert(g_engine.getTableSchema(db, "jobs").len == 2);
    assert(catalog.findType(lifecycleTypeOid));

    // CASCADE removes the dependent column through ALTER TABLE, preserving
    // the surviving row, then removes both pg_type and pg_enum rows.
    assert(!ddl.executeSql("DROP TYPE lifecycle CASCADE", s));
    assert(g_engine.getEnumType(db, "lifecycle").name.empty());
    const auto jobs = g_engine.getTableSchema(db, "jobs");
    assert(jobs.len == 1);
    assert(jobs.cols[0].dataName == "id");
    assert((g_engine.query(db, "jobs", {}, {"id"}) ==
            std::vector<std::string>{"1 "}));
    assert(!catalog.findType(lifecycleTypeOid));
    assert(catalog.findEnumLabels(lifecycleTypeOid).empty());

    assert(!ddl.executeSql(
        "CREATE TYPE standalone AS ENUM ('only')", s));
    const auto* standaloneType =
        catalog.findTypeByName("standalone", publicNamespaceOid);
    assert(standaloneType);
    const dbms::Oid standaloneTypeOid = standaloneType->oid;
    assert(!ddl.executeSql("DROP TYPE standalone", s));
    assert(!catalog.findType(standaloneTypeOid));
    assert(catalog.findEnumLabels(standaloneTypeOid).empty());

    cleanup(db);
    std::cout << "[ENUM] drop dependency/catalog cleanup OK" << std::endl;
}

static void test_enum_legacy_migration_and_atomic_rewrites() {
    namespace fs = std::filesystem;
    using dbms::DBStatus;

    std::string db = testDbPath("enum_atomic_metadata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    const fs::path metadata = fs::path(db) / ".enums";
    {
        std::ofstream legacy(metadata, std::ios::binary);
        legacy << "legacy|one|two\nneighbor|keep\n";
        assert(legacy.good());
    }

    dbms::StorageEngine engine;
    auto legacy = engine.getEnumType(db, "legacy");
    assert((legacy.labels == std::vector<std::string>{"one", "two"}));

    legacy.labels = {"with|pipe", "line\nfeed", ""};
    assert(engine.updateEnumType(db, legacy) == DBStatus::OK);
    const std::string migrated = readBytes(metadata);
    assert(migrated.find("legacy|one|two") == std::string::npos);
    assert(migrated.find("DBMS_ENUM_V2:") == 0);

    dbms::StorageEngine reopened;
    assert(reopened.getEnumType(db, "legacy").labels == legacy.labels);
    assert((reopened.getEnumType(db, "neighbor").labels ==
            std::vector<std::string>{"keep"}));

    dbms::StorageEngine::EnumType invalid;
    invalid.name = "duplicate_labels";
    invalid.labels = {"same", "same"};
    assert(engine.createEnumType(db, invalid) == DBStatus::INVALID_ARGUMENT);
    invalid.name = "nul_label";
    invalid.labels = {std::string("bad\0label", 9)};
    assert(engine.createEnumType(db, invalid) == DBStatus::INVALID_ARGUMENT);
    invalid.name = "no_labels";
    invalid.labels.clear();
    assert(engine.createEnumType(db, invalid) == DBStatus::INVALID_ARGUMENT);
    assert(readBytes(metadata) == migrated);

    const fs::perms originalPermissions = fs::status(db).permissions();
    fs::permissions(db, fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);
    legacy.labels = {"replacement"};
    const DBStatus updateStatus = engine.updateEnumType(db, legacy);
    dbms::StorageEngine::EnumType added;
    added.name = "added";
    added.labels = {"value"};
    const DBStatus createStatus = engine.createEnumType(db, added);
    const DBStatus dropStatus = engine.dropEnumType(db, "legacy");
    fs::permissions(db, originalPermissions, fs::perm_options::replace);
    assert(updateStatus == DBStatus::IO_ERROR);
    assert(createStatus == DBStatus::IO_ERROR);
    assert(dropStatus == DBStatus::IO_ERROR);
    assert(readBytes(metadata) == migrated);

    assert(engine.dropEnumType(db, "legacy") == DBStatus::OK);
    dbms::StorageEngine afterDrop;
    assert(afterDrop.getEnumType(db, "legacy").name.empty());
    assert((afterDrop.getEnumType(db, "neighbor").labels ==
            std::vector<std::string>{"keep"}));

    cleanup(db);
    std::cout << "[ENUM] migration and atomic rewrites OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_enum_basic();
    test_enum_update();
    test_named_enum_alter_updates_dependent_schemas();
    test_enum_labels_round_trip_losslessly();
    test_enum_catalog_label_oid_stability();
    test_enum_drop_dependencies_and_catalog_cleanup();
    test_enum_legacy_migration_and_atomic_rewrites();
    std::cout << "[ENUM] all passed" << std::endl;
    return 0;
}

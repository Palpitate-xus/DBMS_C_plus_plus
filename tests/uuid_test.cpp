// ============================================================================
// UUID type test — Phase 4 Wave 4.11
// UUIDs use a 16-byte heap datum while preserving PostgreSQL's permissive
// input and canonical output, binary ordering, v4/v7 generation and extractors.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "access/BPTree.h"
#include "access/HashIndex.h"
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "types/uuid.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

static void test_uuid_input_formats() {
    std::string db = testDbPath("uuid_fmt");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE u (id INT PRIMARY KEY, g UUID)", s));
    const dbms::TableSchema schema = g_engine.getTableSchema(db, "u");
    assert(schema.cols[1].dsize == 16);
    assert(!schema.cols[1].isVariableLength);

    const std::string canon = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    // Canonical, uppercase, unhyphenated, and braced forms all normalize.
    assert(g_engine.insert(db, "u", {{"id","1"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "u", {{"id","2"}, {"g","A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "u", {{"id","3"}, {"g","a0eebc999c0b4ef8bb6d6bb9bd380a11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "u", {{"id","4"}, {"g","{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"}}) == dbms::DBStatus::OK);
    for (int id = 1; id <= 4; ++id)
        assert(fetchOne(db, "u", {"=id " + std::to_string(id)}, "g") == canon);
    assert(g_engine.query(db, "u", {"=g A0EE-BC99-9C0B-4EF8-BB6D-6BB9-BD38-0A11"},
                          {"id"}).size() == 4);

    cleanup(db);
    std::cout << "[UUID] input formats normalize OK" << std::endl;
}

static void test_uuid_codec_and_functions() {
    dbms::UuidValue uuid;
    assert(dbms::UuidValue::parse(
        "{A0EE-BC99-9C0B-4EF8-BB6D-6BB9-BD38-0A11}", uuid));
    assert(uuid.toString() == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert(uuid.indexKey().size() == 16);
    assert(static_cast<unsigned char>(uuid.indexKey().front()) == 0xa0);
    assert(static_cast<unsigned char>(uuid.indexKey().back()) == 0x11);
    assert(!dbms::UuidValue::parse(
        "a-0eebc999c0b4ef8bb6d6bb9bd380a11", uuid));

    const dbms::UuidValue v4 = dbms::UuidValue::generateV4();
    assert(v4.version() == 4);
    assert(v4.isRfc9562Variant());
    dbms::UuidValue parsedV4;
    assert(dbms::UuidValue::parse(v4.toString(), parsedV4));
    assert(parsedV4 == v4);

    dbms::UuidValue firstV7;
    dbms::UuidValue secondV7;
    assert(dbms::UuidValue::generateV7At(1740365184503623LL, true, firstV7));
    assert(dbms::UuidValue::generateV7At(1740365184503623LL, true, secondV7));
    assert(firstV7.version() == 7);
    assert(firstV7 < secondV7);
    int64_t extractedMicros = 0;
    assert(firstV7.extractUnixMicros(extractedMicros));
    assert(extractedMicros / 1000 == 1740365184503LL);

    dbms::UuidValue v1;
    assert(dbms::UuidValue::parse(
        "f81d4fae-7dec-11d0-a765-00a0c91e6bf6", v1));
    assert(v1.version() == 1);
    assert(v1.extractUnixMicros(extractedMicros));
    assert(extractedMicros == 854991792216875LL);

    dbms::ExprEvaluator evaluator;
    auto literal = [](const std::string& type, const std::string& value) {
        auto expression = std::make_unique<dbms::LiteralExpr>();
        expression->typeName = type;
        expression->value = value;
        return expression;
    };
    auto call = [&](const std::string& name,
                    std::vector<std::unique_ptr<dbms::Expr>> arguments = {}) {
        auto expression = std::make_unique<dbms::FunctionCallExpr>();
        expression->funcName = name;
        expression->args = std::move(arguments);
        return evaluator.eval(expression.get(), {});
    };
    const dbms::ExprValue generatedV4 = call("uuidv4");
    assert(generatedV4.typeName == "uuid");
    assert(dbms::UuidValue::parse(generatedV4.value, uuid));
    assert(uuid.version() == 4);
    const dbms::ExprValue generatedAlias = call("gen_random_uuid");
    assert(dbms::UuidValue::parse(generatedAlias.value, uuid));
    assert(uuid.version() == 4);
    const dbms::ExprValue generatedV7 = call("uuidv7");
    assert(dbms::UuidValue::parse(generatedV7.value, uuid));
    assert(uuid.version() == 7);

    std::vector<std::unique_ptr<dbms::Expr>> versionArguments;
    versionArguments.push_back(literal("uuid", generatedV7.value));
    assert(call("uuid_extract_version", std::move(versionArguments)).value ==
           "7");
    std::vector<std::unique_ptr<dbms::Expr>> timestampArguments;
    timestampArguments.push_back(literal(
        "uuid", "019535d9-3df7-79fb-b466-fa907fa17f9e"));
    const dbms::ExprValue timestamp = call(
        "uuid_extract_timestamp", std::move(timestampArguments));
    assert(timestamp.typeName == "timestamptz");
    assert(timestamp.value.rfind("2025-02-24 02:46:24.503623", 0) == 0);

    auto cast = std::make_unique<dbms::CastExpr>();
    cast->operand = literal(
        "character varying", "A0EEBC999C0B4EF8BB6D6BB9BD380A11");
    cast->typeName = "uuid";
    assert(evaluator.eval(cast.get(), {}).value ==
           "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
}

static void test_uuid_legacy_storage() {
    const std::string db = testDbPath("uuid_legacy");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "legacy_uuid";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column legacy = dbms::makeUuidColumn("g", false);
    legacy.dsize = 36;
    schema.append(legacy);
    assert(g_engine.createTable(db, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "legacy_uuid",
        {{"id", "1"}, {"g", "550E8400E29B41D4A716446655440000"}}) ==
        dbms::DBStatus::OK);
    assert(fetchOne(db, "legacy_uuid", {"=id 1"}, "g") ==
           "550e8400-e29b-41d4-a716-446655440000");
    cleanup(db);
}

static void test_uuid_order_and_indexes() {
    const std::string db = testDbPath("uuid_indexes");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "indexed_uuid";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column uuidColumn = dbms::makeUuidColumn("g", false);
    uuidColumn.isUnique = true;
    schema.append(uuidColumn);
    assert(g_engine.createTable(db, schema) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(db, "indexed_uuid", "g") ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(db, "indexed_uuid", "g") ==
           dbms::DBStatus::OK);

    assert(g_engine.insert(
        db, "indexed_uuid",
        {{"id", "1"}, {"g", "00000000-0000-0000-0000-000000000002"}}) ==
        dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "indexed_uuid",
        {{"id", "2"}, {"g", "ffffffff-ffff-ffff-ffff-ffffffffffff"}}) ==
        dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "indexed_uuid",
        {{"id", "3"}, {"g", "00000000-0000-0000-0000-000000000001"}}) ==
        dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "indexed_uuid",
        {{"id", "4"}, {"g", "0000-0000-0000-0000-0000-0000-0000-0001"}}) ==
        dbms::DBStatus::DUPLICATE_KEY);

    dbms::StorageEngine::OrderBySpec order;
    order.colName = "g";
    assert(g_engine.query(db, "indexed_uuid", {}, {"id"}, {order}) ==
           (std::vector<std::string>{"3 ", "1 ", "2 "}));
    assert(g_engine.query(
        db, "indexed_uuid",
        {"=g 0000-0000-0000-0000-0000-0000-0000-0001"}, {"id"}) ==
        std::vector<std::string>{"3 "});

    dbms::UuidValue indexed;
    assert(dbms::UuidValue::parse(
        "00000000-0000-0000-0000-000000000001", indexed));
    const std::string key = indexed.indexKey();
    assert(g_engine.getSecondaryIndex(db, "indexed_uuid", "g")
               ->searchMulti(key).size() == 1);
    assert(g_engine.getHashIndex(db, "indexed_uuid", "g")
               ->search(key).size() == 1);

    dbms::TableSchema primarySchema;
    primarySchema.tablename = "uuid_primary";
    primarySchema.formatVersion = 2;
    primarySchema.append(dbms::makeUuidColumn("g", false, true));
    assert(g_engine.createTable(db, primarySchema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "uuid_primary",
        {{"g", "00000000-0000-0000-0000-000000000001"}}) ==
        dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "uuid_primary",
        {{"g", "00000000-0000-0000-0000-000000000002"}}) ==
        dbms::DBStatus::OK);
    assert(g_engine.insert(
        db, "uuid_primary",
        {{"g", "0000-0000-0000-0000-0000-0000-0000-0001"}}) ==
        dbms::DBStatus::DUPLICATE_KEY);
    cleanup(db);
}

static void test_uuid_invalid() {
    std::string db = testDbPath("uuid_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE u (id INT PRIMARY KEY, g UUID)", s));

    // Non-hex digit.
    assert(g_engine.insert(db, "u", {{"id","1"}, {"g","z0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too few hex digits.
    assert(g_engine.insert(db, "u", {{"id","2"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too many hex digits.
    assert(g_engine.insert(db, "u", {{"id","3"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1122"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "u", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[UUID] invalid input rejected OK" << std::endl;
}

static void test_uuid_update() {
    std::string db = testDbPath("uuid_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE u (id INT PRIMARY KEY, g UUID)", s));

    assert(g_engine.insert(db, "u", {{"id","1"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}) == dbms::DBStatus::OK);

    // Valid update (uppercase) canonicalizes.
    assert(g_engine.update(db, "u", {{"g","FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "u", {"=id 1"}, "g") == "ffffffff-ffff-ffff-ffff-ffffffffffff");

    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "u", {{"g","not-a-uuid"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "u", {"=id 1"}, "g") == "ffffffff-ffff-ffff-ffff-ffffffffffff");

    cleanup(db);
    std::cout << "[UUID] update normalize/reject OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_uuid_codec_and_functions();
    test_uuid_input_formats();
    test_uuid_invalid();
    test_uuid_update();
    test_uuid_legacy_storage();
    test_uuid_order_and_indexes();
    std::cout << "[UUID] all passed" << std::endl;
    return 0;
}

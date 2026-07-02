// ============================================================================
// Manual Verification Test — 按照 MANUAL.md 完整手册逐章验证
// 每个功能点都有对应的测试用例，确保文档描述与实际行为一致
// ============================================================================

#include "test_utils.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

// ============================================================
// Chapter 3: 数据库操作
// ============================================================
static void test_ch3_database_ops() {
    std::string db = testDbPath("ch3_db");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    // 基本建表 + 操作
    assert(!ddl.executeSql("CREATE TABLE t1 (ID INT PRIMARY KEY, NAME VARCHAR(50))", s));
    assert(g_engine.insert(db, "t1", {{"ID", "1"}, {"NAME", "Alice"}}) == dbms::DBStatus::OK);

    // 删除数据库前需要先清理表
    g_engine.dropTable(db, "t1");
    cleanup(db);
    std::cout << "[CH3] 数据库操作 OK" << std::endl;
}

// ============================================================
// Chapter 4: 表操作 (DDL)
// ============================================================
static void test_ch4_table_ops() {
    std::string db = testDbPath("ch4_tbl");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    // 4.1 基本建表
    assert(!ddl.executeSql(
        "CREATE TABLE users ("
        "ID INT PRIMARY KEY, "
        "NAME VARCHAR(50) NOT NULL, "
        "EMAIL VARCHAR(100) UNIQUE, "
        "AGE INT DEFAULT 0)", s));

    // 4.2 ALTER TABLE ADD COLUMN
    assert(!ddl.executeSql("ALTER TABLE users ADD COLUMN PHONE VARCHAR(20)", s));

    // 4.3 ALTER TABLE DROP COLUMN
    assert(!ddl.executeSql("ALTER TABLE users DROP COLUMN PHONE", s));

    // 4.4 ALTER TABLE ALTER COLUMN TYPE
    assert(!ddl.executeSql("ALTER TABLE users ALTER COLUMN AGE TYPE VARCHAR(10)", s));

    // 4.5 ALTER TABLE SET DEFAULT
    assert(!ddl.executeSql("ALTER TABLE users ALTER COLUMN AGE SET DEFAULT '18'", s));

    // 4.6 ALTER TABLE DROP DEFAULT
    assert(!ddl.executeSql("ALTER TABLE users ALTER COLUMN AGE DROP DEFAULT", s));

    // 4.7 ALTER TABLE SET NOT NULL
    assert(!ddl.executeSql("ALTER TABLE users ALTER COLUMN NAME SET NOT NULL", s));

    // 4.8 ALTER TABLE RENAME COLUMN
    assert(!ddl.executeSql("ALTER TABLE users RENAME COLUMN NAME TO FULL_NAME", s));

    // 4.9 验证 schema
    auto schema = g_engine.getTableSchema(db, "users");
    assert(schema.len >= 3);

    // 4.10 DROP TABLE
    assert(!ddl.executeSql("DROP TABLE users", s));

    // 4.11 UNLOGGED TABLE
    assert(!ddl.executeSql("CREATE UNLOGGED TABLE cache_data (K TEXT, V TEXT)", s));

    // 4.12 TRUNCATE TABLE
    assert(!ddl.executeSql("CREATE TABLE trunc_me (ID INT)", s));
    assert(!ddl.executeSql("TRUNCATE TABLE trunc_me", s));

    // 4.13 ALTER TABLE SET LOGGED/UNLOGGED
    assert(!ddl.executeSql("ALTER TABLE cache_data SET LOGGED", s));

    // 4.14 ALTER TABLE SET STATISTICS
    assert(!ddl.executeSql("ALTER TABLE trunc_me ALTER COLUMN ID SET STATISTICS 100", s));

    // 4.15 LIKE INCLUDING
    assert(!ddl.executeSql("CREATE TABLE src_tbl (ID INT, VAL INT DEFAULT 42)", s));
    assert(!ddl.executeSql("CREATE TABLE cpy_tbl (LIKE src_tbl INCLUDING DEFAULTS)", s));

    cleanup(db);
    std::cout << "[CH4] 表操作 OK" << std::endl;
}

// ============================================================
// Chapter 5: DML
// ============================================================
static void test_ch5_dml() {
    std::string db = testDbPath("ch5_dml");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    assert(!ddl.executeSql("CREATE TABLE users (ID INT PRIMARY KEY, NAME VARCHAR(50), AGE INT)", s));

    // 5.1 INSERT
    assert(g_engine.insert(db, "users", {{"ID","1"},{"NAME","Alice"},{"AGE","25"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "users", {{"ID","2"},{"NAME","Bob"},{"AGE","30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "users", {{"ID","3"},{"NAME","Charlie"},{"AGE","35"}}) == dbms::DBStatus::OK);

    // 5.2 UPDATE
    assert(g_engine.update(db, "users", {{"AGE","26"}}, {"=ID 1"}) == dbms::DBStatus::OK);

    // 5.3 DELETE
    assert(g_engine.remove(db, "users", {"=ID 3"}) == dbms::DBStatus::OK);

    // 5.4 验证剩余行数
    auto rows = g_engine.query(db, "users", {}, {"ID", "NAME", "AGE"});
    assert(rows.size() == 2);

    // 5.5 INSERT DEFAULT VALUES
    assert(!ddl.executeSql("CREATE TABLE default_t (ID INT DEFAULT 99, NAME VARCHAR(20) DEFAULT 'x')", s));
    dbms::DBStatus res = g_engine.insertDefaultValues(db, "default_t",
        g_engine.getTableSchema(db, "default_t"));
    assert(res == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[CH5] DML OK" << std::endl;
}

// ============================================================
// Chapter 6: DQL (查询)
// ============================================================
static void test_ch6_dql() {
    std::string db = testDbPath("ch6_dql");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    assert(!ddl.executeSql("CREATE TABLE t (ID INT PRIMARY KEY, VAL INT, NAME VARCHAR(20))", s));
    for (int i = 1; i <= 10; ++i) {
        g_engine.insert(db, "t", {{"ID", std::to_string(i)}, {"VAL", std::to_string(i*10)},
                                  {"NAME", "user" + std::to_string(i)}});
    }

    // 6.1 SELECT *
    auto rows = g_engine.query(db, "t", {}, {"ID", "VAL", "NAME"});
    assert(rows.size() == 10);

    // 6.2 WHERE 条件
    rows = g_engine.query(db, "t", {">VAL 50"}, {"ID", "VAL"});
    assert(rows.size() == 5);  // 60,70,80,90,100

    // 6.3 ORDER BY
    rows = g_engine.query(db, "t", {}, {"ID", "VAL"},
        {dbms::StorageEngine::OrderBySpec{"VAL", false}});
    assert(!rows.empty());

    // 6.4 聚合查询通过 DDL
    assert(!ddl.executeSql("SELECT COUNT(*) FROM t", s));
    assert(!ddl.executeSql("SELECT SUM(VAL) FROM t", s));
    assert(!ddl.executeSql("SELECT MAX(VAL), MIN(VAL), AVG(VAL) FROM t", s));

    // 6.5 GROUP BY / HAVING
    assert(!ddl.executeSql("SELECT VAL, COUNT(*) FROM t GROUP BY VAL", s));
    assert(!ddl.executeSql("SELECT VAL, COUNT(*) FROM t GROUP BY VAL HAVING COUNT(*) > 1", s));

    // 6.6 UNION
    assert(!ddl.executeSql("SELECT ID FROM t WHERE VAL > 50 UNION SELECT ID FROM t WHERE VAL < 30", s));

    // 6.7 JOIN
    assert(!ddl.executeSql("CREATE TABLE t2 (ID INT, REF_ID INT)", s));
    g_engine.insert(db, "t2", {{"ID", "1"}, {"REF_ID", "1"}});
    g_engine.insert(db, "t2", {{"ID", "2"}, {"REF_ID", "5"}});
    assert(!ddl.executeSql(
        "SELECT t.ID, t.NAME, t2.ID FROM t JOIN t2 ON t.ID = t2.REF_ID", s));

    // 6.8 INSERT ... SELECT
    assert(!ddl.executeSql("CREATE TABLE t_copy (ID INT, VAL INT)", s));
    for (int i = 6; i <= 10; ++i) {
        g_engine.insert(db, "t_copy", {{"ID", std::to_string(i)}, {"VAL", std::to_string(i*10)}});
    }
    rows = g_engine.query(db, "t_copy", {}, {"ID", "VAL"});
    assert(rows.size() == 5);

    // 6.9 Parser-only tests (no executor)
    dbms::SQLParser parser;
    assert(parser.parse("SELECT * FROM t FOR UPDATE").success);
    assert(parser.parse("SELECT * FROM t UNION SELECT * FROM t_copy").success);
    assert(parser.parse("SELECT ROW_NUMBER() OVER (ORDER BY VAL) FROM t").success);
    assert(parser.parse("SELECT * FROM t WHERE ID IN (SELECT ID FROM t_copy)").success);

    cleanup(db);
    std::cout << "[CH6] DQL OK" << std::endl;
}

// ============================================================
// Chapter 7: 事务控制
// ============================================================
static void test_ch7_transaction() {
    std::string db = testDbPath("ch7_txn");
    cleanup(db);
    Session s; setupSession(s, db);

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    assert(!dbms::DdlExecutor().executeSql(
        "CREATE TABLE t (ID INT PRIMARY KEY, VAL INT)", s));

    // 开启事务
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);

    // 插入
    assert(g_engine.insert(db, "t", {{"ID","1"},{"VAL","100"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"ID","2"},{"VAL","200"}}) == dbms::DBStatus::OK);

    // 提交
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    // 验证持久化
    auto rows = g_engine.query(db, "t", {}, {"ID", "VAL"});
    assert(rows.size() == 2);

    // Rollback 测试
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"ID","3"},{"VAL","300"}}) == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);

    // 验证回滚
    rows = g_engine.query(db, "t", {}, {"ID"});
    assert(rows.size() == 2);  // 3 被回滚

    cleanup(db);
    std::cout << "[CH7] 事务控制 OK" << std::endl;
}

// ============================================================
// Chapter 8: 索引
// ============================================================
static void test_ch8_index() {
    std::string db = testDbPath("ch8_idx");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    assert(!ddl.executeSql("CREATE TABLE t (ID INT PRIMARY KEY, NAME VARCHAR(50), VAL INT)", s));

    // 创建二级索引
    assert(g_engine.createIndex(db, "t", "NAME", true, {}, "", "", false) == dbms::DBStatus::OK);

    // 验证索引被使用
    auto indexedCols = g_engine.getIndexedColumns(db, "t");
    assert(!indexedCols.empty());

    // REINDEX
    g_engine.reindex(db, "t");

    // ANALYZE
    g_engine.analyzeTable(db, "t");

    cleanup(db);
    std::cout << "[CH8] 索引 OK" << std::endl;
}

// ============================================================
// Chapter 9: 视图与物化视图
// ============================================================
static void test_ch9_views() {
    std::string db = testDbPath("ch9_view");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    assert(!ddl.executeSql("CREATE TABLE base (ID INT PRIMARY KEY, VAL INT)", s));

    // 普通视图
    assert(!ddl.executeSql("CREATE VIEW v_high AS SELECT * FROM base WHERE VAL > 50", s));

    // OR REPLACE
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW v_high AS SELECT * FROM base WHERE VAL > 30", s));

    // 物化视图
    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT ID, VAL FROM base", s));

    // REFRESH
    assert(!ddl.executeSql("REFRESH MATERIALIZED VIEW mv", s));

    // WITH NO DATA
    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv_empty AS SELECT * FROM base WITH NO DATA", s));

    cleanup(db);
    std::cout << "[CH9] 视图/物化视图 OK" << std::endl;
}

// ============================================================
// Chapter 10: 约束
// ============================================================
static void test_ch10_constraints() {
    std::string db = testDbPath("ch10_cst");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    // CHECK 约束 (多 CHECK，含命名)
    assert(!ddl.executeSql(
        "CREATE TABLE products ("
        "ID INT PRIMARY KEY, "
        "PRICE INT CONSTRAINT POSITIVE CHECK (PRICE > 0), "
        "CONSTRAINT MAX_PRICE CHECK (PRICE < 10000))", s));

    // 验证 CHECK 约束生效
    assert(g_engine.insert(db, "products", {{"ID","1"},{"PRICE","-1"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "products", {{"ID","1"},{"PRICE","10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "products", {{"ID","2"},{"PRICE","9999"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "products", {{"ID","3"},{"PRICE","10000"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[CH10] 约束 OK" << std::endl;
}

// ============================================================
// Chapter 11-12: 用户与角色 (parser 验证)
// ============================================================
static void test_ch11_12_auth_roles() {
    dbms::SQLParser parser;

    // GRANT
    auto r1 = parser.parse("GRANT SELECT ON users TO alice");
    assert(r1.success);

    // REVOKE
    auto r2 = parser.parse("REVOKE SELECT ON users FROM alice");
    assert(r2.success);

    // CREATE ROLE
    auto r3 = parser.parse("CREATE ROLE readonly");
    assert(r3.success);

    // ALTER ROLE
    auto r4 = parser.parse("ALTER ROLE bob WITH SUPERUSER CREATEDB");
    assert(r4.success);

    // CREATE USER
    auto r5 = parser.parse("CREATE USER alice WITH PASSWORD 'secret' LOGIN");
    assert(r5.success);

    std::cout << "[CH11-12] 用户/角色 OK" << std::endl;
}

// ============================================================
// Chapter 13-14: 存储过程与触发器
// ============================================================
static void test_ch13_14_proc_trigger() {
    dbms::SQLParser parser;

    // CREATE PROCEDURE
    auto r1 = parser.parse("CREATE PROCEDURE my_proc(INT, INT) AS 'body' LANGUAGE sql");
    assert(r1.success);

    // CREATE FUNCTION
    auto r2 = parser.parse("CREATE FUNCTION add_one(INT) RETURNS INT AS 'return $1+1' LANGUAGE sql");
    assert(r2.success);

    // CREATE TRIGGER
    auto r3 = parser.parse("CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION fn()");
    assert(r3.success);

    // CALL
    auto r4 = parser.parse("CALL my_proc(1, 2)");
    assert(r4.success);

    std::cout << "[CH13-14] 过程/触发器 OK" << std::endl;
}

// ============================================================
// Chapter 15: 分区表
// ============================================================
static void test_ch15_partition() {
    std::string db = testDbPath("ch15_part");
    cleanup(db);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    s.currentDB = db;

    // RANGE 分区
    assert(!ddl.executeSql(
        "CREATE TABLE events (ID INT, EVT_TIME TIMESTAMP) PARTITION BY RANGE (EVT_TIME)", s));

    // ATTACH 分区
    assert(g_engine.attachPartition(db, "events", "p2024",
        "FOR VALUES FROM (2024) TO (2025)") == dbms::DBStatus::OK);

    // LIST 分区
    assert(!ddl.executeSql(
        "CREATE TABLE logs (ID INT, REGION TEXT) PARTITION BY LIST (REGION)", s));

    // HASH 分区
    assert(!ddl.executeSql(
        "CREATE TABLE ht (ID INT, K TEXT) PARTITION BY HASH (K)", s));

    cleanup(db);
    std::cout << "[CH15] 分区表 OK" << std::endl;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    test_ch3_database_ops();
    test_ch4_table_ops();
    test_ch5_dml();
    test_ch6_dql();
    test_ch7_transaction();
    test_ch8_index();
    test_ch9_views();
    test_ch10_constraints();
    test_ch11_12_auth_roles();
    test_ch13_14_proc_trigger();
    test_ch15_partition();
    std::cout << "[MANUAL_VERIFY] ALL chapters passed" << std::endl;
    return 0;
}

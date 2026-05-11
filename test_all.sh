#!/bin/bash

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="/tmp/dbms_build_$$"
mkdir -p "$BUILD_DIR"

echo "========================================"
echo "DBMS 全功能测试脚本"
echo "========================================"
echo ""

# 编译选项
CXX="g++"
CXXFLAGS="-std=c++17 -O2 -pthread -I$SRC_DIR"
SOURCES="$SRC_DIR/TableManage.cpp $SRC_DIR/ExecutionPlan.cpp $SRC_DIR/BufferPool.cpp $SRC_DIR/PageAllocator.cpp $SRC_DIR/Page.cpp $SRC_DIR/BPTree.cpp $SRC_DIR/LockManager.cpp $SRC_DIR/TxnIdGenerator.cpp"

PASS=0
FAIL=0

run_test() {
    local name="$1"
    local cmd="$2"
    echo "----------------------------------------"
    echo "[$name]"
    if eval "$cmd"; then
        echo "  -> PASS"
        ((PASS++))
    else
        echo "  -> FAIL"
        ((FAIL++))
    fi
    echo ""
}

# ========== 测试 1: VARCHAR 基础功能 ==========
cat > "$BUILD_DIR/test_varchar.cpp" << 'EOF'
#include <iostream>
#include <filesystem>
#include "TableManage.h"
using namespace dbms;

int main() {
    std::filesystem::remove_all("test_varchar_db");
    StorageEngine engine;
    engine.createDatabase("test_varchar_db");

    TableSchema tbl;
    tbl.tablename = "users";
    tbl.append(makeIntColumn("id", false, 2, true));
    tbl.append(makeVarCharColumn("name", false, 50));
    tbl.append(makeIntColumn("age", true, 1));
    engine.createTable("test_varchar_db", tbl);

    std::map<std::string, std::string> vals;
    vals["id"] = "1"; vals["name"] = "Alice"; vals["age"] = "25";
    engine.insert("test_varchar_db", "users", vals);
    vals["id"] = "2"; vals["name"] = "Bob"; vals["age"] = "30";
    engine.insert("test_varchar_db", "users", vals);
    vals["id"] = "3"; vals["name"] = "Charlie"; vals["age"] = "";
    engine.insert("test_varchar_db", "users", vals);

    // Query all
    auto rows = engine.query("test_varchar_db", "users", {}, {});
    if (rows.size() != 3) { std::cerr << "Expected 3 rows, got " << rows.size() << std::endl; return 1; }

    // WHERE =
    rows = engine.query("test_varchar_db", "users", {"=name Alice"}, {});
    if (rows.size() != 1) { std::cerr << "Expected 1 Alice" << std::endl; return 1; }

    // LIKE
    rows = engine.query("test_varchar_db", "users", {"likename Ch%"}, {});
    if (rows.size() != 1) { std::cerr << "Expected 1 Ch% match" << std::endl; return 1; }

    // UPDATE
    engine.update("test_varchar_db", "users", {{"name", "Alicia"}}, {"=id 1"});
    rows = engine.query("test_varchar_db", "users", {"=name Alicia"}, {});
    if (rows.size() != 1) { std::cerr << "Update failed" << std::endl; return 1; }

    // ORDER BY
    rows = engine.query("test_varchar_db", "users", {}, {}, "name", true);
    if (rows.size() != 3) { std::cerr << "Order by failed" << std::endl; return 1; }

    // DELETE
    engine.remove("test_varchar_db", "users", {"=name Bob"});
    rows = engine.query("test_varchar_db", "users", {}, {});
    if (rows.size() != 2) { std::cerr << "Delete failed, expected 2, got " << rows.size() << std::endl; return 1; }

    // Aggregate
    auto agg = engine.aggregate("test_varchar_db", "users", {}, {{"count", "*"}});
    if (agg.size() != 1) { std::cerr << "Aggregate failed" << std::endl; return 1; }

    std::filesystem::remove_all("test_varchar_db");
    std::cout << "VARCHAR test PASSED" << std::endl;
    return 0;
}
EOF

$CXX $CXXFLAGS "$BUILD_DIR/test_varchar.cpp" $SOURCES -o "$BUILD_DIR/test_varchar"
run_test "VARCHAR 基础功能" "$BUILD_DIR/test_varchar"

# ========== 测试 2: JOIN 功能 ==========
cat > "$BUILD_DIR/test_join.cpp" << 'EOF'
#include <iostream>
#include <filesystem>
#include "TableManage.h"
using namespace dbms;

int main() {
    std::filesystem::remove_all("test_join_db");
    StorageEngine engine;
    engine.createDatabase("test_join_db");

    TableSchema a;
    a.tablename = "a";
    a.append(makeIntColumn("id", false, 2, true));
    a.append(makeVarCharColumn("name", false, 20));
    engine.createTable("test_join_db", a);

    TableSchema b;
    b.tablename = "b";
    b.append(makeIntColumn("oid", false, 2, true));
    b.append(makeIntColumn("aid", false, 1));
    b.append(makeVarCharColumn("item", false, 20));
    engine.createTable("test_join_db", b);

    std::map<std::string, std::string> v;
    v["id"] = "1"; v["name"] = "Alice"; engine.insert("test_join_db", "a", v);
    v["id"] = "2"; v["name"] = "Bob";   engine.insert("test_join_db", "a", v);

    v["oid"] = "1"; v["aid"] = "1"; v["item"] = "book"; engine.insert("test_join_db", "b", v);
    v["oid"] = "2"; v["aid"] = "1"; v["item"] = "pen";  engine.insert("test_join_db", "b", v);
    v["oid"] = "3"; v["aid"] = "2"; v["item"] = "desk"; engine.insert("test_join_db", "b", v);

    // INNER JOIN: Alice(2) + Bob(1) = 3
    auto j = engine.join("test_join_db", "a", "b", "id", "aid", {}, {});
    if (j.size() != 3) { std::cerr << "INNER JOIN expected 3, got " << j.size() << std::endl; return 1; }

    // LEFT JOIN: Alice(2 matches) + Bob(1 match) = 3 rows
    auto lj = engine.leftJoin("test_join_db", "a", "b", "id", "aid", {}, {});
    if (lj.size() != 3) { std::cerr << "LEFT JOIN expected 3, got " << lj.size() << std::endl; return 1; }

    std::filesystem::remove_all("test_join_db");
    std::cout << "JOIN test PASSED" << std::endl;
    return 0;
}
EOF

$CXX $CXXFLAGS "$BUILD_DIR/test_join.cpp" $SOURCES -o "$BUILD_DIR/test_join"
run_test "JOIN 功能" "$BUILD_DIR/test_join"

# ========== 测试 3: MVCC 事务 ==========
cat > "$BUILD_DIR/test_mvcc.cpp" << 'EOF'
#include <iostream>
#include <filesystem>
#include "TableManage.h"
using namespace dbms;

int main() {
    std::filesystem::remove_all("test_mvcc_db");
    std::filesystem::remove(".txnid");
    StorageEngine engine;
    engine.createDatabase("test_mvcc_db");

    TableSchema t;
    t.tablename = "t";
    t.append(makeIntColumn("id", false, 2, true));
    t.append(makeVarCharColumn("name", false, 20));
    engine.createTable("test_mvcc_db", t);

    // Insert baseline
    std::map<std::string, std::string> v;
    v["id"] = "1"; v["name"] = "Alice"; engine.insert("test_mvcc_db", "t", v);

    // Begin txn, insert
    engine.beginTransaction("test_mvcc_db");
    v["id"] = "2"; v["name"] = "Bob"; engine.insert("test_mvcc_db", "t", v);

    // Same txn sees Bob
    auto rows = engine.query("test_mvcc_db", "t", {"=name Bob"}, {});
    if (rows.size() != 1) { std::cerr << "Txn should see own insert" << std::endl; return 1; }

    engine.commitTransaction();

    // After commit, Bob visible
    rows = engine.query("test_mvcc_db", "t", {"=name Bob"}, {});
    if (rows.size() != 1) { std::cerr << "After commit Bob should be visible" << std::endl; return 1; }

    // Rollback test
    engine.beginTransaction("test_mvcc_db");
    v["id"] = "3"; v["name"] = "Charlie"; engine.insert("test_mvcc_db", "t", v);
    engine.rollbackTransaction();

    rows = engine.query("test_mvcc_db", "t", {"=name Charlie"}, {});
    if (rows.size() != 0) { std::cerr << "Rollback should remove Charlie" << std::endl; return 1; }

    std::filesystem::remove_all("test_mvcc_db");
    std::filesystem::remove(".txnid");
    std::cout << "MVCC test PASSED" << std::endl;
    return 0;
}
EOF

$CXX $CXXFLAGS "$BUILD_DIR/test_mvcc.cpp" $SOURCES -o "$BUILD_DIR/test_mvcc"
run_test "MVCC 事务" "$BUILD_DIR/test_mvcc"

# ========== 测试 4: Checkpoint 恢复 ==========
cat > "$BUILD_DIR/test_checkpoint.cpp" << 'EOF'
#include <iostream>
#include <filesystem>
#include "TableManage.h"
using namespace dbms;

int main() {
    std::filesystem::remove_all("test_cp_db");
    std::filesystem::remove(".txnid");

    {
        StorageEngine engine;
        engine.createDatabase("test_cp_db");
        TableSchema t;
        t.tablename = "items";
        t.append(makeIntColumn("id", false, 2, true));
        t.append(makeVarCharColumn("name", false, 20));
        engine.createTable("test_cp_db", t);

        std::map<std::string, std::string> v;
        v["id"] = "1"; v["name"] = "apple"; engine.insert("test_cp_db", "items", v);
        v["id"] = "2"; v["name"] = "banana"; engine.insert("test_cp_db", "items", v);

        engine.checkpoint("test_cp_db");

        v["id"] = "3"; v["name"] = "cherry"; engine.insert("test_cp_db", "items", v);
    }

    // Restart
    {
        StorageEngine engine;
        auto rows = engine.query("test_cp_db", "items", {}, {});
        if (rows.size() != 3) {
            std::cerr << "Checkpoint recovery failed, expected 3, got " << rows.size() << std::endl;
            return 1;
        }
    }

    std::filesystem::remove_all("test_cp_db");
    std::filesystem::remove(".txnid");
    std::cout << "Checkpoint test PASSED" << std::endl;
    return 0;
}
EOF

$CXX $CXXFLAGS "$BUILD_DIR/test_checkpoint.cpp" $SOURCES -o "$BUILD_DIR/test_checkpoint"
run_test "Checkpoint 恢复" "$BUILD_DIR/test_checkpoint"

# ========== 测试 5: 聚合 + GROUP BY ==========
cat > "$BUILD_DIR/test_aggregate.cpp" << 'EOF'
#include <iostream>
#include <filesystem>
#include "TableManage.h"
using namespace dbms;

int main() {
    std::filesystem::remove_all("test_agg_db");
    StorageEngine engine;
    engine.createDatabase("test_agg_db");

    TableSchema t;
    t.tablename = "scores";
    t.append(makeIntColumn("id", false, 2, true));
    t.append(makeVarCharColumn("dept", false, 10));
    t.append(makeIntColumn("score", false, 2));
    engine.createTable("test_agg_db", t);

    std::map<std::string, std::string> v;
    v["id"] = "1"; v["dept"] = "CS"; v["score"] = "80"; engine.insert("test_agg_db", "scores", v);
    v["id"] = "2"; v["dept"] = "CS"; v["score"] = "90"; engine.insert("test_agg_db", "scores", v);
    v["id"] = "3"; v["dept"] = "EE"; v["score"] = "70"; engine.insert("test_agg_db", "scores", v);

    // COUNT(*)
    auto agg = engine.aggregate("test_agg_db", "scores", {}, {{"count", "*"}});
    if (agg.size() != 1) { std::cerr << "Aggregate failed" << std::endl; return 1; }

    // GROUP BY
    auto grp = engine.groupAggregate("test_agg_db", "scores", {}, {{"count", "*"}}, "dept", {});
    if (grp.size() != 2) { std::cerr << "GROUP BY expected 2 groups, got " << grp.size() << std::endl; return 1; }

    std::filesystem::remove_all("test_agg_db");
    std::cout << "Aggregate test PASSED" << std::endl;
    return 0;
}
EOF

$CXX $CXXFLAGS "$BUILD_DIR/test_aggregate.cpp" $SOURCES -o "$BUILD_DIR/test_aggregate"
run_test "聚合与 GROUP BY" "$BUILD_DIR/test_aggregate"

# ========== 测试 6: 索引 ==========
cat > "$BUILD_DIR/test_index.cpp" << 'EOF'
#include <iostream>
#include <filesystem>
#include "TableManage.h"
using namespace dbms;

int main() {
    std::filesystem::remove_all("test_idx_db");
    StorageEngine engine;
    engine.createDatabase("test_idx_db");

    TableSchema t;
    t.tablename = "data";
    t.append(makeIntColumn("id", false, 2, true));
    t.append(makeIntColumn("score", false, 2));
    engine.createTable("test_idx_db", t);

    std::map<std::string, std::string> v;
    for (int i = 1; i <= 10; i++) {
        v["id"] = std::to_string(i);
        v["score"] = std::to_string(i * 10);
        engine.insert("test_idx_db", "data", v);
    }

    // Create index on score
    engine.createIndex("test_idx_db", "data", "score");

    // Query using index
    auto rows = engine.query("test_idx_db", "data", {">score 50"}, {});
    if (rows.size() != 5) {
        std::cerr << "Index query expected 5, got " << rows.size() << std::endl;
        return 1;
    }

    // Drop index
    engine.dropIndex("test_idx_db", "data", "score");

    std::filesystem::remove_all("test_idx_db");
    std::cout << "Index test PASSED" << std::endl;
    return 0;
}
EOF

$CXX $CXXFLAGS "$BUILD_DIR/test_index.cpp" $SOURCES -o "$BUILD_DIR/test_index"
run_test "二级索引" "$BUILD_DIR/test_index"

# ========== 总结 ==========
echo "========================================"
echo "测试结果: $PASS 通过, $FAIL 失败"
echo "========================================"

rm -rf "$BUILD_DIR"

if [ $FAIL -eq 0 ]; then
    echo "全部测试通过!"
    exit 0
else
    echo "存在失败的测试!"
    exit 1
fi

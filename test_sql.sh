#!/bin/bash

DBMS_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DBMS_DIR"

PASS=0
FAIL=0

cleanup() {
    rm -rf testdb* user.dat logs.txt slow_queries.txt .txnid
}

# 初始化 admin 用户
init_user() {
    echo "admin admin admin" > user.dat
}

# 运行 SQL 并返回输出
run_sql() {
    local input="$1"
    {
        echo "admin admin"
        echo "$input"
        echo "exit"
    } | ./dbms_main 2>&1
}

run_test() {
    local name="$1"
    local sql="$2"
    local check="$3"
    echo "========================================"
    echo "[$name]"
    echo "========================================"
    cleanup
    init_user
    local output
    output=$(run_sql "$sql")
    echo ""
    echo "--- SQL ---"
    echo "$sql"
    echo ""
    echo "--- OUTPUT ---"
    echo "$output"
    echo ""
    if echo "$output" | grep -q "$check"; then
        echo "--- RESULT: PASS ---"
        ((PASS++))
    else
        echo "--- RESULT: FAIL ---"
        echo "  Expected: '$check'"
        ((FAIL++))
    fi
    echo ""
}

echo "========================================"
echo "DBMS SQL 测试脚本"
echo "========================================"
echo ""

# 编译
echo "编译 dbms_main..."
if ! g++ -std=c++17 -O2 -pthread main.cpp TableManage.cpp ExecutionPlan.cpp BufferPool.cpp PageAllocator.cpp Page.cpp BPTree.cpp LockManager.cpp NetworkServer.cpp TxnIdGenerator.cpp -o dbms_main 2>&1; then
    echo "编译失败!"
    exit 1
fi
echo "编译完成"
echo ""

# ========== 测试 1: 创建数据库和表 ==========
run_test "创建数据库和表" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )" \
"Table create succeeded"

# ========== 测试 2: 插入数据 ==========
run_test "插入数据" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
insert into users (id,name,age) values (2,'Bob',30)" \
"Data inserted"

# ========== 测试 3: SELECT 查询 ==========
run_test "SELECT 查询" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
insert into users (id,name,age) values (2,'Bob',30)
select * from users" \
"alice"

# ========== 测试 4: WHERE 条件 ==========
run_test "WHERE 条件" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
insert into users (id,name,age) values (2,'Bob',30)
select * from users where id = 1" \
"alice"

# ========== 测试 5: UPDATE ==========
run_test "UPDATE" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
update users set name = 'Alicia' where id = 1
select * from users where id = 1" \
"alicia"

# ========== 测试 6: DELETE ==========
run_test "DELETE" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
insert into users (id,name,age) values (2,'Bob',30)
delete from users where id = 2
select * from users" \
"1 alice"

# ========== 测试 7: 事务 COMMIT ==========
run_test "事务 COMMIT" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
begin transaction
insert into users (id,name,age) values (1,'Alice',25)
commit
select * from users" \
"Transaction committed"

# ========== 测试 8: 事务 ROLLBACK ==========
run_test "事务 ROLLBACK" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
begin transaction
insert into users (id,name,age) values (2,'Bob',30)
rollback
select * from users" \
"1 alice"

# ========== 测试 9: 聚合 COUNT ==========
run_test "聚合 COUNT" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
insert into users (id,name,age) values (2,'Bob',30)
select count(*) from users" \
"2"

# ========== 测试 10: Checkpoint ==========
run_test "Checkpoint" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
checkpoint
select * from users" \
"Checkpoint completed"

# ========== 测试 11: 索引 ==========
run_test "创建索引" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
insert into users (id,name,age) values (1,'Alice',25)
create index idx_name on users(name)" \
"Index created"

# ========== 测试 12: DROP TABLE ==========
run_test "DROP TABLE" \
"create database testdb
use database testdb
create table users ( id int not null primary key , name varchar 20 not null , age int )
drop table users
create table users ( id int not null primary key , name varchar 20 not null )" \
"Table create succeeded"

cleanup

echo "========================================"
echo "测试结果: $PASS 通过, $FAIL 失败"
echo "========================================"

if [ $FAIL -eq 0 ]; then
    echo "全部测试通过!"
    exit 0
else
    echo "存在失败的测试!"
    exit 1
fi

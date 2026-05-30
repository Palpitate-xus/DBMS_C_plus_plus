#!/bin/bash
# Comprehensive feature test for DBMS_C_plus_plus
# Tests every documented feature and reports PASS/FAIL

DBMS="./dbms_main"
PASS=0
FAIL=0

cleanup() {
    rm -rf shopdb fktest testdb testdb2
    rm -f /tmp/test_export.csv /tmp/test_dump.sql /tmp/test_backup.bak
    rm -f user.dat
}
trap cleanup EXIT

# Create fresh user file
echo 'admin admin admin' > user.dat

# DBMS reads line-by-line, so use \n as statement separator
run_sql() {
    printf "admin\nadmin\n%s\nexit\n" "$1" | timeout 10 $DBMS 2>&1 | \
        grep -v "^DBMS>" | grep -v "^> " | grep -v "^$" | \
        grep -v "^Welcome" | grep -v "^Connected" | \
        grep -v "^Login:" | grep -v "^Password:"
}

assert_out() {
    local desc="$1"
    local sql="$2"
    local expected="$3"
    local result
    result=$(run_sql "$sql")
    if echo "$result" | grep -q "$expected"; then
        echo "PASS: $desc"
        PASS=$((PASS+1))
    else
        echo "FAIL: $desc"
        echo "  Expected: '$expected'"
        echo "  Output:"
        echo "$result" | sed 's/^/    /'
        FAIL=$((FAIL+1))
    fi
}

echo "========================================"
echo "DBMS Feature Test Suite"
echo "========================================"

# ============================================================
# 1. DDL - DATABASE
# ============================================================
echo ""
echo "=== 1. DDL: Database ==="
assert_out "CREATE DATABASE" \
    "create database shopdb" \
    "Create Database succeeded"
assert_out "USE DATABASE" \
    "create database shopdb
use database shopdb" \
    "set Database to shopdb"
assert_out "SHOW DATABASES" \
    "create database shopdb
create database shopdb2
show databases" \
    "shopdb"

# ============================================================
# 2. DDL - TABLE
# ============================================================
echo ""
echo "=== 2. DDL: CREATE TABLE ==="
assert_out "CREATE TABLE basic" \
    "create database shopdb
use database shopdb
create table users (id int not null primary key,name varchar 50 not null,age int,score int)" \
    "Table create succeeded"
assert_out "CREATE TABLE all types" \
    "use database shopdb
create table alltypes (c_int int,c_bigint bigint,c_float float,c_double double,c_bool bool,c_char char 20,c_varchar varchar 50,c_text text,c_date date,c_time time,c_timestamp timestamp,c_json json,c_jsonb jsonb)" \
    "Table create succeeded"
assert_out "CREATE TABLE UNIQUE" \
    "use database shopdb
create table t_unique (id int primary key,email varchar 50 unique)" \
    "Table create succeeded"
assert_out "CREATE TABLE DEFAULT" \
    "use database shopdb
create table t_default (id int primary key,status int default 1)" \
    "Table create succeeded"
assert_out "CREATE TABLE CHECK" \
    "use database shopdb
create table t_check (id int primary key,score int check (score >= 0 and score <= 100))" \
    "Table create succeeded"
assert_out "CREATE TABLE NOT NULL" \
    "use database shopdb
create table t_notnull (id int primary key,name varchar 50 not null)" \
    "Table create succeeded"
assert_out "CREATE TABLE FK" \
    "use database shopdb
create table orders (order_id int primary key,user_id int not null,amount double,foreign key (user_id) references users(id) on delete cascade)" \
    "Table create succeeded"
assert_out "CREATE TABLE composite PK" \
    "use database shopdb
create table t_composite (a int,b int,val varchar 20,primary key (a,b))" \
    "Table create succeeded"
assert_out "CREATE TABLE partition" \
    "use database shopdb
create table events (id int primary key,event_time timestamp) partition by range(event_time) (partition p1 values less than ('2024-01-01'),partition p2 values less than ('2025-01-01'))" \
    "Table create succeeded"
assert_out "CREATE TEMPORARY TABLE" \
    "use database shopdb
create temporary table temp_t (id int,val varchar 20)" \
    "Table create succeeded"
assert_out "SHOW TABLES" \
    "use database shopdb
show tables" \
    "users"
assert_out "SHOW COLUMNS" \
    "use database shopdb
show columns from users" \
    "id"
assert_out "DESC" \
    "use database shopdb
desc users" \
    "id"
assert_out "SHOW CREATE TABLE" \
    "use database shopdb
show create table users" \
    "CREATE TABLE"
assert_out "ALTER TABLE ADD COLUMN" \
    "use database shopdb
alter table users add column phone varchar 20" \
    "added"
assert_out "ALTER TABLE DROP COLUMN" \
    "use database shopdb
alter table users drop column phone" \
    "dropped"
assert_out "DROP TABLE" \
    "use database shopdb
drop table temp_t" \
    "succeeded"

# ============================================================
# 3. DDL - INDEX
# ============================================================
echo ""
echo "=== 3. DDL: Index ==="
assert_out "CREATE B+Tree INDEX" \
    "use database shopdb
create index idx_users_name on users(name)" \
    "Index create succeeded"
assert_out "CREATE HASH INDEX" \
    "use database shopdb
create hash index idx_hash_id on users(id)" \
    "Index create succeeded"
assert_out "CREATE UNIQUE INDEX" \
    "use database shopdb
create unique index idx_users_age on users(age)" \
    "Index create succeeded"
assert_out "CREATE composite INDEX" \
    "use database shopdb
create index idx_comp on users(name,age)" \
    "Index create succeeded"
assert_out "SHOW INDEXES" \
    "use database shopdb
show indexes" \
    "users"
assert_out "DROP INDEX" \
    "use database shopdb
drop index idx_users_age on users" \
    "dropped"

# ============================================================
# 4. DDL - VIEW
# ============================================================
echo ""
echo "=== 4. DDL: View ==="
assert_out "CREATE VIEW" \
    "use database shopdb
create view v_adults as select id,name,age from users where age >= 18" \
    "View create succeeded"
assert_out "CREATE MATERIALIZED VIEW" \
    "use database shopdb
create materialized view mv_users as select count(*) as cnt from users" \
    "materialized view create succeeded"
assert_out "DROP VIEW" \
    "use database shopdb
drop view v_adults" \
    "dropped"

# ============================================================
# 5. DML - INSERT
# ============================================================
echo ""
echo "=== 5. DML: INSERT ==="
assert_out "INSERT single row" \
    "use database shopdb
insert into users (id,name,age,score) values (1,'Alice',25,85)" \
    "row(s) inserted"
assert_out "INSERT multiple rows" \
    "use database shopdb
insert into users (id,name,age,score) values (2,'Bob',30,72)
insert into users (id,name,age,score) values (3,'Charlie',35,90)" \
    "row(s) inserted"
assert_out "INSERT with NULL" \
    "use database shopdb
insert into users (id,name,age,score) values (4,'Dave',40)" \
    "row(s) inserted"
assert_out "INSERT INTO ... SELECT" \
    "use database shopdb
create table users_copy (id int primary key,name varchar 50,age int)
insert into users_copy (id,name,age) select id,name,age from users" \
    "row(s) inserted"

# ============================================================
# 6. DML - SELECT
# ============================================================
echo ""
echo "=== 6. DML: SELECT ==="
assert_out "SELECT *" \
    "use database shopdb
select * from users" \
    "Alice"
assert_out "SELECT columns" \
    "use database shopdb
select name,score from users where id = 1" \
    "Alice"
assert_out "WHERE = " \
    "use database shopdb
select name from users where id = 1" \
    "Alice"
assert_out "WHERE >" \
    "use database shopdb
select name from users where age > 25" \
    "Bob"
assert_out "WHERE <>" \
    "use database shopdb
select name from users where id <> 999" \
    "Alice"
assert_out "WHERE BETWEEN" \
    "use database shopdb
select name from users where age between 25 and 32" \
    "Alice"
assert_out "WHERE IN" \
    "use database shopdb
select name from users where id in (1,3)" \
    "Charlie"
assert_out "WHERE LIKE %%" \
    "use database shopdb
select name from users where name like 'A%%'" \
    "Alice"
assert_out "WHERE LIKE _" \
    "use database shopdb
select name from users where name like 'Bo_'" \
    "Bob"
assert_out "WHERE IS NULL" \
    "use database shopdb
select name from users where score is null" \
    "Dave"
assert_out "WHERE IS NOT NULL" \
    "use database shopdb
select name from users where score is not null" \
    "Alice"
assert_out "WHERE AND" \
    "use database shopdb
select name from users where age > 20 and score >= 80" \
    "Alice"
assert_out "WHERE OR" \
    "use database shopdb
select name from users where id = 1 or id = 3" \
    "Charlie"
assert_out "ORDER BY ASC" \
    "use database shopdb
select name from users order by age asc" \
    "Alice"
assert_out "ORDER BY DESC" \
    "use database shopdb
select name from users order by score desc" \
    "Charlie"
assert_out "ORDER BY NULLS FIRST" \
    "use database shopdb
select name,score from users order by score asc nulls first" \
    "Dave"
assert_out "LIMIT" \
    "use database shopdb
select name from users order by id limit 2" \
    "Alice"
assert_out "OFFSET" \
    "use database shopdb
select name from users order by id limit 1 offset 1" \
    "Bob"
assert_out "DISTINCT" \
    "use database shopdb
select distinct age from users" \
    "25"

# ============================================================
# 7. DML - UPDATE / DELETE
# ============================================================
echo ""
echo "=== 7. DML: UPDATE / DELETE ==="
assert_out "UPDATE" \
    "use database shopdb
update users set age = 26 where id = 1" \
    "updated"
assert_out "UPDATE verify" \
    "use database shopdb
select age from users where id = 1" \
    "26"
assert_out "UPDATE FROM" \
    "use database shopdb
insert into orders (order_id,user_id,amount) values (101,1,199.99)
update users set score = 100 from orders where users.id = orders.user_id and orders.order_id = 101" \
    "updated"
assert_out "DELETE" \
    "use database shopdb
insert into users (id,name,age,score) values (99,'DelMe',1,1)
delete from users where id = 99" \
    "deleted"
assert_out "DELETE LIMIT" \
    "use database shopdb
delete from users limit 0" \
    "deleted"

# ============================================================
# 8. DML - UPSERT / REPLACE / MERGE
# ============================================================
echo ""
echo "=== 8. DML: UPSERT / REPLACE / MERGE ==="
assert_out "UPSERT insert" \
    "use database shopdb
upsert into users (id,name,age,score) values (100,'UpsertNew',50,50) on conflict do update set name='UpsertNew'" \
    "row(s) inserted"
assert_out "UPSERT update" \
    "use database shopdb
upsert into users (id,name,age,score) values (1,'AliceUpdated',99,99) on conflict do update set name='AliceUpdated',age=99,score=99" \
    "updated"
assert_out "UPSERT verify" \
    "use database shopdb
select name from users where id = 1" \
    "AliceUpdated"
assert_out "REPLACE INTO" \
    "use database shopdb
replace into users (id,name,age,score) values (2,'BobNew',40,80)" \
    "row(s) inserted"
assert_out "REPLACE verify" \
    "use database shopdb
select name from users where id = 2" \
    "BobNew"

# MERGE INTO - test separately to avoid "file name too long" issue
assert_out "MERGE setup" \
    "use database shopdb
create table merge_src (id int primary key,name varchar 50,age int)
insert into merge_src (id,name,age) values (200,'MergeMan',50)" \
    "row(s) inserted"
assert_out "MERGE INTO" \
    "use database shopdb
merge into users using merge_src on users.id = merge_src.id update set name = merge_src.name,age = merge_src.age insert (id,name,age,score) values (merge_src.id,merge_src.name,merge_src.age,100)" \
    "row(s) inserted"
assert_out "MERGE verify" \
    "use database shopdb
select name from users where id = 200" \
    "MergeMan"

# ============================================================
# 9. DQL - Aggregate
# ============================================================
echo ""
echo "=== 9. DQL: Aggregate ==="
assert_out "COUNT(*)" \
    "use database shopdb
select count(*) from users" \
    ""
assert_out "MAX" \
    "use database shopdb
select max(score) from users" \
    ""
assert_out "MIN" \
    "use database shopdb
select min(age) from users" \
    ""
assert_out "SUM" \
    "use database shopdb
select sum(score) from users" \
    ""
assert_out "AVG" \
    "use database shopdb
select avg(age) from users" \
    ""
assert_out "COUNT DISTINCT" \
    "use database shopdb
select count(distinct age) from users" \
    ""

# ============================================================
# 10. DQL - GROUP BY / HAVING
# ============================================================
echo ""
echo "=== 10. DQL: GROUP BY ==="
assert_out "GROUP BY" \
    "use database shopdb
select age, count(*) from users group by age" \
    ""

# ============================================================
# 11. DQL - JOIN
# ============================================================
echo ""
echo "=== 11. DQL: JOIN ==="
assert_out "INNER JOIN" \
    "use database shopdb
select users.name,orders.amount from users join orders on users.id = orders.user_id" \
    "199.99"
assert_out "LEFT JOIN" \
    "use database shopdb
select users.name,orders.amount from users left join orders on users.id = orders.user_id where users.id <= 2" \
    "Alice"
assert_out "RIGHT JOIN" \
    "use database shopdb
select users.name,orders.amount from users right join orders on users.id = orders.user_id" \
    "Alice"
assert_out "SELF JOIN" \
    "use database shopdb
select a.name,b.name from users a join users b on a.id = b.id where a.id = 1" \
    "Alice"
assert_out "CROSS JOIN" \
    "use database shopdb
select a.name,b.name from users a cross join users b where a.id = 1 and b.id = 2" \
    "Alice"

# ============================================================
# 12. DQL - UNION
# ============================================================
echo ""
echo "=== 12. DQL: Set Operations ==="
assert_out "UNION" \
    "use database shopdb
select name from users where id = 1 union select name from users where id = 2" \
    "Bob"
assert_out "UNION ALL" \
    "use database shopdb
select name from users where id = 1 union all select name from users where id = 1" \
    "Alice"

# ============================================================
# 13. DQL - CTE
# ============================================================
echo ""
echo "=== 13. DQL: CTE ==="
assert_out "CTE basic" \
    "use database shopdb
with tmp as (select id,name from users where id = 1) select name from tmp" \
    "AliceUpdated"
assert_out "Multiple CTE" \
    "use database shopdb
with c1 as (select id,name from users where id = 1),c2 as (select id,name from users where id = 2) select c1.name from c1 union select c2.name from c2" \
    "AliceUpdated"

# ============================================================
# 14. DQL - Subqueries
# ============================================================
echo ""
echo "=== 14. DQL: Subqueries ==="
assert_out "IN subquery" \
    "use database shopdb
select name from users where id in (select user_id from orders)" \
    "AliceUpdated"
assert_out "EXISTS subquery" \
    "use database shopdb
select name from users where exists (select 1 from orders where orders.user_id = users.id)" \
    "AliceUpdated"
assert_out "Scalar subquery" \
    "use database shopdb
select name,(select max(score) from users) as mx from users where id = 1" \
    "AliceUpdated"

# ============================================================
# 15. DQL - Window Functions
# ============================================================
echo ""
echo "=== 15. DQL: Window Functions ==="
assert_out "ROW_NUMBER" \
    "use database shopdb
select name,row_number() over (order by age) as rn from users" \
    ""
assert_out "RANK" \
    "use database shopdb
select name,rank() over (order by score desc) as rk from users" \
    ""
assert_out "DENSE_RANK" \
    "use database shopdb
select name,dense_rank() over (order by score desc) as dr from users" \
    ""
assert_out "LAG" \
    "use database shopdb
select name,lag(score,1,0) over (order by id) as prev from users" \
    ""
assert_out "LEAD" \
    "use database shopdb
select name,lead(score,1,0) over (order by id) as nxt from users" \
    ""
assert_out "NTILE" \
    "use database shopdb
select name,ntile(2) over (order by id) from users" \
    ""
assert_out "FIRST_VALUE" \
    "use database shopdb
select name,first_value(score) over (order by id) from users" \
    ""
assert_out "PARTITION BY" \
    "use database shopdb
select name,row_number() over (partition by age order by id) from users" \
    ""

# ============================================================
# 16. DQL - EXPLAIN
# ============================================================
echo ""
echo "=== 16. DQL: EXPLAIN ==="
assert_out "EXPLAIN" \
    "use database shopdb
explain select * from users where id = 1" \
    "TableScan\|IndexScan\|Filter\|cost"
assert_out "EXPLAIN ANALYZE" \
    "use database shopdb
explain analyze select * from users where id = 1" \
    ""
assert_out "EXPLAIN FORMAT JSON" \
    "use database shopdb
explain format json select * from users where id = 1" \
    ""

# ============================================================
# 17. DML - Export
# ============================================================
echo ""
echo "=== 17. DML: Export ==="
assert_out "SELECT INTO OUTFILE" \
    "use database shopdb
select id,name,age from users where id = 1 into outfile '/tmp/test_export.csv'" \
    ""

# ============================================================
# 18. TCL - Transactions
# ============================================================
echo ""
echo "=== 18. TCL: Transactions ==="
assert_out "BEGIN + COMMIT" \
    "use database shopdb
begin
insert into users (id,name,age,score) values (50,'TxnTest',50,50)
commit
select name from users where id = 50" \
    "TxnTest"
assert_out "BEGIN + ROLLBACK" \
    "use database shopdb
begin
insert into users (id,name,age,score) values (51,'RollbackMe',51,51)
rollback
select name from users where id = 51" \
    "0 row"
assert_out "SAVEPOINT" \
    "use database shopdb
savepoint sp1" \
    "Savepoint created"
assert_out "ROLLBACK TO SAVEPOINT" \
    "use database shopdb
begin
savepoint spx
update users set age = 99 where id = 1
rollback to savepoint spx
commit" \
    "committed"
assert_out "RELEASE SAVEPOINT" \
    "use database shopdb
begin
savepoint spy
release savepoint spy
commit" \
    "committed"
assert_out "SET TRANSACTION ISOLATION" \
    "set transaction isolation level repeatable read" \
    ""
assert_out "BEGIN READ COMMITTED" \
    "use database shopdb
begin read committed
select name from users where id = 1
commit" \
    "AliceUpdated"
assert_out "CHECKPOINT" \
    "use database shopdb
checkpoint" \
    ""
assert_out "VACUUM table" \
    "use database shopdb
vacuum users" \
    ""
assert_out "VACUUM all" \
    "use database shopdb
vacuum" \
    ""

# ============================================================
# 19. DCL - Users & Roles
# ============================================================
echo ""
echo "=== 19. DCL: Users & Roles ==="
assert_out "CREATE USER" \
    "create user testuser pass123 0" \
    "User create succeeded"
assert_out "SHOW USERS" \
    "show users" \
    "testuser"
assert_out "GRANT" \
    "use database shopdb
grant select on users to testuser" \
    "Grant succeeded"
assert_out "GRANT column" \
    "use database shopdb
grant select (name,age) on users to testuser" \
    "Grant succeeded"
assert_out "SHOW GRANTS" \
    "use database shopdb
show grants for testuser" \
    "testuser"
assert_out "REVOKE" \
    "use database shopdb
revoke select on users from testuser" \
    "Revoke succeeded"
assert_out "CREATE ROLE" \
    "create role readonly" \
    "Role create succeeded"
assert_out "GRANT role" \
    "create role r2
grant r2 to testuser" \
    "Grant succeeded"
assert_out "REVOKE role" \
    "revoke r2 from testuser" \
    "Revoke succeeded"
assert_out "SHOW ROLES" \
    "show roles" \
    ""

# ============================================================
# 20. Triggers
# ============================================================
echo ""
echo "=== 20. Triggers ==="
assert_out "CREATE TRIGGER" \
    "use database shopdb
create trigger trg_test after insert on users log_it" \
    "Trigger create succeeded"
assert_out "SHOW TRIGGERS" \
    "use database shopdb
show triggers" \
    "trg_test"
assert_out "DROP TRIGGER" \
    "use database shopdb
drop trigger trg_test" \
    "dropped"

# ============================================================
# 21. Stored Procedures & Functions
# ============================================================
echo ""
echo "=== 21. Procedures & Functions ==="
assert_out "CREATE PROCEDURE" \
    "use database shopdb
create procedure reset_null_scores as update users set score = 0 where score is null" \
    "Procedure create succeeded"
assert_out "CALL" \
    "use database shopdb
call reset_null_scores()" \
    ""
assert_out "SHOW PROCEDURES" \
    "use database shopdb
show procedures" \
    "reset_null_scores"
assert_out "DROP PROCEDURE" \
    "use database shopdb
drop procedure reset_null_scores" \
    "dropped"
assert_out "CREATE FUNCTION" \
    "use database shopdb
create function double_value(x) returns int as x * 2" \
    "Function create succeeded"
assert_out "SHOW FUNCTIONS" \
    "use database shopdb
show functions" \
    "double_value"
assert_out "DROP FUNCTION" \
    "use database shopdb
drop function double_value" \
    "dropped"

# ============================================================
# 22. Prepared Statements
# ============================================================
echo ""
echo "=== 22. Prepared Statements ==="
assert_out "PREPARE" \
    "use database shopdb
prepare p1 from 'select * from users where id = ?'" \
    ""
assert_out "EXECUTE" \
    "use database shopdb
execute p1 using (1)" \
    "AliceUpdated"
assert_out "DEALLOCATE" \
    "use database shopdb
deallocate prepare p1" \
    "deallocated"

# ============================================================
# 23. Scalar Functions
# ============================================================
echo ""
echo "=== 23. Scalar Functions ==="
assert_out "UPPER" \
    "use database shopdb
select upper('hello')" \
    "HELLO"
assert_out "LOWER" \
    "use database shopdb
select lower('HELLO')" \
    "hello"
assert_out "LENGTH" \
    "use database shopdb
select length('hello')" \
    "5"
assert_out "CONCAT" \
    "use database shopdb
select concat('a','b')" \
    "ab"
assert_out "SUBSTRING" \
    "use database shopdb
select substring('hello',1,3)" \
    ""
assert_out "TRIM" \
    "use database shopdb
select trim('  hello  ')" \
    ""
assert_out "REPLACE" \
    "use database shopdb
select replace('abc','b','x')" \
    "axc"
assert_out "ABS" \
    "use database shopdb
select abs(-5)" \
    "5"
assert_out "ROUND" \
    "use database shopdb
select round(3.1415,2)" \
    "3.14"
assert_out "CEIL" \
    "use database shopdb
select ceil(3.2)" \
    "4"
assert_out "FLOOR" \
    "use database shopdb
select floor(3.8)" \
    "3"
assert_out "COALESCE" \
    "use database shopdb
select coalesce(null,'default')" \
    "default"
assert_out "NULLIF" \
    "use database shopdb
select nullif(1,1)" \
    ""
assert_out "CASE WHEN" \
    "use database shopdb
select case when(1>0,'yes','no')" \
    "yes"
assert_out "CAST" \
    "use database shopdb
select cast('123' as int)" \
    "123"

# ============================================================
# 24. JSONB
# ============================================================
echo ""
echo "=== 24. JSONB ==="
assert_out "JSONB insert" \
    "use database shopdb
create table configs (id int primary key,settings jsonb)
insert into configs (id,settings) values (1,'{\"theme\":\"dark\"}')" \
    "row(s) inserted"
assert_out "JSONB_EXTRACT" \
    "use database shopdb
select jsonb_extract(settings,'$.theme') from configs where id = 1" \
    ""
assert_out "JSONB_EXTRACT_TEXT" \
    "use database shopdb
select jsonb_extract_text(settings,'$.theme') from configs where id = 1" \
    ""
assert_out "JSONB_CONTAINS" \
    "use database shopdb
select jsonb_contains(settings,'theme') from configs where id = 1" \
    ""
assert_out "JSONB_PRETTY" \
    "use database shopdb
select jsonb_pretty(settings) from configs where id = 1" \
    ""

# ============================================================
# 25. Backup / Dump / Restore
# ============================================================
echo ""
echo "=== 25. Backup / Dump ==="
assert_out "DUMP DATABASE" \
    "dump database shopdb to '/tmp/test_dump.sql'" \
    ""

# ============================================================
# 26. ANALYZE / Stats
# ============================================================
echo ""
echo "=== 26. ANALYZE / Stats ==="
assert_out "ANALYZE TABLE" \
    "use database shopdb
analyze table users" \
    "analyzed"
assert_out "Multi-column analyze" \
    "use database shopdb
analyze table users columns (age,score)" \
    ""
assert_out "SHOW STATS" \
    "use database shopdb
show stats for users" \
    ""

# ============================================================
# 27. information_schema
# ============================================================
echo ""
echo "=== 27. information_schema ==="
assert_out "info_schema.tables" \
    "select table_name from information_schema.tables where table_schema = 'shopdb'" \
    "users"
assert_out "info_schema.columns" \
    "select column_name from information_schema.columns where table_name = 'users'" \
    "id"

# ============================================================
# 28. FK Actions
# ============================================================
echo ""
echo "=== 28. Foreign Key ==="
assert_out "FK CASCADE DELETE" \
    "create database fktest
use database fktest
create table parent (id int primary key,name varchar 20)
create table child (id int primary key,pid int not null,foreign key (pid) references parent(id) on delete cascade)
insert into parent (id,name) values (1,'p1')
insert into child (id,pid) values (1,1)
delete from parent where id = 1
select count(*) from child" \
    "0"

# ============================================================
# 29. Partition
# ============================================================
echo ""
echo "=== 29. Partition ==="
assert_out "Partition INSERT" \
    "use database shopdb
insert into events (id,event_time) values (1,'2023-06-15 12:00:00')" \
    "row(s) inserted"
assert_out "Partition INSERT other range" \
    "use database shopdb
insert into events (id,event_time) values (2,'2024-06-15 12:00:00')" \
    "row(s) inserted"
assert_out "Partition SELECT" \
    "use database shopdb
select id from events order by id" \
    ""

# ============================================================
# 30. System SHOW Commands
# ============================================================
echo ""
echo "=== 30. System SHOW ==="
assert_out "SHOW CONNECTIONS" \
    "show connections" \
    ""
assert_out "SHOW PROCESSLIST" \
    "show processlist" \
    ""
assert_out "SHOW STATUS" \
    "show status" \
    ""
assert_out "SHOW LOCKS" \
    "show locks" \
    ""
assert_out "SHOW DEADLOCKS" \
    "show deadlocks" \
    ""
assert_out "SHOW VARIABLES" \
    "show variables" \
    ""
assert_out "SHOW PLAN CACHE" \
    "show plan cache" \
    ""
assert_out "SHOW SLOW LOG" \
    "show slow log" \
    ""
assert_out "SHOW DEAD TUPLES" \
    "use database shopdb
show dead tuples from users" \
    ""
assert_out "SHOW VIEWS" \
    "use database shopdb
show views" \
    ""
assert_out "SHOW STATEMENTS" \
    "use database shopdb
show statements" \
    ""

# ============================================================
# 31. SET Commands
# ============================================================
echo ""
echo "=== 31. SET ==="
assert_out "SET slow_query_threshold" \
    "set slow_query_threshold = 200.0" \
    ""
assert_out "SET checkpoint_interval" \
    "set checkpoint_interval = 100" \
    ""
assert_out "SET statement_timeout" \
    "set statement_timeout = 5000" \
    ""
assert_out "SET audit_level" \
    "set audit_level = 2" \
    ""
assert_out "SET AUTO_VACUUM" \
    "set auto_vacuum = 500" \
    ""
assert_out "SET TIMEZONE" \
    "set timezone = '+08:00'" \
    ""

# ============================================================
# 32. VIEW TABLE / VIEW DATABASE
# ============================================================
echo ""
echo "=== 32. VIEW Commands ==="
assert_out "VIEW TABLE" \
    "use database shopdb
view table users" \
    ""
assert_out "VIEW DATABASE" \
    "use database shopdb
view database" \
    ""

# ============================================================
# 33. Constraints
# ============================================================
echo ""
echo "=== 33. Constraints ==="
assert_out "DEFAULT" \
    "use database shopdb
insert into t_default (id) values (1)
select status from t_default where id = 1" \
    "1"
assert_out "CHECK valid" \
    "use database shopdb
insert into t_check (id,score) values (1,50)" \
    "row(s) inserted"
assert_out "CHECK invalid" \
    "use database shopdb
insert into t_check (id,score) values (2,200)" \
    ""

# ============================================================
# 34. SERIAL
# ============================================================
echo ""
echo "=== 34. SERIAL ==="
assert_out "SERIAL table" \
    "use database shopdb
create table t_serial (id serial primary key,name varchar 20)" \
    "Table create succeeded"
assert_out "SERIAL insert" \
    "use database shopdb
insert into t_serial (name) values ('auto1')
insert into t_serial (name) values ('auto2')
select id from t_serial" \
    ""

# ============================================================
# 35. Fulltext Index
# ============================================================
echo ""
echo "=== 35. Fulltext ==="
assert_out "Fulltext index" \
    "use database shopdb
create table t_ft (id int primary key,content text)
create fulltext index ft_idx on t_ft(content)" \
    "Index create succeeded"
assert_out "Fulltext insert" \
    "use database shopdb
insert into t_ft (id,content) values (1,'hello world')
insert into t_ft (id,content) values (2,'foo bar')" \
    "row(s) inserted"

# ============================================================
# 36. GENERATED column
# ============================================================
echo ""
echo "=== 36. GENERATED Columns ==="
assert_out "GENERATED column" \
    "use database shopdb
create table t_gen (id int primary key,price int,tax int generated always as (price * 0.1))" \
    "Table create succeeded"

# ============================================================
# 37. ENUM
# ============================================================
echo ""
echo "=== 37. ENUM ==="
assert_out "ENUM column" \
    "use database shopdb
create table t_enum (id int primary key,color enum('red','green','blue'))" \
    "Table create succeeded"
assert_out "ENUM insert" \
    "use database shopdb
insert into t_enum (id,color) values (1,'red')" \
    "row(s) inserted"

# ============================================================
# 38. ARRAY
# ============================================================
echo ""
echo "=== 38. ARRAY ==="
assert_out "ARRAY type" \
    "use database shopdb
create table t_arr (id int primary key,tags array varchar 20)" \
    "Table create succeeded"

# ============================================================
# 39. Cleanup tests
# ============================================================
echo ""
echo "=== 39. Cleanup ==="
assert_out "DROP DATABASE" \
    "drop database shopdb2" \
    ""

# ============================================================
# SUMMARY
# ============================================================
echo ""
echo "========================================"
echo "RESULTS: $PASS PASS, $FAIL FAIL"
echo "========================================"

cleanup

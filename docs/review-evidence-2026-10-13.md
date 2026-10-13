# 2026-10-13 审查的 SQL 与协议原始观测

对应[审查报告](code-review-2026-10-13.md)及其基准提交/二进制摘要。
以下输出原样保留 SQL、结果行、SQLSTATE 和 OID；本机路径只记录运行来源，不是重现所必需的固定路径。
SQL 参考实例已验证为 PostgreSQL 18.6；双方使用独立测试数据，参考侧事务最终回滚。
前两组 SQL 共 29 条语句，其中 14 条查询不同；后续DML和EXECUTE对照追加后共80条语句、47条不同观测（含前面错误写入的后续影响，不能当作47个独立问题）。观测脚本正常退出只表示收集完成，不表示结果通过。
二进制浮点测试要求 SELECT $1 的输入和输出字节、OID、格式码相同，6 项中 4 项失败。

## 窗口和数值 SQL

```text
CASE 0 SQL CREATE TABLE w (id INTEGER, v INTEGER)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 1 SQL INSERT INTO w VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)
PG18 ([], None, '', [], 'INSERT 0 6', [])
ROOT ([], None, '', [], 'INSERT 0 6', [])
MATCH True
CASE 2 SQL SELECT id,ntile(4) OVER (ORDER BY id) FROM w ORDER BY id
PG18 ([['1', '1'], ['2', '1'], ['3', '2'], ['4', '2'], ['5', '3'], ['6', '4']], None, '', ['id', 'ntile'], 'SELECT 6', [23, 23])
ROOT ([['1', '1'], ['2', '1'], ['3', '2'], ['4', '3'], ['5', '3'], ['6', '4']], None, '', ['id', 'ntile'], 'SELECT 6', [23, 23])
MATCH False
CASE 3 SQL SELECT id,ntile(8) OVER (ORDER BY id) FROM w ORDER BY id
PG18 ([['1', '1'], ['2', '2'], ['3', '3'], ['4', '4'], ['5', '5'], ['6', '6']], None, '', ['id', 'ntile'], 'SELECT 6', [23, 23])
ROOT ([['1', '1'], ['2', '2'], ['3', '3'], ['4', '5'], ['5', '6'], ['6', '7']], None, '', ['id', 'ntile'], 'SELECT 6', [23, 23])
MATCH False
CASE 4 SQL SELECT id,lag(v,0) OVER (ORDER BY id),lead(v,0) OVER (ORDER BY id) FROM w ORDER BY id
PG18 ([['1', '10', '10'], ['2', '20', '20'], ['3', '30', '30'], ['4', '40', '40'], ['5', '50', '50'], ['6', '60', '60']], None, '', ['id', 'lag', 'lead'], 'SELECT 6', [23, 23, 23])
ROOT ([['1', None, None], ['2', None, None], ['3', None, None], ['4', None, None], ['5', None, None], ['6', None, None]], None, '', ['id', 'lag', 'lead'], 'SELECT 6', [23, 23, 23])
MATCH False
CASE 5 SQL SELECT id,lag(v,-1) OVER (ORDER BY id),lead(v,-1) OVER (ORDER BY id) FROM w ORDER BY id
PG18 ([['1', '20', None], ['2', '30', '10'], ['3', '40', '20'], ['4', '50', '30'], ['5', '60', '40'], ['6', None, '50']], None, '', ['id', 'lag', 'lead'], 'SELECT 6', [23, 23, 23])
ROOT ([['1', None, None], ['2', None, '10'], ['3', None, '20'], ['4', None, '30'], ['5', None, '40'], ['6', None, '50']], None, '', ['id', 'lag', 'lead'], 'SELECT 6', [23, 23, 23])
MATCH False
CASE 6 SQL SELECT id,ntile(0) OVER (ORDER BY id) FROM w ORDER BY id
PG18 ([], '22014', 'argument of ntile must be greater than zero', ['id', 'ntile'], None, [23, 23])
ROOT ([['1', '1'], ['2', '1'], ['3', '1'], ['4', '1'], ['5', '1'], ['6', '1']], None, '', ['id', 'ntile'], 'SELECT 6', [23, 23])
MATCH False
CASE 7 SQL SELECT id,ntile(-1) OVER (ORDER BY id) FROM w ORDER BY id
PG18 ([], '22014', 'argument of ntile must be greater than zero', ['id', 'ntile'], None, [23, 23])
ROOT ([['1', '1'], ['2', '1'], ['3', '1'], ['4', '1'], ['5', '1'], ['6', '1']], None, '', ['id', 'ntile'], 'SELECT 6', [23, 23])
MATCH False
CASE 8 SQL SELECT sqrt(10000000000000000000000000000000000000000::numeric)
PG18 ([['100000000000000000000']], None, '', ['sqrt'], 'SELECT 1', [1700])
ROOT ([['-9223372036854775808']], None, '', ['sqrt'], 'SELECT 1', [1700])
MATCH False
CASE 9 SQL SELECT sqrt(9999999999999999999999999999999999999999::numeric)
PG18 ([['100000000000000000000']], None, '', ['sqrt'], 'SELECT 1', [1700])
ROOT ([['-9223372036854775808']], None, '', ['sqrt'], 'SELECT 1', [1700])
MATCH False
CASE 10 SQL SELECT abs(-2147483648::integer)
PG18 ([], '22003', 'integer out of range', [], None, [])
ROOT ([], '22003', 'integer out of range', [], None, [])
MATCH True
CASE 11 SQL SELECT abs(-32768::smallint)
PG18 ([], '22003', 'smallint out of range', [], None, [])
ROOT ([], '22003', 'smallint out of range', [], None, [])
MATCH True
```

## 窗口类型、分区与 frame SQL

```text
CASE 0 SQL CREATE TABLE w (id INTEGER, v INTEGER)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 1 SQL INSERT INTO w VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)
PG18 ([], None, '', [], 'INSERT 0 6', [])
ROOT ([], None, '', [], 'INSERT 0 6', [])
MATCH True
CASE 2 SQL SELECT id,sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM w ORDER BY id
PG18 ([['1', None], ['2', '10'], ['3', '20'], ['4', '30'], ['5', '40'], ['6', '50']], None, '', ['id', 'sum'], 'SELECT 6', [23, 20])
ROOT ([['1', '10'], ['2', '30'], ['3', '50'], ['4', '70'], ['5', '90'], ['6', '110']], None, '', ['id', 'sum'], 'SELECT 6', [23, 20])
MATCH False
CASE 3 SQL SELECT id,sum(v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM w ORDER BY id
PG18 ([['1', '50'], ['2', '70'], ['3', '90'], ['4', '110'], ['5', '60'], ['6', None]], None, '', ['id', 'sum'], 'SELECT 6', [23, 20])
ROOT ([['1', '30'], ['2', '60'], ['3', '100'], ['4', '150'], ['5', '210'], ['6', '210']], None, '', ['id', 'sum'], 'SELECT 6', [23, 20])
MATCH False
CASE 4 SQL SELECT id,sum(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM w ORDER BY id
PG18 ([], '42P20', 'frame starting from current row cannot have preceding rows', [], None, [])
ROOT ([['1', '10'], ['2', '20'], ['3', '30'], ['4', '40'], ['5', '50'], ['6', '60']], None, '', ['id', 'sum'], 'SELECT 6', [23, 20])
MATCH False
CASE 5 SQL CREATE TABLE wt (id INTEGER,k TEXT)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 6 SQL INSERT INTO wt VALUES (1,'2'),(2,'10'),(3,'1'),(4,'01')
PG18 ([], None, '', [], 'INSERT 0 4', [])
ROOT ([], None, '', [], 'INSERT 0 4', [])
MATCH True
CASE 7 SQL SELECT id,k,row_number() OVER (ORDER BY k),rank() OVER (ORDER BY k),min(k) OVER () FROM wt ORDER BY id
PG18 ([['1', '2', '4', '4', '01'], ['2', '10', '3', '3', '01'], ['3', '1', '2', '2', '01'], ['4', '01', '1', '1', '01']], None, '', ['id', 'k', 'row_number', 'rank', 'min'], 'SELECT 4', [23, 25, 20, 20, 25])
ROOT ([['1', '2', '3', '3', '1'], ['2', '10', '4', '4', '1'], ['3', '1', '1', '1', '1'], ['4', '01', '2', '1', '1']], None, '', ['id', 'k', 'row_number', 'rank', 'min'], 'SELECT 4', [23, 25, 20, 20, 25])
MATCH False
CASE 8 SQL CREATE TABLE wb (id INTEGER,k BIGINT)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 9 SQL INSERT INTO wb VALUES (1,9007199254740993),(2,9007199254740992),(3,9007199254740994)
PG18 ([], None, '', [], 'INSERT 0 3', [])
ROOT ([], None, '', [], 'INSERT 0 3', [])
MATCH True
CASE 10 SQL SELECT id,k,row_number() OVER (ORDER BY k),rank() OVER (ORDER BY k),min(k) OVER () FROM wb ORDER BY id
PG18 ([['1', '9007199254740993', '2', '2', '9007199254740992'], ['2', '9007199254740992', '1', '1', '9007199254740992'], ['3', '9007199254740994', '3', '3', '9007199254740992']], None, '', ['id', 'k', 'row_number', 'rank', 'min'], 'SELECT 3', [23, 20, 20, 20, 20])
ROOT ([['1', '9007199254740993', '1', '1', '9007199254740993'], ['2', '9007199254740992', '2', '1', '9007199254740993'], ['3', '9007199254740994', '3', '3', '9007199254740993']], None, '', ['id', 'k', 'row_number', 'rank', 'min'], 'SELECT 3', [23, 20, 20, 20, 20])
MATCH False
CASE 11 SQL CREATE TABLE wn (id INTEGER,k NUMERIC)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 12 SQL INSERT INTO wn VALUES (1,1.0),(2,1.00),(3,1),(4,2)
PG18 ([], None, '', [], 'INSERT 0 4', [])
ROOT ([], None, '', [], 'INSERT 0 4', [])
MATCH True
CASE 13 SQL SELECT id,k,count(*) OVER (PARTITION BY k) FROM wn ORDER BY id
PG18 ([['1', '1.0', '3'], ['2', '1.00', '3'], ['3', '1', '3'], ['4', '2', '1']], None, '', ['id', 'k', 'count'], 'SELECT 4', [23, 1700, 20])
ROOT ([['1', '1.0', '1'], ['2', '1.00', '1'], ['3', '1', '1'], ['4', '2', '1']], None, '', ['id', 'k', 'count'], 'SELECT 4', [23, 1700, 20])
MATCH False
CASE 14 SQL CREATE TABLE ws (id INTEGER,v BIGINT)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 15 SQL INSERT INTO ws VALUES (1,9223372036854775807),(2,9223372036854775807)
PG18 ([], None, '', [], 'INSERT 0 2', [])
ROOT ([], None, '', [], 'INSERT 0 2', [])
MATCH True
CASE 16 SQL SELECT id,sum(v) OVER (ORDER BY id),avg(v) OVER (ORDER BY id) FROM ws ORDER BY id
PG18 ([['1', '9223372036854775807', '9223372036854775807'], ['2', '18446744073709551614', '9223372036854775807']], None, '', ['id', 'sum', 'avg'], 'SELECT 2', [23, 1700, 1700])
ROOT ([['1', '9223372036854775807', '9223372036854775807'], ['2', '18446744073709551614', '9223372036854775807']], None, '', ['id', 'sum', 'avg'], 'SELECT 2', [23, 1700, 1700])
MATCH True
```

## 正常 binary float 参数回传

```text
FROZEN_BINARY=/tmp/dbms-root-sql-value.x5X8O5vm/dbms_main.root-quoted-session.frozen
ISOLATED_DATA=/tmp/dbms-pgdiff-_3nat0fa
BINARY_FLOAT oid=701 input=1.25 raw=3ff4000000000000 output=1.25 output_raw=3ff4000000000000 exact=True errors=[]
BINARY_FLOAT oid=701 input=0.1234567890123456 raw=3fbf9add3746f659 output=0.123457 output_raw=3fbf9ae0c1765775 exact=False errors=[]
BINARY_FLOAT oid=701 input=1e-12 raw=3d719799812dea11 output=0.0 output_raw=0000000000000000 exact=False errors=[]
BINARY_FLOAT oid=700 input=1.25 raw=3fa00000 output=1.25 output_raw=3fa00000 exact=True errors=[]
BINARY_FLOAT oid=700 input=1.2345678806304932 raw=3f9e0652 output=1.2345679998397827 output_raw=3f9e0653 exact=False errors=[]
BINARY_FLOAT oid=700 input=9.999999960041972e-13 raw=2b8cbccc output=0.0 output_raw=00000000 exact=False errors=[]
NORMAL_PROTOCOL_CONTROLS=6 exact_failures=4
```

## DML、HAVING 与排序别名观测

```text
CASE 0 SQL CREATE TABLE d (id INTEGER PRIMARY KEY, v INTEGER)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 1 SQL INSERT INTO d VALUES (1,10),(2,20),(3,30)
PG18 ([], None, '', [], 'INSERT 0 3', [])
ROOT ([], None, '', [], 'INSERT 0 3', [])
MATCH True
CASE 2 SQL SELECT count(*) FROM d HAVING 1
PG18 ([], '42804', 'argument of HAVING must be type boolean, not type integer', [], None, [])
ROOT ([], None, '', ['count'], 'SELECT 0', [20])
MATCH False
CASE 3 SQL SELECT count(*) FROM d HAVING 'notboolean'
PG18 ([], '22P02', 'invalid input syntax for type boolean: "notboolean"', [], None, [])
ROOT ([], None, '', ['count'], 'SELECT 0', [20])
MATCH False
CASE 4 SQL SELECT id AS x,v AS x FROM d ORDER BY x
PG18 ([], '42702', 'ORDER BY "x" is ambiguous', [], None, [])
ROOT ([['1', '10'], ['2', '20'], ['3', '30']], None, '', ['x', 'x'], 'SELECT 3', [23, 23])
MATCH False
CASE 5 SQL SELECT 1 AS x,2 AS x ORDER BY x
PG18 ([], '42702', 'ORDER BY "x" is ambiguous', [], None, [])
ROOT ([['1', '2']], None, '', ['x', '?column?'], 'SELECT 1', [23, 23])
MATCH False
CASE 6 SQL UPDATE d SET v=(SELECT v FROM d) RETURNING id,v
PG18 ([], '21000', 'more than one row returned by a subquery used as an expression', [], None, [])
ROOT ([], '21000', 'more than one row returned by a subquery used as an expression', [], None, [])
MATCH True
CASE 7 SQL SELECT id,v FROM d ORDER BY id
PG18 ([['1', '10'], ['2', '20'], ['3', '30']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '10'], ['2', '20'], ['3', '30']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
MATCH True
CASE 8 SQL UPDATE d SET v=v+1 RETURNING id,v,(SELECT count(*) FROM d)
PG18 ([['1', '11', '3'], ['2', '21', '3'], ['3', '31', '3']], None, '', ['id', 'v', 'count'], 'UPDATE 3', [23, 23, 20])
ROOT ([['1', '11', '(SELECT count ( * ) FROM d)'], ['2', '21', '(SELECT count ( * ) FROM d)'], ['3', '31', '(SELECT count ( * ) FROM d)']], None, '', ['id', 'v', '(SELECT count ( * ) FROM d)'], 'UPDATE 3', [23, 23, 1043])
MATCH False
CASE 9 SQL SELECT id,v FROM d ORDER BY id
PG18 ([['1', '11'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '11'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
MATCH True
CASE 10 SQL INSERT INTO d VALUES (4,40) RETURNING id,(SELECT count(*) FROM d)
PG18 ([['4', '3']], None, '', ['id', 'count'], 'INSERT 0 1', [23, 20])
ROOT ([['4', '(SELECT count ( * ) FROM d)']], None, '', ['id', '(SELECT count ( * ) FROM d)'], 'INSERT 0 1', [23, 1043])
MATCH False
CASE 11 SQL DELETE FROM d WHERE id=4 RETURNING id,(SELECT count(*) FROM d)
PG18 ([['4', '4']], None, '', ['id', 'count'], 'DELETE 1', [23, 20])
ROOT ([['4', '4']], None, '', ['id', 'count'], 'DELETE 1', [23, 20])
MATCH True
CASE 12 SQL INSERT INTO d VALUES(1,100) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 1 RETURNING id,v
PG18 ([], '42804', 'argument of WHERE must be type boolean, not type integer', [], None, [])
ROOT ([['1', '100']], None, '', ['id', 'v'], 'INSERT 0 1', [23, 23])
MATCH False
CASE 13 SQL SELECT id,v FROM d ORDER BY id
PG18 ([['1', '11'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '100'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
MATCH False
CASE 14 SQL INSERT INTO d VALUES(1,100) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 'notboolean' RETURNING id,v
PG18 ([], '22P02', 'invalid input syntax for type boolean: "notboolean"', [], None, [])
ROOT ([['1', '100']], None, '', ['id', 'v'], 'INSERT 0 1', [23, 23])
MATCH False
CASE 15 SQL SELECT id,v FROM d ORDER BY id
PG18 ([['1', '11'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '100'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
MATCH False
CASE 16 SQL DELETE d WHERE id=3 RETURNING id
PG18 ([], '42601', 'syntax error at or near "d"', [], None, [])
ROOT ([['3']], None, '', ['id'], 'DELETE 1', [23])
MATCH False
CASE 17 SQL SELECT id,v FROM d ORDER BY id
PG18 ([['1', '11'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '100'], ['2', '21']], None, '', ['id', 'v'], 'SELECT 2', [23, 23])
MATCH False
CASE 18 SQL SELECT (SELECT v FROM d WHERE id=0)
PG18 ([[None]], None, '', ['v'], 'SELECT 1', [23])
ROOT ([[None]], None, '', ['v'], 'SELECT 1', [23])
MATCH True
CASE 19 SQL SELECT id,(SELECT v FROM d AS i WHERE i.id=d.id) FROM d ORDER BY id
PG18 ([['1', '11'], ['2', '21'], ['3', '31']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '100'], ['2', '21']], None, '', ['id', 'v'], 'SELECT 2', [23, 23])
MATCH False
```

## 严格模式的写入原子性与语法观测

```text
COMPATIBILITY_MODE postgresql18
CASE 0 SQL CREATE TABLE e (id INTEGER PRIMARY KEY, v INTEGER)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 1 SQL INSERT INTO e VALUES (1,10),(2,20),(3,30)
PG18 ([], None, '', [], 'INSERT 0 3', [])
ROOT ([], None, '', [], 'INSERT 0 3', [])
MATCH True
CASE 2 SQL INSERT INTO e VALUES (4,40) RETURNING id,(SELECT 1/0)
PG18 ([], '22012', 'division by zero', [], None, [])
ROOT ([['4', '(SELECT 1 / 0)']], None, '', ['id', '(SELECT 1 / 0)'], 'INSERT 0 1', [23, 1043])
MATCH False
CASE 3 SQL SELECT id,v FROM e ORDER BY id
PG18 ([['1', '10'], ['2', '20'], ['3', '30']], None, '', ['id', 'v'], 'SELECT 3', [23, 23])
ROOT ([['1', '10'], ['2', '20'], ['3', '30'], ['4', '40']], None, '', ['id', 'v'], 'SELECT 4', [23, 23])
MATCH False
CASE 4 SQL UPDATE e SET v=99 WHERE id=1 RETURNING id,(SELECT 1/0)
PG18 ([], '22012', 'division by zero', [], None, [])
ROOT ([['1', '(SELECT 1 / 0)']], None, '', ['id', '(SELECT 1 / 0)'], 'UPDATE 1', [23, 1043])
MATCH False
CASE 5 SQL SELECT id,v FROM e WHERE id=1
PG18 ([['1', '10']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
ROOT ([['1', '99']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
MATCH False
CASE 6 SQL INSERT INTO e VALUES (5,50) RETURNING (SELECT v FROM e)
PG18 ([], '21000', 'more than one row returned by a subquery used as an expression', [], None, [])
ROOT ([['(SELECT v FROM e)']], None, '', ['(SELECT v FROM e)'], 'INSERT 0 1', [1043])
MATCH False
CASE 7 SQL SELECT id,v FROM e WHERE id=5
PG18 ([], None, '', ['id', 'v'], 'SELECT 0', [23, 23])
ROOT ([['5', '50']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
MATCH False
CASE 8 SQL INSERT INTO e VALUES (2,200) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 'f' RETURNING id,v
PG18 ([], None, '', ['id', 'v'], 'INSERT 0 0', [23, 23])
ROOT ([['2', '200']], None, '', ['id', 'v'], 'INSERT 0 1', [23, 23])
MATCH False
CASE 9 SQL SELECT id,v FROM e WHERE id=2
PG18 ([['2', '20']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
ROOT ([['2', '200']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
MATCH False
CASE 10 SQL INSERT INTO e VALUES (6,600) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 'notboolean' RETURNING id,v
PG18 ([], '22P02', 'invalid input syntax for type boolean: "notboolean"', [], None, [])
ROOT ([['6', '600']], None, '', ['id', 'v'], 'INSERT 0 1', [23, 23])
MATCH False
CASE 11 SQL SELECT id,v FROM e WHERE id=6
PG18 ([], None, '', ['id', 'v'], 'SELECT 0', [23, 23])
ROOT ([['6', '600']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
MATCH False
CASE 12 SQL DELETE e WHERE id=3 RETURNING id
PG18 ([], '42601', 'syntax error at or near "e"', [], None, [])
ROOT ([['3']], None, '', ['id'], 'DELETE 1', [23])
MATCH False
CASE 13 SQL SELECT id,v FROM e WHERE id=3
PG18 ([['3', '30']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
ROOT ([], None, '', ['id', 'v'], 'SELECT 0', [23, 23])
MATCH False
CASE 14 SQL INSERT e VALUES (7,700) RETURNING id
PG18 ([], '42601', 'syntax error at or near "e"', [], None, [])
ROOT ([['7']], None, '', ['id'], 'INSERT 0 1', [23])
MATCH False
CASE 15 SQL SELECT id,v FROM e WHERE id=7
PG18 ([], None, '', ['id', 'v'], 'SELECT 0', [23, 23])
ROOT ([['7', '700']], None, '', ['id', 'v'], 'SELECT 1', [23, 23])
MATCH False
CASE 16 SQL PREPARE audit_badwhere AS INSERT INTO e VALUES(8,800) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 1 RETURNING id
PG18 ([], '42804', 'argument of WHERE must be type boolean, not type integer', [], None, [])
ROOT ([], None, '', [], 'PREPARE', [])
MATCH False
```

## SQL EXECUTE 的参数求值次数

```text
COMPATIBILITY_MODE postgresql18
CASE 0 SQL CREATE SEQUENCE s
PG18 ([], None, '', [], 'CREATE SEQUENCE', [])
ROOT ([], None, '', [], 'CREATE SEQUENCE', [])
MATCH True
CASE 1 SQL PREPARE p(bigint) AS SELECT $1,$1
PG18 ([], None, '', [], 'PREPARE', [])
ROOT ([], None, '', [], 'PREPARE', [])
MATCH True
CASE 2 SQL EXECUTE p(nextval('s'))
PG18 ([['1', '1']], None, '', ['?column?', '?column?'], 'SELECT 1', [20, 20])
ROOT ([['1', '2']], None, '', ['nextval', 'nextval'], 'SELECT 1', [20, 20])
MATCH False
CASE 3 SQL SELECT currval('s')
PG18 ([['1']], None, '', ['currval'], 'SELECT 1', [20])
ROOT ([['2']], None, '', ['currval'], 'SELECT 1', [20])
MATCH False
CASE 4 SQL CREATE SEQUENCE s_unused
PG18 ([], None, '', [], 'CREATE SEQUENCE', [])
ROOT ([], None, '', [], 'CREATE SEQUENCE', [])
MATCH True
CASE 5 SQL PREPARE p_unused(bigint) AS SELECT 7
PG18 ([], None, '', [], 'PREPARE', [])
ROOT ([], None, '', [], 'PREPARE', [])
MATCH True
CASE 6 SQL EXECUTE p_unused(nextval('s_unused'))
PG18 ([['7']], None, '', ['?column?'], 'SELECT 1', [23])
ROOT ([['7']], None, '', ['?column?'], 'SELECT 1', [23])
MATCH True
CASE 7 SQL SELECT currval('s_unused')
PG18 ([['1']], None, '', ['currval'], 'SELECT 1', [20])
ROOT ([], '55000', 'currval of sequence "s_unused" is not yet defined in this session (SQLSTATE 55000)', [], None, [])
MATCH False
CASE 8 SQL CREATE SEQUENCE s_rows
PG18 ([], None, '', [], 'CREATE SEQUENCE', [])
ROOT ([], None, '', [], 'CREATE SEQUENCE', [])
MATCH True
CASE 9 SQL CREATE TABLE input_rows(id INTEGER)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 10 SQL INSERT INTO input_rows VALUES(1),(2)
PG18 ([], None, '', [], 'INSERT 0 2', [])
ROOT ([], None, '', [], 'INSERT 0 2', [])
MATCH True
CASE 11 SQL PREPARE p_rows(bigint) AS SELECT id,$1 FROM input_rows ORDER BY id
PG18 ([], None, '', [], 'PREPARE', [])
ROOT ([], None, '', [], 'PREPARE', [])
MATCH True
CASE 12 SQL EXECUTE p_rows(nextval('s_rows'))
PG18 ([['1', '1'], ['2', '1']], None, '', ['id', '?column?'], 'SELECT 2', [23, 20])
ROOT ([['1', '1'], ['2', '2']], None, '', ['id', 'int8'], 'SELECT 2', [23, 20])
MATCH False
CASE 13 SQL SELECT currval('s_rows')
PG18 ([['1']], None, '', ['currval'], 'SELECT 1', [20])
ROOT ([['2']], None, '', ['currval'], 'SELECT 1', [20])
MATCH False
```


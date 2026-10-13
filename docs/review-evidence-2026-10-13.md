# 2026-10-13 审查的 SQL 与协议原始观测

对应[审查报告](code-review-2026-10-13.md)及其基准提交/二进制摘要。
以下输出原样保留 SQL、结果行、SQLSTATE 和 OID；本机路径只记录运行来源，不是重现所必需的固定路径。
SQL 参考实例已验证为 PostgreSQL 18.6；双方使用独立测试数据，参考侧事务最终回滚。
复现时，参考侧使用独立随机 schema 和外层事务，每条被测语句前建立 SAVEPOINT，失败后回滚至该 SAVEPOINT，成功则释放；最后整体回滚。Root 使用独立目录、随机端口和普通自动提交会话。不能把含预期错误的日志直接连续粘贴进一个没有 SAVEPOINT 的 PostgreSQL 事务，否则后续会话状态不同。
前两组 SQL 共 29 条语句，其中 14 条查询不同；前六组（含后续DML、EXECUTE和递归CTE）共89条语句、49条不同观测（含前面错误写入的后续影响，不能当作49个独立问题）。末尾Extended Parse/Describe定位另含27个协议动作、两个失败描述。观测脚本正常退出只表示收集完成，不表示结果通过。MATCH仅比较rows、SQLSTATE和OID，不比较诊断文本、列名或command tag；原输出保留这些字段便于另行检查。
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

## 递归 CTE 的类型及作用域控制

```text
COMPATIBILITY_MODE postgresql18
CASE 0 SQL WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT (n+1)::bigint FROM q WHERE n<2) SELECT n FROM q
PG18 ([], '42804', 'recursive query "q" column 1 has type integer in non-recursive term but type bigint overall', [], None, [])
ROOT ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [23])
MATCH False
CASE 1 SQL WITH RECURSIVE q(n) AS (SELECT 1::bigint UNION ALL SELECT (n+1)::bigint FROM q WHERE n<2) SELECT n FROM q
PG18 ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [20])
ROOT ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [20])
MATCH True
CASE 2 SQL WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT 2::bigint) SELECT n FROM q
PG18 ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [20])
ROOT ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [20])
MATCH True
CASE 3 SQL WITH q(n) AS (SELECT 1 UNION ALL SELECT 2::bigint) SELECT n FROM q
PG18 ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [20])
ROOT ([['1'], ['2']], None, '', ['n'], 'SELECT 2', [20])
MATCH True
CASE 4 SQL WITH RECURSIVE q(n) AS (SELECT 'a'::varchar(1) UNION ALL SELECT (n||'b')::varchar(2) FROM q WHERE length(n)<2) SELECT n FROM q
PG18 ([], '42804', 'recursive query "q" column 1 has type character varying(1) in non-recursive term but type character varying overall', [], None, [])
ROOT ([['a'], ['ab']], None, '', ['n'], 'SELECT 2', [1043])
MATCH False
CASE 5 SQL SELECT (SELECT x.v FROM (SELECT 2 AS z) x) FROM (SELECT 1 AS v) x
PG18 ([], '42703', 'column x.v does not exist', [], None, [])
ROOT ([], '42703', 'column "x.v" does not exist', [], None, [])
MATCH True
CASE 6 SQL CREATE TABLE input_rows(id integer)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 7 SQL INSERT INTO input_rows VALUES (1),(2)
PG18 ([], None, '', [], 'INSERT 0 2', [])
ROOT ([], None, '', [], 'INSERT 0 2', [])
MATCH True
CASE 8 SQL SELECT id,(WITH t AS (SELECT id) SELECT * FROM t) FROM input_rows ORDER BY id
PG18 ([], '42702', 'ORDER BY "id" is ambiguous', [], None, [])
ROOT ([], '42702', 'ORDER BY name is ambiguous', [], None, [])
MATCH True
```

## PgPage 正常API数据完整性控制

对应R27。原实现独立O2编译通过；驱动实际exit1，3项中2项失败，重复执行同样失败。
从仓库根目录将下列驱动保存到独立临时目录后编译，不需要修改生产代码：

```sh
g++ -std=c++17 -O2 -Wall -Wextra -Isrc /path/to/compact_normal_api_probe.cpp src/storage/PgPage.cpp -o /path/to/compact_normal_api_probe
/path/to/compact_normal_api_probe
```

正常初始化、插入、删除和压缩构造了测试状态，没有直接改写页头或使用外部损坏页。驱动逐项验证原有tuple字节，不能仅凭checksum通过判断数据保持正确。

```cpp
#include "storage/PgPage.h"
#include "storage/HeapTupleHeader.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

using namespace dbms;

static std::vector<char> smallintTuple(int16_t value) {
    const size_t headerSize = computeHeapHeaderSize(1);
    std::vector<char> row(headerSize + sizeof(value), 0);
    auto* header = reinterpret_cast<HeapTupleHeaderData*>(row.data());
    initHeapTupleHeader(header, 1, 1);
    setNotNull(header, 0);
    setXminCommitted(header);
    std::memcpy(row.data() + headerSize, &value, sizeof(value));
    return row;
}

static int mismatches(const PgPage& page,
                      const std::vector<OffsetNumber>& slots,
                      const std::vector<std::vector<char>>& expected) {
    int bad = 0;
    for (size_t i = 0; i < slots.size(); ++i) {
        const char* data = nullptr;
        size_t len = 0;
        const bool found = page.get(slots[i], data, len);
        if (i == 0) {
            if (found) ++bad;
        } else if (!found || len != expected[i].size() ||
                   std::memcmp(data, expected[i].data(), len) != 0) {
            ++bad;
        }
    }
    return bad;
}

static bool runCase(int initialCount) {
    alignas(8) std::array<char, PgPage::PAGE_SIZE> buffer{};
    PgPage page(buffer.data());
    page.init(1);
    std::vector<OffsetNumber> slots;
    std::vector<std::vector<char>> expected;
    for (int i = 1; i <= initialCount; ++i) {
        expected.push_back(smallintTuple(static_cast<int16_t>(i)));
        OffsetNumber slot = 0;
        if (!page.insert(expected.back().data(), expected.back().size(), slot)) {
            std::cerr << "normal insert unexpectedly failed at " << i << '\n';
            return false;
        }
        slots.push_back(slot);
        if (!page.isValid(1)) {
            std::cerr << "normal insertion produced invalid page at " << i << '\n';
            return false;
        }
    }
    if (!page.remove(slots.front())) return false;
    const bool beforeValid = page.isValid(1);
    const int beforeBad = mismatches(page, slots, expected);
    const size_t beforeFree = page.freeSpace();
    const unsigned beforeLive = page.liveCount();
    page.compact();
    const bool compactValid = page.isValid(1);
    const int compactBad = mismatches(page, slots, expected);
    const size_t compactFree = page.freeSpace();
    const unsigned compactLive = page.liveCount();
    std::cout << "initial=" << initialCount
              << " tuple_bytes=" << expected.front().size()
              << " before_valid=" << beforeValid
              << " before_live=" << beforeLive
              << " before_free=" << beforeFree
              << " before_mismatch=" << beforeBad
              << " compact_valid=" << compactValid
              << " compact_checksum=" << page.verifyChecksum()
              << " compact_live=" << compactLive
              << " compact_free=" << compactFree
              << " compact_mismatch=" << compactBad << '\n';

    // Follow only normal insert/canFit APIs, without modifying any page bytes.
    // This detects whether the compactor incorrectly advertised a live tuple
    // region as reusable free space.
    for (int i = 0; i < 2; ++i) {
        const auto row = smallintTuple(static_cast<int16_t>(1000 + i));
        OffsetNumber slot = 0;
        if (!page.canFit(row.size()) ||
            !page.insert(row.data(), row.size(), slot)) {
            std::cerr << "advertised reclaimed-space insert failed\n";
            return false;
        }
    }
    const int afterBad = mismatches(page, slots, expected);
    // The deleted line pointer is legitimately reusable; exclude its reuse
    // from survivor integrity verification.
    const char* deletedData = nullptr;
    size_t deletedLen = 0;
    const int reusedDeleted = page.get(slots.front(), deletedData, deletedLen) ? 1 : 0;
    const int survivorBad = afterBad - reusedDeleted;
    std::cout << "initial=" << initialCount
              << " after_two_inserts_valid=" << page.isValid(1)
              << " after_two_inserts_live=" << page.liveCount()
              << " survivor_mismatch=" << survivorBad << '\n';
    return beforeValid && beforeBad == 0 &&
           beforeLive == static_cast<unsigned>(initialCount - 1) &&
           compactValid && compactBad == 0 && compactLive == beforeLive &&
           survivorBad == 0;
}

int main() {
    int failures = 0;
    for (int initial : {257, 258, 270}) {
        if (!runCase(initial)) ++failures;
    }
    std::cout << "cases=3 failed=" << failures << '\n';
    return failures == 0 ? 0 : 1;
}
```

实际输出（本机来源：`/tmp/dbms-review-pgpage-compact.gocRrhal/compact_normal_api_probe.log`）：

```text
initial=257 tuple_bytes=26 before_valid=1 before_live=256 before_free=454 before_mismatch=0 compact_valid=1 compact_checksum=1 compact_live=256 compact_free=480 compact_mismatch=0
initial=257 after_two_inserts_valid=1 after_two_inserts_live=258 survivor_mismatch=0
initial=258 tuple_bytes=26 before_valid=1 before_live=257 before_free=424 before_mismatch=0 compact_valid=0 compact_checksum=1 compact_live=257 compact_free=476 compact_mismatch=0
initial=258 after_two_inserts_valid=1 after_two_inserts_live=259 survivor_mismatch=1
initial=270 tuple_bytes=26 before_valid=1 before_live=269 before_free=64 before_mismatch=0 compact_valid=0 compact_checksum=1 compact_live=269 compact_free=428 compact_mismatch=0
initial=270 after_two_inserts_valid=0 after_two_inserts_live=271 survivor_mismatch=1
cases=3 failed=2
```

## Extended Parse/Describe 会话上下文

对应R30。双方先创建独立非public schema并设置search_path，以下SQL中的表和函数均在该schema。Q表示Simple Query，PDS表示Parse/Describe/Sync，未发送Execute。两组共27个动作，其中两个描述失败；MATCH仍仅比较行、SQLSTATE和OID，原始消息另外保留。失败前后的顺序是复现条件，不能以错误之后的成功重试替代新鲜连接控制。

### probe7_aggregate_describe.log

```text
COMPATIBILITY_MODE postgresql18
CASE 0 MODE Q SQL CREATE TABLE inputs(i INT,t TEXT)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 1 MODE Q SQL INSERT INTO inputs VALUES(1,'alpha'),(2,''),(NULL,NULL)
PG18 ([], None, '', [], 'INSERT 0 3', [])
ROOT ([], None, '', [], 'INSERT 0 3', [])
MATCH True
CASE 2 MODE PDS SQL SELECT sum(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS total,avg(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS mean FROM inputs
PG18 ([], None, '', ['total', 'mean'], None, [20, 1700])
ROOT ([], '42P01', 'relation "inputs" does not exist', [], None, [])
PG18 RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x02total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00mean\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\xa4\xff\xff\xff\xff\xff\xff\x00\x00'), (b'Z', b'T')]
PG18 DIAGNOSTICS []
ROOT RAW_MESSAGES [(b'E', b'SERROR\x00VERROR\x00C42P01\x00Mrelation "inputs" does not exist\x00\x00'), (b'Z', b'I')]
ROOT DIAGNOSTICS [{b'S': b'ERROR', b'V': b'ERROR', b'C': b'42P01', b'M': b'relation "inputs" does not exist'}]
MATCH False
CASE 3 MODE Q SQL SELECT sum(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS total,avg(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS mean FROM inputs
PG18 ([['2', '0.66666666666666666667']], None, '', ['total', 'mean'], 'SELECT 1', [20, 1700])
ROOT ([['2', '0.66666666666666666667']], None, '', ['total', 'mean'], 'SELECT 1', [20, 1700])
MATCH True
CASE 4 MODE PDS SQL SELECT sum(CASE WHEN i > 0 THEN 1 ELSE 0 END) AS total,avg(CASE WHEN i > 0 THEN 1 ELSE 0 END) AS mean FROM inputs
PG18 ([], None, '', ['total', 'mean'], None, [20, 1700])
ROOT ([], None, '', ['total', 'mean'], None, [20, 1700])
PG18 RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x02total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00mean\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\xa4\xff\xff\xff\xff\xff\xff\x00\x00'), (b'Z', b'T')]
PG18 DIAGNOSTICS []
ROOT RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x02total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00mean\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\xa4\xff\xff\xff\xff\xff\xff\x00\x00'), (b'Z', b'I')]
ROOT DIAGNOSTICS []
MATCH True
CASE 5 MODE Q SQL CREATE TYPE rank_type AS ENUM ('zeta','','alpha')
PG18 ([], None, '', [], 'CREATE TYPE', [])
ROOT ([], None, '', [], 'CREATE TYPE', [])
MATCH True
CASE 6 MODE Q SQL CREATE TABLE ranks(id INT PRIMARY KEY,r rank_type)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 7 MODE Q SQL INSERT INTO ranks VALUES (1,'zeta'),(2,''),(3,'alpha'),(4,NULL)
PG18 ([], None, '', [], 'INSERT 0 4', [])
ROOT ([], None, '', [], 'INSERT 0 4', [])
MATCH True
CASE 8 MODE Q SQL CREATE SEQUENCE enum_argument_seq
PG18 ([], None, '', [], 'CREATE SEQUENCE', [])
ROOT ([], None, '', [], 'CREATE SEQUENCE', [])
MATCH True
CASE 9 MODE Q SQL CREATE TABLE enum_argument_effect(seq BIGINT DEFAULT nextval('enum_argument_seq'),v INT)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 10 MODE Q SQL CREATE FUNCTION enum_argument_writer(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO enum_argument_effect(v) VALUES(i); RETURN i; END;$$
PG18 ([], None, '', [], 'CREATE FUNCTION', [])
ROOT ([], None, '', [], 'CREATE FUNCTION', [])
MATCH True
CASE 11 MODE PDS SQL SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks
PG18 ([], None, '', ['total'], None, [20])
ROOT ([], None, '', ['total'], None, [20])
PG18 RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'T')]
PG18 DIAGNOSTICS []
ROOT RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'I')]
ROOT DIAGNOSTICS []
MATCH True
CASE 12 MODE Q SQL SELECT v FROM enum_argument_effect ORDER BY seq
PG18 ([], None, '', ['v'], 'SELECT 0', [23])
ROOT ([], None, '', ['v'], 'SELECT 0', [23])
MATCH True
CASE 13 MODE Q SQL SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks
PG18 ([['3']], None, '', ['total'], 'SELECT 1', [20])
ROOT ([['3']], None, '', ['total'], 'SELECT 1', [20])
MATCH True
CASE 14 MODE Q SQL SELECT v FROM enum_argument_effect ORDER BY seq
PG18 ([['1'], ['2']], None, '', ['v'], 'SELECT 2', [23])
ROOT ([['1'], ['2']], None, '', ['v'], 'SELECT 2', [23])
MATCH True
CASE 15 MODE PDS SQL SELECT sum(enum_argument_writer(id)) AS total FROM ranks
PG18 ([], None, '', ['total'], None, [20])
ROOT ([], None, '', ['total'], None, [20])
PG18 RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'T')]
PG18 DIAGNOSTICS []
ROOT RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'I')]
ROOT DIAGNOSTICS []
MATCH True
```

### probe8_fresh_enum_describe.log

```text
COMPATIBILITY_MODE postgresql18
CASE 0 MODE Q SQL CREATE TYPE rank_type AS ENUM ('zeta','','alpha')
PG18 ([], None, '', [], 'CREATE TYPE', [])
ROOT ([], None, '', [], 'CREATE TYPE', [])
MATCH True
CASE 1 MODE Q SQL CREATE TABLE ranks(id INT PRIMARY KEY,r rank_type)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 2 MODE Q SQL INSERT INTO ranks VALUES (1,'zeta'),(2,''),(3,'alpha'),(4,NULL)
PG18 ([], None, '', [], 'INSERT 0 4', [])
ROOT ([], None, '', [], 'INSERT 0 4', [])
MATCH True
CASE 3 MODE Q SQL CREATE SEQUENCE enum_argument_seq
PG18 ([], None, '', [], 'CREATE SEQUENCE', [])
ROOT ([], None, '', [], 'CREATE SEQUENCE', [])
MATCH True
CASE 4 MODE Q SQL CREATE TABLE enum_argument_effect(seq BIGINT DEFAULT nextval('enum_argument_seq'),v INT)
PG18 ([], None, '', [], 'CREATE TABLE', [])
ROOT ([], None, '', [], 'CREATE TABLE', [])
MATCH True
CASE 5 MODE Q SQL CREATE FUNCTION enum_argument_writer(i INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN INSERT INTO enum_argument_effect(v) VALUES(i); RETURN i; END;$$
PG18 ([], None, '', [], 'CREATE FUNCTION', [])
ROOT ([], None, '', [], 'CREATE FUNCTION', [])
MATCH True
CASE 6 MODE PDS SQL SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks
PG18 ([], None, '', ['total'], None, [20])
ROOT ([], '42804', 'CASE types integer and text cannot be matched', [], None, [])
PG18 RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'T')]
PG18 DIAGNOSTICS []
ROOT RAW_MESSAGES [(b'1', b''), (b'E', b'SERROR\x00VERROR\x00C42804\x00MCASE types integer and text cannot be matched\x00\x00'), (b'Z', b'I')]
ROOT DIAGNOSTICS [{b'S': b'ERROR', b'V': b'ERROR', b'C': b'42804', b'M': b'CASE types integer and text cannot be matched'}]
MATCH False
CASE 7 MODE PDS SQL SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks
PG18 ([], None, '', ['total'], None, [20])
ROOT ([], None, '', ['total'], None, [20])
PG18 RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'T')]
PG18 DIAGNOSTICS []
ROOT RAW_MESSAGES [(b'1', b''), (b't', b'\x00\x00'), (b'T', b'\x00\x01total\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x00\x08\xff\xff\xff\xff\x00\x00'), (b'Z', b'I')]
ROOT DIAGNOSTICS []
MATCH True
CASE 8 MODE Q SQL SELECT v FROM enum_argument_effect ORDER BY seq
PG18 ([], None, '', ['v'], 'SELECT 0', [23])
ROOT ([], None, '', ['v'], 'SELECT 0', [23])
MATCH True
CASE 9 MODE Q SQL SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks
PG18 ([['3']], None, '', ['total'], 'SELECT 1', [20])
ROOT ([['3']], None, '', ['total'], 'SELECT 1', [20])
MATCH True
CASE 10 MODE Q SQL SELECT v FROM enum_argument_effect ORDER BY seq
PG18 ([['1'], ['2']], None, '', ['v'], 'SELECT 2', [23])
ROOT ([['1'], ['2']], None, '', ['v'], 'SELECT 2', [23])
MATCH True
```

### TEMP原驱动诊断

保留原SQL、顺序和断言，仅调整独立副本的仓库定位并打印read_message，实际exit1。正常TEMP SELECT可以Parse，标量子查询却报42P01，与R30的会话查找遗漏一致。未增加PG差分矩阵。

```text
[TEMP PREPARE PARSE-ONLY RAW] SELECT id FROM prep_empty_7faa05e50a24; (b'1', b'')
[TEMP PREPARE PARSE-ONLY] SELECT id FROM prep_empty_7faa05e50a24;
[TEMP PREPARE PARSE-ONLY RAW] SELECT (SELECT id FROM prep_empty_7faa05e50a24 LIMIT 1); (b'E', b'SERROR\x00VERROR\x00C42P01\x00Mrelation "prep_empty_7faa05e50a24" does not exist\x00\x00')
```

## 完整注册回归终态

基准a502ffa3；原命令为`bash scripts/build_tests.sh`，未修改输入或放宽期限。实际exit1，日志终态输入核对通过。1232个唯一注册项与实际运行记录逐项一致，无遗漏/重复/额外项：785普通native中783通过、2失败；3个真实Main入口全通过；444个Python驱动中436返回成功、8失败。成功数包含`statement_timeout_tls_protocol_e2e_test.py`因无OpenSSL而显式跳过，不能当作真实TLS通过。

完整日志本机路径`/tmp/dbms-review-20261013.f9Ywep98/full-regression.log`，SHA-256为`5c00bd608190058c75ff829f2f4894c81e711a9cc249e04a03ec6b317a59f006`。下面按原顺序保留十个失败及终态的关键行；省略中间通过输出，未把临时诊断副本的输出冒充原注册结果。

```text
[test-build] Compiling tests/stale_temp_startup_recovery_test.cpp ...
[test-build] Linking/running /home/xusheng/workspace/DBMS_C_plus_plus/build/stale_temp_startup_recovery_test ...
terminate called after throwing an instance of 'std::runtime_error'
  what():  WAL crash recovery failed; startup aborted to protect data
/home/xusheng/workspace/DBMS_C_plus_plus/scripts/build_common.sh: line 631: 3869415 Aborted                 (core dumped) ( cd "$work_dir" && "$binary" )
[test-build] stale_temp_startup_recovery_test FAILED
[test-build] Compiling tests/table_owner_atomicity_test.cpp ...
[test-build] Linking/running /home/xusheng/workspace/DBMS_C_plus_plus/build/table_owner_atomicity_test ...
table_owner_atomicity_test: tests/table_owner_atomicity_test.cpp:105: int main(): Assertion `g_engine.alterTableOwner(db, table.tablename, newOwnerName) == dbms::DBStatus::OK' failed.
/home/xusheng/workspace/DBMS_C_plus_plus/scripts/build_common.sh: line 631: 3876750 Aborted                 (core dumped) ( cd "$work_dir" && "$binary" )
[test-build] table_owner_atomicity_test FAILED
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/enum_aggregate_argument_binding_protocol_e2e_test.py", line 130, in <module>
    if __name__=='__main__': main()
                             ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/enum_aggregate_argument_binding_protocol_e2e_test.py", line 83, in main
    messages=client.read_until_ready(sock);fields=client.row_description_fields(messages)
                                                  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/postgres_protocol_test.py", line 336, in row_description_fields
    raise AssertionError("RowDescription was not returned")
AssertionError: RowDescription was not returned
[test-build] tests/enum_aggregate_argument_binding_protocol_e2e_test.py FAILED
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/aggregate_result_root_owner_protocol_e2e_test.py", line 96, in <module>
    if __name__ == "__main__": main()
                               ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/aggregate_result_root_owner_protocol_e2e_test.py", line 65, in main
    describe("SELECT sum("+expr+") AS total,avg("+expr+") AS mean FROM inputs", [20, 1700], ["total", "mean"])
    ~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/aggregate_result_root_owner_protocol_e2e_test.py", line 39, in describe
    messages = client.read_until_ready(sock); fields = client.row_description_fields(messages)
                                                       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/postgres_protocol_test.py", line 336, in row_description_fields
    raise AssertionError("RowDescription was not returned")
AssertionError: RowDescription was not returned
[test-build] tests/aggregate_result_root_owner_protocol_e2e_test.py FAILED
SET META DESCRIBE SELECT ARRAY[1] UNION ALL SELECT ARRAY[2147483648] None [(b'array', 0, 0, 1016, -1, -1, 0)]
SET META SQL ROLLBACK TO set_meta_case; ([], None, '', [], 'ROLLBACK', [])
SET META PORTAL SELECT ARRAY[1] UNION ALL SELECT ARRAY[2147483648] ([], None, '', ['array'], None, [1016]) [(b'array', 0, 0, 1016, -1, -1, 0)]
SET META SQL SAVEPOINT set_meta_case; ([], None, '', [], 'SAVEPOINT', [])
SET META DESCRIBE SELECT id FROM source UNION ALL SELECT wide FROM source 42P01 []
SET META SQL ROLLBACK TO set_meta_case; ([], None, '', [], 'ROLLBACK', [])
SET META PORTAL SELECT id FROM source UNION ALL SELECT wide FROM source ([], '26000', 'prepared statement does not exist', [], None, []) []
SET META SQL SAVEPOINT set_meta_case; ([], '25P02', 'current transaction is aborted, commands ignored until end of transaction block', [], None, [])
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/set_operation_type_metadata_known_gap.py", line 111, in <module>
    if __name__ == "__main__": main()
                               ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/set_operation_type_metadata_known_gap.py", line 61, in main
    assert simple("SAVEPOINT set_meta_case;")[1] is None
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError
[test-build] tests/set_operation_type_metadata_known_gap.py FAILED
SET CLAUSE FAILURES [('SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1', 'metadata state', '42P01', None), ('SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1', 'metadata types', [], [23]), ('SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1', 'metadata labels', [], ['n'])]
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/set_operation_clause_ownership_protocol_e2e_test.py", line 117, in <module>
    if __name__ == "__main__": main()
                               ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/set_operation_clause_ownership_protocol_e2e_test.py", line 109, in main
    assert not failures, failures
           ^^^^^^^^^^^^
AssertionError: [('SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1', 'metadata state', '42P01', None), ('SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1', 'metadata types', [], [23]), ('SELECT id AS n FROM clause_source UNION ALL SELECT 0 ORDER BY n LIMIT 1', 'metadata labels', [], ['n'])]
[test-build] tests/set_operation_clause_ownership_protocol_e2e_test.py FAILED
[test-build] Running tests/matview_refresh_protocol_e2e_test.py ...
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/matview_refresh_protocol_e2e_test.py", line 179, in <module>
    main()
    ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/matview_refresh_protocol_e2e_test.py", line 93, in main
    _, state, message, _, command_tag, _ = query(
                                           ~~~~~^
        runner, client, server,
        ^^^^^^^^^^^^^^^^^^^^^^^
        "REFRESH MATERIALIZED VIEW unpopulated_mv;")
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/matview_refresh_protocol_e2e_test.py", line 10, in query
    client.simple_query(server["sock"], sql), include_types=True)
    ~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/postgres_protocol_test.py", line 235, in simple_query
    return read_until_ready(sock)
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/postgres_protocol_test.py", line 53, in read_until_ready
    message = read_message(sock)
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/postgres_protocol_test.py", line 43, in read_message
    kind = read_exact(sock, 1)
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/postgres_protocol_test.py", line 34, in read_exact
    chunk = sock.recv(size)
TimeoutError: timed out
[test-build] tests/matview_refresh_protocol_e2e_test.py FAILED
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/index_corruption_protocol_e2e_test.py", line 77, in <module>
    main()
    ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/index_corruption_protocol_e2e_test.py", line 68, in main
    assert expect(sql, "XX001") == []
           ~~~~~~^^^^^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/index_corruption_protocol_e2e_test.py", line 21, in expect
    assert state == expected_state, (sql, rows, state, message)
           ^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: ('SELECT EXISTS(SELECT 1 FROM corrupt_items WHERE state=8);', [['t']], None, '')
[test-build] tests/index_corruption_protocol_e2e_test.py FAILED
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/explain_join_protocol_e2e_test.py", line 91, in <module>
    main()
    ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/explain_join_protocol_e2e_test.py", line 83, in main
    query("EXPLAIN (FORMAT JSON,ANALYZE TRUE) " + sql, state)
    ~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/explain_join_protocol_e2e_test.py", line 21, in query
    assert state == expected_state, (sql, state, message)
           ^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: ('EXPLAIN (FORMAT JSON,ANALYZE TRUE) SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k OFFSET 1;', None, '')
[test-build] tests/explain_join_protocol_e2e_test.py FAILED
Traceback (most recent call last):
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/temp_prepared_access_protocol_e2e_test.py", line 213, in <module>
    main()
    ~~~~^^
  File "/home/xusheng/workspace/DBMS_C_plus_plus/tests/temp_prepared_access_protocol_e2e_test.py", line 150, in main
    assert client.read_message(sock) == (b"1", b""), parsed_sql
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: SELECT (SELECT id FROM prep_empty_ebabf705d8ee LIMIT 1);
[test-build] tests/temp_prepared_access_protocol_e2e_test.py FAILED
[test-build] tests/compat_fallback_registry_test.py PASSED
[test-build] Some tests failed
[REVIEW] driver_actual_exit=1
[REVIEW] terminal_input_verification=PASS
```

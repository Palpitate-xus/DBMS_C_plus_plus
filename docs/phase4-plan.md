# Phase 4 实施计划：类型系统 / 约束 / DDL 完整化

> 目标：把 `docs/implementation-plan.md` 中 Phase 4 的 **4.1 ~ 4.40** 全部子功能实现完毕，并补齐测试、文档与提交。  
> 原则：逐条落地，不省略、不阉割；在现有代码基础上按“先基础、再类型、再约束、再 DDL、最后生态”的顺序推进。  
> 状态：Phase 2（Catalog）与 Phase 3（存储/MVCC/WAL）已完成，Phase 4 具备实施条件。

---

## 1. 总体推进节奏

| 阶段 | 主题 | 覆盖子任务 | 预计提交次数 |
|------|------|-----------|-------------|
| **Wave 0** | 基础设施 | 类型注册表、表达式求值器、DDL AST 桥接、DDL 事务化骨架 | 4 ~ 5 |
| **Wave 1** | 核心类型系统 | 4.1 ~ 4.18（numeric、bytea、timezone、enum、range、array、composite、domain、伪类型等） | 8 ~ 10 |
| **Wave 2** | 函数/聚合/窗口 | 4.19 ~ 4.21（函数全集、聚合、窗口） | 4 ~ 6 |
| **Wave 3** | 约束与默认值 | 4.22 ~ 4.25（DEFAULT、GENERATED、EXCLUDE、DEFERRABLE） | 4 ~ 5 |
| **Wave 4** | DDL 完整化 | 4.26 ~ 4.38（CREATE/ALTER TABLE、VIEW、TRIGGER、SEQUENCE、DOMAIN、FUNCTION/PROCEDURE、POLICY、MATVIEW、STATISTICS） | 10 ~ 12 |
| **Wave 5** | DDL 事务化 | 4.39（移除隐式提交，实现 catalog WAL 日志与回滚） | 2 ~ 3 |
| **Wave 6** | 测试、文档、验收 | 全量测试、implementation-plan 更新、最终提交 | 2 ~ 3 |

---

## 2. Wave 0：基础设施（先让上层有地方可落）

### 2.1 类型注册表（`pg_type` 风格的 TypeRegistry）

**目标**：结束“类型只是字符串”的状态，建立 `TypeRegistry`/`TypeInfo`，统一类型名规范化、类型修饰符校验、I/O 函数、比较/哈希行为。

- **文件**：
  - 新增 `src/catalog/TypeRegistry.h`、`src/catalog/TypeRegistry.cpp`
  - 修改 `src/interfaces/table_schema.h`：`Column` 的 `dataType` 仍保留字符串，但增加 `typeOid` 可选字段；新增 `TypeModifier` 结构
  - 修改 `src/catalog/CatalogManager.h`：将 `TypeRegistry` 作为成员，启动时 bootstrap 标准类型
- **内容**：
  - 内置类型：把现有 40+ 种类型全部注册，包括 `int2/int4/int8`、`numeric`、`float4/float8`、`char/varchar/text`、`bytea`、`bool`、`date/time/timetz/timestamp/timestamptz/interval`、`json/jsonb`、`xml`、`uuid`、`point/line/lseg/box/path/polygon/circle`、`inet/cidr/macaddr/macaddr8`、`bit/bit varying`、`tsvector/tsquery`、`int4range/int8range/numrange/tsrange/tstzrange/daterange`、数组类型、复合类型、enum、domain、伪类型 `record/anyelement/anyarray/void`
  - 类型别名规范化：`int`→`int4`、`integer`→`int4`、`varchar`→`character varying`、`real`→`float4`、`double precision`→`float8`、`bool`→`boolean`、`datetime`→`timestamp` 等；保留 `TINYINT`、`BLOB`、`NCHAR/NVARCHAR` 作为进入 PG 兼容映射的兼容别名，内部统一映射为 `int2`/`bytea`/`char`/`varchar`
  - 类型修饰符解析：长度（`varchar(255)`）、精度/标度（`numeric(10,2)`）、时区标志、`bit(8)`、`interval hour to minute` 等
  - I/O 钩子：每个类型注册 `input()`/`output()`/`receive()`/`send()` 函数指针；基础类型的输入输出先由现有代码承担，后续逐步替换
- **测试**：`tests/type_registry_test.cpp`

### 2.2 表达式求值器（Expression Evaluator）

**目标**：让 `CHECK`、`DEFAULT`、`GENERATED`、`WHERE`、`USING`、`WITH CHECK` 都能基于 AST 求值。

- **文件**：
  - 新增 `src/expression/ExprEvaluator.h`、`src/expression/ExprEvaluator.cpp`
  - 修改 `src/executor/ExecutionPlan.cpp`：复用/整合现有 `evalCondRaw()`
  - 修改 `src/commands/TableManage.cpp`：替换 `parseCheckConditions` 与 `evaluateGeneratedExpr`
- **内容**：
  - 支持常量、列引用、`NULL`、`CAST`、二元/一元运算、`CASE`/`COALESCE`/`NULLIF`/`GREATEST`/`LEAST`
  - 支持比较、`BETWEEN`/`IN`/`LIKE`/`ILIKE`/`SIMILAR TO`、逻辑 `AND/OR/NOT`
  - 支持函数调用（通过 FunctionRegistry）
  - 支持简单子查询（标量子查询、`EXISTS`、`IN (SELECT ...)`）
  - 提供 `evalOnRow(row, schema)` 与 `evalDefault(rowContext)` 两种入口
- **测试**：`tests/expression_evaluator_test.cpp`

### 2.3 DDL AST 执行桥（DDL Executor）

**目标**：让 `execute()` 中庞大字符串分支开始消失，DDL 从 AST 直接驱动。

- **文件**：
  - 新增 `src/commands/DdlExecutor.h`、`src/commands/DdlExecutor.cpp`
  - 修改 `src/main.cpp` 或当前 `execute()` 入口：对 DDL 类命令先 `parser.parse()`，再交给 `DdlExecutor`
- **内容**：
  - 实现 `DdlExecutor::execute(StmtPtr, dbname)`，按 `Stmt` 类型分发到 `StorageEngine`/`CatalogManager` 方法
  - 先迁移 `CREATE/DROP/ALTER TABLE`、`CREATE/DROP INDEX`、`CREATE/DROP VIEW`、`CREATE/DROP SEQUENCE`、`CREATE/DROP DOMAIN`、`CREATE/DROP TYPE`、`CREATE/DROP FUNCTION/PROCEDURE`、`CREATE/DROP TRIGGER`、`CREATE/DROP POLICY`、`CREATE/DROP MATERIALIZED VIEW`、`CREATE STATISTICS`、`COMMENT ON`、`SECURITY LABEL`
  - 保留 DML/UTILITY 在现有入口，后续 Phase 5 再迁
- **测试**：`tests/ddl_ast_bridge_test.cpp`

### 2.4 DDL 事务化骨架

**目标**：为 4.39 做准备，在 catalog 层引入“事务内 catalog 变更缓存 + WAL 日志”。

- **文件**：
  - 新增 `src/catalog/CatalogTransaction.h`、`src/catalog/CatalogTransaction.cpp`
  - 修改 `src/catalog/CatalogManager.cpp`：写 catalog 文件前先写 `CatalogTransaction` 日志；commit 时 flush，rollback 时回退内存状态
- **内容**：
  - `CatalogTransaction` 记录：创建/删除/更新的系统表行（`pg_class`、`pg_attribute`、`pg_type`、`pg_depend`、`pg_description`、`pg_authid` 等）
  - DDL 执行不再直接写 CSV/文件，而是通过 `CatalogManager::transactCreateClass(...)` 等方法；commit 时批量持久化
  - 与 `StorageEngine` 的事务边界对齐：`beginTransaction` 开启 catalog tx；`commit`/`rollback` 一并结束
- **测试**：`tests/ddl_transaction_test.cpp`

---

## 3. Wave 1：核心类型系统（4.1 ~ 4.18）

按“先标量、再复合、再特殊”顺序实现。

| 子任务 | 内容 | 关键文件 | 测试文件 |
|--------|------|---------|---------|
| **4.1 numeric / decimal** | 支持任意精度/标度，NaN/Infinity；输入输出与四则运算使用现有 decimal 或引入轻量实现；与 TypeRegistry 集成 | `src/types/numeric.h/cpp` | `tests/numeric_test.cpp` |
| **4.2 非 PG 类型映射** | `TINYINT`→`int2`、`DATETIME`→`timestamp`、`BLOB`→`bytea`、`NCHAR/NVARCHAR`→`char/varchar`；Parser 与 TypeRegistry 同时识别别名与标准名 | `src/catalog/TypeRegistry.cpp` | `tests/type_alias_test.cpp` |
| **4.3 collation** | 内置 `C`、`POSIX`、`en_US.utf8` 等排序规则；列定义支持 `COLLATE`；比较/排序时读取 `Column::collation`；为 4.3 先实现 provider 框架（ICU 占位，可用 `std::locale`） | `src/catalog/collation.h/cpp` | `tests/collation_test.cpp` |
| **4.4 bytea** | 支持 hex/escape 输入输出；存储为原始字节；在 `Column::isVariableLength` 路径走 TOAST | `src/types/bytea.h/cpp` | `tests/bytea_test.cpp` |
| **4.5 timezone / infinity / BC / interval 字段限定** | 解析 `timestamptz`、`time with time zone`；支持 `infinity`、`-infinity`、BC 日期；`interval '1 year 2 months'`；实现 `interval field` 限定 | `src/types/datetime.h/cpp` | `tests/datetime_test.cpp` |
| **4.6 CREATE TYPE ... AS ENUM / ALTER TYPE ADD VALUE** | 在 `pg_type` 中存 enum 标签；insert 时校验值在标签集合中；`ALTER TYPE ADD VALUE` 追加标签 | `src/catalog/CatalogManager.cpp` | `tests/enum_test.cpp` |
| **4.7 几何类型完整集** | 实现 `line`、`lseg`、`box`、`path`、`polygon`、`circle` 的解析、输出、基本运算（ containment、intersection、distance 等），注册到 TypeRegistry | `src/types/geometry.h/cpp` | `tests/geometry_test.cpp` |
| **4.8 macaddr / macaddr8 / 网络函数** | 校验与格式化；网络函数 `host()`、`masklen()`、`broadcast()`、`network()` 等 | `src/types/network.h/cpp` | `tests/network_test.cpp` |
| **4.9 bit / bit varying** | 长度约束、位运算 `&` `|` `^` `~`、`>>` `<<`、按位计数 | `src/types/bitstring.h/cpp` | `tests/bit_test.cpp` |
| **4.10 tsvector / tsquery** | 文本解析为词位，权重标记；`tsquery` 支持 `&` `|` `!` 与 `<->`；实现 `to_tsvector`、`to_tsquery`、`plainto_tsquery`、`@@` 匹配 | `src/types/textsearch.h/cpp` | `tests/textsearch_test.cpp` |
| **4.11 uuid / uuidv7()** | RFC 4122 解析输出；实现 `uuidv7()` 生成函数 | `src/types/uuid.h/cpp` | `tests/uuid_test.cpp` |
| **4.12 XML 类型函数 / XPath / XMLTABLE** | 先基于字符串存储；实现 `xmlparse`、`xmlserialize`、`xpath`、`XMLTABLE`（简单表函数形式） | `src/types/xml_ops.h/cpp` | `tests/xml_test.cpp` |
| **4.13 jsonpath / SQL/JSON / JSON_TABLE** | 实现 `jsonb_path_query`、`jsonb_path_query_array`、`jsonb_path_query_first`、`JSON_TABLE`（行级展开） | `src/types/jsonpath.h/cpp` | `tests/jsonpath_test.cpp` |
| **4.14 多维数组 / 切片 / unnest / ANY/ALL** | 数组文本输入输出、下标访问、切片、`array_append/prepend/position`、`unnest` 表函数、`ANY`/`ALL` 表达式求值 | `src/types/array.h/cpp` | `tests/array_test.cpp` |
| **4.15 Composite type** | `CREATE TYPE foo AS (...)` 注册复合类型；row constructor、字段访问 `.field`、嵌套复合类型 | `src/types/composite.h/cpp` | `tests/composite_test.cpp` |
| **4.16 Range / Multirange** | 实现 canonicalization、`@>` `&&` `<@` `<<` 等操作符、空范围、离散范围步进、multirange 文本 I/O | `src/types/range.h/cpp` | `tests/range_test.cpp` |
| **4.17 Domain 多约束 / 依赖 / 数组自动类型** | `CREATE DOMAIN` 真正解析为底层类型；insert 时执行 domain 的 CHECK 约束；删除 domain 时检查依赖；`domain[]` 自动派生 | `src/catalog/CatalogManager.cpp` | `tests/domain_test.cpp` |
| **4.18 伪类型约束** | `record`、`anyelement`、`anyarray` 注册到 TypeRegistry；函数签名解析时检查伪类型匹配规则 | `src/catalog/TypeRegistry.cpp` | `tests/pseudotype_test.cpp` |

---

## 4. Wave 2：函数 / 聚合 / 窗口（4.19 ~ 4.21）

### 4.1 FunctionRegistry

- **文件**：`src/catalog/FunctionRegistry.h/cpp`
- **内容**：
  - 统一注册内置函数（数学、字符串、日期时间、JSON/XML/Array/Range/Network/Geometry）
  - 函数重载：按名称+参数类型解析
  - 命名参数、默认参数、variadic
  - 易变性标记：`IMMUTABLE`/`STABLE`/`VOLATILE`
  - UDF/UDF-SQL 注册

### 4.2 内置函数全集（4.19）

按 PG 文档常用函数分组实现：

- 数学：`abs`、`round`、`ceil`、`floor`、`power`、`sqrt`、`log`、`ln`、`exp`、`mod`、`sign`、`trunc`、`random`、`setseed`
- 字符串：`length`、`substring`、`position`、`overlay`、`trim`、`ltrim`、`rtrim`、`lower`、`upper`、`initcap`、`replace`、`split_part`、`concat`、`format`、`lpad`、`rpad`、`chr`、`ascii`、`md5`、`sha256`
- 日期时间：`now()`、`current_timestamp`、`current_date`、`current_time`、`extract`、`date_trunc`、`age`、`justify_days`、`justify_hours`、`make_date`、`make_time`、`make_interval`、`make_timestamp`
- JSON：`jsonb_each`、`jsonb_each_text`、`jsonb_object_keys`、`jsonb_array_elements`、`jsonb_typeof`、`jsonb_strip_nulls`、`jsonb_pretty`
- 数组/范围/网络/几何：对应 Wave 1 类型操作函数

### 4.3 聚合函数（4.20）

- 文件：`src/executor/Aggregate.h/cpp`（扩展现有或新建）
- 实现：`count`、`sum`、`avg`、`min`、`max`、`string_agg`、`array_agg`、`jsonb_agg`、`jsonb_object_agg`、`bool_and`、`bool_or`、`every`
- 有序集/假设集：`percentile_cont`、`percentile_disc`、`mode`、`rank`、`dense_rank` 作为聚合形式
- 统计回归：`regr_slope`、`regr_intercept`、`regr_r2`、`covar_pop`、`covar_samp`、`corr`、`stddev`、`variance`

### 4.4 窗口函数（4.21）

- 文件：`src/executor/WindowFunction.h/cpp`
- 实现完整 frame 语义：
  - `ROWS`/`RANGE`/`GROUPS`、`
`/`BETWEEN`、`
`/`EXCLUDE`
  - 命名窗口 `WINDOW w AS (...)`
  - 窗口函数：`row_number`、`rank`、`dense_rank`、`percent_rank`、`cume_dist`、`ntile`、`lag`、`lead`、`first_value`、`last_value`、`nth_value`

---

## 5. Wave 3：约束与默认值（4.22 ~ 4.25）

| 子任务 | 内容 | 关键文件 | 测试 |
|--------|------|---------|------|
| **4.22 DEFAULT 表达式** | DEFAULT 支持任意表达式（含函数调用、稳定/易变函数）、序列 `nextval`；insert 未指定列时调用 ExprEvaluator | `src/commands/TableManage.cpp` | `tests/default_test.cpp` |
| **4.23 GENERATED 列** | `GENERATED ALWAYS AS (expr) STORED` 在 insert/update 时自动计算并存储；`VIRTUAL` 不存储但可在查询时计算 | `src/commands/TableManage.cpp` | `tests/generated_test.cpp` |
| **4.24 EXCLUDE 约束** | 使用 GiST 或 B+ tree + operator class 实现排他约束；insert/update 时检查是否冲突 | `src/commands/TableManage.cpp`、`src/storage/BPTree.h` | `tests/exclude_test.cpp` |
| **4.25 SET CONSTRAINTS / DEFERRABLE** | 约束增加 `DEFERRABLE INITIALLY DEFERRED/IMMEDIATE` 状态；事务内维护延迟检查队列；commit 时批量检查；`SET CONSTRAINTS` 切换模式 | `src/commands/TableManage.cpp`、`src/transaction/ConstraintDeferral.h` | `tests/deferrable_test.cpp` |

同时补齐 FK 的 `ON DELETE` 动作与复合 FK 优化：
- `ON DELETE CASCADE`/`SET NULL`/`SET DEFAULT`/`RESTRICT`/`NO ACTION`
- 复合外键在引用列上建立/使用多列索引

---

## 6. Wave 4：DDL 完整化（4.26 ~ 4.38）

### 6.1 CREATE TABLE 选项补全（4.26）

- `LIKE INCLUDING` 全集：
  - `INCLUDING DEFAULTS`、`INCLUDING CONSTRAINTS`、`INCLUDING INDEXES`、`INCLUDING STORAGE`、`INCLUDING COMMENTS`、`INCLUDING ALL`
- `OF type_name`：基于复合类型建表
- `PARTITION BY` 语法已解析，需实现 range/list/hash 分区元数据与路由
- `TABLESPACE`：已有表空间支持，接入 `CREATE TABLE`
- `WITH ( ... )` 存储参数：现有 `fillfactor`，补充 `autovacuum_*`、`toast_tuple_target`、`parallel_workers`
- `ON COMMIT {PRESERVE ROWS | DELETE ROWS | DROP}`（临时表）
- `IDENTITY` 列：`GENERATED ALWAYS/AS IDENTITY (sequence_options)`

### 6.2 ALTER TABLE 全量子命令（4.27）

确保解析出的所有子命令都有执行：

- `ADD COLUMN`、`DROP COLUMN`、`ALTER COLUMN TYPE`、`ALTER COLUMN SET/DROP DEFAULT`、`SET/DROP NOT NULL`
- `ADD CONSTRAINT`、`DROP CONSTRAINT`、`ALTER CONSTRAINT`
- `RENAME COLUMN/TO/CONSTRAINT`
- `SET SCHEMA`
- `ATTACH PARTITION`、`DETACH PARTITION`
- `ENABLE/DISABLE TRIGGER`、`ENABLE REPLICA ALWAYS`
- `SET WITH OIDS`（本系统可空实现或记录）
- 重写表：当 `ALTER COLUMN TYPE` 导致行宽变化时，扫描全表重写并更新索引

### 6.3 VIEW（4.28）

- `CREATE [OR REPLACE] VIEW`、`CREATE RECURSIVE VIEW`
- `WITH CHECK OPTION`（LOCAL/CASCADED）：insert/update 时验证新行满足视图条件
- `SECURITY BARRIER` / `INVOKER`：视图查询时以视图 owner 或 invoker 权限执行（先实现权限检查框架）
- `ALTER VIEW`：重命名、改 owner、改 schema、改 security barrier

### 6.4 TRIGGER（4.29）

- 完整执行：`BEFORE`/`AFTER`/`INSTEAD OF`，行级/语句级
- 触发器函数调用：通过 FunctionRegistry 调用 PL/SQL 或 SQL 函数
- Transition tables（`REFERENCING OLD TABLE AS ... NEW TABLE AS ...`）
- Constraint triggers 与 deferred triggers
- Event triggers：DDL 事件钩子（parser 已支持，实现 catalog 事件分发）

### 6.5 TYPE（4.30）

- `CREATE TYPE` 的四种形态：
  - `AS ENUM`
  - `AS (...)` 复合类型
  - `AS RANGE`
  - `BASE TYPE`（shell + I/O 函数注册）
- `ALTER TYPE`：重命名、改 owner、改 schema、`ADD VALUE`、`RENAME VALUE`

### 6.6 CREATE TABLE AS（4.31）

- 解析 `CREATE TABLE AS` / `SELECT INTO`
- 从 SELECT 结果推断列类型
- `WITH [NO] DATA`
- 创建表并插入数据

### 6.7 CREATE STATISTICS（4.32）

- 元数据对象：`pg_statistic_ext`
- 支持 `dependencies`、`ndistinct`、`mcv`
- `ANALYZE` 时读取扩展统计并写入统计文件

### 6.8 SEQUENCE 完整语义（4.33）

- 扩展现有 sequence：`CACHE`、`CYCLE`、`MINVALUE`、`MAXVALUE`、`START`、`INCREMENT`、`NO MAXVALUE/MINVALUE`
- `OWNED BY` 列
- 事务级序列：回滚时不回退已分配值（PG 语义）
- `nextval`/`currval`/`lastval`/`setval`

### 6.9 DOMAIN（4.34）

- `CREATE DOMAIN ... AS type [DEFAULT expr] [CONSTRAINT ... CHECK (...)]`
- `ALTER DOMAIN`：`SET DEFAULT`、`DROP DEFAULT`、`ADD/DROP CONSTRAINT`、`VALIDATE CONSTRAINT`
- 全表 revalidation：`ALTER DOMAIN ... VALIDATE CONSTRAINT` 或 `ADD CONSTRAINT` 时扫描所有使用 domain 的表

### 6.10 FUNCTION / PROCEDURE（4.35 / 4.36）

- `CREATE FUNCTION`：支持 `LANGUAGE SQL`/`PLPGSQL`（PLPGSQL 先解释执行）、`RETURNS`、volatility、strict、parallel、cost、security definer
- `CREATE PROCEDURE`：支持事务控制 `COMMIT`/`ROLLBACK` 语句
- `ALTER`/`DROP` 完整子命令
- 函数依赖追踪（对 return type、language、schema 的依赖）

### 6.11 POLICY（4.37）

- `CREATE POLICY`：支持 `USING`、`WITH CHECK`、多角色
- `ALTER POLICY`、`DROP POLICY`
- 启用/禁用 RLS：`ALTER TABLE ... ENABLE ROW LEVEL SECURITY`
- 使用 ExprEvaluator 求值 `USING`/`WITH CHECK`，替代现有字符串拼接

### 6.12 MATERIALIZED VIEW（4.38）

- `CREATE MATERIALIZED VIEW [IF NOT EXISTS] ... AS ... WITH [NO] DATA`
- 底层使用 `__mv_<name>` 表存储
- `REFRESH MATERIALIZED VIEW [CONCURRENTLY]`：
  - 普通刷新：truncate + re-execute
  - Concurrent 刷新：新建临时表，完成后交换，并记录 `pg_matview` 依赖

---

## 7. Wave 5：DDL 事务化（4.39）

### 7.1 目标

彻底移除 DDL 的隐式提交，实现 `CREATE TABLE`/`DROP TABLE`/`ALTER TABLE` 等命令在显式事务内可回滚。

### 7.2 实现要点

- **Catalog WAL 日志**：
  - 新增 `XLOG_CATALOG_INSERT`、`XLOG_CATALOG_DELETE`、`XLOG_CATALOG_UPDATE` 资源管理器/记录类型
  - `CatalogTransaction` 的 redo/undo 信息写入 WAL
  - 崩溃恢复时重放 catalog 变更
- **文件系统原子性**：
  - DDL 创建/删除关系文件使用临时命名 + commit 时 rename
  - rollback 时删除临时文件或恢复旧文件
- **与 StorageEngine 事务集成**：
  - `beginTransaction` 创建 `CatalogTransaction`
  - DDL 命令自动加入当前事务，不再 `commitTransaction()`
  - `commitTransaction`：先写 catalog WAL，再 flush WAL，再持久化 catalog 文件，最后更新 CLOG
  - `rollbackTransaction`：用 catalog WAL 的 before-image 回退内存与文件状态
- **pg_depend 与依赖**：
  - 延迟写入，rollback 时撤销

### 7.3 测试

- `tests/ddl_transaction_test.cpp`：
  - BEGIN; CREATE TABLE t(...); ROLLBACK; → t 不存在
  - BEGIN; CREATE TABLE t(...); INSERT; ALTER TABLE ADD COLUMN; COMMIT; → 全部持久
  - 并发事务 A 创建表，事务 B 在 A commit 前看不到

---

## 8. Wave 6：测试、文档、验收

### 8.1 测试矩阵

| 测试文件 | 覆盖 |
|---------|------|
| `tests/type_registry_test.cpp` | 类型注册、别名、修饰符 |
| `tests/numeric_test.cpp` | numeric I/O 与运算 |
| `tests/bytea_test.cpp` | hex/escape 输入输出 |
| `tests/datetime_test.cpp` | timezone、infinity、BC、interval |
| `tests/enum_test.cpp` | CREATE TYPE ... AS ENUM / ALTER TYPE |
| `tests/geometry_test.cpp` | 几何类型与函数 |
| `tests/network_test.cpp` | macaddr/inet/cidr |
| `tests/bit_test.cpp` | bit 运算 |
| `tests/textsearch_test.cpp` | tsvector/tsquery |
| `tests/uuid_test.cpp` | uuid / uuidv7 |
| `tests/xml_test.cpp` | xml/xpath/xmltable |
| `tests/jsonpath_test.cpp` | jsonpath / JSON_TABLE |
| `tests/array_test.cpp` | 数组 ANY/ALL/unnest |
| `tests/composite_test.cpp` | 复合类型 |
| `tests/range_test.cpp` | range / multirange |
| `tests/domain_test.cpp` | domain 约束 |
| `tests/expression_evaluator_test.cpp` | 表达式求值 |
| `tests/function_registry_test.cpp` | 函数重载/默认参数 |
| `tests/aggregate_test.cpp` | 聚合与有序集 |
| `tests/window_function_test.cpp` | 窗口 frame |
| `tests/default_test.cpp` | DEFAULT 表达式 |
| `tests/generated_test.cpp` | 生成列 |
| `tests/exclude_test.cpp` | EXCLUDE 约束 |
| `tests/deferrable_test.cpp` | DEFERRABLE / SET CONSTRAINTS |
| `tests/fk_on_delete_test.cpp` | FK ON DELETE 动作 |
| `tests/create_table_options_test.cpp` | LIKE INCLUDING、OF、PARTITION BY、IDENTITY |
| `tests/alter_table_test.cpp` | ALTER TABLE 全量子命令 |
| `tests/view_test.cpp` | VIEW 完整语义 |
| `tests/trigger_test.cpp` | trigger 执行 |
| `tests/sequence_full_test.cpp` | sequence cache/cycle/owned by |
| `tests/domain_full_test.cpp` | domain 多约束/revalidation |
| `tests/function_procedure_test.cpp` | CREATE FUNCTION/PROCEDURE |
| `tests/policy_test.cpp` | RLS policy |
| `tests/materialized_view_test.cpp` | CREATE/REFRESH MV |
| `tests/ddl_transaction_test.cpp` | DDL 可回滚 |

### 8.2 文档更新

- 在 `docs/implementation-plan.md` 的 Phase 4 下新增“Phase 4 已完成内容”章节，逐项列出完成的功能与对应测试。
- 在 `docs/phase4-plan.md` 末尾记录实际变更与偏差。

### 8.3 提交策略

每次 Wave 完成后使用指定身份提交：

```bash
git -c user.name=Palpitate-xus -c user.email=palpitate.xus@outlook.com commit -m "Phase 4.X: ..."
```

建议提交主题：
1. `Phase 4.0: TypeRegistry and expression evaluator foundation`
2. `Phase 4.0: DDL AST bridge and catalog transaction framework`
3. `Phase 4.1: numeric/decimal precision and NaN/Infinity`
4. `Phase 4.2-4.5: bytea, timezone, type aliases`
...（按 Wave 1~6 顺序）

---

## 9. 风险与简化约定

- **ICU collation**：先提供框架与 `C`/`POSIX` 排序；完整 ICU 集成列为后续增强。
- **jsonpath / XML**：功能完整但语法覆盖 PG 子集，优先满足 SQL/JSON 与 XPath 常用场景。
- **PL/pgSQL**：Phase 4 中实现解释执行器核心（变量、IF、LOOP、赋值、RETURN、RAISE），复杂控制流逐步扩展。
- **GiST/SP-GiST**：EXCLUDE 约束先基于 B+ tree + expression index 模拟；GiST 完整 AM 放到 Phase 6。
- **DDL 事务化**：先覆盖最常用 DDL（TABLE/INDEX/VIEW/SEQUENCE/DOMAIN/TYPE），极复杂对象（FUNCTION/PROCEDURE/TRIGGER 编译产物）的 DDL 回滚先保证 catalog 层面可回滚，底层文件通过临时文件机制保证。

---

## 10. 验收标准

- `scripts/build.sh` 主程序编译通过，无新增 warning（-Wall -Werror 模式下）。
- `scripts/build_tests.sh` 全部新增与既有测试通过；Phase 4 完成后测试总数从 24 增至 55+。
- `docs/implementation-plan.md` Phase 4 的 40 个子任务全部标记完成。
- 每个子任务至少有一个对应测试用例。
- 所有提交使用指定 git 身份：
  - `user.name=Palpitate-xus`
  - `user.email=palpitate.xus@outlook.com`

---

*计划生成日期：2026-06-18*  
*来源：基于 `docs/implementation-plan.md` Phase 4 逐条展开*

# 存储函数写入应属于整个调用语句

## 问题与范围

在 `7bc76204` 基线上，函数中的 writing CTE 成功后，外层 `SELECT fn()`
因 `SELECT INTO STRICT` 无行报 `P0002`，或局部 INT 赋值报 `22P02`，
已写入的行仍然保留。原 `executeWithCteInheritance` 只为顶层 DML/locking SELECT
建立隐式事务；普通 SELECT 不拥有事务，递归执行的函数写入便提前提交。

本次修复整个 SQL dispatcher 调用语句的事务所有权，并修复配套的函数内
command-ID/snapshot、DML host 与原生单次函数调用所有权。它不是完整 routine
或所有表达式位置支持的完成声明。相关 `FUNC-04/05/06`、`TXN-02/05` 保持 partial。

## 实现

- 所有需要 query snapshot 的顶层语句以及 EXECUTE/EXPLAIN 共用 outer owner；
  函数 body、derived/CTE children 与后续表达式不各自 begin/commit。
- 用户显式事务不被本层提前提交；原有协议 failed-transaction/savepoint 恢复路径
  继续负责 `Ready E`、`25P02`、ROLLBACK TO 与此前成功写入的保留。
- 私有 thread-local function frame 保存 engine/database/session/volatility，RAII
  在返回和异常路径撤销，不增加 header/API 或 StorageEngine 字段。
- VOLATILE 的 body SQL 推进全事务 CID，READ COMMITTED/RU 更新 body snapshot；
  返回时恢复 caller 的固定 ReadView，但不倒退全事务 CID、不清除新增 Combo CIDs。
  STABLE/IMMUTABLE 的直接写入、writing CTE、TEMP 写入和行锁查询被拒绝；
  它们仍可通过 SELECT 调用 VOLATILE writer，自己的后续读取不应看到其新写入。
- Restore 不跨 SQL 执行保留 context/CLOG 指针：重新获取相同 live xid/database
  的 context，并在 cache mutex 下采用当前 CLOG。已结束或更换的事务不恢复旧 view。
- PL execStmt 与 query 均走完整 session dispatcher。无 RETURNING 的 DML 是正常
  0-column success；旧 display success diagnostic 不能当成列名或结果。
- 原生 no-host callUDF 没有调用者事务时拥有整个函数-call transaction。有限 INSERT
  fallback 检查 target/value width、真实列名和表达式，使用 insertRow 保留 SQL NULL、
  text `null`、空文本、引号及 column-list 顺序；复杂不支持形状仍 fail closed。
- SQL-language 函数保持现有 FROM-less scalar scope。参数用实际 AST/private positional
  bindings，不借用 PL/pgSQL FOUND。每个已解析的存储函数调用用私有唯一 callback key，
  避免 registry case folding 合并 quoted Foo/foo；准备阶段只解析 metadata，执行阶段
  才调用 UDF，保留 nullable arguments、return type 与 volatility。
- direct function TCL 和从 PL host 间接递归到 TCL 均在执行前拒绝，避免提前 commit
  outer owner。一般顶层 CALL/DO 的旧边界未在本提交改写。
- 新 outer query transaction 暴露了 materialization 的 CID 隐藏：扫描阶段已允许
  registered command-internal relation，但条件过滤后的最终 RID fetch 又采用 caller
  CID，导致普通 CTE WHERE 返回空。最终 fetch 现与 scan 使用同一 internal view；
  普通用户表或用户 TEMP 表没有这种豁免。

## 证据记录与边界

独立 worktree：`/tmp/dbms-stored-function-atomicity.D0JmfO/repo`。
先基于 `7bc76204` 复现，随后安全保留自己的修改并 fast-forward 至已验证
`8cd860e7` 的 quoted/native SQLSTATE 修复基线。没有修改 ROOT 工作树。

| 检查 | 结果与范围 |
| --- | --- |
| 原 wire baseline | 终端 3281 exit 1；`atomic_strict(1)` 报 P0002 后仍存行 1。root 另复现 cast 报 22P02 后仍存行 2。 |
| 原 native baseline | 终端 98325 exit 134；多条 INSERT 后转换失败，零残留断言失败。 |
| 初轮 owner candidate | native 87753 通过，但不是最后版本；protocol 19303 的 direct INSERT 非 rowset 误拒绝 0A000 保留，随后修正 descriptor 判定。 |
| 第二轮 fresh dev candidate | main 95903 exit 0；TM 98272 固定数组 `.begin()` 编译失败保留，修正后 52117 exit 0。 |
| 4 项 matching dev natives | 19569 为早期 PASS；最终 91228 修正后 fresh relink 的 4860 exit 0：stored_function_atomicity、plpgsql_query_host、plpgsql_quoted_scalar_binding、function_procedure，每项 fresh test CWD。 |
| 原完整专项 gate | 44099 exit 1，direct WHERE 预期 P0002、实际 42883；未将这个失败改成通过。 |
| expression 诊断 | 8480 是打印实际值的 exit 0 probe，不是 PASS gate；13945 的 checked-in 预期断言 exit 1。二者证明 direct WHERE 无执行、ORDER BY 跳过调用、WHERE scalar-subquery 吞错。 |
| EXPLAIN 诊断 | 97154 FROM-less 42601；42195/93412 table-backed EXPLAIN ANALYZE 成功但未执行 UDF；保持失败。 |
| MVCC/相邻发现 | 43328 同-xid ComboCID control 失败；83535 证明 ordinary UPDATE 的 CAST predicate 自身 XX000，而 literal predicate 成功。70008 into_cte 返回 NULL 暴露 internal RID fetch regression；91228 编译修正后的 TM exit 0、9468 最新 link exit 0。 |
| 最终专项 | 97101 exit 0；supported atomicity 的全部控制通过，包括 same-xid ComboCID、explicit user savepoint/DDL recovery、STABLE→VOLATILE、真实 SQL 参数 namespace、native full-call owner，以及 deferred FK 的 row-before-error/无 durable write 边界。 |
| 最终 10 项相邻协议 | 70186 的 SELECT INTO、quoted scalar、function result、stored CTE namespace、snapshot characteristics、commit failure recovery；25313 的 autocommit deferred、RETURNING deferred、SELECT table lock、DDL upgrade timeout，两个终端均 exit 0。 |
| 最终永久失败诊断 | 36070 exit 1；checked-in known-gap 的 9 个控制全部保持真实预期并失败，没有注册成绿色 suite。 |

上述最后版本是独立 worktree development O0 检查：两个改动 CPP fresh 编译，复用
同 ABI 的 55-object development 基线的其余对象，fresh test stubs；没有变更 header/字段。
正式 O2/ROOT combined build 与集成复查仍待 root 核实，没有据此宣称全注册套件通过。
曾有 61694/87757 启动阶段 ConnectionAbortedError，失败保留；隔离 startup probe 随后
启动成功、SELECT 1 正常、process exit 0，没有证明启动失败已由本源修改解决。

66042 的新增 deferred-FK fixture 曾错误要求没有 DataRow，实际 row 42 后报 23503。
核对现有 RETURNING fixture 和 PG18 [tcop primary source](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/tcop/postgres.c)
的 PortalRun→finish_xact_command→EndCommand 顺序后，纠正为 DataRow→Error→Ready I、
没有 CommandComplete，并强制后继查询无持久写入；97101 通过。不是降低原子性断言。

本地 PostgreSQL 参考诊断实际是 `server_version_num=170002`（PostgreSQL 17.2），
所有临时表/函数位于 BEGIN…ROLLBACK，没有 durable reference writes。
它确认 primary failure 后无残留、STABLE wrapper 返回 -1 而 VOLATILE 写入 140、
同-xid outer UPDATE 查询返回旧 id 1/2 与 child 11/12，后继查询看到 11/12，
以及 SQL 参数 FOUND=7。不是 PostgreSQL 18.6 differential 通过声明。

PostgreSQL 18 的 [volatility 文档](https://www.postgresql.org/docs/18/xfunc-volatility.html)
和 [SPI visibility 文档](https://www.postgresql.org/docs/18/spi-visibility.html)
规定 caller/body snapshot 与 STABLE→VOLATILE 的可见性边界；
[SPI primary source](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/executor/spi.c)
说明 read-only command 检查及 non-read-only command CID 推进。本次只落实已测试的窄项。

## 必须继续修复的失败 gate

`tests/stored_function_clause_execution_known_gap.py` 是 checked-in diagnostic，
保留 PostgreSQL 的 P0002/22P02/正确行序和真实 side-effect 预期，当前应 exit 1。
未注册为已支持的绿色 E2E，不能用错误 42883 或无写入来冒充执行/回滚正确。

- direct WHERE 存储函数被误认 undefined；成功 writer 也没有执行。
- scalar-subquery predicate 的函数错误被吞掉。
- ORDER BY 直接函数或 scalar-subquery 不求值，错误和 side effects 被忽略。
- EXPLAIN ANALYZE 的函数投影没有真实执行，FROM-less 形状也被错误拒绝。
- ordinary UPDATE 的 CAST predicate 报 XX000，影响以 typed 参数形成的 body DML。

这些是下一阶段独立执行/错误传播问题，不能以此提交缩减总目标或标全部 routine 完成。
原生低层 queryExpr 等 API 若绕开 SQL dispatcher，一条表达式内多次 standalone callUDF
仍需要调用者提供共同事务；本原生 owner 只证明单次完整函数调用。
PL/pgSQL exceptions/SubXID、完整 binding-before-write、SQL function inlining/overloads/
polymorphism/default/named/variadic、plans/dependencies、安全属性等仍未完成。

另有 procedure TCL 差异：PG17.2 的 LANGUAGE sql procedure 含 COMMIT 在调用时为 0A000；
当前 legacy indirect function TCL guard 为 2D000。PL procedure 创建仍 unsupported，
不能将这项控制声称为全 procedure 错误码 parity。DO/顶层 CALL 的事务语义另待复查。

全默认 protocol 已有独立性能 timeout 失败，不属于本 atomicity 修复；本次没有重跑第二个
完整 protocol suite 或 200k guard，没有 PG18.6 全差分，没有 push 或开启 GitHub Actions。

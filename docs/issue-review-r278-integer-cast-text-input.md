# R278：共享整数 CAST 的文本语法与位宽错误顺序

范围：TYPE-02 / SQL-08 的有限修复。只接受共享 CAST 的整数文本输入修复，
不宣称整个类型族、原 273 项或全部测试库存已经完成。

## 缺陷、复现与修复

`castToInteger` 使用仅十进制的 `stoll`，拒绝合法的进制前缀和数字分隔符；
先按 int64 解析再检查目标位宽，还会给某些“窄位宽提前溢出、尾部含非法字符”
输入报错的顺序不符。以下是固定矩阵包含的例子：

| SQL | PostgreSQL 18.6 | 修复前 |
|---|---|---|
| `SELECT CAST('0O_10' AS integer)` | `8` | `22P02` |
| `SELECT CAST('1_000' AS smallint)` | `1000` | `22P02` |
| `SELECT CAST('-0x8000_0000_0000_0000' AS bigint)` | `-9223372036854775808` | `22P02` |
| `SELECT CAST('2147483648bad' AS smallint)` | `22003` | `22P02` |
| `SELECT CAST('2147483648bad' AS integer)` | `22P02` | `22P02`，保留正控制 |

修改 `src/expression/ExprEvaluator.cpp` 中 `castToInteger` 的整数字符串分支，
按目标 16/32/64 位调用已有 `parsePostgresIntegerText`，映射原有结构化
`22P02`/`22003` 错误。删除因此不再使用的事后范围检查函数。
NUMERIC 先舍入到整数、float4/float8 的自身宽度和舍入、BOOLEAN、BIT、BYTEA
的独立转换分支保持原实现；NULL 保留其真实 NULL 位与目标类型。
没有新建另一套整数解析器，没有公共头、对象布局或磁盘格式改动。

整数位宽范围见 [PostgreSQL 18 整数类型](https://www.postgresql.org/docs/18/datatype-numeric.html#DATATYPE-INT)。
进制、分隔符和错误检测顺序按 [官方整数输入实现](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/numutils.c)
核实，并以实际 PG `server_version_num=180006` 执行同一固定矩阵作为对照。

## 固定验证与注册

- 新 native 864 个控制：三种目标宽度、36 组输入、unknown/TEXT/VARCHAR/BPCHAR，
  原始与绑定 evaluator，精确结果、SQLSTATE、类型和 NULL 位。
- 新可移植 wire helper 972 个控制：三种目标宽度、36 组输入、unknown/TEXT/VARCHAR，
  Simple、Extended 和真实参数绑定；同时检查 ReadyForQuery、command tag 和返回 OID。
- 同一新夹具修复前两端各 native 864/224 失败、wire 972/252 失败；
  实际 PG 180006 的完整 972 为 0 失败。失败计数是控制断言数，不是独立缺陷数。
- 修复后两端同一 native 864、wire 972 全部通过；既有浮点转整数完整 1190 控制
  原断言全部保留且通过。新 helper 追加在既有已注册 driver 的原 complete 断言之后，
  没有改动注册清单、降低错误要求或删除原检查。

最终另有两端共 48 个有限执行记录：2 个主 native、24 个 native 库回归、
6 个真实 Main、16 个协议入口，均为实际 exit 0。
native 覆盖整数 codec、浮点/内部 char/BIT 转整数、整数算术、科学计数法、
numeric/text 绑定、PL/pgSQL 和 stored-function atomicity；完整入口列表见 receipt。
全部旧断言、原协议超时及测试数据隔离约定保持不变。
首次相邻回归驱动把 internal char 协议文件名写错，在文件存在检查时中止，
尚未开始该阶段编译；改为实际的 `internal_char_operations_protocol_e2e_test.py`
后重新运行完整相邻矩阵。该启动失败单独记录，未改写成通过。

本轮 fresh 编译两个生产 ExprEvaluator TU、两个新 native TU 和 22 个相邻 native TU，
服务器及 30 个 native/Main 程序新链接当前库。其他生产对象、stub、6 个 Main 对象为
源文件/头/编译器/依赖逐 SHA 匹配的复用对象，不冒称全冷构建或 fresh Main TU。
未运行全库 sanitizer、TLS runtime、全部注册测试或新的原始 full72/full1492。

## 版本边界与仍开放事项

Root 的 ExprEvaluator 原有其他未接受改动保留；本次 commit 使用纯 HEAD 加本项
两处手术式修改的已测试 blob。默认原 18 项暂存 diff 及全部其他工作区文件保持不变。
R276 按用户要求跳过；它的 TableManage 候选、驱动追加内容、测试与失败日志未提交，
本轮构建投影没有夹带该候选。R277 FOR 候选也未混入本轮生产版本。

R277 整数 FOR 的 BY、int4 边界、NULL/错误顺序与 FOUND 仍待独立完成：
其首轮候选 native 94/2 失败触发本共享 CAST 前置修复；该失败记录保留。
另有实际 PG/项目对照确认的 `FOR found ...` 结束后 FOUND 恢复差异，尚未修复。
这些不属于 R278 已完成声明，不将未提交候选计作功能完成。

其他用户暂缓项不恢复，历史 DROP/cleanup 失败仍开放。原 273 项总账和冻结
1492 的历史结果不改写；`--require-complete` 预期仍 exit 1。
不 push，不启用 GitHub Actions。

## 原始证据

实验目录：`/tmp/dbms-r278-integer-cast-text.lQsDhViI`。
临时目录不是长期归档承诺；摘要与下列哈希进入 Git 文档。

| 证据 | SHA-256 |
|---|---|
| `regression-driver-correction.json` | `2df3150c1031f0e222863969f79a53f90d7f4763e29f7daf718b5060a3cf37f6` |
| `completed-preflight-receipt.json` | `97c7e49395f8e52306a9042a34e1c87b8b53e6e47f23ccd73a9fc794aa0a1db6` |
| `completed-old-receipt.json` | `f75f3664abcc45e45714e98bc8c6b8b2e453a6450f631eff6bf1c8eefbe32721` |
| `completed-integration-receipt.json` | `179f075a35efd7f36faa1144f6035220fcfce8461b8ddd5fae6e02e28cea2794` |
| `completed-build-receipt.json` | `db7f09e7564e21c37b401bc6a9e59ec971d71dbcb79590d76925eabaafd6458e` |
| `completed-primary-receipt.json` | `3ae7b6a9673fb8ddbc8fe9bde6357037df862b9424f282b88c5e8222c7fa409f` |
| `completed-regression-build-receipt.json` | `d0ad92d952728a5bd3ced5152ee583c599710cbf04386d53e9bb1527f8a95d14` |
| `completed-candidate-receipt.json` | `c604dd38d1b50e9b710febe3f9bf5657e581613f0665bdc8b84f5b7d3e5d9776` |

# R280：统一 PL/pgSQL 条件布尔求值

范围：FUNC-06 / SQL-11 的一项有限运行时正确性修复，不是完整 PL/pgSQL 或原 273 项验收。
用户刚跳过的 [R279 声明与创建时校验](issue-review-r279-plpgsql-declaration-validation.md) 保持暂缓。

## 问题与修改方法

IF/ELSIF 原先只识别 `t`、`1`、`true`，WHILE 与 EXIT WHEN 却只识别 `t`。
结果是相同条件在不同控制结构中给出不同分支；非法输入通常被静默当成 false。
表达式求值失败时，IF 还可能先执行 ELSE，之后才由外层捕获错误。

| 条件 | 实际 PG18.6 | 修复前 |
|---|---|---|
| `WHILE 1` / `EXIT WHEN 1` | true | false |
| `IF 'yes'` / `IF 'on'` | true | false |
| `IF 2` / `IF 'o'` / `IF 'null'` | `22P02` | 静默 false |
| SQL NULL | 不进入真分支 | 需与普通文本 `null` 区分 |
| 整数 `0001` / NUMERIC `1.0` / 文本 `'0001'` | true / `22P02` / `22P02` | 原先混用源码拼写与真值判断 |

仅修改 `src/utils/plpgsql.cpp`，增加解释器内部 `evalCondition`，四种结构复用它：

- 每次条件测试只求值一次，保留真实 NULL 位与错误 SQLSTATE；NULL 不满足条件。
- 使用已有 `BooleanCodec` 的布尔输入规则，支持大小写、空白和无歧义前缀，拒绝非法输入。
- 按 SQL 源类型规范化整数载体的输出，不能把文本或 NUMERIC 小数位也去掉。
- 不用显式 SQL `CAST(int AS boolean)` 的“非零为真”替代 procedural coercion：
  `IF 2` 应报错，但用户显式写 `IF CAST(2 AS BOOLEAN)` 仍为 true。
- 无 scalar host 时使用 shared SQL evaluator，保留原表达式错误；失败后不执行任何分支或后续语句。
- 保留 WHILE 的逐轮重算、最终 false 检查，以及 IF/ELSIF 的短路顺序。

语义依据：[PostgreSQL 18 控制结构](https://www.postgresql.org/docs/18/plpgsql-control-structures.html)、
[布尔输入规则](https://www.postgresql.org/docs/18/datatype-boolean.html) 与
[官方 PL/pgSQL 执行器](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/pl/plpgsql/src/pl_exec.c)
中的条件求值及 procedural coercion。此处处理当前内建标量载体，不宣称已支持自定义 assignment cast。

## 固定对照与验证

188 个固定案例涵盖四个入口、布尔/整数/NUMERIC/浮点/文本、真实 NULL、变量与 FOUND、
前缀与非法值、表达式错误、循环重算和短路。案例数不是独立 bug 数。

- native 752 控制：typed host 与 hostless，比较完整结果、NULL 与精确 SQLSTATE。
- API 102 控制：单次求值、逐轮求值、错误后不调用任何 statement host、scalar 元数据、
  legacy 回调，以及整数源码规范化与显式 CAST 的正反控制。
- wire 2260 控制：同一矩阵实际 PG 180006 全 0；Simple/Extended、savepoint、错误后的 E/25P02、
  回滚后恢复 T 及最终 I。参考端只在回滚事务中创建唯一函数，无永久用户对象提交。
- 额外 80 个实际 PG 控制验证 literal/显式 CAST 的 API 预期；不是新增 80 个功能或 bug。

初始 API 42 与最终 API 102 的旧版原始结果分别保存，最终旧版使用相同最终 native 夹具重新编译执行。
相同最终夹具的旧两端各为 native 752/203 失败、API 102/60 失败、wire 2260/284 失败；
最初 API 42 两端各 21 失败。失败断言数不是独立缺陷数，不把数据采集 exit 0 冒充兼容性通过。
最终当前两端 native 752/API102/wire2260 均 0 失败；完整 RAISE 及原 FOR724 均通过。
原 RAISE 驱动的全部断言及 R277 的 724 控制原样保留，只追加新 helper，不改注册清单或 socket deadline。

本轮有限回归包含 86 个实际执行记录：2 个主 native、44 个库 native、6 个真实 Main、34 个协议入口。
这 86 个当前记录与另 2 个 scoped UBSan 记录均实际 exit 0；不是全库存、full72 或 full1492 通过声明。
选入原整数 CAST、PL/pgSQL RAISE/INTO/query host/绑定/NULL/错误、stored function atomicity 等相邻测试。
另有两端 scoped UBSan，仅当前 `plpgsql.cpp` 插桩；普通库及测试对象未全部插桩，不是全库 SAN/ASan/TLS 验收。

没有公共头、ABI/layout 或磁盘格式变化。两个生产 TU、两个新 native TU 与 16 个附加 PL native TU fresh 编译；
其余 32 个 native/Main 对象按源、全部头、MMD 与编译器逐 SHA 匹配复用。
两台服务器及 50 个 native/Main 程序新链接；6 个 Main 对象不是本轮 fresh TU。
接受库存为 975 native / 597 registered Python / 3 Main，共 1575 入口；本轮并未全量执行该库存。

## 未完成边界与版本管理

完整标量子查询准备仍有独立问题：本轮观察到 `(SELECT 1)` 条件在参考端为 true，旧产品为 false。
本项不修改 query preparation，也不以一个较窄的通过替代该问题；仍普通开放。
legacy string-only 回调不能完整表示 SQL NULL 元数据；完整自定义类型输出/assignment casts、
完整 procedural compiler、CONTINUE 与 R279 创建时校验均不在本项完成声明中。
历史 DROP/cleanup、排斥约束 NULL/deferrable timing 等问题仍开放，其他用户跳过项不恢复。

只用独立本地 commit 提交本项源码、固定夹具、既有驱动追加与本记录，不 push，不启用 GitHub Actions。
其他工作区和原暂存 18 项逐字节保留；冻结的 1492 历史日志、原 273 项账本状态不改写，goal 保持 active。
实验目录：`/tmp/dbms-r280-plpgsql-conditions.wCc4Jxz5`。临时目录不是长期归档承诺。

## 协议超时历史

首轮候选两端 native 752/API102 已通过，但 Root 完整协议在新 helper 的 CREATE FUNCTION
处触发原有 15 秒 socket 超时，finally 中 ROLLBACK 也超时；初次日志未记录具体案例。
没有生成 completed primary/candidate receipt，accepted 本轮尚未执行。
该失败与完整日志保留在 `initial-candidate-timeout-receipt.json`，未提高 deadline 或删除案例。
后续在实验目录的 helper/driver 副本中只增加逐请求 TRACE_BEGIN/TRACE_DONE，所有 SQL、
预期与 deadline 保持不变。重查即使通过也不是该超时根因已经消除的证明，仍普通开放。
16 个附加 native 的旧版对照均实际 exit 0；它们不能代替上述失败协议的通过记录。

## 证据哈希

| 文件 | SHA-256 |
|---|---|
| `completed-preflight-receipt.json` | `1064793cc72cbfeab1cd4f18cef85afb5cefeefa254279abaa0ede00dcafd2a0` |
| `completed-observations.json` | `1be75639bee64288e4154177b567f579ecb0149e4ba6acdd06985afd4430ffca` |
| `completed-test-plan.json` | `4510739f9ed32fed253c6f2642efd1cfad7a5fce2fd47bfe6159fce34ff05c98` |
| `completed-reference-receipt.json` | `a83ea596aaaced9d0d340394705558008d472c1174ef29d2a32cc2d10875c8fd` |
| `completed-reference-literal-receipt.json` | `0f530d0ed38d02803a76c3cacdc122294968da33b8edc5f7e83b98a6419efdb6` |
| `completed-old-receipt.json` | `96e92b4557d166e6b77c07129f219f9301f070c655e4835df59660109d00bdf1` |
| `completed-old-final-receipt.json` | `997a154106112a072b3643a2b65bd00b638226960fabb44cd9141c04315e4a07` |
| `initial-candidate-timeout-receipt.json` | `5ee197d4ec1d6771ca4dd63625a85f239b9eac3794310a0aee278f4686587c77` |
| `completed-old-additional-runtime-receipt.json` | `7f327f7cd0269ffc9705efd344637fc822d3f207c260d07edce2f7bf1c08f9b8` |
| `completed-early-statics-receipt.json` | `be53e103b3e55b9922cf1b7505a902d2ae48b84079e4959c5851fb7df9f35304` |
| `completed-integration-receipt.json` | `ec99d9b08b2fd4c8c0757305021f90c53778ecd184364c2b40d0e61f118330d7` |
| `completed-build-receipt.json` | `e0f55f6e78c94ab918b3b681ff224cc71ff4216a4655b6f7163336b76f426073` |
| `completed-primary-receipt.json` | `b65cc9401ab416ed675971dcbf676ee7b8e8ba9ad4bec3b62f60eeb46efbd50a` |
| `completed-regression-build-receipt.json` | `7ec97e4d09fc504d09f3e1b97afe7bf0352cecbabc00e7a314fa44a7bb0d8b2b` |
| `completed-candidate-receipt.json` | `79848c9d9d4d07e71a702d452ebaf667e80f1f349b70566ee41ec80e821d0759` |
| `completed-scoped-ubsan-receipt.json` | `1f4a23d0738d888f98a05bbd441d269d00416f15f5a77a3c39a9780e475305ac` |

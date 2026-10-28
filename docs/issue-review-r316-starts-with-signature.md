# R316：STARTS_WITH 参数声明及布尔协议元数据

状态：本项有限修复已通过以下专项验收，按独立本地提交交付；原 273 总清单未完成。

旧产品没有为真实 `starts_with` 回调发布 `TEXT,TEXT → BOOLEAN` 声明，
因此错误参数数量、整数/布尔等错误实参也可能成功或返回 NULL；空表和
未执行分支没有一致的纯绑定检查。Extended 描述可能将返回值当成 TEXT，
连带影响未知参数推断和二进制输入/输出。

独立 PostgreSQL 18.6 完整 444 项全部通过，工作区及已接受源码投影旧产品
各 420 项断言失败；原生完整 81 项旧两侧各 69 项失败。数量为断言，
不是独立 bug 数。冻结夹具在生产修改前完成，SQL、期望值、SQLSTATE、
严格帧顺序、类型 OID 和布尔二进制字节不随修复放宽。

候选方法：为真实内建回调标记声明所有权，复用实际 typed overload binder
进行参数数量/隐式转换/NULL 和返回声明处理；只将本函数接入已有有限
前端声明绑定入口。同名 opaque 原生替换保留自己的参数、结果类型和执行
次数，不把函数拼写当成内建身份；不重开通用 presentation/计划阶段迁移。

检查包括零/一/三实参、错误类型位于任一参数、quoted/schema 名称、空表、
WHERE false/LIMIT 0、CASE/COALESCE 未执行分支、NULL/字符串 NULL、空前缀、
UTF-8、CHAR/VARCHAR/NAME 输入、真实表行/WHERE、Parse/Describe/Bind/Execute、
TEXT 未知参数推断和二进制布尔结果。原生另检查同名 opaque 回调的 0/1/2/3
参数、NULL、CHAR 原始 datum、不变/易变属性及有无数据库上下文。

证据目录：`/tmp/dbms-r316-starts-with-signature.3UbGRHY9/`。
`completed-fixtures-frozen-before-production-receipt.json` 记录冻结，
`*-old-observation-v2.log` 和 `*-old-native-runtime.log` 为完整旧基线。
第一次参考运行因脚本未传 `--reference18` 找不到本地 server，日志保留；
只修正参考启动参数后才获得参考端全 444 项通过，没有改变任何期望。

参考声明：[PostgreSQL 18 字符串函数文档](https://www.postgresql.org/docs/18/functions-string.html)。

R315 按用户要求暂缓，相关五个严格 DELETE 失败断言保持不变，未用本项
关闭。所有其他用户暂缓保持；原 273 总清单未完成，不 push，Actions 禁用。

## 当前有限验证结果

普通矩阵完整 156 条执行记录：144 条退出 0、12 条非零；scoped UBSan
完整 40 条：32 条退出 0、8 条非零，无 UBSan 诊断。合计 196 条，
176 条退出 0、20 条非零，不称全绿。上一轮全部 188 条输入完整保留，
新增 native81 / wire444 在普通与 scoped UBSan、两侧均实际全 0。
既有非零记录逐项对照失败身份和严格实际/期望报文，只有 factorial 夹具
自己生成的 schema UUID 在 SQL 标签中做等价命名，其余字节不改。
这些既有失败属于用户暂缓的 numeric/planning/TEMP VIEW/DO/R315；
原错误、默认超时和断言仍在，并未将“暂缓”称为修复或通过。

三处生产 CPP 各两侧全新编译：Evaluator、Network、Main，共 6 个；
116 个生产对象及 2 个 stub 按源码、公共头、编译器、flags 和 MMD 依赖
匹配后复用。6 个包含真实 Main 的测试单元全部重新编译，原窗口测试
保留 O2，另两个保留既有 O0 规则；新 native 两侧全新编译，70 个旧 native
对象匹配复用。78 个测试程序、两个服务器全部重新链接当前库。
没有公共头改动，不冒称所有 122 个生产单元都 fresh。

scoped UBSan 新编译当前 Evaluator/Network 及新 native 各两侧共 6 个单元，
配合 32 个逐源码/头/编译器验证的旧插桩对象，重新链接 22 个 native、
两个服务器。还保留 parser/TableManage/OutputCapture/DDL/DmlExecutor
插桩，数学检查另带 Planner。Main 和其他库为普通对象，不宣称全库插桩。

Root 及纯副本的版本、文档、兼容契约、总账、测试注册检查均实际通过；
`--require-complete` 仍实际失败。冻结历史 1492 原日志及 receipt 不覆盖，
原 273 状态仍为 22 complete / 166 partial / 70 unverified / 15 deferred。

当前 receipt：`completed-runtime-build-receipt.json`、
`completed-regression-build-receipt.json`、`completed-candidate-receipt.json`、
`completed-scoped-ubsan-receipt.json` 和
`completed-classified-candidate-receipt.json`。只提交隔离后的本项差异，
保留全部外来工作区内容及原有 18 项暂存变更；不 push，不启用 Actions。

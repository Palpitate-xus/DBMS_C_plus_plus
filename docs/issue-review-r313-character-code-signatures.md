# R313：ASCII / CHR 参数签名与转换

状态：本项有限子集已修复并通过下述验收；原始 273 项总清单仍未完成。

对应原始条目 `FUNC-01`、`TYPE-04`、`SQL-07` 和 `PROTO-05` 的有限子集。
不重新开展 R312 序列会话状态、TEMP VIEW 或其他用户已暂缓的模块。

## 已复现问题与修复方法

| 问题 | 旧产品实际结果 | PostgreSQL 18.6 | 修复方法 |
| --- | --- | --- | --- |
| 非法参数类型 | `ascii(1)` 返回 49；`chr(65::bigint)` 返回 A | `42883` | 声明 `ascii(text)` / `chr(integer)`，使用现有隐式转换图，不把任意 datum 当字符串 |
| 缺少或多余参数 | `ascii()` 返回 NULL；`chr(65,66)` 返回 A | `42883` | 在绑定阶段选择真实签名并校验 arity |
| 非法 integer 输入 | `chr('65x')` 返回 NULL | `22P02` | unknown literal 通过已声明的 int4 输入转换，而非解析失败返回 NULL |
| integer 输入越界 | `chr('2147483648')` 返回 `54000` | `22003` | 先校验 int4 输入范围，再执行 Unicode 码点检查 |
| 空输入或未执行分支漏检 | 空表 / `LIMIT 0` 的错误调用成功；未选中 CASE/COALESCE 分支漏检 | 分析阶段报错 | 将这两个函数接入现有查询、Parse 和参数推断的有限声明入口；不以执行输入来推断类型 |

实现只为真正的 builtin callback 附加声明身份。通过 `registerFunction` 替换
同名 callback 时，保留替换者自己的 arity、参数 datum、NULL、返回类型与
volatility，不继承 builtin 的转换规则，也不把该 callback 误认作 builtin。
不修改 UTF-8 编解码、NUL/代理区/超范围码点的既有规则。

## 复现和回归边界

证据目录：`/tmp/dbms-r313-character-code-signatures.PKGxOcUE/`。
`completed-observations-receipt.json` 保存三侧 24 项初始观察及完整原始报文。
私有 PostgreSQL 18.6 的独立 533 项协议回归全部通过；旧 Root 隔离投影和
纯接受副本各 533 项、各失败 289 项。原生专项各 112 项、各失败 88 项，
其中包含新增声明身份下的 opaque callback 防回归检查。

最初协议 fixture 的四个运行时错误检查误用共享 helper 的 `rows=[]` 分支，
导致 helper 在错误响应中仍期待 `SELECT 0`。已在生产修改前改用严格报文
校验，保留全部 533 项 SQL、预期值和 SQLSTATE；初始 fixture 和失败日志
另存，未改动共享 helper 或删去失败断言。最终冻结记录为
`completed-final-fixtures-frozen-receipt.json`。

协议测试覆盖 Simple、Extended、binary output，准确 OID/长度/typmod/format、
DataRow 原始 UTF-8 字节、NULL 与空字符串、合法 int2 到 int4 转换、非法
TEXT/bigint/numeric 输入、真实表行/谓词、空表、WHERE false、LIMIT 0、
CASE/COALESCE、非法 OVER、缺失列、quoted/schema-qualified 名称，以及
参数推断、Parse/Describe/Bind/Execute/Close 和 text/binary 参数。
预期来自独立声明和 PostgreSQL 实际运行，不从产品当前输出生成。

第一候选只修改 evaluator 后，原生 112 项通过，但协议仍各有 230 项失败。
原始请求、断言、报文均保留；不能把原生通过说成 SQL / wire 已修复。
第二候选补接现有前端有限声明入口，重新编译改动生产单元及六个真实
Main 测试单元，仍使用同一份冻结的 533 项断言。

## 验收记录

第二候选的 Root 隔离投影和纯接受副本均通过原生 112 项及全部协议 533 项；
scoped UBSAN 下也均通过这两组检查。原有字符串函数、字符串长度签名的
原生测试及各 654 项长度协议回归同时通过。533 项冻结输入和断言未删减。

当前普通矩阵 126 条执行记录：116 条退出 0，10 条既有暂缓差异退出非零；
scoped UBSAN 矩阵 28 条：22 条退出 0，6 条既有功能差异退出非零，无 UBSAN
诊断。共 154 条有限执行记录，138 条退出 0、16 条非零；不是整库全绿。
保留上一轮全部 140 条输入，未以缩小矩阵消除失败。既有非零为 R271
numeric 边界、R303 prepared planning、R310 TEMP VIEW、R288 DO 等已暂缓
行为，逐条比对失败身份；fixture UUID 只在 SQL 标签中作重命名比较，
实际与预期协议字节不作归一化。不会把这些差异标记为已修复。

构建实际新编译六个生产单元（evaluator / Network / Main 各两份）、六个
原生测试单元及六个真实 Main 测试单元；另外 116 个生产单元、54 个原生
测试对象和两个 stub 经 source/header/MMD/toolchain/flags 匹配后复用，
全部 66 个测试程序和两个服务器重新链接。头文件未修改，不宣称本轮
122 个生产单元全新编译。scoped UBSAN 使用本项当前 evaluator / Network
插桩及匹配的既有插桩对象，Main 和其他库仍为普通构建，不是全库插桩。

实际构建、运行、失败分类记录分别为
`completed-runtime-build-v2-receipt.json`、
`completed-regression-build-v2-receipt.json`、
`completed-candidate-v2-receipt.json`、
`completed-scoped-ubsan-v2-receipt.json` 和
`completed-classified-candidate-receipt.json`。
第一候选的源文件已另存完整 archive，其运行记录不冒充第二候选的验收。

版本、文档状态、兼容契约、总账、测试注册静态检查分别在 Root 和纯副本
验证；`--require-complete` 仍必须失败，原始总账完成数不变。测试清点不等于
全量运行；历史 1492 基线及其失败记录保持不变。

既有暂存区的 18 项变更和无关工作区草稿必须原样保留；最终仅提交经过
隔离验证的本项差异。不 push，不启用 GitHub Actions。

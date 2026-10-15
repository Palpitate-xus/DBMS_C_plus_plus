# R83：固定 float8 标量函数的真实声明签名

状态：本范围已修复。二十个完整原生驱动及十六个完整协议驱动通过；
另一个完整协议驱动发生 CREATE TABLE socket 超时，保留为未解决观测，未宣称全绿。
这是[R62](issue-review-r62-scalar-signatures.md)的有限扩展，不代表全部回调、
stored routine、命名参数、自定义 cast/domain 或整个 PostgreSQL 函数族完成。

## 问题与修改方法

原实现主要按名字找到数学回调，错误 arity、TEXT/BOOLEAN 参数在普通调用、
空表、WHERE false 或 LIMIT 0 中可能返回值或被跳过。有效 NUMERIC 输入的
sin/cos/tan 又被旧 helper 错报为 NUMERIC 输出，协议 OID 应为 float8 的 701。
错误签名加 OVER 应先报 42883；合法签名但错误角色报 42809；普通调用中
不可解析的 UNKNOWN 字面量应报 22P02，不能因没有输出行而漏掉输入分析。

本次新增的真实声明如下：

| 函数 | 输入 | 输出 |
| --- | --- | --- |
| sin、cos、tan、asin、acos、atan、sinh、cosh、tanh、asinh、acosh、atanh、degrees、radians、cot、cbrt | 一个 double precision | double precision |
| atan2 | 两个 double precision | double precision |
| pi | 无参数 | double precision |

实现复用真实 namespace、exact/preferred/UNKNOWN 类别和合法隐式转换解析，
随后才检查窗口/聚合角色。Main 的 AST 路由覆盖普通查询及 SQL PREPARE 的
声明阶段；不把这十八个名字加入 ABS 的常量执行清单，不扩大数学 domain
错误的空输入常量折叠范围。参数只按最终选定声明转换，准备阶段不执行参数。

实际 callback 用私有 marker 持有声明：registerFunction 替换后的原生回调
仍保留自己的 arity、输入字节、返回类型和 volatility，不继承被替换 builtin
的强制 float8 转换。存储 UDF 的输出来自真实选中的声明；同名 cos(TEXT)
可以接收 UNKNOWN/NULL 或 TEXT 列，不能被默认数学注册表抢走。

ExprHelper 可同步借用调用方的 const evaluator，只读其实际函数表，不保存
借用指针、不执行回调；未传入时保留原来的局部默认 evaluator。legacy 未绑定
复杂参数仍先执行 R80 的保护；真正固定输出的 builtin 可提供输出声明，但
不得检查未绑定输入或把该声明附到用户 routine。

## 原始失败与中间结果

临时证据位于 `/tmp/dbms-review-r46.ifu6SWLp/`，不属于发行包；未显示参考库密码。

- 旧实现 discovery 15 条中 12 条失败，实际 PostgreSQL 18.6 同 15 条全部通过。
- 纠正计数守卫后的旧原生 897 条中 777 条失败；加入三个嵌套控制后，旧原生
  900 条中 779 条失败。最终 906 条比该旧基线另加六个 TEXT UDF 控制，未冒称
  最终 906 条全部在旧实现运行过。
- 旧协议完整 933 条中 751 条失败；参考库 933、936 和最终 951 条均全通过。
- 第一版产品的 900 条原生通过，但协议 936 条仍有五个 NUMERIC 输出 OID 错误；
  由 helper 固定结果声明修正，保留 `r83-first-protocols.log` 的失败，不改写。
- 未标记 callback 所有权的候选破坏原生替换；独立旧对象基线 217 条全过、
  候选 217 条中 214 条失败。该临时旧对象证明仅是历史差分，不是当前 Root 验收。
- 加强为 237 条后，借用实际 evaluator 之前仍有一条真实数据库上下文中的
  `abs(sin('payload'))` 被默认注册表错误解析为 float8，见
  `r83-first-owned-native.log`。最终实际 evaluator 传递消除此回归。
- 第一轮最终 951 条 Root 协议仍有三条 TEXT 列 UDF 失败：空来源输出两次
  错误 OID 701，以及派生表实际调用 42883。见 `r83-final-protocols.log` 及
  `r83-final-protocol-logs/`，不能因为测试名带 final 就记为通过。
  Main 的无列绑定探测先命中 catalog，丢弃正确 whole-query 准备结果后又进入
  legacy 元数据/执行路径；现在对真实 search_path 中存在同名用户声明的
  新增十八个名字，交由既有 prepared value 接收器保留绑定后的 AST 与输出。
  没有用户候选的数学调用不扩大执行路由，也没有撤销 R80 未绑定输入保护。
  仍沿用接收器的支持形状检查；窗口或其他不支持形状的全部 legacy 接收器
  不属于这三个失败的修复证明，未宣称它们全部完成。

## 构建与当前验收

公共 helper 头文件变化后，首次真正重新编译 58 个生产单元的批次有 57 个
实际编译成功；expr_helper.cpp 因两个旧内部调用缺少新参数而失败，批次退出
123，未链接，见 `r83-owned-fresh58.log` 和对应逐单元日志目录。
补默认参数后，标准构建实际重新编译 ExprEvaluator.cpp、expr_helper.cpp 并
链接；重复构建零重编译，均退出 0，见 `r83-corrected-owned-build.log`。
因此最终 58 个对象均来自本工作区当前公共头的真实编译（57 个首批成功对象，
其中 ExprEvaluator 随后重编，以及修正后 helper），不是宣称首批 fresh58 全过。

首次实际 evaluator 修正后的生产二进制 SHA256：
`43e672ce0f323de5d3120afef5610b45fbabddc7516e34a4a5109b850c02b0c6`。
随后 Main 接收器修正实际只重新编译 Main，标准构建及重复构建均退出 0，
见 `r83-udf-frontend-build.log`；其余 57 个当前公共头对象逐一匹配。
新二进制 SHA256：
`681ce8f6d015a0469e07c02397a5e82622deceac704909bfbd6d37603c79a9af`。

最终原生核心门禁 `r83-final-native.log` 退出 0，十四个完整驱动全部通过：
新固定签名 906、原生 callback 替换 237、原 R62 签名 100、R80 未绑定 17、
R79 二进制别名 30，以及 math_functions、math_projection_error、NUMERIC SQRT、
ABS 范围、常量规划、窗口 routine/frame、存储函数排序 metadata、SQL-value keyword。
额外六个完整数组/集合驱动亦全部通过：array_expr、array_functions、prepared
query array metadata、routine array signature、generate_series bound metadata
及 cursor，见 `r83-neighbor-native.log`。两批各核对全部 58 份 receipt，生产
输入签名首尾一致；这些是二十个完整驱动，而非只抽取其中新增断言。

Main 接收器修正后的 `r83-accepted-native.log` 又完整运行上述二十个驱动，
全部退出 0，包含 906/237 最终矩阵，且首尾全部 58 份 receipt 及输入签名一致。

Main 修正后的 `r83-accepted-protocols.log` 中，前十三个完整驱动均通过：
新 float8 951、原 scalar signature 132、ABS 67、legacy window type 44，
普通 scalar ORDER/WHERE、存储函数排序、prepared read root、窗口 routine、
NUMERIC SQRT、SQL-value keyword、routine array signature 和 generate_series。
第十四个 postgres_protocol_test 在 line 860 的
`CREATE TABLE toasted_unicode_varchar (v VARCHAR(3000))` 等待响应超时，
整批退出 1；此时未运行其后的取消、CLI 超时、COPY 三项，不冒称已运行。
之后另行串行运行这三个原完整驱动均退出 0，见
`r83-timeout-neighbor-protocols.log` 及同名日志目录，生产输入/binary 与全部
58 份 receipt 首尾一致。因此协议合计是 16/17 通过、1 个超时，而不是全部通过。
没有修改原完整协议的 SQL、断言、顺序或 socket 期限，也没有证明此超时
与 R82 两个驱动共享根因；具体 SQL 和栈位置作为后续普通超时诊断输入保留。

Root 最终新增协议 951 条全通过；
真实 PostgreSQL 18.6/180006 的最终协议矩阵 951 条全通过，包含 TEXT UDF、
UNKNOWN/NULL、PREPARE、嵌套、空来源及准备阶段 volatile 参数不执行的检查，
见 `r83-final-reference-full.log`。

这里不改写[冻结 d974c80d 全量](issue-full-registered-d974c80d.md)的 1362 入口、
3 个失败及 TLS skip；R82 仍未解决，用户暂缓范围保持不变。新增两个原生驱动、
一个注册协议驱动后，新的完整组合为 862 native + 3 实际 Main frontend +
500 Python = 1365 个入口，尚未执行该新组合全量。

本次文档与机械检查：505 Markdown、本地 inline 目标 839 次、520 Python
AST、13 shell 语法、500 unique 注册/482 standalone E2E 均通过；文档状态、
兼容性契约、六个总账单元测试及 diff 空白检查通过。这些检查不代替运行时验收，
原 273 总账仍为 22 complete / 166 partial / 70 unverified / 15 deferred。

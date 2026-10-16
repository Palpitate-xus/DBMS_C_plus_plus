# R107：字符串长度函数的真实签名与输出单位

状态：本范围已修复。第三版607原生/PG与Root654、24完整原生/18完整协议/
3真实Main有限验收全部实际0，最终独立核对0；按单项本地提交。首两版失败保留。
R106已按用户要求独立暂缓，不重开其显式地址CAST分析专项。

## 已复现的问题

`char_length(1)`、`character_length(1)`、`octet_length(1)`和`bit_length(1)`
错误地按整数的文本表示计数；`NULL::integer`同样被接受。空来源、未选
CASE/COALESCE分支也未拒绝不存在的签名；错误调用带OVER先报42809而非42883。
实际PostgreSQL18.6（180006）均先报42883，不能靠执行回调后检查修复。

实际参考pg_proc给出11个内置声明：前两名各TEXT/CHARACTER两条；
OCTET_LENGTH有TEXT/CHARACTER/BYTEA/BIT四条；BIT_LENGTH有TEXT/BYTEA/BIT三条。
后者没有CHARACTER重载，应经已有隐式TEXT转换去除CHAR填充；OCTET_LENGTH
的CHARACTER重载则计入填充字节。字符数、UTF8字节数和位数不能混淆。

## 修改方法

复用现有纯签名解析与运行期参数转换，把上述11声明接入真实绑定入口。
只给实际内置回调附加声明标记，原生registerFunction替换不得继承INTEGER
结果、arity或参数转换；纯类型查询返回unknown，不通过执行回调发现类型。
同名用户INTEGER重载继续参与真实namespace/签名选择，不按名字误判为builtin。
不修改公共头、输入codec、通用隐式cast图或provider生命周期。

## 已执行的基线

两个永久入口是`tests/string_length_signature_test.cpp`和
`tests/string_length_signature_protocol_e2e_test.py`，后者已加入注册表一次。
原生607控制在未改生产代码时完整执行，440失败，实际退出1；最终协议654控制
在实际180006参考上零失败/实际0，在固定旧d697程序上392失败/实际1。
原638协议（参考0失败/旧384失败）与早期原生606/440均另保留。
新增16控制保护同名INTEGER用户函数从真实typed数据行取参、嵌套ABS、LIMIT 0
和TEXT内置重载，不能以只验source-free用户调用代替实际执行路径。

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`中的
`r107-discovery-reference.log`保存真实pg_proc声明与24条观察；
`r107-discovery-old-d697.log`为同24条旧行为。
`r107-initial-reference.log`、`r107-initial-old-protocol.log`、
`r107-old-native.log`、`r107-final-old-native.log`分别保留各完整基线。
最终654参考/旧程序日志为`r107-expanded-reference.log`与
`r107-expanded-old-protocol.log`。
固定旧程序路径为该目录的`r106-pre-fix-dbms-main`（这里只复用二进制快照，
不是重开R106），SHA256为
`d6970b8546dded1cbdac5dedfcfbd3a7dc8e2d8514e1aeba0d607a8400ae5519`。

## 验收边界

首版只改ExprEvaluator/expr_helper两个CPP，normal实际编译2份、repeat无重编，
实际退出均0。607新原生与原string_functions完整入口均0，原scalar_signature
132协议也0；但638新协议仍104失败，扩展654后112失败。其artifact fence虽0，
不证明行为通过，也不接受为修复终态。日志为`r107-candidate1-core-driver.log`、
各`r107-candidate1-core-*.log`及`r107-expanded-candidate1-protocol.log`。
失败确认前端空结果/SQL PREPARE尚未接入真实签名校验，同名用户重载数据行
执行也丢失已绑定的callee。因此第二版增加Main的有限分析路由与whole-query
callee保留，不修改R98诊断通道或触发器结果候选。

第二版normal只重编Main、repeat无重编，实际0；新607原生/原string_functions/
原132协议均0，654协议仍16失败，core整体1，artifact fence0，未启动有限全批。
剩余失败为Parse已发布成功后才在Describe报错，或未用真实BYTEA/BIT参数OID
拒绝字符长度调用。相关日志以`r107-final-`为前缀，属于第二版而非最终验收。
第三版仅把四个长度名字纳入NetworkServer既有纯metadata Parse绑定入口；保留
普通表达式/参数/来源不执行契约，不涉及连接、安全或暂缓专项。首两版二进制
另以`r107-candidate1-dbms-main`和`r107-candidate2-dbms-main`固定保存。

第三版normal仅重编NetworkServer、repeat无重编，实际退出均0；匹配当前
Root对象、fresh stubs后，完整新增607原生和原string_functions均0，新增654
协议及原scalar_signature132协议均0。core首尾binary/cache/双stamp/全部58
receipt/57测试库字节/预构建source-test-script seal核对0。记录为
`r107-c3-core-driver.log`及各`r107-c3-core-*.log`。

最终候选二进制SHA256：
`f478fcc9d922ed23df5b48a9ee6691a947a7852f2fed9b4e635c352920034df5`；
cache：`e70afdd4e89d5f6797a902ef2c423449504f072223d9b6d60baa4ef4c27f922a`；
seal：`e080180913ca3fe8d0e013af50dd629e0dd81e7011f278fc8f3c83bc86ea8772`。
四个CPP累计实际编译2/1/1份；公共头未改，共同编译签名始终为
`fab51659cc0085b8253040241dd88de6ec4c1c51917a487cd0cc29104fbdb932`，
没有将增量构建描述成全58重新编译。

第三版有限相邻批次的24个完整原生入口和18个完整协议入口（包含默认
`postgres_protocol_test`）均实际0，三个真实Main前端也均0。各入口分别见
`r107-full-native.log`、`r107-full-protocols.log`及
`r107-full-protocol-results/`，真实Main结果在`r107-final-full-driver.log`。
窗口驱动按原O2配置编译并实际完成58控制/零失败，会话入口完成14控制，
EXPLAIN为零失败。EXPLAIN/会话驱动是O0 same-TU，窗口与57份生产库为正常O2；
三个真实Main均不链接test_stubs，不执行重命名的原程序入口。合计45个不同
完整入口实际0，不是新1401/1405的完整注册套件，也不是整Engine sanitizer。
最终batch与独立终态分别见`r107-final-full-driver.log`和
`r107-c3-final-terminal-fence.log`，后者对同一候选的binary/cache/双stamp/
全部58 receipt/57测试库字节/冻结输入及未改公共头再次核对0。

## 文档、版本管理及仍未完成范围

本项22文件独立暂存，仅截取Main、NetworkServer、注册表及文档清单的自身
改动；不纳入R98、R99或R106草稿。隔离index检查覆盖530份拟提交Markdown，
预期注册886 native+3真实Main+512 Python=1401；工作区含R98时为1405。
工作区532份Markdown/535 Python AST/13 shell的检查属于UTF8、本地内联目标、
语法/注册/状态一致性，不证明历史文中每个技术结论或外部链接均成立。
原273项JSON items与原273勾选行不变，require-complete真实1仍表示未完成。
最终文档/状态检查及隔离dry index检查均实际0，分别见
`r107-final-static-before-staging.log`与`r107-final-dry-index.log`。
隔离暂存后，R98原六个源码文件的未接受diff与原38875字节基线一致
（只规范化git index hash及hunk行号），没有夹带提交或删除候选。
工作区仍含未提交R98/R99候选和R106未注册草稿，不可冒称干净HEAD全量通过。
原273完成状态不提升，既有失败保留，不push，Actions保持禁用。

# R157：普通 SET 点号目标名的完整身份

状态：有限修复已验收；没有实现完整通用 SET 值语法或自定义 GUC 存储，
没有恢复 R140 TimeZone 值语法、R151 枚举自动数组等用户暂缓模块。

## 问题、修复与边界

~~~sql
SET search_path.extra TO 'one';
~~~

已接受 parser 只把第一个 token 写入 name，后面的点和名称落进 values；
部分 AST 消费者会误选真正的 search_path 分支。
R152 工作区通过来自旧待定代码中的拼接循环，不能替代干净版本验证。

| 路径 | 修复方法 |
| --- | --- |
| 普通点号目标 | 在所有专用内建分派前读取完整 var_name，保留每个引用分量的原始拼写及正确 values 起点 |
| LOCAL/SESSION 同名前缀 | 后面接点时属于目标分量，不先当 scope 吞掉；真正可选 scope 保留 |
| transaction 同名前缀 | 分类器及事务判定都识别点号边界，不把独立设置当事务控制 |
| 目标分量 | 校验标识符边界；U/column-name 可裸用，reserved/type-function-name 要引用 |
| 不完整名称与分隔符 | 缺失/非法分量、缺 TO/等号/值、不完整 FROM CURRENT、插入其它语句均 42601 |
| 既有表达/值合约 | 本项仍保存原始 token 值，不冒充完整 var_list 转换、真实自定义 GUC 执行或全部 SET 实现 |

[parser.cpp](../src/parser/parser.cpp)只改 CPP 私有实现。
引用规则继续由既有 classifier 提供，23 个 type/function-name 关键字单独分为 category 3；
quoteIdentifier 的非 unreserved 引用行为及搜索路径值允许非保留关键字的规则不变。
没有增加另一套生产关键字表，没有改共享头、Main、权限或持久化格式。
旧待定 R140 的六行一般名称循环保持原样、未纳入提交；本项独立的提前分派不依赖它。

规则对照 [PG 18.6 SET/var_name 语法](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/gram.y)
和 [PG 18.6 关键字分类](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/include/parser/kwlist.h)，
全部 494 条事实再由实际 PG 协议验证，不从被测 classifier 生成期望。

## 完整实际验证

| 输入 | 完整控制 | 结果 |
| --- | --- | --- |
| PG 18.6/180006，最终协议夹具 | 1538 | 0 失败 |
| 修复前 Root / R152 accepted-only clean | 各 1538 wire | 各 226 失败、退出 1 |
| 修复前 Root | 3075 native | 474 失败、退出 1 |
| 修复前 accepted-only clean | 3075 native | 3072 失败、退出 1 |
| 修复后 Root / 新 accepted-only clean | 各 3075 native / 1538 wire | 均 0 失败 |
| Root 完整相邻入口 | 27 native + 22 wire + 3 Main = 52 | 全部退出 0 |
| clean 完整相邻入口 | 18 native + 16 wire + 3 Main = 37 | 全部退出 0 |

[原生夹具](../tests/set_qualified_target_test.cpp)保留 494 关键字的三种目标形态，
同时通过普通 parse 和 parseForBinding 验证，共 2964 项；
另有 28 个合法、25 个错误目标的双入口、关键词总数和四个事务归属控制，共 3075。
覆盖点/空白/注释、多级名称、引用分量、点或等号位于引号内、双引号转义、UTF-8、
美元符号、内建近似名、真实 scope、FROM CURRENT、DEFAULT 和缺失分量。
所有错误积累后输出完整计数，不在第一个失败处中止。

[协议夹具](../tests/set_qualified_target_protocol_e2e_test.py)执行 1482 个关键字 Parse/Describe，
28 个合法与 25 个错误目标及三个状态控制，共 1538。
成功形态严格为 ParseComplete/零参数描述/NoData/Ready(I)，错误为单个 42601/Ready(I)；
没有 Bind/Execute，不允许 Parse 改变真实搜索路径，后续 SELECT 确认同步恢复。
没有借本项声称项目支持所有自定义 GUC 的运行时设置与显示。

## 既有失败的关闭证据与保留的历史

R152 当轮 clean 的 search_path_values_grammar_test 在第 86 行断言退出 134；
冻结 R155 旧版同一夹具也退出 134，原 30 入口终态 1 保留。
本项没有改动[原夹具](../tests/search_path_values_grammar_test.cpp)的任何字节，
两端重新完整执行到 checks=254 passed；完整 52/37 入口及独立严格审计全 0。
这是新源输入的新验证，不改写 R152 原始失败、不重跑个别失败后冒充原批次通过。
R152 的 5197/1675 名称显示专项也在两端完整重新通过。

首轮 wire 假定固定默认搜索路径，两端为 public 而非参考环境默认值；改为先校验真实
单行非 NULL TEXT 基线、再断言前后相等，仍保留全部控制。
首次参考误连旧默认 PG17.2，被 180006 检查拒绝，未当作参考结果；
使用已建立的私有 PG18.6 端点后，完整 1538 尚有一项测试预期错误：
PG 允许末尾多余空分号。将该原用例从错误组移到合法组，SQL 与总数未删减；
重新完整实测才得到最终 PG 全 0、旧两端 226 失败、原生 474/3072 失败。
首轮 229/227、native 476、参考 1 及全部旧日志仍保留，未用差值代替实测。

早期两组各六条 PG 观察也保留：多参数 search_path_extra/search_path.extra 的运行时
错误是 22023，不是语法 42601；单参数无点未知名为 42704，dotted 自定义名则可 SET/SHOW，
真正的搜索路径不变。AST 接受 var_list、仅 Parse/Describe 与执行时的 GUC 参数检查
是不同阶段，不能把项目的目标名修复说成这些运行时能力全部实现。

## 构建、归属和总账

本轮没有共享头/Main 改动：Root 正常只编一个 Parser CPP O2，repeat0，
58 个 receipt 当前有效、另外 57 个生产对象字节与 R152 相同。
clean 从已接受 d65d387b 加本项两个 partial 源/注册和两个完整夹具独立生成：
一个 fresh Parser O2，加 57 个已核对 R152 fresh58 的生产对象；真实 server 无测试 stubs，
没有复制未核实对象或伪造 cache stamp。所有已构建 clean/donor 的已跟踪文件永久冻结。

两端核对六个真实 Main 源输入、全部头/flags 及对象字节后，复用 R152 各三个 Main 对象，
新链接各自当前 57 个 O2 库并隔离实际运行；不冒充本轮 fresh TU。
session 用已接受 320 命令，不执行 R140 额外 21 条。
初次外部构建审计将绝对 receipt 路径误作相对路径而退出 1，实际生产编译/链接成功；
单独修正路径比较的只读审计通过，没有覆盖原失败或重编冒充 fresh。
Main 归属检查器初稿正则过度转义，两端均在链接前退出 1；修正检查器后新链接、再实际执行，
原错误脚本/日志保留，没有更改产品或测试预期。

本地 commit 仅包含本项源/注册、两个夹具及相关文档，其余原始 29 个待定路径内容保持不变。
已接受 577 MD、909 native + 548 Python + 3 Main = 1460；
工作区 580 MD、915 + 554 + 3 = 1472，仍有 12 个未接受入口。
[R156 物理 REGTYPE 常量](issue-review-r156-regtype-physical-constants.md)与 R150 仍开放，
R151 及其它用户暂缓项不重开。
未运行新 1460 全注册/TLS/整库 sanitizer；原 273 状态仍 22/166/70/15，
总清单未完成，不 push、Actions 禁用。

本机完整原始输入、日志、源/对象清单与独立审计位于
/tmp/dbms-review-r46.ifu6SWLp/r157-*。

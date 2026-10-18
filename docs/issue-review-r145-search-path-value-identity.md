# R145：SET搜索路径参数身份与FROM CURRENT事务赋值

状态：完成有限修复。实际PG18.6、最终Root/clean各3573控制、Root18完整入口、
clean10及独立复核均通过；原273总账仍未完成。R144应用名规范化继续用户暂缓，
不因本项关闭或重新开启R140/R137及其它暂缓项。

## 问题与修改方法

| 问题 | 复现事实 | 修改方法 |
| --- | --- | --- |
| 字符串逗号被当路径分隔符 | SQL SET的'a,b'是一个名字，旧版拆成a和b，真实表查找42P01 | raw AST按var_list逐参数处理，再对字符串/标识符做list-quote |
| 名字数据丢失 | 'Upper'被折成upper，含双引号的Sconst被再次解释；a''b未解码 | 区分Sconst与quoted标识符，SQL转义只解码一次，名字中的双引号按标识符规则双写 |
| 字面量和语法混淆 | 'default'、数字字符串、关键字名字不同于DEFAULT/数值节点 | AST保留各已格式化参数，DEFAULT是重置标记，FROM CURRENT是当前值赋值动作，不在SHOW重新格式化 |
| 空名字表示错误 | SQL SET ''在PG显示为""，旧版显示空值 | 保留空名字；共享解析器允许quoted空元素，仍拒绝空的unquoted元素及尾逗号 |
| FROM CURRENT误作无操作 | BEGIN内先LOCAL Upper再普通SET/SESSION FROM CURRENT，PG的COMMIT后仍Upper，初稿却public | 当前可见值走原persistent/LOCAL赋值栈，不能提前返回成功；ROLLBACK与LOCAL恢复沿用原事务状态 |
| 语法校验缺失 | 垃圾尾串、错误逗号、同一行相邻字符串等被当名字 | raw token/span验证，42601且不改原路径；换行续串保留原始间隔 |

私有raw SET适配器在Main的Set分派中先于旧标量fallback处理本参数，
不重写用户已有诊断候选或其它参数分派。parser专用路径分支使用PG18.6
kwlist.h的164个非unreserved类别，独立夹具核对全部494关键字的quoted datum和
bare规则，没有依赖用户暂缓的TimeZone关键字表。Session头仅三处小修改，
不增加字段或改变布局、ABI、heap格式。

## 参考、旧红与最终验收

| 完整输入 | 实际结果 |
| --- | --- |
| 原3519：PG / 原Root907 / 原accepted a799 | PG全过，两旧版各1074断言失败/实际1 |
| 扩展3573：实际PG18.6/180006 | 全部通过/实际0 |
| 原Root907与accepted a799：扩展3573 | 各1098断言失败/实际1，完整日志保留 |
| 初稿Root c69与独立clean d74：扩展3573 | 各仅4失败，ID3526/3533/3553/3560，均为FROM CURRENT持久化/实际1 |
| 最终Root与独立clean：同字节3573 | 全部通过/实际0 |
| 最终完整入口 | Root5native+10wire+3真实Main=18，clean10wire；完整执行、全0 |
| 独立复核 | 控制ID、旧红/新绿、全部入口、产物/来源、冻结字节逐项核对/实际0 |

最初两份3519旧红曾短暂重叠执行，不计串行验收证据；随后串行完整重跑，
两份1074失败与冻结输入均核对。扩展3573的reference、两份初稿、两份原版及最终
Root18/clean10均为单一DB批次串行执行。1074/1098包含错误后续事务状态检查，
不是相同数量的独立bug。没有删旧断言、降低期望或覆盖失败日志。
附加14条PG FROM CURRENT观察促成新增54个永久事务控制，未把初稿3519通过
冒称完整正确；最终接受扩展夹具与持久化修正，不接受初稿。

## 永久回归与实际数据库使用

[search_path_values_protocol_e2e_test.py](../tests/search_path_values_protocol_e2e_test.py)
最终SHA256：
79a8eb20da643edfdd986026f4c9d149b838b57eb87c087a636a9e0984bbcc58。
覆盖bare/SESSION、TO/等号、quoted名称、Simple/Extended、text/binary SHOW原始字段、
494关键字、逗号/大小写/引号/空名字/E/dollar/换行续串、错误后状态不变、
DEFAULT与字面量default、FROM CURRENT普通/SESSION/LOCAL提交和回滚。
真实事务中创建三个含逗号、引号和大写的schema，各存不同id，核对未限定表查询
分别读到11/12/13；PG侧DDL和数据全部ROLLBACK，不保留参考库对象。

[search_path_values_grammar_test.cpp](../tests/search_path_values_grammar_test.cpp)
254检查覆盖parse/parseForBinding、作用域、数字/字符串、错误语法、quoted空元素、
近似/qualified参数不误匹配。真实
[session_execution_lifetime.cpp](../tests/frontend/session_execution_lifetime.cpp)
保留已接受298控制，新增12个名字/作用域命令及10个FROM CURRENT命令至320，
核对实际caller、COMMIT/ROLLBACK及父会话归属。工作区还含暂缓R140的21控制，
本项只执行已接受测试体加本项22命令的投影，不以工作区341代替320验收。
其余完整协议入口包含R142 quoted目标、R143 SHOW、R141参数身份、配置查询、
搜索路径事务、元数据作用域、事务模式、字面量及routine创建路径，不抽取断言代替入口。

## 构建与来源

Session头e660变更后，首轮Root与初稿clean均正常fresh编译全部58个生产O2对象、
链接、repeat零重编并核对冻结输入与receipt；三Main首次fresh编译。
这些只是初稿构建记录，不代替最终修正后的前端证据。
四个事务失败核实后，等待所有健康编译和串行旧红结束，再修改私有Main及测试。
最终Root正常重编1个Main O2、repeat零重编，头/编译配置b1ca不变，
其它57个生产对象字节/来源不变、58个receipt匹配当前源。最终Main为0f5e3ce9。
随后fresh重新编译全部三真实Main TU（explain O0/window O2/session320 O0），
新链接当前57生产O2库，无test stubs，并通过既有机制在全局初始化前隔离执行。
没有复用初稿Main7523所编的前端对象。

最终独立clean基于9412，只加本项五个partial源码/测试路径及两个新测试文件。
fresh Main O2链接57个逐一核对全部头、flags、CPP、原58编译receipt及对象SHA的
初稿clean生产donor，无test stubs；不是v2 normal fresh58。两clean全部tracked及
src/tests/scripts保持冻结。

最终Root binary：
519efdaa56d3ed766c8d0a4a2d002d77a92f7f8eece2353c3b7c8cfc5ed8e143；
cache：
bceb334e861ea293fa8b75b841c4e7fb0dad186871294861f9bc3b082cd31d07；
src/tests/scripts封印：
bffe938ee8ba637eb981d9c609b9549e675aaec23b642f896996311987ca5253；
最终clean binary：
ef39c0b7c3047687a20492ce79ddfaad9b268ae1024a06aa1d672d6b41768908。

## 文档、暂缓边界与证据

本机/tmp/dbms-review-r46.ifu6SWLp/保留reference、四份frozen旧红、
normal/repeat/freshMain、final-gates、独立及提交后日志。
独立检查从最终源码精确撤回本轮迭代差异，重建初稿cfaa封印，核实真实来源。
边界检查器两次因本项临时投影/本项新夹具末尾空行失败，原文件与失败日志保留。
更正版仅显式核对、恢复恰好一个已知EOF空行来重建旧字节，不宽松strip，
不改产品、夹具控制、期望或冻结输入；全部2171份前置源码及R137/R140 archive精确通过。
原56933字节补丁与三草稿仍由前置来源链保护，R98/R99/R109候选不混入提交。

已接受567MD、903native+542Python+3Main=1448；
工作区570MD、908native+547Python+3Main=1458，仍含十个未接受入口。
无新1448全注册、真实TLS或整库sanitizer证明，旧ecf1436两失败/1TLS skip不改写。
完整GUC、startup raw格式、全部数字/Unicode标识符/截断、quote_all_identifiers、
其它参数事务语义不据此完成。R144及其它用户暂缓保持未修复。
原273及22complete/166partial/70unverified/15deferred不变。不push，Actions禁用。

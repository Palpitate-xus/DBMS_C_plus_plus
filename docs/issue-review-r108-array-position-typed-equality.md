# R108：ARRAY_POSITION绕过真实元素相等语义

状态：本范围已修复，通过第五版本项验收，按本项独立本地commit，不push。
验收是下列匹配构建与有限完整入口，不是1403/1407全注册验收。
本项继续原TYPE-16/FUNC-01/SQL-09普通工作，不恢复R106或其他用户暂缓专项。

## 已确认的错误

首组24条观察有10条结果/错误行为不同（不以显示列标签差异计数）：

| 输入/needle | PostgreSQL18.6 | 固定旧程序 |
| --- | --- | --- |
| NUMERIC数组1.00 / NUMERIC 1 | 下标1 | NULL |
| NUMERIC数组9007199254740993.00 / 同值整数写法的NUMERIC | 下标1，保持精度 | NULL |
| REAL数组-0 / REAL 0 | 下标1 | NULL |
| INTEGER数组1 / NUMERIC 1.0 | 下标1 | NULL |
| CHAR(3)数组a加填充 / CHAR(1) a | 下标1 | NULL |
| INTERVAL数组1 mon / INTERVAL 30 days | 下标1 | NULL |
| LSEG非空数组及非NULL needle | 42883，缺少默认元素相等操作符 | 错误返回下标2 |
| JSON非空数组及非NULL needle | 42883，缺少默认元素相等操作符 | 错误返回下标1 |
| 下界0的NUMERIC数组1.00 / NUMERIC 1 | 下标0 | NULL |
| 同数组从下标1开始查找NUMERIC 1 | 下标2 | NULL |

源码回调仅解码数组文本后比较`value == a[1].value`，未使用元素声明、共同
输入类型或真实相等实现。TEXT数组中的01/1仍须不同，不能用“能解析成数值”
作为相等规则；NUMERIC也不能降为double。NULL元素与文本NULL必须保持区分。

后续48条能力/优先级观察核对七种几何类型、JSON/JSONB、起始下标和NULL
快捷路径，并补充JSONB数字/数组、TIME、TIMETZ、NUMRANGE、TSVECTOR与真实
TEXT needle。参考pg_opclass的默认btree/hash声明另已采集。观察程序的实际
退出0只证明采集完成，不表示数据库行为通过，更不能冒称永久回归。
该参考构建的XML输入返回0A000（未构建XML功能），这四条不能用来证明
ARRAY_POSITION的XML元素相等语义，也不能据此关闭本项目已支持的XML功能。

## 修改与验收计划

1. 先把合法typed查找、无默认相等操作符、NULL/空数组/起点顺序和真实数据行
   固化为永久原生/协议矩阵，在固定旧程序实际失败，实际参考验证预期。
2. 用数组元素的声明与真实typed值比较，保留维度/下界、NULL bitmap、精确
   NUMERIC、浮点NaN/±0、CHAR padding、TEXT/NAME、INTERVAL、JSONB与range
   等相等语义。不得仅对所展示的小数字符串裁剪零，也不得猜测字符串类型。
3. 普通SQL的anycompatible声明/UNKNOWN输入转换应在分析阶段完成，不执行
   参数、函数或子查询发现类型；真实用户callee与原生registerFunction替换
   保持各自所有权。原生直接参数cell的既有起点codec行为要与SQL声明检查
   区分，原R86断言不因新实现而放宽。
4. 缺少默认元素相等操作符不是任意文本比较；确认NULL、无NULL bitmap、
   空数组、NULL start、越界start与能力查找的优先级，再作正确拒绝。
5. 按最终真实改动重编、匹配58 receipt/57库对象与冻结输入，运行完整新增
   入口及相关数组、类型、数值/字符/JSON/range和真实前端邻居。逐项保存失败，
   更新文档后独立本地commit，不push，不借R107的45入口或旧全量代替本项。

## 实际修改方法

| 位置 | 修改 |
| --- | --- |
| `ExprEvaluator.cpp` | 真实builtin私有签名/回调标记；两/三参数anycompatible共同类型；重载评分区分伪类型声明和推导后的转换目标；按typed元素比较而不是显示文本 |
| `expr_helper.cpp` | 未声明结果的调用方原生回调保留unknown，发现类型不执行回调、不凭无参数猜成TEXT |
| `prepared_query_execution.cpp` | 常量规划保留UNKNOWN输入身份，仅明确选中的真实builtin可折叠，不调用用户函数发现常量，不规划未选COALESCE后臂 |
| `main.cpp` | 普通SQL及空结果先分析真实声明；保留同名用户callee而非按函数名套用INTEGER元数据 |
| `NetworkServer.cpp` | Parse签名预检先读真实参数OID，Describe复用同一元数据绑定及真实session；包括数组参数，不拿未绑定$n当UNKNOWN重选重载 |

NUMERIC复用精确类型比较，REAL/FLOAT8保留NaN/±0；JSONB用显式栈比较解析后的
值、数组顺序和object key/value（重复key最后值），不递归C++调用栈，也不把
数字转double。range按真实边界类型/包含标志/无穷/empty比较，其他已覆盖类型
复用既有比较器。无NULL bitmap、NULL/空数组、start及默认相等能力按实际参考
顺序处理。原生raw TEXT数组载体只在明确的无SQL来源literal边界转换，不放宽
SQL typed TEXT或全局隐式转换图；opaque替换仍接收自己的原始cell。

## 证据与边界

永久协议入口`tests/array_position_typed_equality_protocol_e2e_test.py`初版239控制，
实际180006为零失败/实际0，固定f478旧程序153失败/实际1，日志为
`r108-initial-reference.log`与`r108-initial-old-protocol.log`。补入真实同名用户
数组重载、全UNKNOWN歧义和Parse/Describe输出OID后为258控制；实际180006仍
零失败/实际0，固定旧f478为158失败/实际1，日志`r108-final-reference258.log`、
`r108-final-old-protocol258.log`。初版原生320控制，旧程序
183失败/实际1，日志`r108-final-old-native.log`；早期319/183另保留。
最终原生扩为354控制，额外检查真正的常量planning、用户callee、低层datum
校准及旧SDK raw TEXT数组载体；不把320旧版观察改写成354已经执行。两套入口保留真实DATA/声明检查、Parse阶段、NULL/起点、JSONB/range和回调
所有权控制，不借用R107通过。协议已注册一次，原生按正式命名入口注册。

候选修改ExprEvaluator、expr_helper、prepared_query_execution、Main和NetworkServer
五个CPP，暂未更改公共头或provider生命周期。先用四份自己按原O2 flags编译
的库覆盖、53份来源/凭据一致的Root库和fresh stubs运行开发原生检查，不链接
Main，不冒称真实Main/正常生产构建通过。第一次开发编译因未包含既有
array_type.h而失败、没有执行测试；已补CPP include，失败日志
`r108-development-candidate1.log`保留。第二次开发检查新增320控制有2失败：
未声明结果的原生needle回调被误推断成TEXT；原R86的208、scalar100与R107的
607控制均零失败。修复类型发现后第三次相同四个完整入口均零失败/实际0，
冻结输入及53份Root库字节校验通过；日志`r108-development-candidate3.log`。
这仍只是开发证据。五CPP的正常O2生产构建及重复构建均实际0，分别重编5/0个
CPP；未改公共头，不冒称重新编译全部58。该首版正式二进制SHA256为
`c18b2477b0e8b6c39721395712e320b14da7f4fe014a088223bb92d00292dd3f`。
首版正常cache为`bed86450801c2467a19279c6cb6819616e53e5b8cb166d4f2fbe4e2b90a6b9ad`，
首版构建前源/测试/脚本seal为
`d7ebba03266a101e0396bb0341695612116f54aaf555323ff3f5e11b2fcec198`。
该版匹配cache/fresh stubs的完整320/原208原生零失败，但239协议仍有2失败，
扩为258后7失败；原R86协议80、R107协议654及终态artifact fence均0。
失败是常量planner把UNKNOWN字面量变为VARCHAR，以及用推导后的TEXT目标参与
多态重载评分；同名用户函数还暴露数组参数Describe回退到UNKNOWN、误报INTEGER。
不是运行时数据比较已经通过就可验收。第二版修正前两项、正常/repeat重编2/0
个CPP均0；其固定二进制的完整258控制为1失败/实际1，日志
`r108-candidate2-protocol258.log`：typed数组参数在Parse前置检查时先以UNKNOWN
重载，误报42725。第三版仅修Describe，正常/repeat1/0个CPP实际0，没有跑行为
验收；第四版继续修正stored签名预检的元数据优先顺序与真实会话，正常/repeat
重编1/0个CPP实际0，完整258/原80/原654协议和终态fence均0。
新342原生有1失败，详细复验确认`COALESCE('ok',user_call)`的query descriptor
为TEXT、值为ok，未规划时低层evaluate返回VARCHAR datum，夹具误当最终投影
转换后的TEXT。Native改用明确TEXT first input，并补两条无数组/函数的原始
字面量datum/descriptor校准；真实协议中的未标注COALESCE原SQL及其TEXT OID
断言完全保留，失败日志`r108-native342-detailed-failure.log`保留。新增校准首次
又误把bare字符串的内部bound descriptor当成wire TEXT：实际为unknown、原始
datum为VARCHAR，详细日志`r108-native344-calibration-detail.log`保留；按真实
内部接口分别固定unknown/VARCHAR及显式TEXT/TEXT，未修改协议预期。第四版
最终344/原208、258/原80/原654及fence完整通过。协议字节格式、输入codec
及公共头未改变，不以342或早期344失败版算通过。

第四版完整相邻native在第三个入口`array_functions_test`实际134：它的旧原生
API用无SQL来源位置的raw TEXT LiteralExpr承载数组，严格SQL签名误拒绝42883。
不是把其断言改成错误；原入口源码/断言不变。已完成的两个native及全部26协议
实际0，独立终态fence0；只停止自己的批次控制器（实际143），让协议子批次
正常完成后才改源码，没有运行该版三Main或虚报29native通过。证据另存
`r108-candidate4-finite-native134.log`、`r108-candidate4-finite-protocols26.log`、
`r108-candidate4-finite-driver143.log`和`r108-candidate4-finite-protocol-results/`。
第五版仅在真实builtin的原生无SQL来源、全raw Literal cell场景识别该TEXT
数组载体并转成真实TEXT[]；parsed SQL、带SQL引号的typed TEXT、运行时行
仍按严格签名拒绝，不新增全局TEXT→数组隐式边。额外六个direct/bound原生
控制及四个opaque原始cell控制使总数成为354。第五版正常/repeat重编1/0个CPP
实际0；匹配cache与fresh stubs的完整354、原208、原array_functions三个native
入口和258、原80、原654三个协议入口均实际0，核心终态fence0。完整相邻
29native/26协议/3真实Main通过，再补三组numeric返回域原生/协议邻居也均0；
最终32native/29协议/3真实Main=64个不重复完整入口全部实际0，不是1403/1407全量。

真实EXPLAIN/session driver按既有O0方式、window driver及57份生产库按O2
编译；三个真实Main不链接test stubs或原程序main。window完整58控制、session
14控制均0，EXPLAIN实际0。新增28条JSONB边缘控制（Unicode/surrogate、转义、
重复key、嵌套、NULL/bool/空容器与精确数字）实际180006和当前程序各28/0，
它们是临时补充控制，不算新的注册入口，也不混入64个完整入口计数。

核心、完整批次、数值补充及最终独立fence全部0：当前binary/cache/双stamp、
全部58 source/flags receipt、57 test库逐字节和构建前冻结seal一致。完整门禁
及补充日志为`r108-final-full-driver.log`、`r108-full-native.log`、
`r108-full-protocols.log`、`r108-supplemental-driver.log`和
`r108-final-independent-artifact-fence.log`；28条补充为
`r108-jsonb-edge-reference.log`与`r108-jsonb-edge-current.log`。没有整库sanitizer
或真实TLS验收声明。24文件暂存隔离排除R98/R99和R106草稿，原273 items不变。

文档/静态检查覆盖工作区533份Markdown及1086个本地inline目标、536份Python
AST与13份shell语法，全部0；这是UTF8/文件目标/语法与状态检查，不是所有历史
技术结论的重新证明。独立暂存清单为531份Markdown、887native/513Python
注册入口，原273 items和checkbox不变；`--require-complete`仍实际1，不能宣称
总清单完成。R98六源文件的未接受差异按原内容保留，不混入本项24文件commit。

第五版正式二进制SHA256：
`75e8e272167aaaacc1f3a013ad34c9c6892d022bc7776d1b947b407947a62bf5`；
cache：`2a1fd75201a7bf23de4c481efcf9c688e7a87eba4714155f9a3df04e97b8f5b6`；
冻结源/测试/脚本seal：
`bae67a347bdee1b03db80636d0f78a73a32348bac453aecdd019bda8a46f0996`。
五个生产CPP累计修改，实际各版normal重编5/2/1/1/1、repeat均0个；无公共头改动，
相同flags/头输入compile signature维持
`fab51659cc0085b8253040241dd88de6ec4c1c51917a487cd0cc29104fbdb932`。

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`r108-array-position-probe.py`、`r108-discovery-reference.log`、
`r108-discovery-old-f478.log`保存首组24条；
`r108-array-equality-capability-probe.py`、`r108-capability-reference.log`、
`r108-capability-old-f478.log`保存后组48条和真实默认operator class声明。
两组参考/旧采集均完整终态实际0；这些是观察，不是行为验收。
固定旧快照为`r108-pre-fix-dbms-main`，SHA256为
`f478fcc9d922ed23df5b48a9ee6691a947a7852f2fed9b4e635c352920034df5`。

R86仅修了空数组NULL-start行为，不代表这些问题已修复。ARRAY_POSITIONS缺失、
任意复合/域/自定义cast及所有多态函数的通用能力不因本项存在而关闭；仍须
逐项验证/实现。原273账目不提升，R98/R99未接受候选与R106草稿保留、排除于
本项提交之外；Engine工作区含R98，不可冒称干净HEAD全量通过。Actions禁用。

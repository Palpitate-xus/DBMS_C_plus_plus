# R141：SET参数身份、引号内分隔符与独立模式设置

状态：完成有限修复。最终389专项、Root13完整入口、clean4及独立核对均实际0。
初稿81失败、第二稿21失败及各批完整终态1保留，没有删断言或改写旧日志。
[R140](issue-review-r140-timezone-set-grammar.md)与[R137](issue-review-r137-foreign-key-full-value.md)
继续用户暂缓，其实现、夹具、注册不进入本项；原273总账未完成。

## 问题、影响与修改方法

| 问题 | 已确认事实与有限处理 |
| --- | --- |
| 普通值被当成参数名 | application_name字符串含compatibility_mode，或search_path使用同名schema时，旧Main误入模式分支报XX000；改为token位置上的完整目标标识符匹配 |
| 相似/带点名称误改模式 | compatibility_mode_extra及compatibility_mode.extra旧版成功并改成extended；完整名称及限定名边界检查后走各自原路径，不再改模式 |
| 字符串等号被当作SET语法 | 普通SET先裸查等号，TO字符串含等号时错切参数名；TO和等号均使用现有引号保护扫描 |
| Extended独立设置误报事务限制 | Parse/Bind持有内部规划事务，Main误当用户BEGIN；协议层为准确识别的合法独立模式工具语句处理未执行规划事务，避免马上重建隐式事务 |
| 保留边界 | 原Main事务限制不变，BEGIN内仍拒绝切换；模式值解析、LOCAL/GLOBAL策略、未知参数错误映射及更多语法域不扩展 |

协议特殊识别仅包含既有bare或SESSION目标、等号及两个已支持的不带引号值
postgresql18/extended，不把任意SET、相似名或任意值当工具语句。
结束规划事务仍要求extendedImplicitTransaction、尚未Execute及非失败状态；
复用既有按值保存portal后结束规划事务的边界，不引用已清空map中的元素。
更多多portal、Flush、pipeline与DDL交互须独立验证，不冒完整事务协议。

## 参考、旧版与真实回归

实际PG18.6/180006的普通application_name/search_path160项控制全部通过。
项目特有compatibility_mode不拿PG不存在的参数作同义参考。
冻结旧Root fa7af302与已接受干净旧版6850729b各完整389/235断言失败/实际1；
包含后续SHOW断言，不是235个独立根因。最初28/12条观察的0只是采集完成。
首次旧389采集期间编辑了候选Main源，该批只保留作观察；随后冻结旧程序及
全部输入再完整运行389，两份旧版仍各235失败，作为最终旧红，不覆盖原日志。

[set_parameter_identity_protocol_e2e_test.py](../tests/set_parameter_identity_protocol_e2e_test.py)
389项涵盖Simple/Extended消息阶段、错误/成功tag、Ready、SHOW列名/OID/值、
完整/前后缀/带点目标、引号内数据、真正模式切换、错误后状态、显式事务限制
及新连接隔离。未知参数只验真实错误与不误改模式，不宣称旧XX000映射已对齐PG。

| 候选与完整入口 | 实际结果 |
| --- | --- |
| v1仅校正身份 | Root/clean各389中81失败；Root13为12通过/1失败、clean4为3通过/1失败，两批及独立检查1 |
| v2再保护分隔符 | Root/clean各389中21失败，揭示Extended规划事务误判；Root13/clean4仍各12+1/3+1，终态及独立检查1 |
| v3补独立工具边界 | 同一389在Root/clean均全过；最终4native+6protocol+3真实Main=13有序唯一完整入口全0，clean4全0 |
| 独立终态 | 逐入口完整性、旧红/中间失败、源/对象/二进制/receipt、冻结候选和暂缓字节均核对，实际0 |

夹具始终同一字节SHA256：
`6f36cd977856913b852e85de8ec4910a39a3066471c5830d8d2d2df5406e39b6`。
Root13还完整执行parser_phase1、SET TRANSACTION、SHOW isolation、search_path状态
原生控制，及pg_settings、search_path事务、协议会话元数据、SET TRANSACTION协议
与SQL字面量保真；没有只抽取其中断言，也没有删除或降低原389的期望。

## 构建与对象来源

v1、v2各正常O2重编1Main/repeat0，各fresh编译三个真实Main测试TU：
explain/session沿用基线O0，window O2。最终v3只有私有Network CPP改变，
正常O2重编1Network/repeat0；Main源3066209b、完整头/compiler/flags 348f43c0及
生产Main对象947bb6f0未变。最终复用v2的fresh TU对象，不冒称v3重编：
核对四份Main/测试源hash、全部头/flags、对象SHA及新-MM依赖；
仅撤回Network四个私有块即可重建v2源封印34c15361，依赖中没有NetworkServer.cpp。
随后新链接当前57个匹配生产O2库，无测试stubs并经既有隔离机制实际执行。

三份测试源来自已接受c63d1ea9真实夹具，仅投影Main包含路径；实际session290次
命令，未运行用户跳过的R140待定311扩展。Main.cpp仍是工作区真实生产源码。
干净组合另以c63加本项Main/Network/注册/协议四路径构成，排除所有原始待定代码。
真实server为fresh O2 Network、核对Main源/全部头/原编译证据/SHA的v2干净Main，
以及56个核对CPP/头/全部58原receipt/SHA的R136生产donor；
不是normal fresh58，也未链接tests/test_stubs.cpp或证明真实TLS。

## 文档、版本与范围

手册模式值示例改用已有实现支持的bare写法，补充普通值不会切换模式的说明；
不是新增quoted模式值。完整GUC语法/参数域、其余quoted目标、事务级LOCAL、
LOCAL/GLOBAL策略与更广pipeline/DDL交互不据本项完成。

最终Root binary为`90b3f70a2ffbaa723380febda8bb4607889da35d7568f6dd1c450953389965d1`，
cache为`9acf2235904976f41311ebb3cf9aa8bc02e0535ec0be7fb1129c09c53a8e71d2`，
源/测试/脚本封印为`8ddaaf74b3d057715aec3dbe47ad67a4c155a89182076b21c0a6e597b44e9d68`；
clean binary为`878094b7395f6d460afeca6ce269de64f2fdce724c1a8c82a90a5e5a4b925252`。
本机目录`/tmp/dbms-review-r46.ifu6SWLp/`保留reference160、两份冻结旧389、
v1/v2全部完整失败日志/快照、v3构建/最终13/clean4及独立日志。
所有旧clean与R136 donor保持全tracked冻结，原始56933字节补丁和三份草稿原样核对。

本项仅接受Main两私有块、Network四私有块、一条注册、新协议夹具与相关文档，
无公共头/布局/heap修改。R140六路径候选、R137实现/两测试和R98/R99/R109等待定
字节均保留，不混入提交；独立检查只撤回本项自身块后逐字节比较。
已接受564MD、902native+539Python+3Main=1444；工作区567MD、
907native+544Python+3Main=1454另含十个未接受入口，不视为已接受全量。
无新1444全注册/TLS/整库sanitizer证明；旧1436两失败/1TLSskip保留。
原273及22complete/166partial/70unverified/15deferred不变。不push，Actions保持禁用。

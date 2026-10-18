# R138：显式SESSION的TimeZone设置误走普通配置路径

状态：本项有限修复和验证完成。最终Root30个不同完整入口、干净候选四入口及独立核对均实际0。
本项独立于按用户要求暂缓的[R137](issue-review-r137-foreign-key-full-value.md)，
不恢复其外键实现或验证。

## 问题、影响与处理方法

| 项目 | 事实与方法 |
| --- | --- |
| 合法命令失败 | `SET SESSION TimeZone TO 'Asia/Shanghai'`、等号写法及`SET SESSION TIME ZONE`在已接受旧版报XX000，SHOW保持旧时区；实际PG18.6成功 |
| 原因 | 专用时区分支只识别set timezone/set time zone；显式SESSION落入通用Config分支，后者没有对应的会话TimeZone实现 |
| 修复 | 仅给时区分支准备移除开头SESSION的正规化语句，其它SET仍使用原sql；复用原来的TO/等号、值解析、POSIX规则、IANA名称与会话字段更新 |
| 错误边界 | 引号内TO、非法名称和空值仍报22023；失败不改变时区，不吞引号内数据 |
| 未改范围 | 不修改公共头、parser、heap、事务GUC快照、LOCAL、DEFAULT、认证/权限处理或所有SET语法；不宣称这些仍开放问题已完成 |

12条观察在实际PG18.6/180006与冻结旧程序各完整采集，诊断退出0只表示
观察完成，不表示兼容通过。有效SESSION命令的执行失败和随后SHOW旧值均保留。

## 回归设计与已完成的旧红

[timezone_set_to_protocol_e2e_test.py](../tests/timezone_set_to_protocol_e2e_test.py)
保留原112项控制，增加相同SESSION语义的112项，总224：
六个时区、TO/等号/专用TIME ZONE、混合关键字空白、Simple与Extended、
完整SQLSTATE/消息帧/command tag/Ready、SHOW列名/OID/值及错误后恢复。
实际PG参考224全过，冻结旧Root和已接受R136干净旧程序各224完整/56失败/实际1；
两份旧红相同，不能归因于本次工作区保留的R137等未接受候选。

[真实session前端](../tests/frontend/session_execution_lifetime.cpp)增加三条实际
dispatcher命令及caller/parent会话隔离、offset和失败保留值断言；原生命周期、
触发器及错误断言保留。Main生产源已改变，本轮三个真实前端测试TU全部fresh编译，
没有复用R136/R132对象；随后新链接当前57个生产O2库，无stubs并实际隔离执行。

## 首次门禁的验证脚本失败

首次Root尝试中12native及15protocol实际全0，但三个Main wrapper读取了不存在的
`r138-real-main-link.log`，在实际driver执行前各退出2；整批实际1。正确生成的
日志名为`r138-main-link.log`。原始日志保留，不把三个未运行入口记为通过或
数据库失败。只修正外部wrapper路径，新目录下用同一冻结输入重新运行完整30项；
最终完整30项全0，初次终态1不覆盖。干净候选的四个完整协议入口也实际全0。

## R139：DEFAULT仍是另一个普通未修复项

另有13条冻结旧程序/实际PG18.6观察，见`probe-timezone-default.py`及
`r139-timezone-default-reference-discovery.log`、
`r139-timezone-default-old83c-discovery.log`。
参考的四种DEFAULT语法均成功恢复本连接启动值；旧版裸DEFAULT报22023，
SESSION形式另误报XX000，仍保留Asia/Shanghai。参考启动Etc/UTC与Root启动UTC
不同不是这项bug；应分别与各自启动基线比较。采集退出0不表示行为通过。

修改方法是识别未加引号的DEFAULT关键字，从`Session.defaultTimeZone`恢复名称，
通过同一个类型值解析器重新计算offset，不把字符串`'DEFAULT'`当关键字；
另检RESET、会话隔离、Simple/Extended和真实时间计算。LOCAL及完整事务GUC恢复
须独立验证。本项只修SESSION路由，DEFAULT仍开放，不计用户暂缓或已修复。

## 验证和版本边界

| 验证 | 实际结果 |
| --- | --- |
| 正常构建/repeat | 正常O2只重编Main一个CPP、repeat为0，两次实际0；完整头/compiler/flags仍348f43c0 |
| 最终Root完整门禁 | 12native+15wire+3真实Main=30个有序、唯一、完整入口全0；新增224、原时间/类型/会话邻居全部执行 |
| 三个Main | 全部fresh TU；explain/session使用既有O0、window O2，生产库保持O2。真实session17控制、window58控制及explain错误清理均0 |
| 干净候选 | 2f429af3加本项三个路径；1个fresh O2 Main+57个逐一验证全部头/CPP/原58receipt/预先对象SHA的R136生产donor库。真实server无stubs，四个完整协议入口0；不是normal fresh58构建 |
| 独立检查 | 30名字/顺序/次数、clean4、旧新224、Main四份源hash/三个对象/新链接字节、当前58receipt/双cache/57库字节、两候选与donor冻结输入均核对0；R137源码/测试/注册与跳过快照逐字节相同 |

干净组合明确排除R137、R98/R99/R109等未接受候选。仅自有生产hunk、两处既有
测试增强与相关文档独立提交；原始候选字节保护与暂存范围检查须在提交前通过。

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`probe-session-timezone.py`、`r138-session-timezone-reference-discovery.log`、
`r138-session-timezone-old83c-discovery.log`、`r138-reference224.log`、
`r138-old83c224.log`及`r138-clean-old224.log`。
本轮源/测试/脚本冻结封印为
`4bcbd8bf644c39ce361fb8e2a761e5930e24cf270d14cd04026367e4a60a2535`。

最终日志为`r138-v2-final-gate.log`、`r138-v2-final-native-results/`、
`r138-v2-final-protocol-results/`、`r138-v2-*-frontend.log`、
`r138-clean-prep.log`、`r138-clean-gate.log`及`r138-v2-independent-final.log`；
正常/repeat日志和首次`r138-final-gate.log`等也保留，没有覆盖失败记录。
Root binary为
`cb786166796fb95e576524aa4ca7c6a1f9598a6f7e6e6593b148302ac02b8cf9`，
cache为`0c076de31ab5a3473f0af04c5c33e4b0d91b8d168f284842af4bf88128223cc1`。
干净源码`/tmp/dbms-r138-clean-source.X4EMclYc`全tracked保持冻结，
程序在`/tmp/dbms-r138-clean-binaries.Mvf2T0v7/dbms_main`，SHA为
`23ee879bd0d989e0487ed931f78dccf0163ac0b51f5a05f5ff1e8ae95f3702d1`。

接受561MD、902native+537Python+3Main=1442，既有入口数量不变；
工作区另含原六入口与R137两候选，为564MD、906native+541Python+3Main=1450。
562份Python AST/13份Bash语法与本地文档目标另核对。没有新1442全量、真实TLS
或整库sanitizer证明；[历史完整1436](issue-full-registered-ecf632e9.md)仍为
1433实际通过/2失败/1TLS环境skip、终态1，不因本轮有限通过改写。

原273条目/勾选保持22complete/166partial/70unverified/15deferred；
新增有限修复不代表总清单完成。原始未提交字节与R137候选保留且排除本项提交。
不push，GitHub Actions保持禁用。

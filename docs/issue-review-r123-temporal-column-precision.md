# R123：时间类型列的声明精度在schema和结果描述中丢失

状态：本范围合法列精度元数据已修复，782项原生、104项协议和共享48个完整
入口均通过。R124独立路由修复先单独提交；原六个失败断言和日志不删除。

上游独立提交为`3d45bd1e68ae8ceddd74fd53b722e4c082ac816c`；其提交前后只读
复核均0，本项四CPP、两个测试及所属文档随后独立提交，不混入该上游版本。

## 实际发现及修改方法

TIME、TIMETZ、TIMESTAMP、TIMESTAMPTZ及数组的合法声明精度被当作未指定
（-1）。空结果/NULL也发布错误typmod；这与显式CAST值尚未按precision舍入
是两个问题，后者继续见[R122](issue-review-r122-time-explicit-precision.md)。

| 路径 | 原因及修复方法 |
| --- | --- |
| TypeRegistry | resolveColumnType仅保留interval/numeric modifier；四种时间类型保留合法precision，validateColumn接受0–6并拒绝其他持久化modifier |
| DDL与目录属性 | 列工厂替换Column时丢失modifier；在工厂后恢复合法声明，目录atttypmod从声明获取，不把precision写进dsize或attlen |
| schema写入/冷读取 | 既有typed-modifier扩展只允许interval/numeric；双向增加真实时间类型及0–6验证，沿用0x4442000c扩展，无修饰符继续原格式 |
| 协议物理投影 | 结构化标量类型不继承时间列precision；仅对真实物理ColumnRef及相同声明身份复制precision，保留权威逻辑OID/长度，不借同名CAST别名猜来源 |

既有物理宽度保持4/4/8/8，逻辑长度仍8/12/8/8，数组为-1。省略(-1)和
显式6不能合并，precision=0不能作为false丢掉；不迁移旧行。

## 已完成的复现与第一版验证

永久`tests/temporal_column_precision_test.cpp`782项检查覆盖四类型的
scalar/array、0/3/6/省略、TypeRegistry、schema、目录modifier/真实OID/attnum、
逻辑与物理宽度分离、新建/无关RENAME/目录evict/独立StorageEngine冷读、NULL
及整秒TIME行保持、旧/扩展格式、非法registry modifier守卫。
32列分为两张16列表，遵守当前MAX_COLUMNS=30；初始化前隔离执行。
完整旧782项218失败、实际1；第一版当前782项零失败、实际0。
更早776夹具的raw C数组.begin()编译错误及单表超30列已修正，日志保留，
未把夹具编译错误当作产品红灯或完整旧782证明。

永久`tests/temporal_column_precision_protocol_e2e_test.py`104项控制在实际
PostgreSQL18.6（180006）全部通过。固定旧程序完整104项94失败、实际1；
第一版当前仍六项失败，均为[R124](issue-review-r124-timetz-postfix-projection.md)
独立的NULL::timetz表投影42703，不是schema修复已经全过。
控制覆盖普通/TEMP、四类型scalar/array、0/3/6/省略、空/NULL、Simple/Describe/
Extended text与binary NULL、RETURNING、真实列别名、同名CAST无来源守卫、
无关RENAME、ALTER TYPE、LIKE及整秒TIME字节，不证明非NULL TIMETZ binary。

日志目录`/tmp/dbms-review-r46.ifu6SWLp/`内保留`r123-old-native782.log`、
`r123-reference104-initial.log`、`r123-old-protocol104.log`、
`r123-c1-native782.log`、`r123-c1-protocol104.log`。
固定旧产物`r123-pre-fix-dbms-main` SHA256：
`dd282eeaf175aea11453fc3fa224cb72e6d906a7a038425bd060e747dd73512f`；
第一版`r123-c1-dbms-main` SHA256：
`5729d2bd85834c53290f6ea88413d436b6ce4694c2a36ce04c171130fac749d4`。
均为本工作区组合而非clean HEAD，历史失败不由后续成功覆盖。

## 数据恢复及未完成边界

旧schema/目录从未保存的precision不能根据数据、小数位或物理宽度恢复。
保留备份，只有原始DDL或其他可信声明明确时，才在副本验证重新声明/导入；
不能声称找回整秒heap已丢失的小数，不自动改写真实用户表。

本项只保留合法列元数据。CAST/常量/表达式precision值和AST typmod、负数/
小数/多参数/+号分析阶段、过高precision warning/clamp、完整时态输入、旧秒
heap升级及binary Bind等仍普通开放，不改记为用户暂缓。
原273仍22complete/166partial/70unverified/15deferred；R109及明确暂缓项不重开，
R98/R99/R106候选不混入；Actions禁用，不push。

## 最终有限验收

本项四CPP的生产normal/repeat实际重编4/0且均0，无公共头/Main变更。
R124另改Main，之后生产Main及三个真实前端TU均按当前源码重编，连接57份
当前正常O2库、无stubs并隔离执行。共享21native+24协议+3真实Main=48个
不同完整入口全部0，含本项782/104、R124的96、schema格式、interval列modifier
冷暖加载、真实日期全域和时间/数组/目录长度等邻居。最终原六个42703全消失。
这是同一次匹配组合的真实终态，不另捏造R124提交后又编译/重跑48入口。

`r123-final-{driver,native,core,neighbor}.log`及结果目录、三个frontend日志、
`r124-real-main-object-compile.log`/link日志和
`r123-r124-shared-independent-check.log`保留完整结果；双缓存、58 receipt、
57份test库字节与封印独立复核0。R123和R124保持两个提交的所属hunk边界。
冻结程序SHA256
`e593e92a7ea662f97e8d47f5c277d7deb6e58a54c3d5ee2030c25630077b194b`，
缓存`26b6ac52029eec3ec463279d16ade9a78e57110697b7d326eeca4125e3246796`，
源/测试/脚本封印`2c34b2a40cd35c01f1c606d4a506268cb0ecbe44309936c9e7fb96fbaa014478`。
公共头/生产flags签名仍为
`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。

本项接受545MD、895native+526Python+3Main=1424；工作区548MD、
898+529+3=1430含原六个待接受入口。550Python AST/13Bash、本地链接/清单
另作静态复核，未获得新完整注册、真实TLS或整库sanitizer证明。
原56933字节候选源补丁和三个草稿字节另行复核，不把保留候选带入版本。

额外完整重采R122原122条观察，四条真实TEMP列的空/NULL text-binary观察现在
精确匹配，剩余118条仍全部是CAST/常量/表达式描述符及声明边界/通知。
`r123-time-precision-after122.log`及其comparison日志保存采集/比较实际0；
这不是122条全过，也不把R122从普通未完成项移走。

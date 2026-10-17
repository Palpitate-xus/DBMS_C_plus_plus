# R132 通用SET TimeZone的TO被当作时区值

状态：本项修复及30入口有限验收完成，不等于整个GUC或原273总账完成。
来源为[R131](issue-review-r131-timestamptz-binary-timezone.md)初版setup发现，独立处理。

## 期望、实际与影响

PostgreSQL支持通用`SET TimeZone TO 'Asia/Shanghai'`和等号写法，以及专门的
`SET TIME ZONE 'Asia/Shanghai'`。TO是通用SET的分隔关键字，不属于时区值；
引号内的`'TO UTC'`却是完整数据，仍应作为无效时区拒绝。失败不应改变原设置。

实际PG18.6（180006）永久112控制全部通过。冻结R131程序完整112中44失败、
实际1：TO写法报22023，随后SHOW仍为之前的时区；等号与TIME ZONE写法及错误后
保持值控制用于区分。不是TO参数真的无效，更不是已经设置成功后只有SHOW错误。

解析器的SetStmt已消费TO；实际Main旧路由却只按固定前缀截取、消费等号，把
`to 'Asia/Shanghai'`整体交给时区值校验。SQL正规化已统一引号外关键字大小写
和空白，同时保留引号内数据，所以这里需要消费一个开头的独立TO token，
不全局删除字符串或改时区名称解析。

## 修改方法与永久回归

| 位置 | 方法 |
| --- | --- |
| Main的TimeZone路由 | 仅通用set timezone前缀后的独立`to `分隔符被消费；等号与专用TIME ZONE现有值路由保持，引号内TO不改 |
| timezone_set_to_protocol_e2e_test.py | 112控制：六个真实时区、TO/混合关键字与制表换行/=/TIME ZONE、Simple/Extended、SHOW值/列名/OID/tag、四种非法值与错误后的原设置/ready恢复 |

本项不称整个GUC语法或事务语义完成：SET LOCAL/SESSION、DEFAULT、所有非法
分隔符的解析阶段、INTERVAL形式和完整事务恢复等仍是普通开放范围，不改记
用户暂缓。特意不把TO消费扩到专用`SET TIME ZONE`语法，或吞掉引号内内容。
没有公共头、解析器或存储格式更改，不重开安全/认证等用户暂缓专项。

## 实际证据与验收计划

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`。

- `r132-reference112.log`实际112/0；`r132-old112.log`完整112/44、实际1。
  旧程序另存`r132-pre-fix-dbms-main`，SHA256
  `4ed66b3fbea69226b8372b8e59d387abffca867b8272725c87b0ddd76fedfc7d`。
- Main已改变，旧R124真实前端对象未复用。生产Main按正常O2实际重编1个CPP、
  repeat为0个，均实际0；三个真实前端也全部fresh编译：explain/session测试TU
  按既有O0、window及生产对象O2。全部编译结束后才链接57份当前正常O2库、
  不链接stubs并实际隔离执行三个前端，均0。编译可并行，数据库/测试批次串行，
  没有停止健康编译或降低生产优化级别。
- 最终12native/15wire/3真实Main=30个不同完整入口实际全0，独立检查0。
  新112协议及R131原158均0；全部58 receipt/双缓存/57库字节/冻结源测试脚本
  终态核对0。日志为`r132-normal-build.log`、`r132-repeat-build.log`、
  `r132-real-main-object-compile.log`、`r132-real-main-link.log`、
  `r132-final-driver.log`、native/protocols及三个frontend日志和
  `r132-final-independent.log`。当前Main/头/flags/对象来源另存
  `r132-reusable-main-unit-receipt.json`，将来只有所有输入匹配才可复用测试TU，
  且仍须新链接当前库并实际运行；不把此轮fresh编译说成旧对象复用。
  旧112失败及R131最初setup六错误均不覆盖；没有新1435完整注册/真实TLS/
  整库sanitizer证明。原8f完整三失败及R129/R130各自有限修复仍按对应输入读取。

程序SHA256为`c3deb2adf12aa29479d372ea86a1846c38fd117fa02aa0ad2f5a4ff4c96d40e7`，
cache为`fce78c013ca8bee1b4b0cce74524b15fc4d06593f380c23ab4d9b4e9674d4728`，
源/测试/脚本seal为
`3dda78497ce2876681b5c15dcfc488a7fb09f4800a7c9285f23f77bc027ee3fb`。
头/生产flags签名仍739bf42b；Main source改变不等于公共头改变或全部58重编。
接受554MD、899native/533Python/3Main=1435，工作区557MD、902/536/3=1441
另含原六候选；557Python AST/13Bash及本地文件目标另核对。隔离暂存只含本项
hunk，按独立本地commit保存；原56933字节候选patch和三个跳过草稿逐字节保护0。

原273统计与勾选维持22complete/166partial/70unverified/15deferred。
不push，Actions保持禁用，原R98/R99/R109及跳过草稿不混入提交。

# R122：显式TIME精度的值、描述符和边界行为缺失

状态：已实际复现，尚未实现，不计完成。继续按问题分别修复、验证和本地
独立提交；不push，不恢复用户已明确跳过的模块。

## 实际证据

R121完整97条复查中，默认基本clock的76条已全部匹配，剩余21条均为
TIME(0–6)的值或结果typmod。进一步在实际PostgreSQL18.6（180006）及固定
R121产物上各采集122条观察，归一化比较的122条都有差异。这不是122个独立
bug，也不代表每个值都错：精度6的部分binary值正确，但描述符仍错误。

`/tmp/dbms-review-r46.ifu6SWLp/r122-time-precision-{reference,old}122.log`保留
原始观察；`r122-time-precision122-comparison.log`记录逐项差异。两次采集和
比较实际0仅表示完成收集，不是数据库行为全过。固定旧产物
`r122-pre-fix-dbms-main` SHA256：
`dd282eeaf175aea11453fc3fa224cb72e6d906a7a038425bd060e747dd73512f`。
它是R121已验收组合，仍含原保留候选，不是clean HEAD全量证明。

| 范围 | 实测影响及方法 |
| --- | --- |
| CAST、::、带精度的TIME常量、数组 | TIME(0–6)被当作原文透传，未执行相应舍入；以实际参考区分默认六位输入舍入和显式precision阶段，再测试text及真实8字节binary |
| NULL、无行和限定/命名类型 | 即使无datum，结果typmod仍应是声明值而非-1；从真实AST声明及真实类型身份推导，不能执行值猜元数据或猜用户OID |
| TEMP表空/NULL的precision元数据 | 实测a TIME(0)、b TIME(3)、c TIME(6)、d TIME(3)[]均发布-1；独立核查Column、目录属性及物理投影路径，另项修复schema/目录元数据丢失，不混同cast值舍入 |
| 负数/小数/多参数/+号的TIME关键字精度 | 这些实测关键字CAST形式参考42601；项目部分错误接受，+3虽报同码但已发布结果描述符；保留声明角色及分析时机，限定/命名类型边界须另实测，不能套用猜测SQLSTATE |
| TIME(7) | 参考降为6并发WARNING、SQLSTATE22023、消息TIME(7) precision reduced to maximum allowed, 6；项目忽略精度且无warning，需独立落实clamp及通知渠道 |

观察覆盖：七个精度的CAST/::/常量/array/NULL/NULL-array/无行各两种格式；
pg_catalog及quoted/named-array/without-zone声明；精度边界；真实TEMP表空和
NULL查询。表来源OID只比较有无来源及attnum，不把不同服务器的OID数字当同一值。
原始notice含PG的P/F/L/R等实现位置字段；永久兼容控制应明确比较用户可见的
severity/SQLSTATE/message，不要求本项目冒充PostgreSQL源文件和行号。

## 实施与验收计划

先独立修复表声明/目录/投影的precision丢失，保持现有物理宽度与来源守卫。
核查新建/ALTER、scalar/array、精度0、冷热加载、普通/TEMP、空/NULL以及
char/bit/numeric/interval邻居；对旧格式未保存的声明不能凭空猜精度，须记录
实际可恢复信息及从已知原DDL重新声明的方法。

随后以独立期望验证显式cast的值及描述符、NULL/无行/Describe/text/binary等，
保留所有旧红灯。默认输入与显式precision是两个阶段，不能只在sender舍入。
边界语法/通知和不同声明角色继续分别实际对照；不能依赖未被接受的R98
emitWarning接口或偷偷混入R98/R99/R109候选。若用已有稳定的协议通知结构，
须验证发出时机、次数及SQLSTATE，不能用普通行文本伪装warning。

既有4字节标量TIME、完整微秒存储/迁移、完整grammar/cast eligibility、binary
Bind及其他时态能力仍普通开放；本项不会通过重写旧行或修改typlen伪装完成。
原273仍22complete/166partial/70unverified/15deferred，勾选不提升，Actions禁用。

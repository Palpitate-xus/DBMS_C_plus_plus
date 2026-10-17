# R115：混合数组星号投影丢失字符列 typmod

状态：本范围已修复并完成有限验收，按本项独立本地提交；不计入原273项完成数。

## 复现与影响

`CREATE TABLE t(c CHAR(3),v VARCHAR(5),a INTEGER[])` 后，简单协议的
`SELECT * FROM t WHERE false` 将字符列 typmod 错报为 -1，而应为7和9。
显式选择 c/v、Parse/Describe 和多数不含数组的表正常。客户端因此无法获得
真实声明长度；与行数、NULL或实际字符串内容无关。

问题在R114的完整28条协议控制中暴露为5条CHAR修饰符失败。对照修复前日志，
这是既有问题，不是R114的逻辑长度修复引入的回归。进一步34条完整观察采集
区分标量和数组伴随字段，数组星号投影才触发；采集退出0不等同全部行为通过。

## 原因与修改方法

包含数组的星号投影会发布规范标量拼写 character / character varying，
而物理声明仍为 char / varchar。协议适配器仅对数组/域规范化类型身份，
普通标量按字符串逐字比较，错误跳过真实列目录的typmod。

仅修改NetworkServer.cpp中已有白名单的类型匹配：标量也比较规范类型身份。
继续要求解析器证明其为真实物理列投影；不按输出别名继承来源、不重新执行
表达式或读取数据猜测类型，不放宽内部"char"、具名类型或用户类型身份。
物理存储、目录写入、参数输入和实际返回值不变。

## 永久验证与中间证据

`tests/type_alias_projection_metadata_protocol_e2e_test.py` 完整228控制：
普通/TEMP表、含/不含数组、空表/全NULL/实际值、星号/列名/互换别名/限定列、
同名计算表达式/显式TEXT转换，以及原14字段混合表的星号对照。三种协议
模式逐条检查OID、逻辑长度、typmod、真实attribute/origin、格式、数据和tag。
原120断言全部保留；额外108控制覆盖不含日期/时间的最小复现。

实际PostgreSQL18.6（180006）228全部通过；固定旧程序228中13失败、实际退出1。
最初54/108控制未包含数组伴随列，旧/参考均通过；后续120旧1失败，再扩充228。
这些历史结果均保留，不把原先未复现说成修复证据。

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r115-reference54-initial.log`、`r115-old54-initial.log`、`r115-reference108.log`、
`r115-old108.log`、`r115-reference120.log`、`r115-old120.log`、
`r115-reference228.log`、`r115-old228.log`、`r115-star-type-old.log`。
旧程序`r115-pre-fix-dbms-main` SHA256：
`510564cbd48b6ff15418e066be066c773728d25b6ccdbdd872229a7fa5f7aff8`。
它含R114及既有保留候选，不是clean HEAD。混合14字段的正确宽度另由R114
提供；本项自身新增的最小数组复现不依赖其日期/时间宽度修复。

## 验收与范围

本项与R114共享最终冻结输入的15个完整native、21个完整协议，共36个不重复
入口，全部实际退出0。含本项228、R114的102/28、目录complete-read691、
冷暖snapshot/clean publication/失败回滚/域/数组/字节类型/真实列来源与默认
完整协议等。证据为`r114-r115-final-{driver,core,native,neighbor}.log`及逐协议
日志；没有只跑通过子集或更改原断言、超时。R114首批整体1保留，不作为最终绿。

自身normal/repeat重编1/0个CPP、均实际0；R114先前3/0及closure0/0不是本项
新编58份。生产及所有库为O2，native fresh stubs/隔离器。最终binary：
`b4128ced2d381c48f7c783550cb69753ce99d7924f851a4f801f576cfce393dc`；cache：
`4432f1879906d8c4b101a16fc5022095961dd083d923956a3c0d07e94a04c917`；
src/tests/scripts seal：
`500f83626c629c534525c21bc6bd52b8c1d221094bb3d9b46c3afdfdfb363dcf`。
终态双stamp、全部58生产receipt、57测试库字节及冻结输入/公共头核对0。

工作区538份Markdown、542个Python AST、13个Bash语法与本地内联目标检查0。
原273一致性0、require-complete真实1；不代表所有历史文档技术结论已经逐条重证。
本项精确12路径：NetworkServer.cpp单个别名匹配hunk、一个协议测试/注册及文档；
不带R114长度hunk、catalog/Ddl改动或其测试/注册。接受投影534MD/888native/
516Python，组合1407。工作区892+3+521=1416含R98/R109、R113和待单独提交R114。
保留原56933字节补丁重建后的源码逐字节对照及三个暂缓草稿SHA检查0；首次
difflib与git切hunk算法不同导致假差异，改用Git应用原补丁后逐字节比较，日志保留。

这不是clean HEAD全量证明；未运行新的完整注册集合，不复用历史全量，未证明
真实TLS或整库sanitizer。R113间隔发送codec、R111规划及R114发现的其他目录/
视图问题仍普通开放。R109用户跳过、候选不提交；不push，Actions禁用。

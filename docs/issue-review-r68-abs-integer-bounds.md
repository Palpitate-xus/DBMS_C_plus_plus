# R68：ABS整数范围与常量分析阶段

状态：本项整数范围与ABS常量规划已修复，通过当前主工作区定向门禁；不是用户暂缓项。

实际PostgreSQL18.6对SELECT abs(-2147483648)报22003，包括FROM空表
WHERE false和LIMIT 0。当前主分支普通执行返回2147483648并声明OID23（INT4），
数值已超出自身声明范围；空来源/LIMIT 0则静默返回零行。
abs('-9223372036854775808'::BIGINT)的普通执行当前确实报22003，但空来源和
LIMIT 0不报错；实际PG三种情况均报22003。不能把已有INT8运行时错误也说成失败。
合法abs(7)与abs(1.25::NUMERIC)的值及23/1700声明在本次对照中正确。

源码builtin ABS按int64解析所有整数，只直接防止int64最低值取负，没有按实际
smallint/integer/bigint结果类型限制最小值。另有纯常量规划阶段差异：现有
immutable常量分析未在空输入/LIMIT 0前观察这些ABS错误。

修改方法：按真实已解析签名的声明结果类型检查范围，保留精确整数而非double；
把合法常量分析接入现有纯immutable常量路径，不执行VOLATILE/STABLE函数或
子查询探测结果。分别测试INT2/INT4/INT8边界、NULL、合法值、精确SQLSTATE、
普通/prepared/空来源/LIMIT 0和错误后的连接可用性。常量阶段和执行阶段均需
验收，不能只补一个运行时if就关闭本项。签名不存在的错误仍归R62。

证据为`/tmp/dbms-review-r46.ifu6SWLp/r62-plain-signature-expanded-discovery.log`，
同目录保留脚本。它逐条记录实际PG及R60集成后主分支的原始rows/SQLSTATE/OID，
参考TEMP表无数据，未改变共享参考对象。这是本机证据路径而非发行产物。

## 实现与实际验收

ABS按已解析的smallint/integer/bigint声明检查最小值和上界，使用精确整数并抛
结构化22003；合法NUMERIC与NULL声明保持不变。既有常量规划只对实际解析为
pg_catalog.abs且参数均是结构常量的调用求值；同名用户routine（即使IMMUTABLE）、
STABLE/VOLATILE callback、参数值、子查询及cursor均不被执行探测。UNKNOWN字面量
先保留选定的ABS输入声明，避免折叠后误变VARCHAR重载。

Main旧CAST/text接收器可能在空输入时绕过native plan，因此真实AST前置分析复用
相同PreparedQueryExecution纯规划，不打开source/provider。SQL PREPARE仍只做
声明分析，不把执行期常量错误提前到PREPARE。CASE/COALESCE的死分支保持不求值。

实际PG18.6新67项协议全0，修改前67项中23失败；原40项纯规划控制中22失败。
最终native加强为42项，包含合法UNKNOWN/NULL及零child/cursor/sequence副作用；
当前九个完整native与五个完整协议驱动全0，包含本项67及R62原132检查。
开始/结束当前生产输入、binary、58份receipt一致。

本机最终日志：`r68-final-accepted-native.log`、`r68-final-accepted-protocols.log`
及对应独立gate/driver目录。标准O2的`r68-root-build.log`保留首轮缺少直接include
的编译失败，后续second/third/fourth-root-build日志保留真正编译/link结果；没有
公共头变化，没有借用其他树对象。第一次九native全0仍有六wire空输入遗漏；
补Main后67全0但R62另有两项UNKNOWN回归，均保留在final/verified旧日志，不改成
通过。最终加强42控制及原132邻居均重新完整运行；未据此宣称全注册回归完成。

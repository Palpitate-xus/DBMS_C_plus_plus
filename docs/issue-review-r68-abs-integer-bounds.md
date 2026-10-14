# R68：ABS整数范围与常量分析阶段

状态：实际复现，尚未修复；不是用户暂缓项。

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

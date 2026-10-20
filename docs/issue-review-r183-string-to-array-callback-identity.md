# R183：STRING_TO_ARRAY原生回调的返回类型身份

状态：有限回调身份修复已验收。后续R182拆分性能已独立完成V2验收，两项仍分项记录。

## 问题与修改方法

| 问题 | 实际影响 | 修复 |
| --- | --- | --- |
| 原生registerFunction替换后仍按builtin名字推断text[] | 返回TEXT/INTEGER/BYTEA被错误送入数组codec，22P02或错类型 | 私有marker只标记真实builtin callback；替换callback不具有marker |
| 元数据仍冒称builtin而非native | opaque返回声明被名字fallback补回数组 | 实际callback身份变native，返回声明未知；复用现有opaque类型推断，不执行callback猜类型 |
| 真正builtin仍需规范数组类型 | 不能为避免异常而取消全部数组结果规范化 | marker拥有text[]元数据，保留原builtin的NULL/空数组/null_string及输出类型 |

仅修改[ExprEvaluator.cpp](../src/expression/ExprEvaluator.cpp)四个拥有hunk：marker、实际builtin返回元数据、
opaque身份及初始化包装。未修改公共头/AST、SQL语法、输入签名、函数arity或数组输入codec。
正确值/类型和真正显式数组转换不因本项变得宽松；callback未知返回值不提前执行。
本项不声称修复其他同名builtin回调、全函数签名或所有数组功能。

## 完整验证

- 新native60完整执行。六builtin阳性含普通/quoted/pg_catalog名称、空输入、NULL和null_string；
  精确验证text[]结果/纯返回元数据/builtin身份与值。
- 54个opaque组合：两种volatility、三种名称、1/2/3实参、TEXT/INTEGER/BYTEA返回；
  检查native身份、未知返回声明、元数据与绑定零执行、实际调用一次、arity/类型/值均保留。
  异常均记录失败后继续，不通过提前中止或截取阳性部分。
- 冻结旧accepted R181的新native60实际1，完整footer60 FAILURES60；不是60个独立bug。
  Root和accepted-only clean各完整21入口实际全0：13native、5wire、3真实Main，
  含原完整数组库、新60、原80/96/294/80/190/654，全部原断言/timeout/持久化行为保留。
- 每侧75对象/17当前程序/127头核对。fresh1生产Evaluator、2native（新60和原数组库）；
  57生产、11native、stub和3真实Main为源/头/编译器/对象hash匹配的donor，未冒称fresh。
  所有程序新链接当前库，Main来自accepted测试体的已验证单一include路径投影，
  不带test_stubs，不夹带Root未接受session测试修改。两侧Main/公共头均未变，合法复用。
- Root normal实际1CPP重编并链接，repeat零编译零链接；clean显式构建，不冒称wrapper缓存测试。
  全部2204已接受源码投影与tested clean相同；Root2323源码只增加新测试及4个CPP hunk，
  2322旧源码与118cache/server先复制核对，其他未接受工作区改动保留，不混入commit。

## R182首轮失败与验收边界（历史）

后续R182 V2已保留原失败、纠正错误oracle并完成独立门禁，见[R182记录](issue-review-r182-utf8-split-linear.md)。
以下保留本项R183验收当时的计划、失败与未执行范围，不将R183历史结果改写为R182通过。

[R182](issue-review-r182-utf8-split-linear.md)首轮错误要求内部ARRAY标记，实际公开eval规范化为text[]；
九个可见类型期望失败及最后opaque调用未捕获22P02导致-6的原始fixture、日志保留。
没有将它伪造为完整76验收或删除实际opaque检查。本项用独立新60夹具复现并修复真实身份问题；
R182需新版本修正自身错误oracle、保留opaque断言并重新完成所有性能路径门禁后才能接受。

原PG329/旧329属于R182标准SQL兼容控制，不冒称本轮R183候选协议329已执行。
原生注册callback无直接PG注册接口对照；本项直接验其API的实际类型/身份/执行约定及既有回归。
不是全1490注册入口、完整72、TLS或整库sanitizer验收。R164历史两次超时仍未解决。
R180及其他用户明确暂缓不恢复；原273仍22 complete/166 partial/70 unverified/15 deferred。
总目标未完成，不push，Actions保持禁用。

## 固定证据

本地证据非可移植release依赖。独立源/产物/完整运行/恢复/真实Main来源核对实际0：
`/tmp/dbms-r183-independent-proof.zzpzejmo/completed-proof.json`，SHA256 `38d77f5ac829f04598dc527f0cb7789c348a691048e1c8190661f303d5adce49`。

- `/tmp/dbms-r183-clean-compiler.sf52u1pc/completed-compiler-receipt.json`：`423ad4644403a6a3c7fbfb26e0806bb5d2cc02f000e71e19fb6b747871739a10`。
- `/tmp/dbms-r183-focused-runtime.36gzh3dn/completed-runtime-receipt.json`：`16b9010ced2b6e6820a2cafe95cae8e6978406956e454ee42737b01d55d4810e`。
- `/tmp/dbms-r183-root-compiler.kudvsxg3/completed-compiler-receipt.json`：`1743e95e871ddc973410a46b7af80ac28519d69f540543566d812940b60d8c68`。
- `/tmp/dbms-r183-focused-runtime.infbiqwz/completed-runtime-receipt.json`：`73eb7042fdb7f20cdf81a63b053ac20f7e3fea3df40fffdce26e640749f6e45c`。
- `/tmp/dbms-r183-old-native.7d35z343/completed-counterproof-receipt.json`：`f874d0afcfdd8722408fbccc7e9cf084a0bcd34294bd103055ff63cd8f931bad`。
- `/tmp/dbms-r183-root-preinstall-recovery.96y65s64/completed-install-receipt.json`：`a56cf3bcebb00a2283717083144ce94719ab7a1a09efb8e34a90022d58797eed`。
- `/tmp/dbms-r183-root-preinstall-recovery.96y65s64/completed-recovery-receipt.json`：`c01a2ded0f3d8afe5e31476e40f694e66547189417c96335310a898be81467ad`。

# R87：MOD的整数宽度、numeric NULL及有限结果声明

状态：本范围已修复。只对已知smallint/integer/bigint/numeric输入选择真实
结果域；不是完整MOD签名、arity、UNKNOWN输入或全部receiver形状的兼容声明。

## 问题与修改方法

原callback对所有NULL及非numeric整数余数统一返回integer，helper只继承左参
类型，忽略右参宽度/NUMERIC。于是bigint值虽然能算出5000000000，运行datum
仍标成int4；后续除以2错误报22003，而真实PG18.6返回bigint 2500000000。
混合smallint/bigint、integer/numeric的静态描述符和typed NULL也不正确。

修复两处：真实callback在NULL处理前仅读取两个已声明类型，NUMERIC优先、
否则取较宽整数域；只替换typed NULL与最后整数余数的结果类型。原numeric
余数/scale、NaN/Infinity、除零及INT64_MIN/-1保护不变，不解析NULL payload。
helper对可可靠推断的有限输入采用同一已有 `%` 类型提升规则。

私有marker只包装真实MOD实现，opaque registerFunction不继承builtin规则；
其helper继续旧arg0回退，stored UDF真实返回声明优先。没有新增严格输入签名，
不收紧现有native float/TEXT容忍，也不执行callback来猜返回类型。

## 旧基线及首版回归必须保留

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 运行 | 实际结果 |
| --- | --- |
| 匹配R86产品的原生旧基线，r87-old-native.log | 254控制、126失败；bigint链实际22003，native float/TEXT及opaque正控未失败 |
| 原binary旧协议，r87-old-protocol.log | 10次查询/130 scalar controls中4次查询失败，包括混合OID、bigint链及空源/零行metadata；不能把组内表达式都称为独立执行 |
| 首版有限域修复，r87-initial-native.log | 原254全0，但不足以接受产品 |
| 独立复杂输入probe，r87-complex-input-probe-old.log / -initial.log | 历史旧库121全0，首版候选121中90失败；原生opaque RHS算术/CASE被新增metadata遍历提前误拒绝，未提交该首版 |
| 扩大永久兼容矩阵，r87-complex-input-expanded-old.log / -guarded.log | 历史旧库和最终候选各153全0，含native mixed-arm CASE及无owner/无绑定列metadata |
| 真实PG18.6最终永久协议，r87-pg18-reference.log | 10接受查询/130 scalar controls全0；另4个只读目录/不支持SQL观察，不计入130接受控制 |

历史probe逐个核对57份Root test库对象与冻结27bac6ee producer记录的SHA256，
确认公共头字节未变，再fresh编译driver/stubs并隔离运行；producer日志分别为
`r87-complex-input-probe-producer.log`、`r87-complex-input-expanded-producer.log`。
这是同API的历史差分，不冒称变化后源码的当前门禁；两个模式实际退出码逐项保存。
原121源另存在 `r87-complex-first121-source.cpp`，不以扩展矩阵覆盖旧失败。

最终保护不catch异常：opaque/左参非有限域/参数不足先沿用旧回退；只有RHS
含真实声明、typed参数、已绑定或有hints的列、显式cast等可靠形状才新增推断。
无声明callee、无绑定列、prepared subquery及无自身声明的CASE保守回退，
避免将旧native的惰性求值变成新的静态TEXT算术/CASE合型错误。
既有collector解析顺序及RoutineResultTypes::empty语义不因全树bookkeeping改变。

## 永久测试及最终验收

`mod_result_domain_test.cpp` 共254控制：98值/metadata、48真实PreparedQuery
描述符、64 native typed datum、4原宽松float/TEXT、36 opaque控制、3真实
qualified UDF及1个guard。覆盖16组类型组合/NULL、负号、整数下界、任意精度、
numeric scale、NaN/Infinity、除零和bigint后续算术。
`mod_native_input_compatibility_test.cpp` 共153控制：144项真实/opaque输入与
direct/ABS的独立metadata/绑定/执行，8无owner或无绑定输入metadata及1个guard。
有意保留的native mixed-arm CASE不是合法PG SQL，不伪装成PG兼容正控。

新永久协议已注册，检查结果/NULL/OID、空源/LIMIT 0、command tag、ReadyI及
精确22012。真实PG拒绝float8/REAL/TEXT MOD的42883仅作参考观察，不借此收紧native。

`r87-build.log` 实际重编ExprEvaluator和helper；两次保护迭代各只重编helper，
最终 `r87-final-owned-build.log` 标准/重复构建exit0。公共头未改，其他56份
当前生产对象保持匹配，不宣称全部58重新编译。最终binary SHA256：
`f171966569541d5aa815613a2f0a0b17d906b6a9dac424133a5dbc666e9eeb23`。
`r87-final-native.log`：254/153新控制及十四完整native全0，包含math、numeric
arithmetic/gcd-lcm、R62/ABS、R83/R84、R85/R86与R80未绑定保护；全部58 receipt
及输入签名一致。`r87-final-protocols.log`：十一完整协议全0，包括新130、
scientific arithmetic、gcd/lcm、R62/ABS、R83/R84、R86、真实取消、CLI和COPY
error/lock release；生产输入/binary/全部58 receipt首尾一致。

独立终态 `r87-terminal-fence.log` 的全部58份receipt/cache/build stamp及重复
构建均0。本项提交前510份Markdown/872个本地inline目标、523 Python AST、
13 shell语法、503 unique注册/485 standalone E2E、文档状态/兼容契约及六个
总账单测均通过。文档检查不证明全部历史技术结论已经在当前版本验收。

## 未完成范围

无真实返回声明的部分builtin作为RHS、一般UNKNOWN与多态/完整arity、quoted
身份等仍属[R62未完成范围](issue-review-r62-scalar-signatures.md)。保守回退不是
这些范围的完成证明。[最新冻结全量](issue-full-registered-27bac6ee.md)的六项
失败及COPY同步统计/慢sync观察不由本项修复；原完整PG、大COPY及新1373组合
未在本项重跑。最新为867 native +3真实Main frontend +503 Python =1373入口。
原273总账仍22 complete/166 partial/70 unverified/15 deferred，require-complete
实际exit1，日志 `r87-require-complete-actual-exit.log`。不标总目标完成、不push，
GitHub Actions工作流继续禁用，用户暂缓专项不重开。

# R89：ROUND 单参数的有限 numeric/float8 结果声明

状态：本范围已修复、定向验收通过。范围仅为单参数的已知数值/UNKNOWN 结果声明，
不是 ROUND/TRUNC 完整签名、双参数精度或不支持输入已完成。

## 问题与修复方法

真实 ROUND callback 对整数/REAL 返回 float8，对 NUMERIC/DECIMAL 返回
numeric，包括 typed NULL；旧 helper 却直接继承第一个输入类型，导致结果
描述符和数据不一致。在空源、WHERE false、LIMIT 0 中也会暴露错误 OID。

修复 helper 的有限单参数规则：smallint/integer/bigint/real/float8/UNKNOWN
选择 double precision，numeric/decimal 选择 numeric。其它输入仍保留旧
回退，不因修改 metadata 就将不支持的 SQL 或 native 外层表达式误判为合法。
零参数、双参数及更多参数不走本项规则，原值算法与运行 datum 完全未改。

复用私有 callback ownership marker 与单参数 AST 的 opaque 标记，仅真实
内建实现拥有新规则；registerFunction 的默认/immutable 自定义回调沿用旧
输入推断，保留参数类型、NULL、执行次数和错误阶段。真正 stored routine
的返回声明优先，不按 callee 名字覆盖用户函数。

## 旧基线与首版回归

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 运行 | 实际结果 |
| --- | --- |
| R88 后匹配旧产品，r89-old-native.log | 最初 108 项中 48 失败，另记录 16 项 native 嵌套行为；实际退出 1 |
| 永久加强矩阵，r89-old-permanent-native.log | 124 项中同 48 项失败，16 项嵌套兼容控制均通过；实际退出 1 |
| 原 binary 永久协议，r89-old-protocol/round_result_type_protocol_e2e_test.log | 91 次检查中 44 次失败，含实际值/NULL/空源/零行/nested 的结果 OID；实际退出 1 |
| 真实 PG18.6，r89-pg18-reference.log | 同一永久协议 91 次检查全部通过、实际退出 0 |
| 历史旧库，r89-compat-old.log | 新增 97 项 native TEXT/零参数/双参数兼容矩阵全部通过 |
| 首版未限输入域，r89-compat-initial.log | 同 97 项中 6 项失败：错误改变内建 native TEXT 的 metadata 及外层 ABS 原 binding 42883；该首版未提交 |

97 项的历史对照逐个核对冻结 27bac6ee 的 57 份 Root test 库对象 SHA256，
与 r85-full-cache-preparation.log 的 producer 记录相同；公共头字节未变，
fresh 编译 driver/stubs，分别隔离链接旧库和首版当前库。独立 producer 为
r89-compat-history-producer.log，两个模式的实际状态逐项保留；不是变化后
源码的当前门禁，也不把包装器最后退出 0 解释为候选全部通过。

据此收窄单参数类型规则，保持其它输入回退，不 catch 或放宽原错误断言。
不以“TEXT 不是合法 PG overload”为由破坏原 native API 或扩大本项 scope。

## 永久测试与最终验收

`round_result_type_test.cpp`：124 控制，涵盖七类数值声明、typed NULL、
UNKNOWN、空表/WHERE false/LIMIT 0、nested ABS/ROUND、pg_catalog 名称、
qualified stored UDF，以及原生默认/immutable 回调、真实/空数据库上下文。
16 项旧观察已转换为精确兼容断言，包括 TEXT 在 binding 阶段 42883 且回调
未执行；零参数 native 外层维持 float8。没有把 native 扩展伪装成 PG SQL。

`round_native_input_compatibility_test.cpp`：97 控制，独立检查内建/两种替换
模式、真实/空数据库、直接/ABS 嵌套的 metadata、纯绑定、原参数、执行次数、
值/NULL 与错误阶段，保护 TEXT 与零参数/双参数的旧行为。

`round_result_type_protocol_e2e_test.py` 已注册：91 次 simple-query 检查，
检查值/NULL/OID、command tag、RowDescription/DataRow 数及 ReadyI；保留
qualified UDF、空源/零行、float8 half-even、numeric half-up 与 NaN/Infinity。
91 是协议检查次数，不是每个列值都独立发送一次查询。

`r89-build.log` 首版标准构建实际重编 ExprEvaluator.cpp 与 expr_helper.cpp；
有限域保护后的 `r89-final-build.log` 仅实际重编 helper，链接/重复构建退出 0。
公共头未改，其它 56 份生产对象保持匹配，不冒称全部 58 份重编或借用对象。
最终 binary SHA256：
`5fbefadf0a5ad05b279cc65252102e970f2b2098454518400d1c4c4aa401b983`。
`r89-final-native.log`：两个永久矩阵分别 124/0 与 97/0，十七个完整 native
全部通过；包括 math/projection errors、scientific arithmetic、gcd/lcm、R62/
ABS、R83/R84/R85/R86/R87、复杂未绑定参数和 R88 两项统计控制。fresh 编译
stubs/driver，链接当前 57 份生产库对象，隔离执行；全部 58 receipt 与输入
签名一致。R88 比较次数仍为 1，96 个统计列/运行控制全部通过。

`r89-final-protocols.log`：十二个完整协议全部通过，包含新 91、R84 266、
scientific arithmetic、gcd/lcm、R62、R83、R87、ABS、R86、真实取消/CLI 与
COPY error/lock-release；生产输入/binary/全部 58 receipt 首尾一致。独立
r89-terminal-fence.log 再核对同一 binary 哈希、cache/build stamp、全部
58 receipt 与重复构建，实际退出 0。

提交前 512 份 Markdown/884 个本地内联目标、524 Python AST、13 shell
语法、504 unique 注册/486 standalone E2E、文档状态/兼容契约/版本及六个
总账单测均通过。文档检查不证明全部历史技术结论当前已验收。
r89-require-complete-actual-exit.log 实际退出 1，原 273 状态仍为 22 complete、
166 partial、70 unverified、15 deferred。最新组合 871 native +3 真实 Main
frontend +504 Python =1378 入口；本项未运行这套组合的完整注册回归或完整
默认 PG/COPY 协议，不用局部通过替代它们。

## 未完成范围

双参数整数转 numeric 的精度/scale、完整 SQL overload/arity、TRUNC 的
MAC-address 与 UNKNOWN 歧义仍独立未完成；本项不实现这些行为，不收紧
原 native 的宽松 API。[R62 广义签名](issue-review-r62-scalar-signatures.md)
仍 partial。[R88](issue-review-r88-analyze-sort-comparison-cost.md)原完整 COPY
超时以及冻结全量六个失败也不因本项通过改写。新注册组合未全量运行，
原 273 总账不冒称完成；逐项本地提交，不 push，Actions 禁用，暂缓专项不重开。

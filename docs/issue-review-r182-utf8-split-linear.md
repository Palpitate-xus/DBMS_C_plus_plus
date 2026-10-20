# R182：共享UTF-8拆分的平方级前缀扫描

状态：有限性能修复，定向验收通过；不是旧版兼容性错值，不关闭完整FUNC-01。

## 问题与修改方法

| 路径 | 原因 | 修改方法 |
| --- | --- | --- |
| TRIM/LTRIM/RTRIM的输入和字符集合 | splitUtf8Characters逐字符两次utf8ByteAt，重复扫描全部前缀 | 共享helper一次正向移动到下一码点边界 |
| LPAD/RPAD的填充串 | 重复实现同一平方级拆分 | 使用共享helper，保留原长度、周期/余数、大小限制及NULL规则 |
| TRANSLATE的输入、from和to | 私有lambda分别进行平方级拆分 | 三个参数统一使用共享helper；首次重复键优先、缺失目标删除规则不变 |
| STRING_TO_ARRAY的NULL delimiter | 相同逐字符前缀查找 | 使用共享helper；空数组、null_string、非NULL delimiter及元素转义不变 |

仅修改[ExprEvaluator.cpp](../src/expression/ExprEvaluator.cpp)的私有helper及三处重复调用。
预留原码点数量后，一次向前扫描完整UTF-8码点，不进行Unicode正规化、字节级拆分或grapheme聚合。
只宣称拆分工作O(n)，不是整个TRIM字符集合或TRANSLATE查找O(n)；原有线性搜索未冒称消除。
数组组合表达式array_to_string(string_to_array(...), '')仍有明显额外开销：
clean最大样本约6.22秒，直接原生STRING_TO_ARRAY同规模约0.21秒；不是整条数组路径已快速。
该剩余开销尚未定位，作为后续普通查询性能问题保留，不能用拆分循环改善冒称解决。
BYTEA独立分支、CHAR逻辑文本、公共头/AST/声明、参数求值与输入验证没有修改。
不宣称为无效原生TEXT字节增加验证，也不重开用户暂缓的其他模块。

## 实际对照与观测

PG18完整329实际0；冻结旧accepted R183（6d5b7d8e）完整329与V2 native76均实际0。
旧值与元数据本来正确，不伪造旧版兼容性失败。测试检查完整结果而非仅长度或前缀。
以下为Simple模式8192份aé界😀（32768码点/81920字节）的单次round-trip毫秒数；
不是稳定速度保证，回归无耗时/比例阈值断言。

| 参数角色 | PG18 ms | 旧版 ms | clean ms | Root ms |
| --- | --- | --- | --- | --- |
| trim-input | 1.120 | 2363.998 | 33.375 | 33.588 |
| trim-set | 1.202 | 2319.284 | 25.907 | 26.529 |
| lpad-fill | 1.081 | 2348.210 | 35.133 | 34.297 |
| rpad-fill | 0.817 | 2332.323 | 92.774 | 36.777 |
| translate-input | 1.557 | 2331.571 | 29.057 | 27.106 |
| translate-from | 0.471 | 2357.700 | 32.624 | 28.226 |
| translate-to | 0.234 | 2321.113 | 34.835 | 21.693 |
| array-input | 6.053 | 8558.509 | 6221.971 | 6357.772 |

## 验证范围

- 新native76：trim三个方向、不同码点共享字节、补充平面/组合字符、空集合、填充截断与空串、
  translate重复键/删除、数组NULL delimiter/null_string/空元素、typed NULL及完整大值。
  八种大参数角色各三种大小，纯元数据/输入执行一次和四个opaque native覆盖保留。
- 新严格协议329：Simple/extended/binary，精确名称/来源/OID25/宽度-1/typmod-1/格式、
  NULL/字节/tag/ReadyI，typed参数重复绑定、真实表、WHERE false/LIMIT0与序列副作用。
  数组分割以元素/NULL及TEXT结果验证，不把这些检查冒称完整数组binary codec验收。
- Root及accepted-only clean各完整23入口实际全0：14native、6wire、3真实Main。
  新76/329、原80/96/294/80/190/654、原完整数组库及所有相邻字符串断言保留。
- 各76对象/18当前程序/127头核对；fresh1生产Evaluator和1native（本项新夹具）。
  57生产、13native、stub、3真实Main均为源码/公共头/编译器及对象hash匹配的donor，未冒称fresh。
  所有程序重新链接当前库；Main对象来自accepted测试体的已验证include路径投影，
  不带test_stubs，不混入Root未接受session测试修改。两侧Main源码/公共头未变，合法复用。
- Root正常构建实际1CPP重编并链接，repeat零编译零链接；clean显式构建，不冒称缓存wrapper运行。
  全部2206已接受源码投影与tested clean一致。Root2325源码仅4拥有路径变化；
  2323旧源码和118cache/server文件先复制核对，其他未接受改动原样保留、不混入提交。

R180及其他明确暂缓不恢复；R164历史两次超时仍未解决。
不是全1492注册入口、完整72矩阵、TLS或整库sanitizer验收。
原273仍22 complete/166 partial/70 unverified/15 deferred，总目标未完成。
不push，Actions保持禁用。

## 首轮失败与V2修订（完整保留）

V1私树/tmp/dbms-r182-utf8-split-candidate-v1.bsZaIV58及原native76夹具没有改写。
原native错误期望内部ARRAY标记，实际公开eval规范化text[]，产生九个可见失败标签；
最后opaque callback返回TEXT opaque被按builtin数组强转，抛22P02、进程-6，无76完成footer。
不把该失败计为完整76验收或九个产品bug。真正回调身份问题已由[R183](issue-review-r183-string-to-array-callback-identity.md)
独立提交6d5b7d8e修复，本项不覆盖或重写该修复。

V2从accepted R183新建私树，原协议329逐字节不变；原生仅把错误ARRAY期望纠正为text[]，
opaque检查仍要求opaque值，并增加明确TEXT/非NULL检查；异常计失败后继续，而不是提前终止。
全部76检查保留，新旧fixture字节差异单独核对。旧R183本来仍有拆分平方级开销，但完整V2 native76
和协议329均通过，证明没有以性能修改偷换语义oracle。耗时只是观测，不设置速度阈值。

## 固定证据

本地证据不是可移植release依赖。独立源码/产物/完整运行/恢复/真实Main来源核对实际0：
`/tmp/dbms-r182-v2-independent-proof.jsq3lrg1/completed-proof.json`，SHA256 `7d15680f78e108a0b9dc83453924ecffc3242947622df0e3bcdc9b8ef50298d4`。

- `/tmp/dbms-r182-v2-clean-compiler.e5eita0e/completed-compiler-receipt.json`：`4b2f93010c726a2a4d0e5aab123c3e32b0fc855be72169921a51a7bf0979e714`。
- `/tmp/dbms-r182-v2-focused-runtime.r98n6q5h/completed-runtime-receipt.json`：`9711d39a4f6fc694b9f90b6f1ec2594189e3c176d9f2db2f52a289243445f76f`。
- `/tmp/dbms-r182-v2-root-compiler.wfcfdzxe/completed-compiler-receipt.json`：`5206ad4ec11317a21222ddf478fbd0b7caf1b63bd584e16916abf98c56dcd3c9`。
- `/tmp/dbms-r182-v2-focused-runtime.yo3oxhry/completed-runtime-receipt.json`：`9e551bba5cf87814967edaaecda9a4274cf3850d375b002b8d62adc81fecdf30`。
- `/tmp/dbms-r182-v2-old-native-private.0yrta_c5/completed-old-native-receipt.json`：`c8e8b3407beec863600fa4888777001670a3433a8ee5a8ca9844c88a297522f0`。
- `/tmp/dbms-review-r46.ifu6SWLp/r182-v2-reference329.log`：`2984af2a1d32f562ca32644f404ce2fcadc71acb027d0089ce0f8b2967a84b18`。
- `/tmp/dbms-review-r46.ifu6SWLp/r182-v2-old-r183-329.log`：`37679cf60b52c1bf346f1dbdb660b00b32ca0613ca1071c91b4655a49589b443`。
- `/tmp/dbms-r182-utf8-split-candidate-v1.bsZaIV58/tests/utf8_split_linear_test.cpp`：`a6913e444070d212573370e7f350b85039c15f8432f1b8f5ab6b56e591034ebc`。
- `/tmp/dbms-r182-old-native-private.nvq86b9l/completed-old-native-receipt.json`：`6d4cb406cc4e9fe2eed92678b66163fbdfbfde5d9c46905a25238229b1a2e763`。
- `/tmp/dbms-review-r46.ifu6SWLp/r182-old-native76.log`：`234019d424431a5f0898f4d47249f8868f0c6a71cff2e3e33060fd0b62491a76`。
- `/tmp/dbms-r182-v2-root-preinstall-recovery.6ixdi8qv/completed-install-receipt.json`：`69305cdf14e0edc32859646dfd8eb70f491076f834ea72e1d1c1e90b4640fdbb`。
- `/tmp/dbms-r182-v2-root-preinstall-recovery.6ixdi8qv/completed-recovery-receipt.json`：`d7f6d7d7dda055e3a230e4776d6f7e8e236664803f769a5bdfdd68c3a502beb0`。

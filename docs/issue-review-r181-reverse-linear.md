# R181：REVERSE长文本的平方级重复扫描

状态：有限性能修复，定向验收通过；不是旧版兼容性错值，也不关闭整个FUNC-01。

## 问题与修改方法

| 问题 | 原因 | 修改方法 |
| --- | --- | --- |
| 反转普通长文本出现明显查询延迟 | 每输出一个码点，两次utf8ByteAt都重新扫描输入前缀；总工作O(n²) | 从输入尾部退到上一个UTF-8起始字节，追加完整码点，再继续向前；总工作O(n) |
| 优化不能反转UTF-8内部字节或改变字符含义 | 字节、Unicode码点和grapheme cluster不同 | 保持码点级反转；组合字符与ZWJ序列也按码点，而非视觉字形反转 |

仅修改[ExprEvaluator.cpp](../src/expression/ExprEvaluator.cpp)私有REVERSE文本回调的循环。
result仍预留输入字节数；新循环单调向前退，每个输入字节只扫描/复制常数次，额外空间仅结果。
NULL返回、CHAR/BPCHAR去padding、TEXT尾随空格和实参求值位置不变。
BYTEA仍走原有独立的逐字节reverse分支，零字节与非UTF-8字节不会进入文本循环。
公共头、AST、原生API、callee声明及namespace规则没有修改；不宣称修复无效原生TEXT字节的输入验证。

## 同一完整夹具与实际观测

PG18完整96实际0；旧accepted R179完整96和native80实际0，旧版值与元数据原本正确。
这是算法效率缺陷，不把旧版通过记录伪造成兼容性失败。
同一协议夹具在三个模式各测试2048/4096/8192份aé界😀，检查完整结果相等，
不只检查长度或前缀。下面是Simple模式8192份、32768码点/81920输入字节的单次round-trip；
不是稳定速度保证，不设置耗时通过阈值。

| 构建 | 实测秒数 |
| --- | --- |
| PG18 | 0.000857812 |
| 旧版 | 2.241870341 |
| accepted-only clean | 0.037952989 |
| Root | 0.037909554 |

## 验证范围

- 新native80：TEXT/VARCHAR/CHAR/BPCHAR、空/NULL、ASCII、2/3/4字节码点、组合字符、
  双重反转、完整大值、BYTEA零字节/高字节、纯元数据及输入单次执行、原生opaque覆盖。
- 新严格协议96：Simple/extended/binary完整帧；精确名称/来源/OID25/宽度-1/typmod-1/
  格式/NULL/字节内容/tag/ReadyI；真实表、WHERE false/LIMIT0、typed参数重复Bind、
  text与binary输入/输出、Parse/Bind不执行序列及每次Execute恰好执行一次。
- Root及accepted-only clean各完整19入口实际全0：11native、5wire、3真实Main。
  含新80/96、STRPOS294、REPEAT80、排序190、字符串签名654，保留全部原断言和footer。
  相邻native包括BYTEA/string/pad/search/trim/translate/CASE/普通聚合及STRPOS。
- 各73对象/15当前程序及127头核对；fresh1生产Evaluator、2native；clean本轮fresh3真实Main，
  Root的Main源码/公共头未变，验证复用3份真实Main对象，未冒称fresh，仍全部新链接当前库。
  其余57生产、9native和stub为源码/头/编译器/对象hash匹配的donor，未冒称fresh。
  所有native新链接当前57库对象；三个Main用accepted测试体的单一include路径投影，
  不带test_stubs，也不夹带Root的session测试外来修改。
- Root正常构建实际1CPP重编并链接，重复构建零编译零链接。clean显式编译/链接，
  不冒称normal/repeat缓存。全部2203个已接受源码投影与实际tested clean一致。
  Root2322源码仅4个拥有路径变化；2320旧源码及118 cache/server文件先复制并核对，
  其他未接受改动保持原样，不混入提交。

## 保留的诊断与边界

首次手工syntax命令漏了项目公共include路径，C++诊断为dbms_defs.h缺失；它不是代码通过。
使用真实构建receipt中的完整flags/includes后syntax实际0；随后真正编译、链接及完整运行另行记录。
没有提高超时、去掉失败断言、改变持久化或截取通过部分。此轮没有声称全1489注册入口、
完整72矩阵、TLS或整库sanitizer通过。R164两次历史超时仍未解决。
R180按最新skip it暂缓；R177/R173/R156及其他明确暂缓不恢复。
原273仍22 complete/166 partial/70 unverified/15 deferred，总目标未完成。
不push，Actions保持禁用。

## 固定证据

本地证据不是可移植release依赖。独立源码/产物/运行/恢复/真实Main测试体核对实际0：
`/tmp/dbms-r181-independent-proof.e4u3w_a2/completed-proof.json`，SHA256 `b3747b50df091a66a447edfdec1453525feb20fa62d2b98a27bd4640836afac6`。

- `/tmp/dbms-r181-clean-compiler.zmf5qryu/completed-compiler-receipt.json`：`5fa316551e669e1c01a15e0b306cc1da1633c538729b4e14f5111cf3872024c2`。
- `/tmp/dbms-r181-focused-runtime.0r5q6fby/completed-runtime-receipt.json`：`8c19782c498064cd74c445e340866c69e3e3d64236e2d70ea09658272309258c`。
- `/tmp/dbms-r181-root-compiler.a34yqjla/completed-compiler-receipt.json`：`240b6e19c2a61a6ac2f87f62c3b00df054d7be64f5e52fe9c7002762a8b60e3f`。
- `/tmp/dbms-r181-focused-runtime.ao1rs0o8/completed-runtime-receipt.json`：`34a95ca24a9b762ae6b5431e492b247d926bebe67558249dd633cda34e9ca7d3`。
- `/tmp/dbms-review-r46.ifu6SWLp/r181-reference96.log`：`27d5eb6a186b72aaed3c6eef07240ed0feaad4f39ff80580c929ffb77ae30fe7`。
- `/tmp/dbms-review-r46.ifu6SWLp/r181-old96.log`：`5b294e3468ab58010b03a8401021667ee3ef78e5506ff145e665e6c16c840e17`。
- `/tmp/dbms-r181-old-native-private.sjxmmhmn/completed-old-native-receipt.json`：`8758038ff3e92ab9a1683b7fffa15ab68446616e9996e7b125e72fdc5241d780`。
- `/tmp/dbms-review-r46.ifu6SWLp/r181-old-native80.log`：`1db515f18da68648754e2e1312b8fd40cbe35b18c7a1720d3440dff8ee238ad3`。
- `/tmp/dbms-r181-root-preinstall-recovery.m3764d23/completed-install-receipt.json`：`dce57ff775da841b8b07d20b0bcb1d98124f7253ebdfab3d2156484c877dbb3b`。
- `/tmp/dbms-r181-root-preinstall-recovery.m3764d23/completed-recovery-receipt.json`：`72a52b19289950a432d9bdf544e1d47885aa174660c4912186a627633768b1f8`。

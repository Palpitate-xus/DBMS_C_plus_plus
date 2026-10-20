# R178：REPEAT空输入的无意义循环

状态：有限性能修复，定向验收通过。不是兼容性错值修复，也不关闭整个FUNC-01。

## 问题、原因与修改方法

| 问题 | 原因 | 修改方法 |
| --- | --- | --- |
| `SELECT repeat('',2147483647) AS v`旧产品约9.36秒才返回正确空文本 | `repeat`已经校验int4次数及取得逻辑文本，仍执行n次空字符串追加，追加工作O(n) | 校验后发现逻辑文本为空便返回非NULL空TEXT；不进入追加或除法长度检查 |
| 快速返回可能错误略过参数错误或副作用 | 若把判断提前到参数求值/次数校验之前，会改变既有语义 | 保留所有参数求值、NULL规则及int4解析顺序；空输入仍检查非法次数，真实nextval仍执行 |

仅修改[ExprEvaluator.cpp](../src/expression/ExprEvaluator.cpp)的私有REPEAT回调。
非空输入仍保留原负/零次数、输出大小上限、UTF8追加和错误行为；共享头/Main未改。
逻辑CHAR值去尾部padding后为空也采用同一快速返回，但TEXT空格不变为空。
[PG18字符串文档](https://www.postgresql.org/docs/18/functions-string.html)规定REPEAT结果语义；
参考构建本轮也有空追加耗时，不能把本项写成PG兼容差异。

## 同一5条SQL采集器的实际测量

采集次数为0、1、16777216、134217728、2147483647；四侧均5条完整、正确响应。
以下只列最大次数的本轮单次round-trip观察，不作稳定速度保证，不设置wall-clock通过阈值。
返回值均为非NULL零字节TEXT，OID25、宽度-1、typmod-1、名称v、SELECT1和ReadyI的raw完全一致。

| 采集端 | 最大次数实测 | 值与帧 |
| --- | --- | --- |
| PG | 7.908236603 s | 5/5结果与参考raw一致 |
| old | 9.360704853 s | 5/5结果与参考raw一致 |
| clean | 0.029973758 s | 5/5结果与参考raw一致 |
| Root | 0.026690979 s | 5/5结果与参考raw一致 |

## 回归与构建范围

- 原生REPEAT夹具新增4项：真实空列及字面量的int4最大次数、空列非法整数22P02及空字面量超范围22003。
  原有非空/UTF8、字面量NULL与SQL NULL、零/负次数、输出大小54000断言保留。
- 新增严格协议80项：Simple/extended/binary、空/NULL/普通值/CHAR/VARCHAR、真实表及空结果，
  typed参数与重复Bind/Execute、元数据不执行，以及空输入真实nextval仍执行。
  精确检查名称、来源、OID、长度、typmod、格式、NULL/零字节datum、命令tag和消息序列。
  两个错误输入另检查SQLSTATE与无结果；没有声称这些错误的Parse/Execute阶段均已对齐。
- 完整PG80实际0。Root与accepted-only clean各完整11入口实际全0：8个native、3个wire，
  包含本项80、原聚合排序190、字符串签名654；原生相邻包括pad/trim/UTF8 translate/search、
  string library、CASE common type与原聚合61。不是全部1485或完整72入口验收。
- 每侧67对象/9程序及127头核对；各fresh1生产Evaluator、fresh7native，57生产+1native+1stub
  为真实验证后的donor，未冒称fresh。Root正常只编1CPP并链接，repeat零编译/零链接；
  clean显式编译/链接，没有伪造normal/repeat缓存。所有native新链接当前57个库对象。
- accepted源码投影2199文件与实际测试clean相同；Root2318源码文件只安装4个拥有路径。
  安装前2317源码及118个cache/server文件已保存到独立恢复快照，原有其他改动未提交。

## 保留的失败与边界

首轮clean11外层1：8native通过、3wire在startup失败，没有发送SQL。
驱动把DBMS_DATA_DIR设为外层目录，而wire runner使用另一个私有--data-dir；
实际诊断退出1并明确报告`-D/--data-dir conflicts with DBMS_DATA_DIR`。
V2只对wire清除继承目录、native继续使用显式私有目录，整个11重新执行且通过。
第一轮失败及原日志完整保留，没有拼接通过或更改断言、超时、持久化。

R164两次完整矩阵超时原因仍未确定，未因本项变为通过或用户暂缓。
R177按用户要求暂缓，R173/R156等分支不恢复。原273仍22 complete/166 partial/70 unverified/15 deferred，
总目标未完成；全注册、TLS及整库sanitizer未在本轮运行。不push，Actions保持禁用。

## 固定证据

本地日志位于`/tmp/dbms-review-r46.ifu6SWLp/r178-*`，不是可移植release依赖。
独立源/产物/完整运行/旧失败核对：`/tmp/dbms-r178-independent-proof.__1osm1k/completed-proof.json`，SHA256 `75895c22f57e5fef209e02eb0bdbe72aca22568bb023902975c75becdd637385`，实际0；
这个0是证据一致性通过，不是整库通过。

- `/tmp/dbms-r178-clean-compiler.ymo3a97y/completed-compiler-receipt.json`：`0876d54b4075273f3f4231f3faf6f5086414ea13c5e71c3f8c7d46e501ace5d3`。
- `/tmp/dbms-r178-root-compiler.kqh2j2gp/completed-compiler-receipt.json`：`64275474f74a093e732b2926e050247be8da44a19b311463752fad7e48da3617`。
- `/tmp/dbms-r178-focused-runtime.4yh7rinq/completed-runtime-receipt.json`：`71af65bff7410e0c34b359ed801be6bb3137a645aff2cf01555ae9b3110a5fcc`。
- `/tmp/dbms-r178-focused-runtime.80ysfns7/completed-runtime-receipt.json`：`9a0eb901b50fe7f1b35be26ff06afaa7c2a64f3b639fa7a66622d8009791f375`。
- `/tmp/dbms-r178-focused-runtime.1yi9r03h/completed-runtime-receipt.json`：`2477ed9f0ac3268d837d1569df4e0df7122d1200dfdea466202423aeb836b6b5`。

PG80日志：`r178-reference80.log`；完整测量日志分别为`r178-repeat-empty-reference-discovery.log`、
`r178-repeat-empty-old-discovery.log`、`r178-repeat-empty-clean-discovery.log`和`r178-repeat-empty-root-discovery.log`。
所有修改按本项独立本地commit管理，原有未接受测试和文档保持分离。

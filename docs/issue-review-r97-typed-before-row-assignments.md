# R97：BEFORE行赋值的NEW/OLD类型与NULL

状态：本范围已修复；原106检查旧实现65失败，当前原106及扩展154全部通过，
15个完整native及十个完整协议邻居通过，当前1392组合尚未取得完整终态。
这是StorageEngine公开API的显式行赋值扩展，不是完整PostgreSQL触发器兼容声明。

## 复现与影响

`20db4d58`输入（产品仍为R95）上，新原生驱动完整106检查、65失败、实际退出1。
使用独立StorageEngine、真实表和insertRow/updateRows，callback按现有扩展约定
成功返回false。失败检查不是65个独立bug；它们检查操作状态、RETURNING单元格、
重新扫描的值和真实heap NULL位。原58生产receipt、57份test库字节、binary及
输入独立fence退出0，之后才修改生产实现。

| 合法API行赋值/输入 | 原实际结果 | 应有结果 |
| --- | --- | --- |
| `SET txt = 'NEW.a; OLD.a tg_name'`，a为ordinary | ordinary; OLD.a tg_name | 原字符串字面量保持不变 |
| `NEW.txt = NEW.a`，a为O'Reilly; NEW.a | 空串 | 完整文本不变 |
| `SET txt = NEW.a`，a为文本NULL | SQL NULL | 非NULL文本NULL |
| 同一赋值，a为SQL NULL | 非NULL空串 | SQL NULL |
| `SET txt = NEW.ab`，a/ab分别为left/right side | 空串 | right side |
| `SET num = NEW.xx`，x为NULL、xx为42 | SQL NULL | 42 |
| `SET num = NEW.x`，x为SQL NULL | INVALID_VALUE | 成功赋SQL NULL |
| UPDATE的 `SET txt = OLD.a || ' / ' || NEW.a` | 空串 | old value / fresh |

原始驱动、日志及二进制在本机辅助目录 `/tmp/dbms-review-r46.ifu6SWLp/` 和
`/tmp/dbms-r97-old-native.yPe0ygGB/` 保留；原日志r97-old-native.log的SHA256为
`a463abe633a93f20429e4662d20d8f52700b5b3269a7b5bce1633c6e213ecc01`。
这些临时路径不是可移植发行产物，永久复现为
`tests/trigger_assignment_typed_context_test.cpp`。

## 原因与修改方法

INSERT/UPDATE的显式BEFORE行赋值先用循环find/replace将NEW/OLD替换成存储文本，
没有SQL token边界、类型或NULL元数据：因此修改字面量，把NEW.a当成NEW.ab前缀，
并把文本NULL重新解析成SQL NULL。求值失败又回退为已清空的字符串，可能静默
写空串或返回错误。UPDATE旧/新NULL也没有传到表达式求值器。

仅修改TableManage.cpp：共享evalTriggerRowAssignment按schema绑定裸列、new.和
old.的真实值、声明类型、collation及NULL集合；INSERT缺失侧为NULL，UPDATE
从旧bitmap及实际已赋值集合构建两个NULL image。逐条使用原RHS表达式求值，
后续赋值看到之前NEW赋值及NULL位的变化，不重写字面量或重解析存储值。
失败释放本层表锁并返回INVALID_VALUE，由现有语句包装器回滚；不把错误当空串。

保留原callback调用和其它SQL action、WHEN、TG变量替换路径；本项没有修复或
验收它们，也不证明SQLSTATE/触发器函数语言/所有名称绑定已完整。没有修改
R96退出期代码、连接池、DROP/cache、mutation provider或用户暂缓的其它专项。

## 验证与构建边界

新回归自动纳入native集合。原106检查不改SQL/断言，正式O2当前完整106/0失败。
随后追加48项失败路径检查，最终154/0失败：INSERT/UPDATE的文本、相似列名、
空串/文本NULL/真实NULL、大小写/quoted列名、顺序赋值、OLD/NEW、typed LENGTH
和整数表达式；除零、未知列、错误CAST在autocommit及显式事务中拒绝，返回
vector不变、原行及先前写入保留、健康后续写入/commit和锁释放通过。callback
精确47次。未用新增失败路径宣称旧实现跑过154；旧基线是106。

r97-build.log实际退出0：标准O2只重编变更的TableManage.cpp并链接，重复构建
up-to-date；公共头未改，不冒称全58冷编译。当前binary为
`fd69b9220cba1b1d2e0ead6498edbecb356fb0d68388659972ebaa76fa08cbb8`，生产cache为
`a3d37eb9d21f8ec16b8af434406290cb3c21bdcd335fbd1a0463bac00e266111`。
r97-test-cache-preparation.log实际0：复用同一Root匹配57库、fresh stubs，
全部58 receipt/双stamp/57库逐字节及其它生产源码、README/Actions不变独立
fence实际0。不使用旧R95对象冒充当前候选验收。

r97-expanded-and-neighbor-natives.log实际退出0，15个不同完整native：新154、
trigger、before_trigger、generated_insert_trigger、INSERT BEFORE/AFTER失败、
UPDATE/DELETE失败、view_trigger_typed_parameter、generated_columns、
generated_index_update、insert_omitted_null、update_null_preservation、
dml_returning、insert_statement_atomicity。含自有stubs的旧入口按canonical
规则排除公共stubs，未删断言或缩小测试。

首个协议批次因调用者误写不存在的postgres_protocol_e2e_test.py，在预校验
阶段退出2，尚未运行任何驱动。更正为实际注册的postgres_protocol_test.py后，
另开完整十入口批次；原日志保留，不把预校验失败叫作十个协议运行失败。
更正后的r97-protocol-neighbors-v2.log实际退出0，十个不同完整入口全部通过：
typed view trigger、omitted NULL、INSERT SELECT NULL bitmap、RETURNING OLD/NEW、
RETURNING transition binding、WITH RETURNING transition、stored-function atomicity、
原R95完整60、原R93完整152及默认PostgreSQL协议。首尾输入/binary/58 receipt
一致；默认协议验证plaintext协商、Simple/Extended及错误恢复，不是实际TLS。
未把原预校验2改写成0，也未改变SQL、断言或期限。

522份Markdown/本地内联文件目标、527 Python AST、13 shell语法、882 native与
507唯一Python注册/489 standalone盘点、文档/契约/版本及六个总账单测均通过。
独立终态fence再次验证输入/binary/双stamp/58 receipt及57 test库字节一致。
require-complete实际仍1，原总账未完成。

本项不替代[冻结5091b39c的1391完整终态](issue-full-registered-5091b39c.md)，
后者不含R97。当前882 native+3 Main+507 Python=1392尚未全量验收，真实TLS仍
缺失，原DDL超时记录不改写，原273仍22 complete/166 partial/70 unverified/
15 deferred_by_user。每项独立本地commit，不push，Actions禁用，README保持长期入口。

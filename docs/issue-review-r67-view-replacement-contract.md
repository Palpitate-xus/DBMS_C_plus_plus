# R67：替换视图必须保留既有列定义

## 实际缺陷和修复方法

原实现对简单投影猜测输出列，对表达式/常量则可能保存零列catalog描述符。
替换时先移除旧定义，再直接覆盖列元数据，错误接受列重命名、类型/typmod变化和
减少列数。这会改变依赖视图的接口，失败的查询定义也可能覆盖原可用定义。
原view_test甚至把id,name VARCHAR(50)替换成id,score INT当成正确行为。

实际PostgreSQL18.6/180006对照：上述不兼容替换为42P16，允许末尾追加列或
仅改变原列计算方式；显式列名列表可以覆盖前缀，带引号名称区分大小写。
静态校验不应执行SELECT、除零或nextval来探测首行。

实现采用已有whole-query纯绑定，UNKNOWN输出在视图边界确定为TEXT。直接使用
真实输出OID/typmod创建pg_attribute，在任何旧定义删除或目录发布之前检查既有
列前缀。旧版本零列描述符从原存储SQL只读恢复比较；拒绝时不顺便迁移目录。
普通legacy查询物化视图结果时保留视图目录列名，而非直接使用独立body的别名。
DDL bridge保留原视图SQL，避免ARRAY表达式先被legacy处理器改写；未引用的视图
名称仍按SQL规则折叠大小写。协议输出使用真实绑定描述符而非物化/显示适配器
退化的TEXT类型；带参数Describe复用原metadata参数绑定，不执行首行探测类型。
兼容旧物理数组的元素OID+attndims表示：新视图通过真实typarray/typelem发布
数组OID，旧视图同类型比较也先规范化，不把INT[]误注册成INT或误判不兼容。
CREATE VIEW尾部WITH [LOCAL/CASCADED] CHECK OPTION由DDL解析并移出SELECT，
避免新纯绑定误拒绝合法旧查询。此项不实现新的CHECK OPTION写入策略。

原完整view测试改用合法保留列名/类型的score过滤条件，保留OID、SQL更新、
schema、依赖和DROP/cascade断言；目录读取使用R64当前代/旧代lease夹具。

## 对照与未完成的验收

| 输入 | 实际结果 |
| --- | --- |
| PG18.6最初43协议控制 | 全0，r67-reference43.log |
| 原主分支binary，SHA256为3f1c6502e0502758af9fdc11e8c324738465de3b19ea098ed4b32bd74f9f11f4 | 43中25失败，r67-original43.log |
| 扩展50协议，含三个CHECK OPTION和重复列 | PG全0，原binary50中26失败，r67-reference50.log/r67-original50.log |
| 第一轮候选43协议 | 1失败：quoted显式列名在legacy输出丢失，r67-first43-candidate.log；不算通过 |
| 第一轮native | 新11个严格拒绝/无副作用/合法追加/旧代/冷读控制通过；原view_test在WITH CHECK OPTION失败，r67-first-native.log；不算整批通过 |
| 第二轮50协议及八完整native | 新50协议全0，八native全0，r67-second50-candidate.log/r67-final-native.log |
| 第二轮完整协议邻居 | view_trigger_typed_values因数组列丢失数组标记报XX000，r67-final-protocols.log，不能算整批通过；主分支同驱动0，r67-root-view-trigger-control.log |
| 独立描述符探针 | 父view.nums为integer/23，原body为integer[]/23，r67-descriptor-probe.log，证明新视图注册漏数组标记而非provider生命周期错误 |
| 其余八完整协议 | 原样全部0，含完整postgres_protocol，r67-remaining-protocols.log及逐项日志；首个失败未被删除 |
| 扩展58协议（数组追加） | PG58全0，原binary58中30失败，r67-reference58.log/r67-original58.log |
| 数组修正后58协议 | 4失败：legacy协议仍报TEXT，另有CREATE bridge改写ARRAY，r67-accepted-protocols.log；八完整native全0，r67-accepted-native.log |
| 原八个协议邻居 | 数组修正后全0，含原typed view trigger，r67-array-neighbor-protocols.log |
| 第四轮76协议 | 5失败：4个array RowDescription仍为TEXT、未引用大写视图名未折叠，r67-fourth76-candidate.log；ARRAY定义、保存点及错误事务已通过 |
| 第五轮76协议 | 大写名称修复，4个array类型仍失败；补足完整绑定而非只处理缺失类型，r67-fifth76-candidate.log |
| 最终80协议 | PG80全0、原binary80中38失败、当前候选80全0，r67-reference80.log/r67-original80.log及r67-verified-protocol-logs/view_replacement_contract_protocol_e2e_test.log |

第一份native草稿误用单参数getClassByName，编译失败保留在
r67-original-native.log；改为现有resolveRelationSnapshot后，未修复产品在
首个错误替换的严格断言失败，r67-original-native-retry.log。这两种失败不混为一谈。
额外原生物化探针首次使用保留__tmp_名称而无会话，按现有规则被拒绝；
r67-descriptor-probe-second.log保留。改用普通独占测试表后确认物化schema本身
具有integer+array标记，r67-descriptor-probe-third.log，因此没有误改物理数组布局。
全部本机证据位于`/tmp/dbms-review-r46.ifu6SWLp/`。

## 当前组合验收

第六轮标准构建真实重编NetworkServer，之前各轮Main/DdlExecutor/parser也使用
本树实际O2对象，没有复制对象或伪造stamp。最终八完整native全0，
r67-verified-native.log；包含新12个严格拒绝、OID/定义/属性/依赖不变、合法追加、
旧代阳性、目录evict冷加载、旧零描述符及旧数组表示兼容。九完整协议全0，
r67-verified-protocols.log及r67-verified-protocol-logs/；包含新80、原typed view
trigger、WITH namespace、物化lateral/alias/refresh、schema原子性、窗口routine和
DEFAULT。门禁前后输入、binary和全部58份receipt一致。

原完整postgres_protocol_test另跑于r67-verified-full-pg.log，提交此核心修复时
仍在运行，不提前将它计为通过。本项也不把冻结1339入口的失败改写成全绿。

本项仅覆盖普通视图既有列名、OID类型、typmod及列前缀兼容性，不宣称完整view
DDL、collation传播、依赖图、所有更新规则或权限语义已经完成。用户暂缓专项不重启。

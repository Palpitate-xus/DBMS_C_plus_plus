# R03：窗口分区按声明的 SQL 值等价，而不是显示 scale

原 structured executor 用裸字符串判断分区相同，用猜测的 double 排序；
legacy 路径也用裸字符串加分隔符分组。`1.0/1.00/1` 被拆开，相邻大数或
高精度小数可能交错；numeric 状文本 `01/1` 不能获得虚假的数字等价关系。

structured 路径现在用真实 Column 的类型比较排序及判断分区边界，保留
NULL 分组语义和精确 NUMERIC。legacy 从原 query 的结构化值、NULL bitmap
和 row ID 读取源行，不再按空格重拆显示行；多个条件分支按真实 row ID
去重，不能丢掉内容相同的不同物理行。排序同样用声明类型，分区 identity
复用现有 GROUP BY 的长度编码/NULL 标签与精确 NUMERIC canonical key。
此项不修改窗口 ORDER key、peer、MIN/MAX 比较（另由 R02 修复）。

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`：

| 实际检查 | 原实现 | 候选主分支 |
| --- | --- | --- |
| 原生12配置：NUMERIC/TEXT/复合分区、ASC/DESC、全局输出双方向 | 12失败、exit 1 | 全通过、exit 0 |
| 协议18：structured 与强制 legacy、全行/子集/空输入 | 11失败、exit 1 | 全通过、exit 0 |
| 同18协议在实际PostgreSQL18.6 | 全通过、exit 0 | 同一 SQL/值/NULL/OID/标签/计数断言 |
| 新驱动和六完整窗口原生邻居 | — | 七完整native exit 0 |
| 新驱动和九完整协议邻居 | — | 十完整协议exit 0，含EXECUTE与完整postgres_protocol |

数据覆盖不同 scale、signed zero、2^53 附近交错数、20位整数和11位小数、
NUMERIC NULL、TEXT NULL/空串/数字状字符串和复合键。协议无最终 ORDER BY
时核对精确行袋，不要求 SQL 没有承诺的物理行顺序；原生另有明确最终排序。

原日志 `r03-original-native.log`、`r03-original-protocol.log` 保留。最终为
`root-r03-build.log`、`r03-candidate-native.log`、`r03-*_test.py.log`，参考为
`r03-postgres-reference.log`。真实重编 Main/ExecutionPlan 并标准链接exit 0；
全部58份当前生产receipt和输入不变门禁通过。未改公共头或借私树对象。
R02 的未提交新夹具独立工作，不混入本项 commit；这不是全注册终态。

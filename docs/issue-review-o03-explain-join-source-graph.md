# O03：EXPLAIN 展示实际 JOIN 来源及正确 OFFSET 断言

原完整 JOIN 驱动在合法 `OFFSET 1` 处要求 `0A000`，实际查询正确返回成功。
改成阳性后还发现独立产品问题：prepared OFFSET 路径执行 JOIN，却只
显示无子节点的 TypedSource，既没有 JOIN，也没有真实扫描及其计数。

## 修改方法

实际 DmlSourceNode 现在是带 instrumentation 的 materialized operator：
`readAt` 就是执行读取/缓存复用的入口，children 指向该入口真正调用的
物理 TableScan 或左右来源节点。TypedSource 暴露同一节点，不另建展示图。
缓存命中计为 materialized 节点读取，不伪装成重新扫描物理表。

原四行左右表的 OFFSET 1 实际图为：

```text
Offset：输出4
└─ TypedProject / TypedSource：输出5
   └─ MaterializedNestedLoopJoin：输出5
      ├─ Materialize：读取9 → 左 TableScan：读取4
      └─ Materialize：读取16 → 右 TableScan：读取4
```

相关调用在同一 source/cache/join/scan 算子上重绑 caller row：关闭来源、
清空该次缓存、复位 JOIN 匹配位置并取得当前 execution owner，不重建图并
丢失先前计数。现有 logical/derived/CTE producer 的重绑限制保留，未宣称
其完整参数化生命周期已实现。规划/plain EXPLAIN 不读取来源，LIMIT 0
不启动扫描；本项目未启动节点的 JSON 不输出实际计数字段，测试要求字段
缺席，不能伪造一次执行。

原五种 JOIN 和全部负向检查保留；OFFSET 改为精确阳性，新增OFFSET 0/1/5、
self join、空右侧、LIMIT 0、相关子查询重复执行。尾部 `garbage` 原来也
错误要求 `0A000`；旧程序和 PostgreSQL18 实测均为语法错误 `42601`，
现保持严格的正确 SQLSTATE 断言，不修改产品语法。

## 实际证据与验收

证据目录：`/tmp/dbms-review-fix-donor.N9sdas6f/`。

| 检查 | 实际终态 / 日志 |
| --- | --- |
| 原完整驱动 | exit1，合法OFFSET无错误；`o03-original-whole-protocol.log` |
| 原程序OFFSET阳性图 | exit1，缺JOIN/扫描；`o03-final-positive-original-protocol.log` |
| PostgreSQL18.6 / 180006 | OFFSET四行、相关子查询NULL/字面量NULL通过；`o03-offset-correlated-postgres-reference.log` |
| 旧程序及PG语法错误 | 两者42601；`o03-garbage-original-and-pg.log` |
| Root标准构建 | 真实重编Main并链接exit0；`root-o03-build.log` |
| 最终完整JOIN驱动 | 44 plan / 4 correlated / 11 negative / 1 health共60控制全过；`o03-final60-explain-join-protocol.log` |
| 十二完整原生邻居 | 全exit0、58当前receipts/终态输入一致；`o03-final-native.log` |
| 十完整协议/CLI邻居 | 全exit0；`o03-accepted-protocols.log`、`o03-accepted-*_test.py.log` |

原生包含source context、WITH logical source、bound DML children、实际DML
plan、typed EXPLAIN、descriptor/JSON actuals/timing、constant planning及
R13编码/原logical decoding。协议包含JOIN、typed execution、JSON actuals、
timing、原ANALYZE、CLI事务发布、bound children、DML EXPLAIN、R13和完整PG协议。

首次候选的真实扫描名为TableScan，夹具误按SeqScan取节点；第二次夹具误要求
LIMIT0未启动节点有actualRows。两次日志保留。第三次到达原garbage断言才失败。
校准节点与零需求合约、实测语法错误后完整重跑，不删除控制或缩短驱动。

本项不承诺完整PG planner成本/基数估计或所有JOIN形态。R04 frame、O05重复
别名和最终全注册复验仍待完成。

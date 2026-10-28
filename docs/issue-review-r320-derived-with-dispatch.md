# R320：derived table 的 WITH 查询未被执行分派接收

状态：已独立实际复现，OPEN，未修复；不是用户暂缓项。

```sql
SELECT v FROM (
  WITH c AS (SELECT id,v FROM r320_base)
  SELECT id,v FROM c
) q ORDER BY id;
```

独立 PostgreSQL 18.6 与两个 V2 产品服务器各完整观察 14 请求。参考无
SQL 错误，两侧产品各六个 42P01，消息把 `"(with"` 当作关系名。普通
`(SELECT ...)` 来源和顶层 WITH 控制都无 SQL 错误；其行值、命令、Ready
与参考相符。Simple、Extended、二进制输出均复现；不含任何窗口函数，
因此不是只改 LAG/LEAD 来源绑定就能关闭的问题。

`processDerivedTables` 的 `findDerivedStart` 只查找 `(select` 和 `(values`，
没有接收实际语法中的 WITH 子查询，之后把残留 FROM 字符串当作表名。
修复应按真正的 derived query 语法角色接收 WITH 查询，并保留其局部
CTE/继承/遮蔽边界、列和 NULL；不能全局替换 WITH、吞掉 42P01 或假造
空结果。需保留已有 SELECT/VALUES、LATERAL、标量/谓词子查询区别。

R318 的冻结 747 还保留 12 个含窗口的相同 derived-WITH 失败。它们是
OPEN，不得分类为已暂缓 R217 Describe 或 R156 呈现，也不得从夹具删除。
应独立修复、验证并 commit；此文档没有修复/验收声明。

证据：`/tmp/dbms-r318-window-scope-fix.XhbfSKH9/` 中
`completed-r320-observations-receipt.json`、`*-r320-observation.log`；
本轮私有参考集群已停止，默认超时和全部输入未改。
总 273 清单未完成，不 push，不启用 Actions。

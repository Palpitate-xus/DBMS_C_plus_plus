# R319：外层标量表达式没有接收真实窗口结果

状态：已实际复现，尚未修复，OPEN；未被用户暂缓。

R317 完整 800 项包括 Simple/Extended/二进制输出下的两种函数共六个请求：

```sql
SELECT COALESCE(
  lag(v,1,2147483648) OVER(PARTITION BY p ORDER BY id),7
) AS v FROM r317_w ORDER BY id;
```

PostgreSQL 18.6 正常返回六行且类型为 BIGINT；旧 Root/accepted-only 返回
42883。把 LAG 换为 LEAD 同样实际复现。独立窗口目标与多窗口目标的成功
不能替代这些失败，也不能据此将整个 QRY-08 标作完成。

源码线索：legacy 窗口提取及通用标量求值可能没有把表达式内部的真实
WindowCall 交给窗口 owner；尚未完成调用链定位，不能把该推测记作定案。
修改方向：在 query-owned 作用域中提取并绑定窗口节点，计算其真实 typed
结果槽，再让外层 COALESCE/标量投影消费该槽，保留逐行位置、共同类型、
NULL 和实际需求；不能把 LAG/LEAD 注册成假标量回调，或固定返回 NULL。

证据：`/tmp/dbms-r317-window-offset-audit.vcBaloAt/`，完整 800 项索引
775、780、785、790、795、800。原始请求、参考帧和失败断言全部保留。
没有生产修改、验收或通过声明；须独立修复、验证和 commit。
不恢复完整 presentation/ProjectSet/通用 raw analyzer 用户暂缓模块。
原 273 总清单未完成，不 push，不启用 Actions。

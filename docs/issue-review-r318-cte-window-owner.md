# R318：CTE 来源的 LAG/LEAD 执行丢失关系所有者

状态：已实际复现，尚未修复，OPEN；未被用户暂缓。

R317 的完整 800 项保留了两种函数在 Simple/Extended/二进制输出下的六个
CTE 请求。PostgreSQL 18.6 成功返回六行，旧 Root/accepted-only 均返回
42P01。例如：

```sql
WITH w AS (SELECT id,p,v,n,d FROM r317_w)
SELECT lag(v,1,2147483648) OVER(PARTITION BY p ORDER BY id) AS v
FROM w ORDER BY id;
```

源码线索：Main 的 `windowProtocolType` 为取得 LAG/LEAD 共同结果类型，
拼出独立 `SELECT <window> FROM <tnameOrig>` 并重新绑定。逻辑 CTE 名离开
当前 query 的来源/继承绑定后可能被当作实际 catalog 表；这是源码推测，
尚未完成独立调用链跟踪，不能据此宣称根因或修复已确认。

修改方向：让窗口接收端使用当前实际 bound query 的输出和 sourceRange，
或在合法的继承/物化映射下取声明，不孤立重解析原逻辑 FROM 名；保留真实
CTE 遮蔽、作用域、类型和 NULL 行。不得用把名字改成物理字符串、忽略 42P01
或固定返回空结果来绕过。若需公共头/owner 结构修改，必须完整重建受影响 TU。
不恢复用户已暂缓的通用 CTE Describe/CASE label 或全局缓存生命周期改造。

证据：`/tmp/dbms-r317-window-offset-audit.vcBaloAt/`，完整 800 项的索引
773、778、783、788、793、798。原始输入、PG 帧和严格失败全部保留；
没有生产修改、验收或通过声明。须独立复现、修复、验证和 commit。
原 273 总清单未完成，不 push，不启用 Actions。

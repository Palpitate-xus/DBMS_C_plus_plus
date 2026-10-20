# R194：索引损坏测试应区分实际索引读取与独立堆读取

状态：修正一条过期夹具预期，生产代码未修改；不是完整索引损坏检测证明。

## 发现和原因

冻结 2442e4c7 的原全量在 `index_corruption_protocol_e2e_test.py` 失败：
直接 `SELECT count(*) FROM corrupt_items WHERE state=8` 被要求返回 XX001，
实际上返回了精确的 `1`。R193 后的两套当前服务器重新执行完整原夹具，均在同一处失败。

不能因为查询文本含索引列，就假定执行器读取了该索引。
在同样的单行数据与原索引定义上，修复前分别对两套服务器收集了原查询及控制的
EXPLAIN、EXPLAIN ANALYZE 和实际结果，共 40 组观察：

| 查询来源 | 实际计划 | 原夹具指定的索引损坏后实际结果 |
| --- | --- | --- |
| id/state 单列条件的两个 SELECT | IndexScan | XX001 |
| tenant/state 组合条件 SELECT | BitmapHeapScan | XX001 |
| 分组 COUNT，不带或带 HAVING | IndexScan + GroupAggregate | 两者均 XX001 |
| 独立全局 COUNT | TableScan + TypedFilter + Aggregate | 精确 `1`，未读取坏索引 |
| EXISTS 匹配/不匹配、普通全表 SELECT | 独立堆读取路径 | 精确 t/f 和原始行 |

代码路径也吻合：Main 的直接 SUM/COUNT 入口采用已有已绑定来源执行器，真实底层为
TableScan；分组 COUNT/HAVING 则仍使用现有物理索引聚合路径。
因此只有独立 COUNT 被错分到索引错误组。不能把分组 COUNT/HAVING 一起放宽，
也不能让未使用坏索引的堆读取无条件报损坏。

## 修改方法

原夹具中五个实际索引读取者继续精确要求 XX001 和空行；现为五个查询全部先验证
IndexScan/BitmapHeapScan 计划，而不再只检查前三个。
原直接 COUNT 移到堆读取组，保留相同 SQL 和精确结果 `1`；新增 state=99 的
独立 COUNT，精确要求 `0`。两个 COUNT 都先验证真实 TableScan，且不得出现
IndexScan/BitmapHeapScan。原 EXISTS 的 t/f、全表结果和 SELECT 42 控制全部保留。

原数据、SQL、索引定义、相同三份私有索引破坏输入、实际索引读取者的 XX001、
所有原成功结果、超时和持久化设置均未删除或削弱。
没有接受任意状态码或根据运行结果动态生成期望。
破坏操作仅运行于本次新建测试目录，不访问用户数据或已有服务器。

## 验证与边界

纯提交生产库与包含原有未接受改动的工作区生产库，分别完整执行修订夹具，实际均退出 0。
两侧均到达 `INDEX_READER_ROUTE_COMPLETE 5 INDEX_ERRORS 4 EXACT_HEAP_CONTROLS` 和原完成标记，
另外仍检查普通全表结果及 SELECT 42。
没有生产或共享头变更，无新编译；逐一核对各套 58 个生产对象、127 个公共头、编译器、
stub、完整来源字节和精确服务器 hash，不把旧正常构建的默认 dbms_main 当成当前候选。
使用的是 R193 实际新链接的两套匹配服务器。

证据：

- 原计划/ANALYZE/执行观察：`/tmp/dbms-index-reader-plan-observation.bat5e7bx/completed-observation-receipt.json`，SHA256 `18a4f26f40637f00fe77bba48312d1a3077868b3169b2bde0d6ff1f75af2e4be`。
- 同两套当前服务器原红/新绿：`/tmp/dbms-r194-index-readers.uaibdrtc/completed-focused-receipt.json`，SHA256 `ac3a664470379d24f342f1891567dedf5e7171f7a453a1c8b634f4059d54a10a`。
- 当前匹配生产来源：`/tmp/dbms-r193-current-producers.sesfsk_o/completed-producer-receipt.json`，SHA256 `2c6f039fb7b9ba6c03f29564de4ea5876cd972112dd41a03b9b99a7ace3830ac`。

原 1492 的失败原样保留，不以本次定向通过改写原全量。未运行修复后完整 1492，
未关闭原 273 项需求；也不是 PostgreSQL 文件格式破坏差分、全部索引算法、TLS 或 sanitizer 证明。
用户已跳过的模块保持原状；独立本地提交，不 push，不启用 Actions。

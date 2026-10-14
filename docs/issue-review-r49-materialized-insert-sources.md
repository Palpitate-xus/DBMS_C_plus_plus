# R49：INSERT SELECT 必须消费物化视图的实际 backing relation

## 问题与修改

冻结主分支 `f4087f5d` 的完整 matview_test 在原 unpopulated DML 控制失败；
保留原语句的独立诊断返回0A000（prepared source descriptor不匹配），而非
实际 PostgreSQL 的55000。默认 prepared reader 和原生 JOIN source 将物化视图
的逻辑名称当作普通 heap，因而读不到真实 backing schema/rows。

两条实际 reader 现在以 binder 保留的 canonical schema/name 调用现有
`resolveMaterializedView`。未填充视图报55000，已填充视图使用真实 backingTable
建立原 TableScan；不执行定义SQL、不虚构行、不改变逻辑目标的既有权限检查。
临时关系不因同名持久物化视图而被重定向。默认 reader 的递归派生/CTE consumer
也复用此实际路径，JOIN 使用同一元数据生产者，并保留NULL及独立来源身份。

新完整控制暴露 INSERT source 入口未接收 `WITH ... SELECT`，尽管既有 SELECT
parser 已有该语法。入口现在交回该 parser 并保留 ON CONFLICT/RETURNING边界，
仅接受真正 SELECT/VALUES AST，不能把WITH的DML主语句当作INSERT来源。同步
拒绝缺AS、缺括号、未闭合子查询或缺主语句的WITH，保留真实child AST和source span。

原 matview 夹具跨REFRESH/DROP保存借用catalog时会检查旧generation；现在每次
边界后重新查询实际owner，所有原 population/数据/依赖/回滚断言保持不变。

## 实际验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 原始/中间 | 最终候选 |
| --- | --- | --- |
| 完整新协议41控制 | 主分支22条失败，r49-original-protocol.log；实际PG18.6全0 | 全0，r49-third-final-protocol.log |
| 原完整 matview_test | 主分支55000断言失败；第一候选后续旧catalog引用断言失败 | 全0，r49-third-final-native.log |
| 新纯parser24控制 | 主分支首个合法WITH source失败；中间候选接受缺主语句的WITH | 两种解析入口、合法/非法source、冲突和RETURNING全0 |
| 完整native组合 | 不以中间通过覆盖失败 | 十个完整驱动全0，r49-third-final-native.log |
| 完整协议邻居 | 第一/第二候选的已通过邻居单独保留 | 最终八个完整邻居全0，加新41共九个完整入口 |
| 标准构建 | 首次两个CPP和两轮parser增量均实际O2编译/链接 | 最终全部58个当前源/头/flags receipts及结束输入检查通过 |

原始41、第一次候选41（两项失败）、parser原生首失败及缺主语句诊断均保留，
分别见r49-original-protocol.log、r49-first-protocol.log、r49-parser-root-baseline.log、
r49-parser-negative-diagnostic.log；不删除失败SQL或把非法输入改成合法输入。
真实参考日志r49-postgres-reference.log，标准构建r49-first/second/third-standard-build.log。

控制覆盖unpopulated/REFRESH WITH NO DATA拒绝、纯绑定列错误、真实NULL/空串/
文本NULL、四行到五行refresh隔离、plain/alias/derived/CTE/JOIN/order来源和
ON CONFLICT更新/忽略。邻居含原完整INSERT source/冲突/输入阶段、WITH DML、
物化视图刷新、prepared context和catalog snapshot，不冒完整查询族或全注册绿。

用户暂缓的CTAS/后台checkpoint专项、权限/TDE和R15不重开。本项只修复普通
查询消费者的已有物化视图来源，不新增并发REFRESH或其他未支持的维护功能。

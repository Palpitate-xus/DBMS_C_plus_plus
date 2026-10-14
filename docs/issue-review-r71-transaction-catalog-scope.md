# R71：事务启动错误发布其他数据库的目录

状态：BEGIN/延迟晋升的目标目录范围已修复，定向门禁通过；完整新注册组合尚未重跑。

普通BEGIN及延迟ownership晋升调用CatalogService::persistAll，遍历整个engine的
目录缓存。构造engine时加载的其他数据库可能仍有未发布的bootstrap模型；当另一
engine持有那个数据库的DDL锁时，目标数据库的BEGIN会因为这个无关模型返回
IO_ERROR。目标数据库本身并没有I/O故障。

修改方法：给目录持久化增加明确的数据库范围，BEGIN及延迟ownership晋升只发布
目标数据库，保留原generation检查、COW刷新、真实脏冲突和失败状态清理。全库
显式persistAll及shutdown的语义不能悄悄缩小；不重启用户暂缓的后台checkpoint、
DROP竞争或provider生命周期专项。

新native建立两个真实数据库和两个engine：reader保留未发布目录，writer对一个
数据库持有真实DDL事务锁，reader在另一个数据库BEGIN应成功。现程序返回
DBStatus 5/IO_ERROR并触发原断言，见本机
`/tmp/dbms-review-r46.ifu6SWLp/r71-old-native.log`。

还须保留目标目录真实故障、pending metadata、失败后不留下活动事务及健康重试
控制，不能用“忽略所有persist错误”获得成功。原shared_heap_engine_generation、
snapshot_export_import、snapshot_exporter_visibility三个完整驱动须实际重跑；
当前组合已实际重跑三个原完整入口并全部0，但不改写冻结全量的历史FAILED。

CatalogService::persistAll的可选database参数只筛选目标缓存，保留全部既有generation、
peer/COW、真实故障规则；无参数调用仍是全库。两个事务入口传入实际目标DB。
公共头改变后当前工作区真实重编58生产单元，`r71-r73-fresh58.log`全58/标准stamp0。
当前组合也包含R72/R73候选，不将该构建冒充单独R71的冻结输入。

`r71-originals-native.log`六个完整原入口0，包含三个generation/snapshot原失败以及
begin presence、cross-database、deferred transaction；`r71-strong-native.log`新增
实际双DB锁、真实目标description原子rename的线程局部EIO、pending metadata、
失败清理与健康重试全部0。首尾生产输入/58份receipt一致。

最初增强夹具把目录文件移走再恢复后要求dirty model自动发布，实际改变了ctime，
因此触发合法generation冲突。`r71-first-native.log`保留该失败；最终控制直接在
本线程精确目标的真实rename上注入EIO，既验证瞬时I/O恢复，也不削弱脏冲突保护。

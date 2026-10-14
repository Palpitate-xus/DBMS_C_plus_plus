# R71：事务启动错误发布其他数据库的目录

状态：已复现，待修复及完整回归。

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
目前只有共享根因线索，不能据此把历史失败改为通过。

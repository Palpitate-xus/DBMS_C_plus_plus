# R72：重命名数据库重复获取同一名称锁

状态：本项重复锁已修复；两个原完整入口通过，完整新组合另行验收。

databaseTxnLockFor按物理路径canonicalize名称，合法源数据库与指向它的symlink
目标返回同一个shared_mutex。renameDatabase按两个字符串分别获取exclusive锁，
却没有判断实际是同一把。第二次try_lock失败后误报DATABASE_IN_USE/55006，
未到既有目标占用检查；原测试要求TABLE_ALREADY_EXISTS/42P04。

修改方法是按真实mutex身份去重获取，让PhysicalCatalogOwnership引用实际持有的
锁。保留锁内源/目标检查、symlink拒绝、archive占用及事务互斥；不能通过覆盖或
跟随目标完成rename。这是普通rename缺陷，不是用户暂缓的DROP调查。

原冻结全量已记录native与协议失败；当前修复前协议再次原样失败于相同SQL和
55006，保留`/tmp/dbms-review-r46.ifu6SWLp/r72-old-protocol.log`。
修复后须运行两个完整原驱动，保留所有原文件、目录、dangling symlink及archive
sentinel断言，不能通过删除检查、延长期限或改成55006来标绿。

实现只在真实mutex不同才取得第二把锁；同一mutex时PhysicalCatalogOwnership使用
已持有的firstLock。锁内目标占用检查、有效数据库判定、archive规则全部保留。
`r72-r74-first-native.log`的原完整directory_guard入口0，之后owner失败单独归R74/R75，
不冒充该native批次全0。`r71-r73-first-protocols.log`五个完整协议全0，包含原完整
directory_guard；public头组合真实fresh58记录于`r71-r73-fresh58.log`，不是单独R72
冻结输入。原42P04期待及全部文件/symlink/archive sentinel断言没有修改。

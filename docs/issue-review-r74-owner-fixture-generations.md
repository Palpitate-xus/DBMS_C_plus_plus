# R74：两个owner回归夹具的初始化与目录代

状态：夹具修正候选，等待完整原驱动验收；没有修改产品权限或owner策略。

冻结全量中的table_owner_atomicity与table_owner_peer_retry在首次get(info)时
失败于3D000。普通CatalogService不能创建不存在的数据库；夹具应通过现有明确
cluster bootstrap入口建立保留目录，不能为了测试恢复普通getter的隐式创建。

后续断言还需与现有COW目录代一致：peer刷新后用有租约的当前代读实际owner；
刻意制造local-unsaved/peer冲突时，则保留真实被修改的当前model，不能换成新
getter丢掉故障条件。沿用已有CatalogGenerationView，初始化和故障写仍有明确owner。

故意把pg_attribute文件替换成目录时，公开schema读取必须继续报告58030。
夹具在故障期间比较完整原.stc字节和真实当前model的owner，恢复后再用公开API
核对owner；真实脏冲突期间同样保留字节、两个独立model、磁盘描述及健康重试
检查。不能吞掉目录错误、修改owner期待、取消故障或放宽原数据断言来标绿。

原O01修复与历史通过记录仍保留于[旧记录](issue-review-o01-table-owner-health-retry.md)。
本次两个原驱动及目录错误/持久化/快照邻居仍需重新完整运行，不能把初始化修正
当作整项已经通过。本机初次test编译缺少DbError直接include的日志也保留。

## R75：普通owner操作的目录错误边界

修正bootstrap后，两个原完整夹具仍实际abort于domain catalog的58030。
attribute-target阶段的独立标记已定位：alterTableOwner取得metadata锁后，在try
边界外调用getTableSchema；目录错误因此逃出DBStatus接口，锁也未正常释放。
同样的边界在真实local-unsaved/peer冲突中触发。修复应只规范普通metadata读取
错误和锁释放，保留原角色选择及全部policy；58030按现有IO_ERROR合约返回，其他
结构化错误不能被无条件吞掉。须重新运行两个原完整夹具及持久化邻居。

R75产品修复使用作用域metadata锁释放，所有正常/错误/异常出口一致清理；仅把
该读取边界的58030映射为原IO_ERROR合约，其他结构化错误仍抛出。没有修改角色
选择、权限策略、目录generation保护或真实冲突处理。

当前主工作区组合的两个完整owner驱动及七个相邻native均0，保留原所有数据/故障
检查；新增公开58030、完整schema字节和无遗留metadata锁检查亦通过。
日志`/tmp/dbms-review-r46.ifu6SWLp/r74-r75-verified-native.log`；产品构建为
`r73-r75-final-fresh58.log`真正58单元O2及后续normal/repeat 0，门禁首尾生产输入
和58份receipt一致。这里包含R74夹具候选，产品边界与夹具分别提交，不把它写成
未含R74时的独立冻结通过。原abort及首次缺include编译失败不覆盖。

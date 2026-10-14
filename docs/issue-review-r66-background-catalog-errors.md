# R66：后台写回不能让可恢复目录错误终止整个进程

## 原因及修复

R61的最终完整门禁先后在普通冷重载场景出现SIGABRT。外部throw拦截及当前
实际测试二进制符号解析确认：readDomainColumns→getTableSchema→dataPath→
backgroundBufferFlush→backgroundWorkerLoop。目录镜像变化产生的58030在每个
allocator的catch中重新抛出，最终越过工作线程入口并调用terminate。

修复只在普通后台heap写回的每allocator错误边界内：保留真实SQLSTATE/message
诊断，不对出错allocator继续写回，让原有dirty状态留待后续健康轮次重试，
并继续处理其他allocator。原pending undo与55P03的静默延期合约不变。
这不是通过禁用后台线程或延长其间隔得到通过，也没有修改DROP锁竞争、
关停、checkpoint、目录冲突检测或身份验证代码。

## 独立可控复现

新原生入口只在本进程的后台线程、目标数据库的实际namespace镜像lstat上注入
一次EIO，不修改磁盘文件或影响前台查询。持有已标脏的合法heap页，在下一轮
目录读取处建立屏障，检查worker仍存活且dirty/pin状态保留；解除屏障并释放
pin后等待实际heap fsync成功，最后确认真实行值仍17。

该入口使用10ms后台loop及原300000ms checkpoint间隔，没有执行checkpoint专项。
测试进程由既有隔离runner管理，退出后先正常关闭engine，再清理其独占临时
目录；不附带竞态DROP DATABASE断言来重开用户暂缓的R51。

## 实际证据

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 未修正主分支控制 | r66-root-strong-original.log明确记录真实EIO注入后58030/exit134；原r66-root-original.log亦失败 |
| 最终标准生产构建 | r66-first-standard-build.log，真实O2重编变化TableManage并链接0，无公共头变化 |
| 最终完整native门禁 | r66-final-native.log，十三完整入口全0，含新控制、两个完整R61驱动及原partition/unlogged/allocator/CREATE邻居；开始/结束58份当前receipts及输入一致 |
| 重复故障/冷重载 | r66-five-repeat.log，新后台控制和完整七故障initializer交替五轮，共十次全0，不删除原失败 |
| 完整协议门禁 | r66-final-protocols.log，六完整入口全0，含schema、BYTEA、MV、窗口及原missing_index/pending rollback恢复 |

最初协议命令拼错discard驱动名，在全部入口预检查时exit2、未执行任何协议；
保留r66-protocols.log，使用真实`discard_all_pending_protocol_e2e_test.py`后另存
最终六项终态。原R61批次的SIGABRT、固定EIO主分支失败、先前孤立通过均保留。

R61中退役partition缓存的writeback失败警告仍可出现，现有记录不掩盖，也不能
据本修复声称已解决其cache生命周期。主分支1339完整回归继续保持原冻结输入，
本项独立commit后等待终态再集成；不将私树门禁冒充主分支当前全量通过。

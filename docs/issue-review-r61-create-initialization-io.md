# R61：普通建表必须检查初始化写入并保留I/O错误类型

## 实际缺陷与修复方法

未修正主分支的真实EIO控制在物理relation ID持久化失败时返回INVALID_VALUE/
22023。另一个独立控制让新heap的初始化marker fsync失败，CREATE却返回OK、
表仍可见。`PageAllocator::close()`及索引close不能返回刷盘结果，而建表只检查
open，schema输出也在flush/close之前检查stream状态，因此错误被隐藏。

修复限于普通`StorageEngine::createTable`初始化：

- 在新heap、分区heap、TOAST heap、主键及TOAST索引owner仍存活时检查真实
  flush结果；close全部局部owner后再做失败清理，避免清理后析构又写出同名文件。
- schema显式flush并检查close终态，避免缓冲write的错误在成功返回之后才出现。
  这是write/close错误传播，不宣称已验证schema在任意掉电时的完整持久性。
- 初始化I/O返回IO_ERROR/58030；物理身份计数器读取区分不可读I/O、损坏XX001
  与编号耗尽22023，不覆盖损坏内容，不回收已经烧掉的identity。
- UNIQUE索引只传播既有producer的真实status，不把其语义错误统一改成I/O。
  UNIQUE producer内部的完整错误覆盖不在本项验收证明之内。

没有修改CTAS、后台checkpoint或用户暂缓的DROP/关停专项。

## 验证及保留的中间失败

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 原主分支真实身份持久化EIO | 返回22023、exit134，r61-root-original-native.log |
| 原主分支真实初始化fsync EIO | 返回OK/visible=1、exit134，r61-root-initializer-original.log |
| 真实标准O2构建 | 只重编变化TableManage并链接0，r61-first-standard-build.log；无公共头变化 |
| 四种I/O及计数器控制 | 身份/schema open/heap open/table list均58030；损坏XX001/耗尽22023，r61-second-native.log |
| 七种初始化故障 | heap marker/data、partition、TOAST heap、primary/TOAST index、schema write均58030且visible=0，r61-third-initializer.log |
| 完整协议邻居 | 四入口全0，r61-protocols.log；schema原子性、BYTEA目录、MV insert source和R60窗口身份 |

两个新增完整native保留原数据、不发布失败表、同名retry、重复CREATE拒绝、
精确SQLSTATE及cause。七初始化场景每个在失败和健康retry后分别cold exec，
检查实际磁盘重载，不借用父进程的cache或marker owner。

初次候选IO驱动在原不相关FK诊断文案断言失败；按当前未改的原生校验文案
改为精确`unsupported foreign key referential action`，没有改status或语义校验。
初始化驱动最初在显式事务内写分区表，命中既有INVALID_VALUE拒绝边界；独立
未改产品主分支证明确为原有边界，而非初始化修复回归。最终分区阳性使用原有
native非事务route并结束独立writer生命周期，普通表仍跨真实COMMIT。
证据为`r61-first-io-native.log`、`r61-second-initializer.log`及
`/tmp/dbms-review-orphan-controls.vQleNfjM/partition-healthy-supported-original.log`。
首次诊断私有方法编译失败也保留，不能算产品失败或验证通过。

`r61-third-initializer.log`中的background writeback警告保留。首轮十二完整native
批次实际在第二个入口异常退出，不能称十二项通过：`r61-final-native.log`。
改用真实exec的分区writer仍复现，排除了只由同进程第二engine造成的解释，
`r61-cold-writer-initializer.log`。外部throw拦截与当前实际测试二进制符号解析
确认路径为readDomainColumns→getTableSchema→dataPath→backgroundBufferFlush→
backgroundWorkerLoop，未捕获的58030让工作线程调用terminate。证据
`r61-catalog-throw.log`，不是初始化write/fsync结果，也不是checkpoint专项。

后台写回异常边界另列R66继续修复；不改原失败记录、不禁用后台时序、不扩大
用户暂缓范围。I/O核心改动可独立提交，但整体新native验收须待R66闭环，
不能以先前一次完整initializer或十完整相邻native通过冒充最终十二全0。

后续[R66](issue-review-r66-background-catalog-errors.md)独立修复写回错误边界后，
`r66-final-native.log`十三完整native及`r66-final-protocols.log`六完整协议均0，
`r66-five-repeat.log`包含完整七故障initializer五次及新后台控制五次，全0。
这是含R66组合的验收，不反写`7ad3ccca`单独候选的原异常终态；用户暂缓专项
及原主分支冻结1339完整回归保持不变，退役partition缓存警告仍保留。

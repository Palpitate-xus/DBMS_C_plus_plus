# R254：CLOG读取错误不得变成零状态或成功覆盖

状态：有限提交状态段读取修复通过验证；TXN/WAL/P0-14及原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 影响 | 修改方法 |
| --- | --- | --- |
| 冷加载忽略打开或读取错误 | 返回/缓存虚假的InProgress；移除故障后的重试仍可使用错误缓存 | 完整读取私有buffer，硬错误以DbError 58030传播，成功后才加入缓存 |
| 刷新忽略读取错误或把路径错误当缺失 | 已知状态被部分/零段替换，或错误返回过时缓存 | 只将ENOENT按缺失处理，其他元数据/读取错误不发布buffer和新缓存标记 |
| 刷盘合并忽略最新段读取错误 | flush返回成功，磁盘上其他backend已持久化的状态实际被覆盖 | 读取失败返回false，不创建/替换新段，保留dirty/pending供重试 |
| truncate调用上述合并后仍删除原段 | 在未能读取/合并最新状态时移除唯一持久段 | 既有saved门控接收真实失败结果，不删除原段；保留可重试状态 |
| 冷加载在读取后从路径记录inode | 读取途中原子替换文件后，旧字节被误标为新inode，下一次lookup也不刷新 | 从实际读取的fd取得身份，原路径已换代时由既有刷新检查重读 |

三个读取入口共用私有辅助函数，区分Missing/Loaded/Failed，完成正数短读取并重试
open/read的EINTR。真实ENOENT和真实短段EOF继续使用零尾/InProgress约定；不是把
所有短段当损坏，也没有新增PG CLOG格式/checksum承诺。读fd在所有函数出口关闭。
无头字段/对象布局、持久化格式、Main、TDE、权限或全局缓存锁修改。

## 完整49项旧新对照

正式V3同一夹具在Root及独立纯提交投影的旧实现均完整49项、17项失败、退出1；
新实现两侧完整49项零失败、退出0。32个旧正例保持通过，不称为新修复了49个错误。

| 入口/交错 | 控制数 | 旧失败数 | 修复后 |
| --- | --- | --- | --- |
| 冷加载：12种读取/路径情形 | 12 | 5 | 12项通过 |
| 已缓存段的外部换代刷新，同样12种情形 | 12 | 4 | 12项通过 |
| 普通flush合并：同样12种情形 | 12 | 4 | 12项通过 |
| truncate借用已有锁的保存/删除：同样12种情形 | 12 | 3 | 12项通过 |
| 读到旧fd内容后、记录缓存身份前原子替换路径 | 1 | 1 | 两次lookup均观察到正确提交状态 |

12种情形为普通读取、EIO、读取部分字节后EIO、一次EINTR、正数短传输、多次EINTR、
短传输后EINTR、open EIO、open EINTR、真实缺失、真实旧短段EOF，以及pg_xact路径
暂被普通文件阻断。旧ifstream本来已正确处理本轮read EINTR/正数短传输，不把这些
正例误称为旧缺陷；open EINTR在所选刷新/截断控制也原本通过。

合并故障检查原始段全部字节完全未变、自己的pending仍可读取、移除故障后重试
成功，并用新CommitLog验证不同字节位置的原状态/同backend更新/peer更新。旧普通
flush的EIO/部分EIO/open EIO三项实际改坏磁盘段；旧open EINTR合并虽然返回成功，
仍丢失peer状态。旧截断三个硬错误控制实际删除原段，而新实现保留原文件。
本项只在私有测试文件中制造这些故障，没有修改用户数据库。

测试`tests/clog_read_failure_test.cpp`转发真实libc文件操作，仅在arm且匹配路径或
dev/inode时注入确定性错误/传输上限。返回的数据、EOF、原子rename和文件快照均
来自真实文件；没有伪造提前EOF。另检查每项前后fd数量不增长。inode交错通过实际
read返回旧数据后立刻执行rename来控制，不依赖随机线程调度或计时阈值。故障是
测试适配器注入，不声称测到了自然磁盘损坏、内核信号或真实断电。

V1观察器只包装fopen64，六个open故障控制实际未触发；V1的14个失败不能全部归为
产品故障。V2补上本机实际使用的fopen路径后，同36项两侧旧实现均13项失败，新实现
均零失败。V3原样保留36项，再增加12项truncate和1项实际inode交错；原控制/期望
未删除或放宽。V1/V2日志与各版夹具均保留。

## 回归与构建范围

本轮86次有限程序运行全部退出0：每侧新49项、32个既有原生、3个真实Main frontend
及7个完整协议。既有原生覆盖CLOG普通/跨backend合并/刷新/截断、真实事务集成、
COMMIT错误类型、rollback恢复图和pending buffer、insert/update/delete回滚失败、
跨进程XID、redo crash recovery，以及R246至R253缓存、shared heap、derived maps、
页分配、BTree topology、savepoint alias等回归。

协议为savepoint_read_only、index_corruption、explain_json_analyze、explain_json_cache、
explain_timing、commit_failure_recovery和transaction_begin_error_state。保留原断言与
默认超时；这些协议不是通过外部IO注入直接证明58030传输的新专项。

定向ASan/UBSan完整49项零失败、实际退出0，CommitLog.cpp与新测试TU均插桩，使用
原检查选项；不宣称全库sanitizer或TSan通过。新生产TU及夹具编译无警告。

每侧fresh编译CommitLog.cpp、新49项夹具、32个既有原生测试，其他生产对象逐项核对
源码、134/131头、编译器和对象哈希后复用。Root62/纯提交60生产CPP严格分开；并非
本轮全122CPP fresh或normal/repeat build。6个Main frontend复用R247匹配对象，先
重链接还原旧二进制SHA，再链接当前库运行，不链接test_stubs。Root仍采用pre-R229
Main投影；没有恢复Main草稿或使用被中断的R244新布局对象。

2281个提交源码输入与测试纯投影一致。957原生+597注册Python+3Main=1557只是库存。
独立index仅提交本项生产、夹具、本文档3路径；原18路径暂存差异和其他工作区字节
保持不变。冻结1492结果、原273要求及历史状态不重写。本项不证明所有IO/元数据
故障、OOM、CLOG状态更新的全部异常原子性、cache lifetime、SQL可见性全模型或
完整TXN/WAL要求。R244、R243、安全/TDE、后台/CTAS及其他明确暂缓保持暂缓。
不push，Actions禁用。

证据目录：`/tmp/dbms-r254-clog-read.4jIOIkx7`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧V3的49项 | `d5afc51bf3e3bffb7d8e838df67ba64932cbb1420572b671c38b687240237d21` |
| CLOG三读取入口和截断及fd身份验证 | `427ca8a5b667773f0a34fb206c42a05cc4ec6b4710b1282208a5521b20e4c4dc` |
| 匹配对象及新生产CPP构建 | `01008aa89c1a08bfbdc83653ac392f812846f63784a8b4a24a919324fe23dc79` |
| 新49项双侧运行 | `158907eae73798a5cc4092273a46336e4e8d022f4ef00ee5e3ee66ecbc94b5bf` |
| 定向ASan/UBSan完整49项 | `5a74412480fda483e444fe5ff6d8d53b3ae589938a1cfec453a61884b179cb2f` |
| 64原生fresh与6Main对象复现重链接 | `11d74fe93b29104a4812285d1b3c19e437d298c05dabd3b89ccf09b203cb5c93` |
| 完整86次候选运行 | `b7ecc87e58b8e22c14a2fef0108bd2c2018918bd2a4d78c540ce8d49402842e9` |

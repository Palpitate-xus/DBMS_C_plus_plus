# R259：WAL读取完成短传输并重试EINTR

状态：有限WAL读取修复通过；WAL-02/WAL-05、资源治理及原273总清单仍未整体完成。

## 缺陷与修改方法

| 缺陷 | 影响 | 修改方法 |
| --- | --- | --- |
| 每个segment chunk只调用一次read，正数短传输也直接失败 | 完整日志的ReadRecord、下一条记录、checkpoint读取/扫描及重新打开被错误拒绝 | 在同一段fd内累计实际字节，调整buffer和剩余长度，完整后才进入下一段 |
| read/open返回EINTR直接失败 | 可恢复的中断被当作日志不可读；启动完整性检查可能拒绝完整WAL | 对相同未完成位置重试read或open；其他错误仍失败 |
| 短传输即停止，未处理后续真正EIO | 虽然同样返回失败，但没有读取完整范围，也未观察应处理的后续错误 | 相同完成循环持续到所需字节齐全或真实EOF/硬错误 |

修改仅WAL.cpp的readBytes。保留原exists/lseek、16MiB分段、长度上限、8字节对齐、
CRC32C、布尔错误约定和段名格式；不改头字段、布局、Main、append、flush、fsync、
全局锁或资源registry。真实read返回0仍失败，不补零、不把截断WAL当作有效记录。
所有普通成功/失败路径关闭本次打开的段fd，不声明完整异常ResourceOwner修复。

## 同一296项完整旧新对照

Root和独立纯提交投影的旧实现，各完整296控制、156失败、实际进程退出1；修复后
两侧各完整296零失败、实际退出0。旧新夹具逐字节相同，没有删除或放宽原检查。
156是失败控制数，不是156个独立缺陷：140是可恢复传输/中断控制，16是短读后没有
走到EIO的序列检查。其余140个原正例和拒绝守卫本来已通过，仍保留。

| 完整控制 | 数量 | 具体覆盖 |
| --- | --- | --- |
| 4种布局 × 5个公开入口 × 12种I/O模式 | 240 | 同段非零位置、头跨段、载荷跨段、恰好段起点；ReadRecord、ReadNextRecord的非零anchor、readCheckpoint、findLastCheckpointLsn、ensureOpen |
| 4布局 × live-reader/冷打开 × 5种真实损坏 | 40 | 头/载荷实际ftruncate、实际载荷位翻转导致CRC错误、不合法及越界长度；均拒绝，修复私有副本后健康打开/读取 |
| 每布局4个无效LSN | 16 | 非对齐、恰好尾部、超尾部、INVALID_LSN，拒绝且无段open/read |

12种模式为正常、首次read EINTR、连续两次EINTR、头部一次短读、载荷一次短读、
持续短读、EINTR混合持续短读、头EIO、载荷EIO、短读后EIO、open EINTR和open EIO。
open故障作用于入口的第一个匹配段open；read故障只作用于目标记录的真实字节区间，
不截断前置的大记录。重开入口实际运行完整校验/尾状态扫描，未用伪造的open状态。

夹具通过dlsym转发真实libc操作，匹配私有段的路径及dev/inode，在arm期间限制实际
read长度或注入明确errno。返回的成功字节、真实EOF及fd位置来自实际文件；逐次校验
字节、段边界及位置前进，核对解码头/previous LSN/含padding载荷、尾部LSN、段fd
数量和不做清理的健康重试。每个原始seed全段字节哈希在全部控制前后不变。
真实损坏测试只改独立副本；不是假EOF或修改生产校验器。只清理自身mkdtemp目录。
这不是自然信号或内核短读的概率重现，不把测试errno适配器当作真实硬件故障。

## 构建与回归范围

108次有限程序运行全部退出0：每侧新296控制、43个既有native、3个真实Main frontend、
7个完整协议。保留R258结构页68、CLOG状态442/读取49、R246—R253缓存、共享heap/extent、
derived maps、codec、BTree topology、savepoint alias、commit/rollback故障、跨进程txnid
及redo crash；另有WAL basic、truncate、full-page-write、timeline/archive、archive failure
和3个retained/shared WAL目录所有权回归。七个原完整协议与默认超时未改变。

定向ASan/UBSan对WAL.cpp和新夹具同时插桩，完整296项零失败、实际退出0；没有其他
未插桩生产依赖，也未弱化检查选项。不是全库sanitizer、TSan、TDE或安全审查。
本次新生产CPP和新夹具无编译警告；既有load-exception夹具警告原样保留。

每侧fresh编译WAL.cpp、新夹具及43个既有native，其余生产对象按源码、134/131头、
编译器和对象SHA核对后复用。Root62/纯提交60生产CPP分离，不混对象布局，也不是
本轮全部122生产CPP fresh或normal/repeat构建。6个Main frontend复用R247匹配对象，
先重链接还原原二进制SHA，再链接当前库运行；不链接test_stubs。Root服务仍使用
已验证pre-R229 Main投影，不运行工作区R229草稿或旧dbms_main。

2284个提交源码输入与测试纯投影一致。960 native+597注册Python+3 Main=1560只是
库存，不是全1560运行。独立index仅提交本项CPP、新测试、本文档3路径，原暂存18
路径及其他工作区文件字节不变。冻结1492结果和原273历史状态不重写，总目标未完成。

本项不解决WAL append短写、完整错误/EOF分类、所有recovery状态机和断电窗口、
PG continuation/page header、日志padding后的扫描策略或完整并发writer规则。
新夹具不在StorageEngine恢复的每一阶段注入故障，普通恢复回归也不能证明全部窗口。
R257及其他所有用户暂缓项保持暂缓；不push，GitHub Actions保持禁用。

## R259提交时的相邻观察：append短写（后续由R260有限修复）

以下为R259提交时的历史观察；后续[R260](issue-review-r260-wal-append-recovery.md)已有完整246项及有限回退修复，不代表整个WAL恢复协议闭合。

在当时纯投影WAL对象上另跑3组私有手工观察，实际转发write并按段inode仅限制首次
成功传输为7字节，或注入首次EINTR。完整32字节seed之后：正常append成功，文件
64字节且可重试/重开；短写时append返回INVALID_LSN、内存尾仍32、文件实为39，
健康重试仍失败、全新WALManager打开也失败；EINTR返回失败但文件仍32，健康重试
和重开成功。诊断进程退出0只表示3组采集完成，不是产品正确性通过。

R259当时此项尚无正式全矩阵/Root对照或生产修复，不能把R259读取通过当作append已修好。
修改方法：先覆盖write短传输与EINTR的完整追加，再验证部分写入后硬错误的尾部
回退/恢复状态、跨段及peer重试；仅对短写返回false而留下不可打开的尾部不够。
观察器、原始日志和匹配对象凭据保存在证据目录的`append_observation.cpp`及
`completed-append-observation-receipt.json`，后续独立问题应保留这些失败事实。

证据目录：`/tmp/dbms-r259-wal-read.3FUFaoq1`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧296项156失败 | `27f9a37f5a4bb2fa4b64d2db9eed66c29ed43a83e6e4f2ef1b0adb3866d82c29` |
| WAL读完整段内范围并重试EINTR | `9aa7e5927d8ad5de4ed9e6ab4567f6307993292f5c68875395e84738a26a9430` |
| 匹配对象及新生产CPP构建 | `46d9136750036258549363aa10a2bc5977967e90ad2e9a2bbf29e777676b9976` |
| 新296项双侧运行 | `a55ecd243f1093e29a0425056f8514d0b6836617205925aad529c1932481548e` |
| 定向ASan/UBSan完整296项 | `a16e9c4f412b1918e93dd26c3369c4d5e57279d5de9d2050c37d87ebd2a4037a` |
| 86既有native fresh与6Main复现重链接 | `33a4cfd8d7b66cffd7ec7e5862c3c0fe9be13ab8e532d478721b721db0ec093a` |
| 完整108次候选运行 | `b943ad1e93fb5273582d3f70c48b7fdb1c411cf4ddaf56887030dfd77d199c31` |
| 相邻append三组观察，非正确性通过 | `4eae9c1b14187397d93a8e28feb3246bc7ad4e26b31d51605bb205700febc34d` |

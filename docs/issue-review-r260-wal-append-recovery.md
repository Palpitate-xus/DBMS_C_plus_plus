# R260：WAL短写、失败回退和缓存尾刷新

状态：有限追加状态机修复通过；WAL-02/WAL-05、资源治理及原273总清单仍未整体完成。

## 缺陷与修改方法

| 缺陷 | 影响 | 修改方法 |
| --- | --- | --- |
| 单次write不足chunk或返回EINTR直接失败 | 完整记录未追加；7字节短写留下半条记录，后续重试及新manager打开失败 | 在同一段fd内完成实际正数传输，按累计偏移继续写；write/open EINTR重试 |
| 写入失败后未恢复追加前的有效尾 | 跨段EIO、ENOSPC、零进展、seek/open失败可遗留坏尾或多余空段 | 写入前确定全部路径和原始边界；仍持原WAL锁时截回被修改的原文件并同步，删除本次新建段并同步目录 |
| 回退本身失败后仍走热缓存快路径 | 失败残留可能被已有manager忽略 | 当前manager失效，后续须重新验证磁盘；新段逆序删除，unlink失败时保留其前方新段，避免残留被名称空洞隐藏；相邻段检查同样覆盖其他缓存manager |
| 缓存尾恰好在段边界时只检查上一满段 | peer先写下一段后，旧manager仍返回相同LSN并覆盖peer记录 | 验证尾及相邻段；缓存空流重新扫描。发现实际流已前进则在既有锁内刷新后追加 |

生产仅改WAL.cpp。没有新头字段、类布局、段/record格式、Main、TDE或全局registry。
最多18个局部段owner覆盖既有256MiB载荷上限；所有路径分配及前状态检查发生在改字节
之前，实际写入/回退阶段只使用已有路径和POSIX调用。正常成功仍保留最后一个cached
write fd，不新增正常成功路径的fsync；有限失败才同步截断和名字删除。额外metadata
检查有成本，本次没有以性能阈值宣称无开销。

仅截回本次确实修改的原fd、删除本次O_EXCL创建并持有的名字；不把外部已损坏尾部
静默截成成功日志。已存在的非空后段在写入前拒绝或触发刷新，不覆盖其数据。
所有局部fd在普通返回和C++栈展开时关闭，成功仅移交最后一个；不是完整ResourceOwner
或整个XLogInsert异常/OOM路径验收，R257的CLOG资源所有权模块没有恢复。

## 完整旧新对照

最终V2夹具两侧各完整246控制。V1已经完整运行两侧246并各182失败，原源码和日志
保留；V2只增加回退失败后另一台已缓存manager必须拒绝追加且不改文件的断言，不删
原控制或放宽期望，重新在旧/新两侧完整运行。最终结果及凭据见本文末。
失败控制数不代表同样数量的独立缺陷。

V2旧实现两侧各完整246项、182失败、进程退出1；修复后两侧各完整246项、零失败、
实际退出0。旧新V2源文件相同，未移除任何原检查。

| 控制族 | 数量 | 覆盖 |
| --- | --- | --- |
| 6布局 × hot/cold × 12种普通/写入失败模式 | 144 | 空流空载荷、同段、头跨段、载荷跨段、恰好段起点、16MiB以上载荷跨3段；正常、一次/两次EINTR、一次/连续短写、混合、EIO、ENOSPC、短写后错误、零进展、seek错误 |
| 冷manager的open EINTR/EIO/EMFILE | 18 | 实际缺段创建或原段打开，错误不误报成功 |
| 后段及第三段write/open/零进展失败 | 24 | 已写首段/完整中段后回退，不能只处理第一次write |
| 7种回退I/O × hot/cold × 3跨段布局 | 42 | ftruncate EINTR/EIO、数据fsync EINTR/EIO、unlink EIO、目录fsync EINTR/EIO |
| 3起点 × hot/cold × 同进程/真实fork peer先追加 | 12 | peer、原prefix和两条新记录均保留，LSN/previous LSN链正确；这是顺序交错，不是完整并发公平性证明 |
| 3起点 × hot/cold外部非法尾 | 6 | 原字节不变、拒绝追加，不把外来损坏当作本次写入来回退 |

夹具转发真实libc write/open/open64/lseek/ftruncate/fsync/unlink，仅在arm期间针对私有
段路径或实际fd注入确定性返回值/errno。全部成功字节及位置来自真实文件；逐次核对
请求长度、实际偏移、累计字节，核对前状态全段字节/名字、LSN、archive状态、含padding
载荷、CRC、previous LSN、健康重试及新manager读取。段fd数量含已unlink但仍打开的fd。
只清理自身mkdtemp目录，不依赖概率调度或计时阈值，不声称自然内核故障。

ftruncate/unlink硬错误确实阻止回退时，原调用必须失败；原manager、已有缓存peer和
新manager必须拒绝使用残留坏尾。此类控制先观察拒绝，再由夹具显式修复自己的副本，
不把手工修复算作产品自动恢复。fsync失败但截断/删除已经可见时，只证明原调用失败
及后续健康重试；不把可见字节正确等同于断电后已持久。持续硬件错误不能许诺自动修好。

## 验证范围及限制

110次有限程序运行全部退出0：每侧新246、44个既有native、3个真实Main、7个完整
协议。完整R259读取296、R258结构页68、CLOG状态442/读取49及所有既有缓存、共享heap、
extent/derived maps、commit/rollback故障、txnid和redo crash保留；WAL basic/truncate/
full-page-write/timeline/archive failure和retained/shared WAL所有权回归保留。没有恢复
默认postgres_protocol MAIN或跳过的COPY/security/TDE/后台专项，原断言和超时不变。

另对原始1492的11个失败入口中未被用户暂缓的9项进行当前复查：两侧各5个原生
（bit compact receiver、true INTEGER correlated origin、shell type、DROP CASCADE、
physical attribute length）及4个完整协议（BEGIN grammar、index corruption、pg_class、
temp index catalog），共18次实际全0。原5个native各侧fresh编译，使用当前完整匹配库。
110加18是128次有限运行，包含重复的index-corruption入口，不是128个不同测试或
全1561验收。默认协议主入口及COPY两项仍明确暂缓，未执行。R187当前完整28检查
通过，不能倒推出原来exit2的原因已修；原冻结1481/11结果和未定根因原样保留。

定向ASan/UBSan给WAL.cpp及新夹具同时插桩，完整246通过，不弱化检查选项。新原生
正常构建也只链接真实WAL对象，不使用stub或伪造实现；server及既有回归链接当前完整
匹配库。不是全库sanitizer、TSan或完整crash矩阵。fork helper正常写入后用_exit退出，
不声称每个子进程都执行了LeakSanitizer退出期扫描。

每侧fresh编译WAL、新夹具及44个既有native；其余生产对象按源码、134/131头、编译器
和对象SHA核对后复用。Root62/纯提交60生产CPP保持分离。6个Main复用R247匹配对象，
先重链接还原其原二进制SHA，再链接当前库运行，不链接stub；Root仍为pre-R229 Main
投影，不运行跳过草稿或旧dbms_main。不是本轮全部122生产CPP fresh或normal/repeat。

准备期一次独立smoke编译任务在两个夹具对象完成前约1秒启动，存在短暂编译任务重叠，
不能声称全轮CXX并发从未超过2。该准备日志保留；后续正式构建按阶段交接、每个pool
最多2个编译任务，运行使用既有串行锁。此调度记录不算产品故障或测试通过。

2285个提交源码输入与测试纯投影一致；961 native+597注册Python+3 Main=1561只是
库存，不是全1561运行。独立index提交本项CPP、新测试、本文档和R259历史状态链接，
原暂存18路径和其他工作区字节不变。冻结1492结果与原273历史状态不重写。

本项不是durable append intent/崩溃原子追加协议：未证明在写入/回退被SIGKILL、断电、
同步失败后的全部恢复窗口，也不实现损坏尾部的自动修复、PG WAL continuation/page
header、全部并发插入/归档规则或全局缓存生命周期。WAL.h历史single-stat注释尚待后续
整体验证时同步；当前行为以本文及CPP为准。原总目标仍未完成，所有用户暂缓保持暂缓，
尤其R257不恢复。不push，GitHub Actions保持禁用。

证据目录：`/tmp/dbms-r260-wal-append.g4VGj5lk`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整V2旧246项182失败 | `e820edb109f663e8ba098e22712d89fcc74e0e5ea2417c55d9e128f4fae11730` |
| WAL完整追加、前状态回退及相邻段刷新 | `03a3dcc3a0120af502912498223293157cd11e349a8bd5dc5c3fa0eff47a6c6f` |
| 匹配对象及新生产CPP构建 | `b6954a3be79b78d6059a15cc8140a40fd276576417408d45c538fe63f678b8a9` |
| 新246项双侧运行 | `3ebbd187eb4ce658f4fcd194df5f1ba550621575886802098a7bbd2ca15c4f5a` |
| 定向ASan/UBSan完整246项 | `36c25dfbdf487ca27c1e15fc5bc92dc63adf59feb199363b7569b8345e538c7f` |
| 88既有native fresh与6Main复现重链接 | `cbf3c8d268075690f41eaf44985cf5f6f4ee25760972a0441af8c497d411b1ef` |
| 完整110次候选运行 | `8d8f244e33c324359739aa063b0d105c93aa58014b83910b28a8b41749f96a33` |
| 旧V1完整246项182失败保留 | `afbea45bbb7ac385e12ea8f59ec5db582183abe3509e5646a67f9a648eb2a2fe` |
| V2同246控制加强cached peer断言 | `0ac95b3b3d79cadf805b1916f9aeb3bde7e02b2459bf7f97fcdea843477dd7be` |
| 原9未暂缓入口当前10原生构建 | `1421a8f36af55ec577e887811551cd91118dd06e8d6a90ef44b917fe16a47853` |
| 原9未暂缓入口当前18次复查 | `522963d74a373998dab32bb53ba1edf15956884c8902285df150e57c557725f7` |

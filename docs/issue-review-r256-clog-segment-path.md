# R256：CLOG段路径不得吞掉分配异常

状态：R255剩余的两个路径错误已修复；完整442项与本轮88次有限程序运行通过。
TXN/WAL/P0-14及原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 实际影响 | 修改方法 |
| --- | --- | --- |
| segmentPath使用ostringstream且不检查流失败状态 | 冷加载路径构造的513字节分配抛bad_alloc被流吞掉，返回不完整路径；后续把真实段当缺失 | 用16字节栈buffer和to_chars生成uint64十六进制，再用正常std::string构造；必需分配失败原样传播 |

这与R255的pending登记顺序是两个独立问题。R255提交时完整430项仍有2个冷加载
配置失败并退出1，未被改成成功期望。本项继续修复该故障，不更改R255的登记顺序。
旧scalar还暴露错误的其他缓存状态，之后flush使这些状态与先前可见值不一致；旧
batch后续flush碰巧恢复数据，但异常已被吞掉且使用过错误路径，不能称为合法完成。

uint64最多16个十六进制字符，固定buffer足够。输出保留原小写、不补零的文件名，
没有locale/stream状态，也没有新增头字段、对象布局、页格式、Main、权限、TDE或
全局缓存锁变化。没有catch后忽略OOM或把它转换成成功/零值。

## 保留原检查并增加段名边界

`tests/clog_status_atomicity_test.cpp`保留R255全部140个配置及2个INVALID_XID控制，并逐一注入新实现实际观察到的全部286个登记/
冷加载分配点（旧实现288个；两个冷配置由10次降为9次），没有删除冷加载断言或缩小分配故障范围。
新增14个使用真实setStatus/flush/新CommitLog读取的段名控制，字面量期望包括
0、1、9、a、f、10、ff、100、fff、1000、ffff、10000、abcdef和最大TxnId所处段
fffffffffff；检查该段首尾事务状态以及目录中没有错误文件名。

同一扩展夹具在两侧父实现均完整444项、2项失败、退出1，14个新增边界控制原本
就通过，不称为14个新修复。当前两侧完整442项零失败、退出0。442=140个无故障
配置+286个实际分配故障+2个invalid+14个段名控制。逐配置核对：140个输入键、14个
段名和2个invalid完全相同；唯有两个冷配置各消除一次分配。全部新分配ordinal仍
逐一注入，未删除输入配置或放宽期望。最初检查器固定444造成元数据校验失败；
依据原始退出0日志完成上述库存核对后修正计数验证，没有重跑或改写原日志。

夹具转发真实分配、mutex和文件操作；在setStatuses第二次获取同一mutex、实际
进入flush时关闭注入，flush仍正常执行。此范围证明登记/冷加载阶段，不证明所有
保存、WAL、fsync或锁所有权异常。每项检查邻近packed位、失败时磁盘字节、缓存/
持久状态一致性、成功前缀可刷盘和移除故障后的peer更新合并。私有mkdtemp测试
目录用后清理，不使用用户数据库；故障为确定性适配器注入，不是实际物理内存耗尽。

## 回归、插桩与版本管理

88次有限程序运行全部退出0：每侧新442项、33个既有原生、3个真实Main frontend
及7个完整协议。原生包括R254完整49项、CLOG普通/事务集成、回滚恢复图、pending
buffer和insert/update/delete回滚失败、跨进程XID、redo crash以及R246至R253存储
回归。协议为savepoint_read_only、index_corruption、explain_json_analyze、
explain_json_cache、explain_timing、commit_failure_recovery和transaction_begin_error_state。
保留原断言及默认超时，不把88次运行当作全库存验收。

定向ASan/UBSan插桩CommitLog.cpp与新夹具，完整442项零失败、实际退出0。没有
弱化选项，不宣称全库sanitizer或TSan通过。生产与夹具编译没有新警告。

每侧fresh编译CommitLog.cpp、扩展夹具和33个既有原生测试；其他生产对象逐项核对
源码、134/131头、编译器与对象哈希后复用。Root62/纯提交60生产CPP不混用；6个
Main frontend复用R247对象，先重链接还原父二进制SHA再链接当前库，不链接stubs。
不是本轮全122CPP fresh、六次fresh Main或normal/repeat build。pre-R229 Main
投影保持不变，不使用R244被中断的新布局对象。

2282个提交源码输入与测试纯投影一致。958原生+597注册Python+3Main=1558只是库存。
独立index提交本项CPP、扩展夹具、新文档及R255文档当前状态更新4路径，历史430/2
结果保留。原18路径暂存和其他工作区字节不变，冻结1492结果及273原要求/历史状态
不改写。R244、R243、安全/TDE、后台/CTAS和其他明确暂缓保留。不push，Actions禁用。

证据目录：`/tmp/dbms-r256-clog-segment-path.BscKNEN2`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧444项仍2失败 | `cf70d66683660faa1854659768b926b36f1533140271c3b960feb760add5fa01` |
| 不吞异常的段名构造及原登记顺序保留 | `7d8105e1888e25f23edd1fe8f0b1c8567e0558e73ce30e18d3fb5cca2fa65d5c` |
| 匹配对象及新生产CPP构建 | `54b28bee2b7602007b3d61776c37718d436f297da5b4a40f5de133e534b4f21c` |
| 新442项双侧运行 | `83f562074d352c69d550af2faf5745d88e0d256fe82bbbd10925dfee01430f89` |
| 定向ASan/UBSan完整442项 | `44db451f3ac704e2a0769c416fe942ef5c4701c0cf6e46b7f024431c5752103d` |
| 66原生fresh与6Main对象复现重链接 | `782af0ea2a7a59371deab618d64793b5f0e31711222743e587331575691e8321` |
| 完整88次候选运行 | `5fed3919bf16147bbc704c05daca3c61269c3ec45ecb45e7d0861beaa27f2c93` |

# R251：加载代次切换与等待登记的生命周期

状态：有限等待登记修复通过验证；STO-04/P0-14和原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 实测影响 | 修改方法 |
| --- | --- | --- |
| slow fetch用pageId仍在loading中作为同一cv的wait谓词 | A失败删除等待列表并通知；C先开始同页新加载；B重查谓词又等待旧cv，但C列表已无B，C成功返回后B仍挂起 | 每次wake结束本次登记，交给调用方循环重新核实状态，并为当前加载代次重新登记 |
| 虚假唤醒后保留旧登记，再登记时分配失败 | fetch/close/单页失效/全部失效退出等待调用后，loader尝试通知已经完成析构的栈cv | 每次普通或异常返回都在原pool锁内按指针清除本次登记；登记失败也清理新建的空map项，保留其他等待者 |

在BufferPool.cpp内新增等待辅助函数，统一五个加载等待位置。辅助函数拥有栈cv，
登记后只做一次非谓词wait，普通和异常出口均清除登记；外层原循环重查真实状态。
清除时重新find map，不能跨wait保留vector引用，因为loader可能已擦除整条记录。
若map已属于新代次，只移除自己的指针，不清除其他等待者。原异常继续重抛。

只修改BufferPool.cpp，无头字段、对象布局、页格式或Main变更；不增加全局锁，
不改变加载/淘汰/缓存计数/错误返回规则。R246准入、R247统计、R249异常清理、
R248计数和R250失效异常安全保留。orphan vector的等待路径没有在本项改动。

## 完整旧新对照

同一完整8项夹具在两侧旧实现均退出1、6项失败；修复后两侧8项零失败、退出0。
6为控制数，不是6个独立缺陷。8项控制和期望没有删除或放宽：

| 控制 | 数量 | 旧结果 | 修复后 |
| --- | --- | --- | --- |
| fast fetch代次切换，无/有一次虚假唤醒 | 2 | 原本通过 | 继续通过 |
| slow fetch同样的代次切换 | 2 | 后继loader已成功，observer挂起，子进程deadline退出91 | 通过 |
| 虚假唤醒后下一次分配失败，退出fetch/close/单页失效/全部失效的等待 | 4 | 每项都记录一次向已析构cv的通知尝试 | 不再通知已退出等待者，仍保留bad_alloc并可重试读取 |

generation探针转发真实pthread_cond_wait；第一次实际通知后通过受控unlock/relock
让后继loader先得到pool锁，再交回等待者。返回时仍满足cond_wait持锁的后置条件，
显式模拟合法调度。两侧旧日志的所有调度事件均到达。12秒deadline仅收集旧挂起；
外层180秒timeout没有触发。虚假返回始终保持调用方要求的持锁状态。

登记失败检查通过bad_alloc放弃本次等待，不是pthread_cancel线程取消。observer
捕获异常时，被测调用的cv已完成析构；保留observer线程栈直至loader结束，探针
以该已析构地址记录实际broadcast尝试，不转发对已析构对象的危险调用。因此旧4项
证明过期登记导致的通知尝试，不称为实际内存越界、崩溃或ASan use-after-free报告。
其余broadcast/锁/读取均转发真实函数，所有数据文件位于私有测试目录。

首轮正式夹具有一处nonnull参数多余空比较警告。V2保留一次原子读取，只将空检查
放在读取结果上，遵守pthread参数非空契约；控制/期望不变。两侧重新运行完整旧V2
仍为8项/6失败，V2编译无该警告。原始probe及V1日志保留。

## 检查与构建边界

本轮56次有限程序运行全部退出0：每侧新8项、19个既有原生、3个真实Main frontend
及5个完整协议。原生包含R250完整170项、R248计数53项、R249异常15项、R247统计
24项、R246准入35项普通构建及pin/snapshot/shared extent/derived maps/shared heap/
codec/BTree topology/savepoint alias。协议为savepoint_read_only、index_corruption、
explain_json_analyze、explain_json_cache、explain_timing。原断言和默认超时保留。

定向ASan/UBSan实际退出0、完整8项零失败。仅BufferPool.cpp和新测试插桩，
PageCrypto为匹配未插桩对象；未降低检查选项，不宣称全库sanitizer或TSan通过。

两侧各fresh编译BufferPool.cpp、新测试及19个既有原生测试；其余使用各自匹配的
已验收对象，核对源码、134/131头、编译器和对象哈希。最多两个CXX进程，Root62/
纯提交60生产CPP不混用；非normal build或本轮全122CPP fresh声明。6个真实Main
对象复用R247：先以父库重链接复现原二进制SHA，再链接当前库运行，核对原测试体
和头，不链接test_stubs。Root Main草稿不动，仅用记录过的pre-R229投影对象。

2276个提交源码输入与测试纯投影完全一致。954原生+597注册Python+3Main=1554
仅为库存，不是全1554验收。独立index只提交本项生产、测试、本文档3路径；原暂存
18路径和其他工作区字节保持不变。冻结1492结果和273历史状态不重写。

不据本项宣称完整ResourceOwner、所有锁/线程取消/等待异常或全部OOM行为闭合。
orphan等待生命周期及其他STO-04/P0-14缺口仍需复查，R244规划错误阶段仍开放。
R243、安全/TDE、后台/CTAS和全部其他明确暂缓保留；不push，Actions保持禁用。

证据目录：`/tmp/dbms-r251-load-generation.1oeaDwJl`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 初始独立两项代次复现 | `4bb84d173981fcd046a6ed01f816c6dee068a521c56a046607cb9ffea19a2deb` |
| 完整旧V1的8项 | `c1801232bcb7794f496e5457f2d271805522b7145a6051c952ade6dba8e895c3` |
| V2非空契约警告修整不改8控制 | `9026073bc648507b06d5893114c49f00e211a90e9c84b0a479da39a35499d4df` |
| 完整旧V2的8项 | `dbf368cdaabb7585b87a1636f5302055369fc0b2b01ece2cc0480ea50fd088f2` |
| 加载等待登记生命周期修改 | `11eecd734f64e3665ec95b7e260d12223e87f9ca64c2cb6d5d39af15048e83c1` |
| 匹配对象及新生产CPP构建 | `1536e11378a99d37bfcb8474071206d7868e88579a6f29e69a8c6e1a21a84147` |
| 新8项双侧运行 | `716ec823c61b464ea8fb0ade0281cd02be98d1c959fc92b406c004a44d2b8057` |
| 定向ASan/UBSan完整8项 | `6640d3d4af9990dabbe4f5881eb35b222449993b02e96b702b8c861db7b9da38` |
| 38原生fresh与6Main对象复现重链接 | `0308492a80df87bed20c38e0a3fb9c6f22805c7438c40761c0347f4aa602af3f` |
| 完整56次候选运行 | `e085a90a4ea25987047467d421129c92e3a4e3344503790db594e35451d08482` |

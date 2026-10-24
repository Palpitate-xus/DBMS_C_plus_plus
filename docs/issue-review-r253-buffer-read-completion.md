# R253：普通页读取的短传输与EINTR处理

状态：有限普通页读取修复通过验证；STO-03/STO-04及原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 影响 | 修改方法 |
| --- | --- | --- |
| pread返回正数但不足一页就直接补零 | 实际存在的后半页未读取，返回截断的数据；fullPageRead为false，使完整页未进入既有validator | 按累计字节数调整buffer、offset与剩余长度，继续读取；只有实际返回0才按EOF补零，完整页仍校验 |
| pread返回EINTR直接失败 | 可重试的读取中断被当作加载失败 | 在相同偏移重试EINTR；其他错误仍失败，由既有failLoad清理预留frame并通知等待者 |

修改仅在BufferPool.cpp的普通页读取循环；没有新增分配、头字段、对象布局、页格式、
Main或全局缓存锁变更。完整页只在全部字节到达后进入原validator。累计读取之后遇到
硬错误仍返回失败，不发布半页。真实EOF继续保留新页/部分页补零约定，没有把EOF
改成报错。misses仍按一次逻辑load计数，不按多次pread计数。

## 同一19项完整旧新对照

Root及独立纯提交投影的旧实现均完整19项、14项失败、进程退出1；新实现两侧均
完整19项零失败、退出0。控制与期望没有删除或放宽。14是失败控制数，不是14个独立
缺陷；5项旧正例原本就正确。

| 控制 | 数量 | 旧实现 | 修复后 |
| --- | --- | --- | --- |
| 普通完整页、普通校验拒绝、直接EIO、真实部分EOF、空EOF | 5 | 通过 | 通过 |
| 首次/重复EINTR、短读取混合EINTR、空EOF前EINTR | 5 | 失败 | 通过 |
| 一次/多次短读取、连续128次单字节读取 | 3 | 缓存截断页、未校验完整页 | 完整内容且一次校验 |
| 短读取后validator拒绝 | 1 | 未调用validator且错误发布半页 | 拒绝且无残余frame |
| 短读取后EIO、EINTR后EIO、短读取及EINTR后EIO | 3 | 未继续读取到应观察的后续错误 | 按精确读取序列处理，失败后可重试 |
| 真实部分EOF前多次短读取，及混合EINTR | 2 | 丢失实际已有内容 | 保留全部已有字节，仅EOF之后补零 |

夹具`tests/buffer_pool_read_completion_test.cpp`写入真实文件，以dlsym转发实际libc
pread，仅对arm期间匹配dev/inode的文件施加正数传输上限或注入EINTR/EIO。所有返回
字节与EOF来自真实文件；不伪造提前EOF、不修改生产API。中断由测试适配器确定性
模拟，不声称捕捉了自然信号下的内核短读取。未依赖随机调度或性能阈值。

每项核对128字节全部内容、补零位置、每次offset/剩余长度、校验次数与输入、一次
load只计一次miss、缓存命中不重新读盘、失败不发布frame、重试与unpin，以及
close/reopen之后的原始数据。普通部分EOF旧实现一次pread即可正确返回，新实现
增加确认EOF的读取；该正例只校验语义，不误称为旧错误。EINTR后EIO旧实现已经返回
失败，但没有按约定重试到EIO，此项同时检查实际调用序列。

## 构建与验证范围

60次有限程序运行全部退出0：每侧新19项、21个既有原生、3个真实Main frontend及
5个完整协议。原生包括R246的35项、R247的24项、R249的15项、R248的53项、R250的
170项、R251的8项、R252的4项，以及pin/snapshot/shared extent/derived maps/shared
heap/codec/BTree topology/savepoint alias回归。协议为savepoint_read_only、
index_corruption、explain_json_analyze、explain_json_cache和explain_timing。原断言
与默认超时保持不变；60次有限运行不是全库存验收。

定向ASan/UBSan完整19项零失败、实际退出0。只为BufferPool.cpp与新测试TU插桩，
PageCrypto为匹配的未插桩对象，没有弱化检查选项。不宣称全库sanitizer、TSan或TDE
验收。生产编译仅有已有的notifyOrphanDrained未使用参数警告，新夹具无编译警告。

每侧fresh编译BufferPool.cpp、新夹具和21个既有原生测试，其他生产对象逐项核对
源码、134/131头、编译器及对象哈希后复用。Root62/纯提交60生产CPP不混用；并非
本轮全122CPP fresh或normal/repeat build。R244暂缓后原头已逐字节恢复，没有使用
被中断的R244新布局对象。6个Main frontend复用R247匹配对象，先重链接还原原二进制
SHA，再链接当前库运行；不链接test_stubs，也不恢复R229 Main草稿。

2280个提交源码输入与测试纯投影一致。956原生+597注册Python+3Main=1556只是库存。
独立index仅提交本项生产、夹具、本文档3路径，原暂存18路径及其他工作区字节保持
不变。冻结1492结果和273历史状态不重写。R244、R243、安全/TDE、后台/CTAS和所有
其他明确暂缓保持暂缓；本项不是完整ResourceOwner、pin生命周期、所有IO错误或
存储/恢复要求的闭合。不push，Actions保持禁用。

证据目录：`/tmp/dbms-r253-buffer-read.4kIHfRT2`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧19项 | `af2606d116ea81f63a730c9402ca9f70c10877f323cfe4fbef07e11d4b4607e1` |
| 普通页短读取与EINTR修复 | `0a652dc3f550cc2ff59368ad6e68c0bf8b671129248466de3bbae0611f28062f` |
| 匹配对象及新生产CPP构建 | `2ed9cf73dbe7a2070ee8eafee81ca66ffe5735a608c88fa57006899b6df7d1e4` |
| 新19项双侧运行 | `2d81b128991cb85d4134bb2aba6d9d3250e52056b10f0b38d43ddcc925d57f73` |
| 定向ASan/UBSan完整19项 | `2fed23d944936801018f4dde0aa03645c38fa2c3371d906dc94639daa8890559` |
| 42原生fresh与6Main对象复现重链接 | `5e0379d36c0e1ea3b42528cede2a2653c9fe0887b680e1b1be3870f4c13c6a47` |
| 完整60次候选运行 | `8afd109e565f47e9b2c7c09fccf8c0a45f103bf4b9da605595fd3324e3460753` |

# R100：几何坐标分隔符丢失导致非法输入被存储

状态：本范围修复及专项验收通过，使用本问题的独立本地commit记录。
归属原总账 SQL-02、SQL-11、TYPE-09、PROTO-04，不新增或虚改原273项完成状态。
本项不修改尚待用户明确跳过范围的R98/R99候选，也不恢复已暂缓的专项。

## 复现与影响

旧 `GeometryValue.cpp` 将括号替换为空格，再分别比较数字数量和括号skeleton。
它没有核对括号在数字之前、之间还是之后：`(1),2`、`1,(2)`、`()1,2` 都能变成
`(1,2)`。同一问题影响 POINT、LINE、LSEG、BOX、PATH、POLYGON、CIRCLE。
例如 `{1},2,3` 变成合法LINE，`<(1),2,3>` 变成合法CIRCLE，普通INSERT会成功提交
这些本应拒绝的输入；不是只有错误消息或显示形式不同。

参考依据为[PostgreSQL 18几何类型文档](https://www.postgresql.org/docs/18/datatype-geometric.html)
和官方[几何输入实现](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/geo_ops.c)。
实际PostgreSQL 18.6服务的 `server_version_num=180006` 另由驱动核对，最终永久
230项控制全部通过。合法的无外括号、扁平坐标、不同点包装形式及 `1,2,(3,4)`
保留，没有通过仅接受一种规范输出形式来绕过正确输入。

## 修改方法

新增文件内部的有界游标，按原始位置读取数字、逗号和点/对象的外层包装。
POINT逐点验证；LINE显式读取三个系数或两个点；点列表按其类型决定固定点数、
可变点数和外层包装；CIRCLE分别读取中心和半径。最后必须消费全部输入。
共享入口同时覆盖文本规范化、存储输入和binary编码前的文本验证。

不更改公共头/API、binary布局、索引键或既有数值算法。原有POINT非有限值限制、
LINE退化系数限制、CIRCLE负半径限制仍在，尚不代表完整几何函数/访问方法或
TYPE-09全部实现。binary解码的既有结构和验证保持不变。

## 纠正有真实参考证据的旧夹具

首轮完整相邻native在原 `geometric_test.cpp:119` 实际abort134；首个新组件通过，
其余八native及该轮协议尚未运行，不能写成十native或七协议通过。
旧断言将 `[(0,0),(1,1))` 当成非法PATH。实际18.6的相同CAST以及相同三列INSERT
均成功，返回规范化 `[(0,0),(1,1)]`；这不是前移点内括号的非法坐标。
LSEG及CIRCLE的旧式结尾也由实际参考核对，新增四组永久阳性控制。

因此保留SQL，纠正该条旧断言为成功，新增精确存储值断言，并追加真正非法尾随
`extra` 的阴性控制。原奇数坐标和POLYGON方括号阴性断言不变。没有为了原测试
通过而收窄PG已经支持的合法输入，原abort及独立参考日志均保留。

## 验证与失败保留

| 输入/运行 | 实际结果 | 边界 |
| --- | --- | --- |
| 初版旧纯组件，同一54组/216检查 | 退出1，88个断言失败 | 多个断言共享一个错误接受根因，不计88个独立bug |
| 首次旧协议尝试 | 退出1，首次CREATE TEMP TABLE准备超时 | 未完成226项；期限和SQL未改变 |
| 固定旧程序上的同一完整初版协议 | 退出1，226项/131失败 | 含非法INSERT累积后的计数与旧值差异，不计131个独立bug |
| 实际PG18.6/当前完整初版协议 | 各退出0，226项全部通过 | 后续追加四组真实PG合法旧输入，不改原226断言 |
| 当前初版纯组件O2与ASan/UBSan | 各退出0，216项全部通过 | 仅测试TU和GeometryValue.cpp；无StorageEngine/g_engine/ConnectionPool符号 |
| 最终实际PG18.6完整协议 | 退出0，230项全部通过 | 七类型合法输入、22P02、两类参数及写入后状态 |
| 最终旧58组/232组件 | 退出1，104个断言失败 | 同一旧GeometryValue.cpp；含新增合法旧式输入不支持 |
| 最终固定旧程序完整协议 | 退出1，230项/135失败 | 同一最终驱动，不覆盖首次准备超时 |
| 最终当前O2原生组件 | 退出0，232项全部通过 | 标准完整原生入口中实际执行 |
| 最终当前独立ASan/UBSan组件 | 退出0，232项全部通过 | 同一最终组件，无引擎符号；不冒称全库sanitizer |
| 最终当前完整专项协议 | 退出0，230项全部通过 | 与最终参考/旧程序相同SQL和断言 |

最终十个完整native、七个完整协议全部实际退出0，包含原完整默认PG协议。
不是挑选单个断言执行，首次相邻入口abort和首次准备超时仍保留。
正式构建实际只重编GeometryValue.cpp，正常链接及第二次up-to-date均退出0，
不冒称重编58个TU；新全58签名/双stamp/57 test对象字节另作独立终态核对。
该独立终态fence实际退出0，R98的六个生产文件保持原字节，README/Actions未改变。
当前工作区binary SHA256为
`17e6e7f676cccf1493df76368980e13ab06c2704480e5bf59370d9a164d77b70`，
cache/source签名为
`16f1afe925af021b4aff8223acb7d6476db652ddeb6e8ee7240e9c29f31d6218`。

原始临时证据位于 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r100-geometric-delimiter-old-component.log`、`r100-geometric-delimiter-old-protocol.log`、
`r100-geometric-delimiter-old-fixed-binary-protocol.log`、
`r100-geometric-delimiter-reference.log`、`r100-geometric-delimiter-component.log`、
`r100-geometric-delimiter-sanitizer.log`、`r100-geometric-delimiter-current-protocol.log`、
`r100-native-neighbors.log`、`r100-geometry-legacy-closer-reference.log`
及 `r100-geometric-delimiter-build.log`。这些路径不是可移植发行产物。
最终版本证据另存 `r100-geometric-delimiter-final-old-component.log`、
`r100-geometric-delimiter-final-old-protocol.log`、
`r100-geometric-delimiter-final-reference.log`、
`r100-geometric-delimiter-final-sanitizer.log`、`r100-final-native-neighbors.log`
和 `r100-final-protocol-neighbors.log` / `r100-final-protocol-results/`。
独立签名核对见 `r100-independent-terminal-fence.log`，静态检查见
`r100-final-static.log`；完成门禁实际退出1，见
`r100-full-ledger-completion-gate.log`，不把预期的未完成状态写为全部完成。
永久复现在 `tests/geometric_delimiter_test.cpp` 和
`tests/geometric_delimiter_protocol_e2e_test.py`，协议驱动已登记到标准入口。

当前工作区测试的生产组合含未提交R98候选，不能当作仅R100提交的完整整仓验收。
本项公共接口不依赖R98，纯组件独立编译输入仅上述两个TU。
仅本项提交后注册集合应为883 native+3真实Main+508 Python=1394；保留R98候选的
工作区为885+3+510=1398，均未完成新全量回归。最新1391完整结果不适用于这两个
新组合；原273仍22 complete/166 partial/70 unverified/15 deferred_by_user。
不push、不启用Actions，不把专项通过改写为原总账或新完整注册集合完成。

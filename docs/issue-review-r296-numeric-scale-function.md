# R296：scale(numeric) 声明、求值与协议接入

状态：完成本项有限修复及固定回归。不是 NUMERIC 全功能、全 PostgreSQL 函数目录、
原 273 条清单或全库测试完成；R294/R271 及其他用户暂缓项不恢复。

本项对应原清单 TYPE-01（numeric scale/特殊值/协议）与 FUNC-01（数学函数族）
的有限子要求；两个原条目均不因本项提升为 complete，其完整范围继续保留。

## 缺陷及修改方法

| 缺陷 | 数据库使用影响 | 修复方法 |
|---|---|---|
| 没有真正的 `scale(numeric)` 函数声明与实现 | 字面量、列及 text/binary 参数均可能报 42883；R295 完整 431 项留下 141 处失败 | 在共享签名表加入唯一 NUMERIC 输入、INTEGER/int4 输出，复用现有声明解析及隐式转换；从实际 Numeric 对象读取 display scale |
| 仅添加名称/回调不足以进入前端语义检查 | 参数推断、错误阶段、空表和未执行分支可能绕开声明检查 | 将该调用接入现有 Simple/Extended 绑定与元数据入口，保留名称大小写/namespace 的实际解析；不通过执行函数获得类型 |

真实 PostgreSQL 180006 的 35 条预观察确认：整数可隐式转 NUMERIC，TEXT、REAL、
DOUBLE PRECISION 等不匹配；无类型参数应推断为 OID 1700，结果 OID 23/width 4。
NULL、NaN 和正负 Infinity 返回 INTEGER NULL，而不是 0。有限值保留尾零对应的
显示 scale。源码对照为 PostgreSQL 18.6 `pg_proc.dat` 的 OID 3281 及 `numeric_scale()`。

实现只改 ExprEvaluator、ExprHelper、query binding、Main、NetworkServer 五个 CPP；不修改 Numeric 范围、
共享头、AST、解析器、存储格式或 R294 的通用语法入口。不实现 min_scale/trim_scale，
不以此宣称其他函数、全部目录投影或 R271 无约束运行时域问题已修复。
只有实际内建回调拥有新签名；`registerFunction("scale", ...)` 原生替换仍保留自己的
参数个数、值类型、结果域与 volatility，不能继承本内建的 int4 规则。

## 固定测试和实际结果

| 同一最终完整夹具 | PostgreSQL 18.6 | 旧 Root / 旧纯提交 | 新 Root / 新纯提交 |
|---|---:|---:|---:|
| native 124 | 不适用 | 87 / 87 失败 | 0 / 0 失败 |
| 最终新 wire 416 | 0 失败 | 372 / 377 失败 | 0 / 0 失败 |
| 原 R295 wire 431 | 0 失败 | 141 / 141 失败 | 0 / 0 失败 |

新 native 覆盖真实描述符、独立类型推断、求值、错误阶段、实际 native typed row、
同名 opaque callback 与纯绑定、惰性实参需求、immutable 声明。新 wire 覆盖三种
查询模式、空表 WHERE false/LIMIT 0、错误恢复、字段/行/参数元数据的精确字节、
text/binary NUMERIC Bind、1000 位零值边界、NULL/特殊值、参数 OID 推断及显式 OID。
sequence 控制验证 Parse/Describe/Bind-only 不执行参数表达式，而 Execute 正常执行。
最初完整 wire 356 在 PG 全过、旧 Root/纯提交有 323/328 个失败。随后独立三侧
各 20 条错误优先级观察一致，将其扩为三种模式的 60 个永久控制；原 356 条请求、
顺序、断言完全保留，最终 416 又完整运行参考端及旧新两侧，没有只测新增部分。

原 `tests/numeric_binary_zero_scale_known_gap.py` 的 431 项源码/SQL/断言/期限完全未改，
文件名保留历史身份，现在实际全过，并与新 416 项驱动一起注册到默认 E2E 列表，
各一次。不是删除失败控制、只跑 290 项、修改预期 SQLSTATE 或延长超时换取通过。

本轮正常有限 54 入口全部 0：28 native、6 真实 Main、20 wire，包含原 R295 入口、
STRPOS/scalar 签名、原 EXECUTE 419 和 prepared identifier 226 等相邻回归。
另有 2 个 native 124 专项 UBSan 入口全部 0；早期 124/356/431 六个入口亦全部 0，
另保留 26 个 native 和 16 个既有 wire 的提前完整通过记录，不替代最终正常批次。
这是有限验收，不是全部 1589 注册/原生/Main 入口、原 273 条或冻结 1492 全量验收。

初轮 native 124 各有 21 失败、新 wire 356 各有 11 失败，原 431 已各 0；这些非零
日志完整保留。修正独立类型推断、既有 ORDER BY 伪实参的绑定归属、非聚合标记检查，
以及 text NUMERIC 特殊值的 SQL 字面量转换后，保持同一完整夹具重新验证，没有修改预期。
扩展到 416 后首轮正常 54 入口为 52 通过、2 个新 wire 各 4 失败：Extended/Binary
提前名称探测覆盖了 FILTER 的缺列/类型错误。保留该批完整非零结果，将 SCALE 的
这些调用也送入实际 whole-query 转换后，再以同一完整 54 批次验收，未改 416 断言。

最终接收本轮 fresh 的 10 个生产对象；生产编译共 16 次，包括初轮 6 次、修正 8 次
和最终 Network 2 次。最终 Main 两个对象为同源早期成功编译，
Evaluator/helper/binder 六个来自修正，Network 两个来自最终阶段；未复用过期候选对象。
112 个生产对象和 2 个 stub 核对源码/头/依赖/工具链后
复用；28 个 native TU 和 6 个真实 Main TU 全部 fresh，2 服务器/34 测试程序均新链接。
生产 Main 为 O2；真实 Main 测试沿用历史 window O2、cleanup/session O0 配置。
UBSan 仅新编译两侧 evaluator+新 native 共 4 TU，其余库正常构建，Main 未 sanitizer；
插桩对象与最终源码/头完全一致，最终另外重新链接当前全部正常库（含新 Network）并运行；
不能称为全库 ASan/LSan/TSan/TLS 验收。全部共享头保持当前 R293 AST 与已接受 R290 Stats
版本；未接受暂缓的 R291 新头/候选/测试对象。

## 版本管理和总清单

只接受 tested pure projection 的本项代码与文档，Root 其他草稿、原十八处暂存 diff
保持原样；Main/Network/query binding 的已有分支隔离映射随本项更新，其余八个映射保留。
文档/注册/版本/兼容/总账静态检查保留；require-complete 仍应为 1，原 273 状态
仍 22 complete /166 partial /70 unverified /15 deferred。总目标未全部完成。
冻结 1492 历史失败、R291 CHECKPOINT 原因以及全部用户暂缓项均未借本项关闭。
单项本地 commit，不 push；GitHub Actions 继续禁用。

证据目录 `/tmp/dbms-r296-numeric-scale.tycTBhUc` 不保证长期保存。

| 收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `703d2018ab16d08c6dfc2413c1f4387038d2f876932366b9e87f16535fef1c14` |
| completed-observations-receipt.json | `190d79ba1d9a25fbfe2938b9a9e25105847b0988b400a6c1194c696dd1460b18` |
| completed-old-reference-receipt.json | `a22d7b08a62f011312e1080c8737b5dcd5c2ff1539a2c830cb5cf77242908e3f` |
| completed-primary-build-receipt.json | `34d3f5658c658a0e37eaf7f4645803899ffb5180cd4fa2e89db1554b71709259` |
| completed-early-primary-receipt.json | `714f9603376fed2c094b521dd7553ac274470b47f26088cce19549f77cbd20f2` |
| completed-v2-primary-build-receipt.json | `93928ba8a13c1d9a7d6a73588dbd3389160a905b36b5c19b060143fff309b088` |
| completed-early-v2-primary-receipt.json | `6368cc46926867255aee23116e0c411fbe0518e222cd5a4c303429850f3afc4f` |
| completed-regression-build-receipt.json | `8f86e1f51b060243787e3be469329c514ee6da2f659adffeaf8fcc65ad6fcd99` |
| completed-edge-observations-receipt.json | `afde836a881a94db96c98cd1def77eee6e777bc3aad972cc86bfd8e6d1ded926` |
| completed-expanded-fixture-proof.json | `130e3629917b6507c782419fe7d397f39a0ba8471765bc70c9161ee31d5c3329` |
| completed-old-reference-v416-receipt.json | `ff195153c725e31e9f547868bdee244763d2c14c6c2d3c1d70178a06601d40cb` |
| completed-final-runtime-build-receipt.json | `61bd672db7d805427ac692d6fca661eb732b5f33f2d77e4014fae66c77e676f8` |
| completed-candidate-receipt.json | `77ec21ebc24a1e65eae703a96c6fef5f5639f0d9f916d979f4841587e4e8e4f2` |
| completed-scoped-ubsan-receipt.json | `a7e9a45c97fe480fa0eb42a6b68964d2904792a58f807d309c60317c0426574c` |
| completed-v3-integration-receipt.json | `3ccb7c642ab091e8cb97c63f747d234587b9840f92d599d04ab672763ebfcfbd` |
| completed-v3-runtime-build-receipt.json | `1390c5a2402fc15976ef682b5268a5961dcc0ffbcb49c004360b15a1087ee30f` |
| completed-v3-regression-build-receipt.json | `7141df5cd856aebd9bb9b5c71482c54dc4cc7738fdbcacce24c2d668d640b846` |
| completed-v3-candidate-receipt.json | `0d1bf34aee409646b2fd6396c8e492a864f8c3b6bbacbf23b963003d31a4c466` |
| completed-v3-scoped-ubsan-receipt.json | `a1a1a3183348da3fd0f7f037b6f45b159a508cc5a41d42d9468d2d79bef55fbd` |

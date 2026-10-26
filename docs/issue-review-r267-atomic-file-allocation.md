# R267：原子文件写入的分配异常清理（用户暂缓）

2026-10-26 用户要求“skip it”，本项停止验收并暂缓。后续普通 continue 不自动恢复本项。
仅提交本文档；生产头文件及新增测试草稿不提交、不视为已接受修复。原 273 项总清单尚未全部完成。

## 已确认问题与修改草稿

| 问题 | 影响 | 修改方法及当前状态 |
| --- | --- | --- |
| `writeAtomicallyImpl()` 创建临时文件后构造 `std::filesystem::path`，分配失败时没有清理临时文件和描述符 | replace/no-replace 两种写入都可能遗留打开的描述符及空临时文件 | 草稿改用创建前已准备好的 `tempName.c_str()`，并使用不分配路径对象的 POSIX `unlink` 清理；尚未完成集成验收，未提交生产修改 |

同一夹具在工作区基线及纯已提交基线各运行 342 个固定控制，旧实现各失败 108 个，
实际注入 108 次创建后的分配异常，观察到 108 个遗留临时文件及 108 个未关闭描述符。
夹具先记录问题，再清理自己持有的测试资源，不以清理后的状态代替问题观察。

草稿的两侧定向测试各 342/0，定向 ASan/UBSan 运行 342/0；原故障点被消除，
不能把新实现中的零次异常注入说成再次成功注入了 108 次异常。
故障控制还包括写入、文件同步、关闭、发布及目录同步失败。
持久化 no-replace 成功后临时别名删除失败的既有语义未改动。

## 验证边界

122 个生产单元及 2 个测试桩的匹配冷编译、两侧服务器链接已结束，退出 0。
Root Main 使用此前接受的 pre-R229 源码投影，不是已暂缓的工作区 Main 草稿。
冷编译成功不等于运行时验收。计划中的 180 次集成执行尚未运行，
六个真实 Main 前端尚未重编译/执行，不宣称全量测试通过或本项修复完成。
不启动该模块的其余回归，也不继续其 `removeDurably()` 等变体审查。

证据目录：`/tmp/dbms-r267-atomic-file.vF0DbqVP`。

| 证据记录 | SHA256 |
| --- | --- |
| `completed-old-receipt.json` | `2082034ec80d4c9434ebde4a9c7b226c291c32de6b9d41dc3593734935e5a901` |
| `completed-primary-receipt.json` | `929614436009cce5d36358457a7d312b2d224992f5a99393e65edbd2e7342f85` |
| `completed-sanitized-receipt.json` | `b611a04d20bbdeccbdf8a4beed9928521866361f74b01fa93e0bcea6dc0fff77` |
| `completed-cold-build-receipt.json` | `614810ca1b8e3adc7ce70f5be3630a2a1bb786c04851987db35f7a4a635ccef0` |

## 版本及保留内容

已接受的生产实现仍为 R266 提交 `eab88963e23a3ccee89894bb604f19fcf5e31017`。
`src/access/IndexFileUtil.h` 和 `tests/atomic_file_allocation_test.cpp` 留在工作区作为未验收草稿；
本项的冷构建产物不得替代已接受 R266 的构建证据，也不得混入其旧头文件对象。
其他工作区改动、已有暂存内容、冻结 1492 基线及其他用户暂缓项均保持不变。
不 push，GitHub Actions 保持禁用。

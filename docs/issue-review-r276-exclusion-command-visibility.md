# R276：同一命令内排斥约束检查（按用户要求暂缓）

状态：2026-10-27 按最新 `skip it` 停止本项，保留候选，不计作已修复。
本次只提交本记录，不提交 R276 生产代码、测试或驱动追加内容。
此前已接受的 R275 SQLSTATE 修复继续有效；原 273 项总清单没有完成。
普通 `continue` 不自动恢复本项；其他已明确暂缓的模块也不恢复。

## 已观察的问题与候选方法

同一 SQL 命令的普通读取快照会隐藏本命令此前写入的行；排斥约束扫描
沿用该快照时，可能漏掉同批 INSERT/INSERT SELECT/UPDATE 产生的冲突。
候选仅在 `checkExclusionConflict` 中复制当前事务 ReadView，关闭副本的
`commandIdVisibility`，再交给约束扫描；普通 SELECT/source 扫描保持原快照，
其他 MVCC、隔离与子事务字段不变。没有改变公共头、磁盘格式或全局快照。
这是待验收候选方法，不是本次已接受实现。

## 已完成的有限验证与未通过的补查

| 范围 | 观察结果 | 结论边界 |
|---|---|---|
| 固定 native 44 控制 | 旧两端各 8 失败；候选两端 0 失败 | 包括普通读取仍看不到本命令写入的控制；不是完整事务证明 |
| 具名 NOT DEFERRABLE 协议矩阵 372 控制 | 实际 PG 180006 为 0 失败；旧两端各 72 失败；候选两端 0 失败 | 4 类型、复合键、部分谓词、Simple/Extended、savepoint；不能外推所有约束 |
| 候选有限执行入口 | 50 个执行记录均 exit 0 | 非全测试库存；不覆盖之后的失败补查 |
| 未命名约束自动提交补查 96 控制 | PG 为 0 失败；旧 Root/accepted 及新 Root 各 48 失败 | 新 accepted 未执行；仍有冲突被接受及失败后行集不符 |

补查脚本在新 Root 的预期通过断言处退出 1，没有生成 completed 补查 receipt。
96 是控制次数，48 是失败断言数，不是 48 个独立缺陷。
参考端使用 session TEMP 表，项目端使用私有目录普通表，DDL 持久性并不完全相同。
未修改参考服务配置、重启服务或写入永久用户表。
未命名约束与之前具名案例存在差异；约束名称生成/元数据登记只是待核实假设，
尚不能断言补查失败根因，也不能断言自动提交路径已经修好。

## 保留的工作区内容

- `src/commands/TableManage.cpp`：R276 局部候选，混有更早的其他未接受改动；保持原样。
- `tests/exclusion_command_visibility_test.cpp`：未提交 native 夹具。
- `tests/compat/exclusion_command_visibility.py`：未提交 372 控制 helper。
- `tests/autocommit_deferred_constraint_protocol_e2e_test.py`：R276 追加内容未提交；原测试和 R275 内容保留。

恢复时须先独立对照具名/未命名与显式/自动提交两条轴，保留现有失败断言；
核实真正根因后再补实现。不得用改名夹具、删除失败控制或一次较窄的通过替代修复。
若需要修改公共头，必须重新编译所有依赖，不能混用旧布局对象。

空文本与真实 NULL 元数据、部分谓词 NULL 判断、DEFERRABLE INITIALLY IMMEDIATE
的语句末检查时机、opclass/类型比较和并发排斥冲突仍属于普通开放问题；
本记录不将这些独立问题误标为完成，也不扩展为全部 CONS-04 已验收。

## 证据位置与版本管理

原实验：`/tmp/dbms-r276-exclusion-command-view.2bzkybVy`。
临时目录证据不是长期归档承诺；以下摘要与哈希保存在 Git 文档中。
R275 已接受代码基线为 `a7f29f0db446f18cf4f6c19a0ac61ecbd80c34b3`。
本项跳过记录使用独立本地 commit；不 push，不启用 GitHub Actions。
其他工作区文件和原暂存区保持不变；总 goal 继续 active，不标 complete。

| 原证据文件 | SHA-256 |
|---|---|
| `completed-old-receipt.json` | `c5de9cb91fcc094bb5de098bf842c7e314e361b03da086448a62740480db1f51` |
| `completed-integration-receipt.json` | `94501071c6dce7720cb1070fdd150bcb31850d56910c69fa76bad2ace59a1409` |
| `completed-build-receipt.json` | `0c10947bc8321b014ad3e2db030bf765e373eb9c65fd8bc8f4f307c225980b19` |
| `completed-candidate-receipt.json` | `63ae122bc4961d0d3affbf2635a944af4ea3da531f231ef74dc1b136a93245aa` |
| `completed-remaining-gap-observation-receipt.json` | `57f53aceb4ab8767bf9ed7b870372637e735baafc048002a6bff4152f0463637` |
| `autocommit_supplement.py` | `ce38503934b7728ca85eb6cf46c1a934a1cde4afc00ba3e50920ac1a7fdf0b8a` |
| `reference-autocommit-supplement.log` | `af7a093580fee839f1287350195b21c8ed47f3d58ab82b8dbb9d45ca9d1f5ffe` |
| `root-old-autocommit-supplement.log` | `df810de69cb62305b6074e4ae7e6febbecf153074d28413cfb1a531351f64a07` |
| `accepted-old-autocommit-supplement.log` | `716e4f2c0f7276340318354ae775062477da60dbad3a7043e3eaf97d1172a766` |
| `root-new-autocommit-supplement.log` | `df775b98b4e2cb0e24fda249a6b0855cdf0564334c47f8d76b2d198bf28d03ec` |

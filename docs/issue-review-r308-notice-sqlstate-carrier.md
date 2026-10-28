# R308：序列重复创建的 NOTICE 元数据修复

状态：本项 24 个通知断言已修复并完成有限验收；独立 DO/TEMP VIEW 失败仍保留，
不声称所有通知载体、全部当前运行或原始 273 项已经完成。

## 问题与修改方法

CREATE SEQUENCE IF NOT EXISTS 遇到已有关系时应成功并发 NOTICE/42P07。
旧式 stdout 通知没有状态字段，协议填入 00000；对 public/自定义 schema 的
限定名称，正文还错误地使用了完整限定名称，而不是实际对象的非限定名。
独立 PostgreSQL 18.6 的完整 120 项主源对照全通过；两个旧构建各失败 32 项，
其中 24 项是本项通知，8 项是 DO 块未到通知产生处便以 42601 失败。

修复在真实序列重复对象分支产生 NOTICE/42P07/实际解析对象名正文，利用独立
StatementNotice 线程局部作用域传给协议，不从 SQL 或用户文字猜码。嵌套和
异常退出恢复上一接收器，无接收器时保留 CLI/嵌入式打印回退。既有未转换
通知仍沿用原兼容读取，不把 Root 的未接受通用诊断、portal 或例程草稿并入。
Main、旧头文件、parser、TableManage、磁盘格式及 TEMP VIEW 逻辑均未改动。

新增组件断言检验 code/message 独立、换行、普通 `(SQLSTATE ...)` 字面文本、
嵌套/异常/空作用域、8 线程共 2048 条消息精确隔离和 CLI 回退。最初 wire
测试把 DataRow 的 bytes 与 str 直接比较，已保留初版和日志；修正仅编码独立
预期值，120 项 SQL、状态、正文和严格帧断言全部保留，并重新通过主源。

## 实际验收

| 范围 | Root 隔离投影 | 纯接受副本 |
| --- | --- | --- |
| 新组件 20 项 | 全通过 | 全通过 |
| 完整新 wire 120 项 | 112 通过、8 失败 | 112 通过、8 失败 |
| 原冲突 wire 260 项 | 258 通过、2 失败 | 258 通过、2 失败 |
| TEMP sequence wire 182、旧 tag 100 | 全通过 | 全通过 |

120 项的 8 个失败请求与旧基线 DO 失败逐字相同；仍保留在注册测试里，不删
测试、不放宽预期，也不据此恢复用户已暂缓的例程模块。260 项的 18 个旧
NOTICE 失败已消失，剩余 2 个请求精确属于已按用户要求暂缓的 R310 TEMP VIEW。

新增头文件后，两侧全部 122 个生产 TU、2 个 stub、50 个原生测试 TU 和
6 个真实 Main 测试 TU 均重新编译，56 个测试程序及两个服务器重新链接。
原冷构建脚本在全部编译/链接后，因本项文档更新触发最终工作区保护而退出 1；
该真实失败单独保留。逐一校验已生成对象、依赖、源码与工具链后恢复构建
元数据，不重复编译，也不把原脚本的退出码改写成 0。
正常有限矩阵 112 条实际退出记录中 102 条为 0、10 条非 0：除前述 DO/TEMP VIEW，
其余为原有 FACTORIAL/MIN_SCALE 的 R271 边界与已暂缓 R303 规划问题；失败
请求与上一接受记录精确比对不变。FACTORIAL 每次生成独立 schema UUID，跨次
比对只对 SQL 标签中的该标识符做对应；实际/预期协议帧仍逐字相同，测试
本身和原始日志均未修改。所有旧正常/作用域 sanitizer 的 124 个
运行输入均保留，新增 8 条后共 132 条，116 条退出 0、16 条非 0，并非全绿。

作用域 UBSan 20 条中 14 条退出 0、6 条为上述已分类功能差异，无 UB 诊断。
本次新编译 OutputCapture/DDL/Network/新组件共 8 个插桩 TU，复用 18 个
源码、旧头文件及工具链一致的旧插桩对象；Main 和其余库为正常对象，不是
整库 sanitizer。保留全部旧 16 个作用域运行输入。

证据：`/tmp/dbms-r308-notice-carrier.BkHGKUxp/` 中的
`completed-reference-old-v2-receipt.json`、`completed-cold-build-receipt.json`、
`completed-regression-build-receipt.json`、`completed-candidate-receipt.json`、
`completed-scoped-ubsan-receipt.json`、`completed-classified-candidate-receipt.json`
及各原始日志。新主源 oracle 与旧基线保存完整；不把观察脚本自身假失败算作
产品缺陷。候选 source/tests/scripts/cmake 2338 个文件按哈希验收。

当前注册库存为 993 个原生测试、608 个注册 Python、3 个真实 Main，共 1604；
这是库存而非完整运行证明。历史 frozen1492 的 1481 通过/11 失败及原始
273 项 22 完成/166 部分/70 未验证/15 暂缓均不改写。全部总清单尚未完成。
原有工作区/暂存修改保留，独立纯投影本地 commit；不 push，不启用 Actions。

## 历史：R307 首次发现本项时的记录

# R308：IF NOT EXISTS 通知 SQLSTATE 被协议默认值覆盖（OPEN）

状态：实际复现、未修复、非用户暂缓。与 R307 重名 ERROR 的代码缺失是独立
载体问题；通知保持成功不等于通知元数据正确。

R307 完整 260 主源 PG18.6 全通过。十八个 IF NOT EXISTS 请求（普通/自定义/
临时命名空间、已有 sequence/table/view、Simple/Extended）两侧均 NOTICE
后 CREATE SEQUENCE 成功，原对象和值不变，但产品通知 C=00000，PG C=42P07。
完整 severity/SQLSTATE 断言保留，不弱化成“只要收到 N 就算通过”。

已读接受副本 NetworkServer.cpp 的旧式 stdout NOTICE/WARNING 消费处，确实
将 SQLSTATE 硬编码为 00000/01000；当前 DDL 通知文本也不携带该状态元数据。
方法应在真实诊断产生处保留 severity/code/message，按语句和线程生命周期
传递至协议/CLI 消费者；不能从任意用户通知文字或对象名猜码、也不能把普通
RAISE NOTICE 的字面 `(SQLSTATE ...)` 误当元数据。需覆盖用户原样通知、显式
代码、嵌套诊断、无通知语句不串消息和并发隔离后独立提交。

当前不实现新的通知 carrier，不采用 Root 中未接受的 OutputCapture/触发器
诊断通道草稿；R307 只改实际 ERROR 分支。源、全部原始帧和失败见
/tmp/dbms-r307-sequence-duplicate.h7qqyppH/completed-original-duplicate-reference-old-receipt.json。
原 273 未完成；全部已暂缓模块不恢复，不 push，Actions 禁用。


## 后续 R311：跨通道顺序

R308 的序列元数据修复不等于混合通知次序已验证。后续独立组件实际复现
结构化与旧通知跨通道重排；位置/活动接收器修复与全部有限回归见
[R311 审查记录](issue-review-r311-mixed-notice-order.md)。原 R308 的 20/120 项
输入保持不变，包括其 8 个已暂缓 DO 执行失败；不声称全项目载体迁移完成。

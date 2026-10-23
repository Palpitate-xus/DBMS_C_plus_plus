# R236：INTERVAL / INTERVAL[] 的 binary Bind 输入

最新后续：[R237](issue-review-r237-interval-cast-typmod.md)修复独立 typmod 描述错误，原完整143项两侧均全0；新360夹具仍各1个TEXT输入值错误，另列R238继续，不报30全绿。初次失败不重写；以下保留R236提交时状态。

初次提交时状态：接收器修复已实现并验证；完整新夹具仍有独立 typmod 描述错误，不能报全绿。

## 问题与修改方法

R235 未缩减的 997 项检查发现，binary Bind 的 interval 参数直接报 0A000；已有
interval_send 只证明结果发送，并没有提供 interval_recv。数组 1187 同样未接入。
这是 TYPE-06 / PROTO 二进制输入的缺口，与 justify 混合符号算法错误分别提交。

新增精确 16-byte 接收器：网络字节序 int64 微秒、int32 日、int32 月各自保存，
不转换为 double、不把月份/天压平成时间。使用可移植的补码数值转换，只有三个
字段全部为各自正/负极值时才解释为对应无限值；单独的 int64 最小微秒仍是有限值。
共享 interval formatter 保留正负字段和完整微秒，后续已有类型输入/typmod 继续执行。

1187 复用现有数组维度、元素 OID、长度和 NULL 外壳，元素调用同一接收器，再按
数组文本引用规则转交现有绑定路径。没有添加新数组 catalog 或改变文件格式。
scalar/element 短于 16 bytes 精确 08P01，超长精确 22P03；不把编码错误吞成 NULL。

## 完整证据及仍在失败的控制

| 完整程序/驱动 | 旧版两侧 | 当前接收器两侧 |
| --- | --- | --- |
| 新 143 项协议 | 各 135 个断言失败，退出 1 | 各剩 1 个 typmod 描述失败，退出 1 |
| 每侧 4 个原生、6 个既有协议入口 | 非本轮旧版全回归 | 20 次全部退出 0 |
| PostgreSQL 18.6，同一完整 143 项 | 私有新实例正常停止，默认 fsync | 全部通过 |

135/1 是重复断言失败数，不是独立 bug 数。新夹具没有删减、过滤或变更 oracle。
当前唯一失败是 `SELECT $1::interval DAY TO SECOND(3) AS v`：返回值已是正确的
微秒值，OID1186/width16/二进制格式也正确，但 RowDescription typmod 错为 -1，
应为 `(7176 << 16) | 3`。这是已有描述器只接收 CHAR/BIT/TIME modifier 的缺口，
另列 R237 继续修复。当前不是完整 22 次测试通过，不以 142 项通过关闭该失败。

143 项包含 35 组实际 scalar/array 微秒/月/日/无限值、全宽边界、非一数组下界、
NULL、空/二维/六维、全部 0..15 截断长度和 17..20 超长、错误数组元素/flags/OID/
尾字节，且每个错误后再次执行正确 Bind。还检查 Describe、真实 INSERT/UPDATE/
读取、NULL 写入和 precision=3；这个 precision 描述断言继续保留并作为后续 gate。
既有回归覆盖 interval 发送、算术/日历顺序、赋值、存储、符号格式、输入错误码、
参数身份及原完整 builtin binary array 接收器，不以新 scalar 通过代替旧数组验收。

## 构建和版本边界

两侧 fresh NetworkServer TU，自己的其他对象/编译器/头/stub 匹配后复用并新链接；
各 4 个原生也 fresh 编译。Root62生产对象/134头、纯提交60/131，不混布局。
Root Main 用明确的 pre-R229 匹配 donor，跳过草稿/default dbms_main 不编译或运行。
不是 normal/repeat、全部 fresh、当前全工作区源码、全套、TLS 或 sanitizer 验收。

一次编辑辅助脚本复用循环内变量，部分修改 Root 后在纯提交侧断言停止，尚未编译；
仅反向恢复本项编辑，逐文件完整摘要证明 Root 回到 preflight，再修正辅助脚本继续。
首脚本、错误输出与恢复收据保留；没有覆盖失败或将它记成产品编译通过。

2257 个纯源码输入与这次完整有红测试的实际组合完全一致，943 native+591 Python+
3 Main=1537 是入口清单，不是全量通过。独立 index 仅提交本项四路径；原暂存18、
其他工作区及未提交 R235 夹具/注册保留。R235 尚未实施，R237 仍待修复。
R233 及其他暂缓不恢复；原273/冻结1492不重写，总目标仍未完成。不 push，Actions禁用。

本地证据目录：`/tmp/dbms-r236-interval-binary-input.37giZKG0`。

| 记录 | SHA256 |
| --- | --- |
| 旧143完整基线 | `f9f5c87ffb16dd6b96e510db93a10071bbdccb5311ac79403f73868345ed9e10` |
| 完整PG143 | `e43a5c42ec25b03986da57301d8e55e316ec103ec4b8fe821b080614046e7072` |
| 部分编辑恢复 | `d65a3319f227c71561a06ce69220a1e056bd666a8970aad27e368e0a7c5f6aa7` |
| 实际构建 | `3fcc6946a85286936d932cbdc50f41d77c00d498d4a15ec5f20fbe3cd99dbbad` |
| 完整22次运行，两个新夹具仍各1失败 | `4657db819a2f0242ff1283590f9b2778dc2bd81fa900025d05f32daa1d72a78a` |

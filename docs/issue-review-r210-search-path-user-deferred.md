# R210：search_path 默认值及当前角色命名空间（用户暂缓）

状态：按用户最新 `skip it` 停止本项实现和验证，未修复。普通 continue 不恢复本项。

## 已发现的问题与修改方法

`Session::searchPath` 和 `defaultSearchPath` 当前为 `public`，而 PostgreSQL 18.6
内建默认值为 `"$user", public`。只更改 SHOW 输出不能修复实际表、类型和函数的搜索行为。
另在显式使用 `"$user", public` 后，产品的 CURRENT_USER 随 SET ROLE 改变，
但 current_schema 等搜索路径消费者仍使用登录用户名，可能选择错误命名空间。

恢复时应统一默认常量，并让搜索路径消费者使用当前有效角色；保留显式传入用户名的
纯展开函数，不使其隐式读取线程会话。不得更改认证、授权判断或数据库物理格式。
启动参数显式设置的路径必须继续作为该连接 RESET/DEFAULT 的目标，不能覆盖成内建默认值。
共享启动校验器应严格区分内建默认和显式启动值，不得接受任意返回值；已跳过的默认协议
main 不恢复。Session 头文件变更后需要使用两侧各自头文件重新编译全部依赖，而非复用旧对象。

## 实际验证边界

原生默认值夹具完整 99 项，在纯已提交版本与工作区分别实际执行，均有 50 次失败，退出 1。
它尚未修复。96 项有效角色夹具尚未编译或执行，不算已验证。

协议草稿完整 625 项的第一次 PostgreSQL 18.6 参考执行退出 1，109 次检查失败：
夹具共用校验器错误地要求事务内 ReadyForQuery 为 I。保留首版及失败日志，
第二版先精确检查实际事务状态再适配共用校验器；该次参考仍退出 1，
存在启动参数与 options 优先级的期望错误，不能称为参考通过或产品验收通过。
用户要求跳过后不继续纠正或重跑本项。

三个夹具按原字节保存在 `tests/search_path_default_test.cpp.draft`、
`tests/search_path_effective_role_test.cpp.draft`、`tests/search_path_default_protocol_draft.py`；
观察脚本 `tests/search_path_role_observation_draft.py` 也仅为草稿。
它们不注册、不计入已接受测试，不由默认扫描恢复本项。生产代码和共享启动校验器未改动。

## 观察过程中另发现的开放问题

以下为待独立验证/修复的问题，不能用本项暂缓冒充已解决，也不在此实施权限逻辑变更。

| 编号 | 现象与证据边界 | 后续修改方法 |
| --- | --- | --- |
| R218 | 模式名与表名各自小于 63 字节，但组合名称过长时，产品 CREATE TABLE 报 XX000；缩短表名后创建成功。私有 PG 18.6 对原组合创建成功。 | 检查 TableManage 对拼接限定名使用 MAX_TABLE_NAME_LEN 的限制，分离模式/表标识身份；先评估目录与持久化兼容性，不能只截断组合名称。 |
| R219 | 已实际 CREATE/INSERT 的 public 和角色模式表，用模式限定的 GRANT 目标时报 42P01；非限定 public 目标成功，PG 18.6 对限定目标成功。观察脚本不构成 ACL 等价性证明。 | 独立复现 GRANT 目标定位，复用解析后限定标识符和正确命名空间查找；不能用放宽权限检查掩盖对象定位错误。 |

`CREATE SCHEMA ... AUTHORIZATION` 的所有者语义也尚未独立验收；本次不声称角色表权限正确。

## 本地证据与版本边界

资料目录：`/tmp/dbms-r210-search-default-v2.Bn8Ou9Gb`。
原生基线：`completed-native-baseline-receipt.json`；输入快照：`completed-prepared-receipt.json`。
首次完整协议参考：`/tmp/dbms-private-pg18-full-fixture.qnd4wa38/completed-reference-receipt.json`，
SHA256 `3eb0ee80065934f24dad99c0be2d2dc4da0c85cc8fce2e17ce632dd95cb8229b`。
第二次完整协议参考：`/tmp/dbms-private-pg18-full-fixture.u4p7vpdi/completed-reference-receipt.json`，
SHA256 `50d85a65f12cd09d097bca790b63651f2716bce47840928623bfa36c84ffe3ad`。
R218 原始脚本与产品失败保存在上述目录 `v1-search_path_role_observation_draft.py`、
`accepted-role-observation.log`；PG 参考目录为 `/tmp/dbms-private-pg18-full-fixture.qta4cl2i`。
R219 产品失败保存在 `accepted-role-v2-observation.log`、`accepted-role-v4-observation.log`；
PG 参考目录分别为 `/tmp/dbms-private-pg18-full-fixture.vynfjhrb`、
`/tmp/dbms-private-pg18-full-fixture.b0_t53z7`。参考实例已停止，未触碰既有服务。

只提交本暂缓与问题记录，不夹带工作区其他改动或 staged 18 路径。
R211 已接受修复不回退，其他用户暂缓项不恢复；原 273 标签和冻结 1,492 结果不改写。
总清单仍未完成。不 push，不启用 GitHub Actions。

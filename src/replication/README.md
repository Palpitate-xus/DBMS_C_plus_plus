# Replication（复制与容灾）

本模块提供复制槽/发布等管理对象和项目格式的逻辑变更预览，不具备 PostgreSQL 物理流复制或订阅接收/应用的完整运行时。
逻辑change stream目前仅在内存中；管理对象的持久化不能替代变更流的持久化、重放和连续性保证。

`LogicalDecoder` 只注册 `dbms_pgoutput_preview`、`dbms_test_decoding`；`pgoutput` 和 `test_decoding` 保留名称被拒绝。预览格式不能作为 PostgreSQL 插件协议使用。物理槽管理接口的存在也不证明已有 WAL sender/receiver、hot standby 或同步远端确认。

普通 WAL 与单节点恢复主要在存储/命令层实现，必须与复制功能分开验收。SQL 项目管理接口受显式 `extended` 模式约束，详见[使用手册](../../docs/MANUAL.md)和[兼容性契约](../../docs/compatibility-contract.md)。

## 已知问题与后续工作

[代码审查报告](../../docs/code-review-2026-10-13.md)记录了预览行像丢失 NULL/字段边界、publication 读取失败可能漏采变更，以及复制槽持久化测试漏注册的问题。
后续需要版本化的 typed 行像、可检测的流连续性、真实传输与应用、恢复/重放和故障切换验证；当前预览控制通过不能扩大为生产复制能力声明。

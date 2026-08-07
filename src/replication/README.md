# Replication（复制与容灾）

## 当前状态

复制模块目前不是生产功能，当前仅提供管理对象和单节点恢复基础：

- WAL（Write-Ahead Log）与 Checkpoint 机制（`src/commands/TableManage.cpp`）
- 单节点崩溃恢复（crash recovery）

## 未来计划

- 物理流复制（Streaming Replication）
- 逻辑复制（Logical Replication）
- PITR（Point-In-Time Recovery）

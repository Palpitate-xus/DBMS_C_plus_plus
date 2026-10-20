# R201：物理表结合目标列表 UNNEST（用户跳过）

状态：按用户最新 `skip it` 暂缓；不实施、不验收为已修复。

R200 的观察中，`SELECT id, upper(t), unnest(ARRAY[1]) FROM case_null_rows ORDER BY id`
在本项目返回 `0A000`，对应 PostgreSQL 18.6 查询成功。这是 QRY-01 的独立能力缺口，
不是 R200 标量结果 NULL 位修复所覆盖的 SQL 路径。

本项只开始定位已有目标列表集合返回函数的准入分支；没有生产修改、新注册测试或候选构建。
现停止本项检查，保留已有观察，不以其他路径通过关闭该缺口。
后续普通 `continue` 不恢复本项；需要用户明确要求恢复。

R197 COPY、R185 排序中断、默认协议主测试及其他既有暂缓范围保持不变。
原 273 项清单仍未全部完成；不 push，不启用 GitHub Actions。

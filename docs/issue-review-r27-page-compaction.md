# R27：页压缩保留全部存活行

对应[审查清单 R27](code-review-2026-10-13.md)。原 `PgPage::compact()` 使用 256 项固定数组；合法页面存在更多存活行时，未处理行仍保留旧地址，新 `pd_upper` 却只计算前 256 行，后续写入可能覆盖它们。全部删除的路径还通过 `init(0)` 丢失物理页身份和链指针。

## 修改

按实际 line pointer 数动态收集全部存活行；搬移前检查页面、容量及 tuple 重叠，保留 slot 编号、HOT redirect、物理页号、LSN 和 special space。压缩后清除已回收 tuple 的 UNUSED 地址，禁止恢复已被 VACUUM 回收的数据；正确维护空闲 slot 标记。空页不再重置身份。

## 验证

`tests/pg_page_compaction_test.cpp` 使用正常 insert/remove/compact/get API 和真实 SMALLINT heap tuple，覆盖 258/270/272/257/1 行、首/中/尾/间隔/全部删除，共 25 组及一组 HOT redirect。逐字节检查存活行、slot、校验和、页号、链指针、精确空闲空间、重复压缩和后续插入。

- 同一驱动链接冻结的原 `PgPage.cpp`：首组 258 行删首行后压缩，页面有效性断言失败，退出 134。
- 修复实现 O2：26 组全部通过，退出 0。
- 修复实现 ASan/UBSan：26 组全部通过，退出 0。
- 已注册 `tests/pg_page_compaction_protocol_e2e_test.py`：真实 SQL 的 258/270/272 行 DELETE/VACUUM/INSERT、CHECKPOINT 后停止原进程，并用同一数据目录启动新进程检查全部值，退出 0。此 SQL 控制在冻结的旧 Main 上也通过；它是邻近持久化回归，不替代原生单页问题的失败证据。

原生测试仅新编译真实 `src/storage/PgPage.cpp`，未使用旧对象或测试替身。O2、sanitizer 产物位于 `/tmp/dbms-review-fix-donor.N9sdas6f/pg_page_compaction_*`；SQL 测试使用自己创建的数据目录，不访问用户数据库。

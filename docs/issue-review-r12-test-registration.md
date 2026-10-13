# R12：补齐独立端到端测试注册

审查发现五个已有 `*_e2e_test.py` 独立驱动未进入标准 `DBMS_E2E_TESTS`。单独运行通过并不能弥补总回归漏跑。

将以下原驱动加入共享配置；没有改变 SQL、断言或期限：

- replication_slot_persistence_e2e_test.py
- pg_settings_query_protocol_e2e_test.py
- bound_dml_query_children_protocol_e2e_test.py
- on_conflict_constraint_protocol_e2e_test.py
- checksum_verification_e2e_test.py

新增已注册的 `test_registration_inventory_test.py` 实际加载共享 Bash 配置，检查注册非空、唯一、文件存在及 Python 语法，并要求顶层独立 `*_e2e_test.py` 全部注册。不将嵌套参考客户端、helper 或 known-gap 文件盲目加入调度。

验证：新增盘点测试修改前实际退出 1，列出上述五个遗漏；修改后退出 0，当前 452 个唯一 Python driver、435 个顶层 E2E 文件完整。原 build cache routing 驱动和 `bash -n` 通过。Root 标准生产构建退出 0，上述五个完整原驱动均实际退出 0，日志在 `/tmp/dbms-review-fix-donor.N9sdas6f/r12-*.log`。

注册数会随之后的新回归增加，盘点断言不硬编码数量。这是测试调度修复，不是全套回归已经通过的声明。

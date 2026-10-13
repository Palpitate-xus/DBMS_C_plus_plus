# R28：页插入容量不下溢，复用 slot 不重复收费

原 `PgPage::insert()` 对 UNUSED slot 使用 `upper-lower-4` 判断容量。空闲空间不足四字节时，负值转成 `size_t`，使满页插入成功并覆盖 line pointer；空间足够时又多收四字节。`canFit()` 与实际复用需求也不一致。

修改为先校验边界，再计算非负可用空间：复用 slot 只需 tuple 字节，新 slot 另需四字节；`canFit` 使用相同规则并拒绝零长度及超大长度。失败时不修改页或输出 slot。

新增 `tests/pg_page_insert_capacity_test.cpp` 通过正常初始化、insert/remove/restore 构造页面，不直接伪造页头。覆盖复用/新 slot、0–10 字节空闲和 0–8 字节输入的 198 个组合，检查成功的数据、准确空闲量、校验和及拒绝后的整页字节不变，另有满页和 `SIZE_MAX` 控制。

- 修复前实际退出 134：满页 remove 后 insert 1 字节错误地成功。
- 同一测试修复后 O2、ASan/UBSan 均退出 0，198 边界全部通过。
- R27 原生页压缩 26 组重新链接本次真实源并运行，退出 0。

以上均新编译真实 `src/storage/PgPage.cpp`，无测试替身或旧对象复用。产物位于 `/tmp/dbms-review-fix-donor.N9sdas6f/pg_page_insert_capacity_*`。原生驱动由标准测试发现机制自动注册。

# 冻结b5def9ba的完整注册回归终态

2026-10-14标准未过滤`bash scripts/build_tests.sh`完整结束，19:12 UTC终态exit1。
这是该固定输入的历史结果，不把后续修复或定向门禁改写为本轮全绿。

| 输入/产物 | SHA256或固定值 |
| --- | --- |
| HEAD | b5def9ba6a319e6b1edf2281f57db8f746dc708c |
| 全部tracked输入 | 49192597a4925d0225c1d11fe6899ce4a47ddd81d37d2df09c3eeaa15a6f6334 |
| 生产binary | 2e6fda283a11d98b0e2a2d917c83f274891a0ea5b89cd2da999291e68ca7fa8c |
| 生产cache输入 | 955674ce31c2f6f1453516a1a90adbc372c94ea2f94f5384e212100e3cbf5a08 |
| 完整日志 | a26928ce8ce9659ccad50b6853ab8b877ae0c4eb24f9fd4b868f3d96e6999ca7 |

结束时HEAD、tracked输入、binary、cache及58份生产对象receipt全部不变。
22:50 UTC、R80修改前独立只读复核仍一致；生产fence日志为
`/tmp/dbms-review-r46.ifu6SWLp/r78-terminal-fence.log`，exit0。
完整证据目录`/tmp/dbms-review-full-20261014-r78.G8uEoJKh/`含
run-terminal.log、full-regression.log、expected-drivers.txt和driver-summary.log。
本机临时路径是辅助证据，不是可移植发行产物。

| 类别 | 全部入口 | PASSED退出 | FAILED |
| --- | ---: | ---: | ---: |
| native | 858 | 857 | 1 |
| 真实Main frontend | 3 | 3 | 0 |
| Python注册入口 | 499 | 498 | 1 |
| 合计 | 1360 | 1358 | 2 |

严格核对1360个入口各出现一次，missing/extra/duplicate均为空。
成功退出包含1个TLS环境skip，故实际不能记为1358个TLS等运行验证全部通过。
本轮未进行全库sanitizer、CMake或完整PostgreSQL兼容性验收。

生产库对象来自同一主工作区当前O2产物，57份对象逐字节核对后进入canonical
test cache，test stubs由当前头文件重新编译；没有借用私树对象。构建脚本因
注册脚本mtime发生一次标准relink，binary字节摘要保持完全相同，未冒充fresh58。
全部普通native仍逐文件实际编译、链接、隔离执行，三个真实Main入口及499个
Python入口未筛选、未修改原期限；本轮运行期间未编辑tracked文件或提交HEAD。

## 两个失败和修复方法

| 原入口 | 实际问题 | 独立处理 |
| --- | --- | --- |
| length_projection_semantics_test | 原生VARBINARY列length被签名拒绝42883 | [R79](issue-review-r79-native-binary-length.md)：签名身份与raw-byte codec分离，保留原九项及新增30控制 |
| window_legacy_result_order_protocol_e2e_test.py | 原44中abs(id-5)排序误判TEXT减INTEGER，其余43正常 | [R80](issue-review-r80-unbound-scalar-inference.md)：先检查未绑定列，再推断声明；17新控制和原44严格重验 |

[前次18d85f4e全量](issue-full-registered-18d85f4e.md)的1339个入口集合完整保留，
只读集合比较没有删除项。其20个原失败入口在本轮都实际退出0，不能因此改写
前次20 FAILED；尤其COPY原timeout原因未证实，只记录后续原样完整复验通过。

R80已在6456ad4f独立提交：原新17中8失败→0，十一完整native及八完整协议0，
原窗口44与实际PG18.6相符。R79单独验收和提交，其真实门禁以问题页为准。
修复后的组合须另行冻结运行，不能把这份历史1360/2失败当作新组合全绿。
原273功能账仍22 complete、166 partial、70 unverified、15 deferred_by_user；
R62通用签名和用户暂缓范围不自动关闭。未push，GitHub Actions保持禁用。

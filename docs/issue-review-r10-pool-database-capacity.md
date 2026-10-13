# R10: 保留 per-user 容量的跨数据库租借

本提交依赖 R09 `04d053c5`，独立分支 `fix/review-pool-maintenance-oct13`。

## 原因与容量合同

`pool_size` 在 Config.h 和 ConnectionPool 的 map 中均为每 user 的总容量，
不是每 `(user,database)` 额外给一份额度。旧实现只复用同 database 后端，
容量1时一个空闲 db1 后端阻止 db2 创建，db2 永久等待一个没有租借者会归还的同库后端。

本修复保留 per-user 容量，依次尝试：同库 idle 复用、未满时增长、满池时替换一个
idle foreign-database entry 为全新 BackendContext。绝不改 rented entry；
不把旧 Session 或 preparedStatements 复制到新数据库，也不修改其他持有者的旧 context。
没有 idle entry 才等待。configure 增加通知，使已等待的请求也能看到新容量。
公共头及 ABI 保持字节不变；同库复用继续保留原 preparedStatements。

## 回归与真实结果

新增自动枚举的 `tests/connection_pool_database_capacity_test.cpp` 覆盖：

- 容量1、db1 idle → db2，计数保持1，旧对象不改，新 prepared 状态为空。
- 容量1、db1 rented → db2 必须等到 release，不能替换忙后端。
- 已等待请求在 configure 容量1→2后被唤醒；两个忙后端都保留。
- 不同 user 各有一份容量；同库复用仍是同对象并保留 prepared 状态。

在仅含 R09 的原实现上，新测试 O2 实际 exit134：首个正常跨库租借三秒仍未完成，
有界失败清理用 shutdown 唤醒并 join，随后 `assert(ready)` 失败。不是无限挂起或缩减断言。

修复后新增测试 O2、ASan/UBSan均 exit0；O2另重复20次，ASan/UBSan另重复10次，全0。
全部原 `connection_pool_test` 和 R09 八轮 discard 控制，在重新编译的 O2 与 ASan/UBSan 中也均 exit0。

所有测试只链接真实 ConnectionPool.cpp 和对应测试，均在独立工作树 fresh 编译，没有对象缓存或 Root 二进制复用：

```bash
g++ -std=c++17 -O2 -Wall -Wextra -pthread -Isrc -Isrc/utils \
  tests/connection_pool_database_capacity_test.cpp src/network/ConnectionPool.cpp \
  -o build/review-r10/database_capacity_fixed_o2
g++ -std=c++17 -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined \
  -pthread -Isrc -Isrc/utils tests/connection_pool_database_capacity_test.cpp \
  src/network/ConnectionPool.cpp -o build/review-r10/database_capacity_fixed_asan
```

日志在独立 `build/review-r10/`。基线 O2 日志 SHA-256：
`3a7a3ffd8ada3fcabdb00d6f4710cf4eaca1c53f0cdc074895010d9cdd2b4c8d`；
首次新 O2 和 sanitizer 输出 SHA-256 均为
`e3499969677c300767561196b7d7f87d93aafec3556a735afe5ba418e4d9330a`。

## 限定

不新增跨数据库 prepared/session 状态迁移承诺，不重定义 user 容量，
不保证条件变量的 FIFO 公平性。这里验证普通 ConnectionPool API，没有真实网络端点或全注册回归结果。
R15 server shutdown 顺序仍待独立修复。未改 Root 工作区、未 push/启用 Actions，未进入用户暂缓专项。

# R09: discard 后的连接池等待引用

基线为 `b54cc7f0`，独立分支 `fix/review-pool-maintenance-oct13`。
本项只修 `ConnectionPool::acquire` 跨条件变量等待保存可删除容器引用的问题。

## 原因与修改

旧 acquire 保存 `pools_[user]` 内 vector 的引用，随后 `cv.wait` 解锁。
session 断开调用 discard；删除最后一个后端时整个 map 节点被释放。
等待谓词再次遍历该 vector 发生 heap-use-after-free。

acquire 现为持锁重查循环：容器引用只存在于锁内的查找作用域，等待之前作用域结束，
醒来重新解析 user key，再检查可复用后端和可创建容量。discard 腾出的容量也能创建新后端。
shutdown 仍返回 nullptr，totalWaits 仍每个等待过的 acquire 计一次。
未修改公共头、ABI、per-user pool_size 定义、同数据库 prepared statement 复用合同。

## 回归与真实结果

`tests/connection_pool_test.cpp` 保留全部原控制，并增加八次普通 session 模式操作：
pool_size=1，持有后端；另一线程 acquire；通过 waitingRenters 确认已等待；
discard 最后一个后端；验证等待者取得新的同 user/database 后端、计数正确且可清理。
失败分支有三秒限时与 shutdown，不用无限等待作为断言。

以未改的基线 ConnectionPool.cpp 编译同一增强测试，ASan/UBSan 实际 exit 1：

```text
ERROR: AddressSanitizer: heap-use-after-free
READ of size 8
operator() src/network/ConnectionPool.cpp:85
ConnectionPool::acquire ... src/network/ConnectionPool.cpp:83
```

修复后相同完整测试 O2 和 ASan/UBSan 均 exit 0；另外 O2 重复20次、
ASan/UBSan 重复10次，30次全部 exit 0。原 resetForReuse 未使用参数 warning 保留，未混入无关修改。

构建均是独立工作树中的真实源编译，只链接该测试和 ConnectionPool.cpp，无 Root 缓存或二进制复用：

```bash
g++ -std=c++17 -O2 -Wall -Wextra -pthread -Isrc -Isrc/utils \
  tests/connection_pool_test.cpp src/network/ConnectionPool.cpp -o build/review-r09/connection_pool_fixed_o2
g++ -std=c++17 -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined \
  -pthread -Isrc -Isrc/utils tests/connection_pool_test.cpp \
  src/network/ConnectionPool.cpp -o build/review-r09/connection_pool_fixed_asan
ASAN_OPTIONS=detect_leaks=0 timeout 15s build/review-r09/connection_pool_fixed_asan
```

本机日志在独立工作树的 `build/review-r09/`，关键内容已如上保存。
原 ASan 日志 SHA-256 为 `d3f656c10a209f53e83747a549e74ada9e5bfec2451251b035e016c6189066b9`；
修复后两种首次运行输出 SHA-256 均为 `fed7be9d070e9195932009a63637208f8eea262ea3aa7c3fd3d8b13e94b7dd75`。

## 限定

本项没有运行完整注册回归、实际网络 session 或 TSan，不能宣称连接池全族完成。
R10 的满池 foreign-database idle 后端仍需独立修复；R15 的 server join/shutdown 顺序未在本提交修改。
Root 源码、缓存及二进制保持不变；未 push、未启用 Actions，未进入用户暂缓专项。

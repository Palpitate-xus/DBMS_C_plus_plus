# R184：公共测试helper未隔离继承的数据目录

状态：本项有限修复已验证；源码、既有回归的增强和本文在同一独立本地提交中。
这是测试调度的隔离缺陷，不是新增SQL功能或原273要求全部完成。

## 问题、影响与修改方法

[公共helper](../scripts/build_common.sh)原来仅在mktemp创建的私有cwd中启动binary，
仍继承调用者的DBMS_DATA_DIR。绝对路径可继续指向调用者选择的数据根；相对路径、
缺失/空值也没有保证Main初始化所需的显式私有数据目录。工作目录隔离本身不足以
保证这个环境变量的语义。该helper被单项native、整套native及三个真实Main驱动复用。

修复只在已进入私有cwd的子进程启动命令中赋值DBMS_DATA_DIR=.，在C++全局初始化前生效。
点号相对于私有cwd解析，父shell的环境和cwd不变；原退出码传递和临时目录清理不变。
没有改变生产C++、127公共头、SQL/协议、持久化或超时，也没有改动已有Main测试体。

本轮旧实现探针使用/usr/bin/env及本轮自建的临时目录/标记文件，不启动数据库，
未向真实调用者数据目录写入，也没有证据声称用户数据库已经损坏。

## 可重复的旧反例与永久回归

既有[build_cache_routing_test.py](../tests/build_cache_routing_test.py)保留原全部缓存路由和bash语法检查，
追加同一个七控制回归：绝对/相对/缺失/空/点号五种环境值，以及子程序退出0/7。
逐个检查私有数据根、父环境/cwd、调用者标记文件和helper清理；末尾必须输出完整7控制footer。
只测试私有临时目录，不依赖或打印调用者的完整环境。

| 实际运行 | 进程退出 | 完整结果 |
| --- | ---: | --- |
| unchanged accepted-only旧helper，五模式探针 | 1 | 5控制、4失败 |
| unchanged Root旧helper，同一探针 | 1 | 5控制、4失败 |
| 旧helper，增强后的同一永久夹具 | 1 | 7控制、4失败 |
| 候选helper，完全相同永久夹具 | 0 | 7控制、0失败 |
| 完整accepted-only候选，同一永久夹具 | 0 | 7控制、0失败 |

四失败是同一缺陷的控制模式，不是四个独立bug；旧点号模式和退出码控制也保留。
首版和第二版夹具仅把测试自己创建的临时根规范化为resolve()，避免符号链接TMPDIR造成假失败；
七控制无删减，第二版旧实现仍完整4失败。首版文件、结果和第二版反例均保留。

## 使用实际修复helper的数据库回归

accepted-only与Root各四native、三个真实Main均通过。调用者DBMS_DATA_DIR显式指向
本轮自建的带标记目录，不在调用点预先赋点号，由真实修复helper自行覆盖；
每项检查原适用footer、完整调用者目录树未变和私有工作目录已清理。
所有数据库入口串行执行，未与编译器或其他数据库验收重叠。

| 版本 | 永久helper回归 | native | 真实Main | 实际失败 |
| --- | ---: | ---: | ---: | ---: |
| accepted-only候选 | 7/7 | 4/4 | 3/3 | 0 |
| 工作区Root | 7/7 | 4/4 | 3/3 | 0 |

实际七入口：

- `native simple_query_raw_syntax_test`。
- `native composite_namespace_owner_test`。
- `native math_functions_test`。
- `native aggregate_bool_test`。
- `main explain`。
- `main window`。
- `main session`。

复用已验证匹配的当前二进制：每侧97对象/39当前程序、127头和全部生产CPP/native/Main
测试体保持一致，没有生产编译、链接或cache覆盖。三个Main仍是真实main.cpp TU，非test_stubs。
本轮不是fresh TU、normal/repeat build、TLS/sanitizer验收，也不是修复后新跑一次完整72。
修复前[两侧完整72矩阵](issue-review-current-full72-revalidation.md)的C++和二进制未改变，
但该旧源码矩阵与本次helper验收分开记录，不把时间顺序改写。

两侧版本、文档状态、兼容契约、原六项总账单测及完整增强helper回归均退出0；
require-complete仍实际1。登记仍921native+568Python+3Main=1492，Root927+574+3=1504，
未接受的12入口和三份文档继续分离，没有新增注册入口或虚报全1492运行。

## 来源、保留与未完成边界

提交的2206源码投影与完整accepted-only候选一致，只改变common.sh和既有test.py；
工作区2325源文件的其他内容保留。合入前新恢复副本保存2325源码及118生产cache/签名/server文件，
本次118缓存文件完全未变；旧R182恢复副本不是本次恢复点。
暂存从已接受HEAD构造，只包含本项源码、回归和文档，不夹带工作区注册项或其他修复。
本轮跟踪605MD，Root608MD；文档数量不是功能验收数量。

原273仍22complete/166partial/70unverified/15deferred，complete标签没有在本项重新逐条证明。
R164历史超时原因仍未确定，当前通过不倒推原因已修；原用户暂缓不重开、不新增暂缓ID。
继续[要求级计划](review-closeout-plan.md)，本项不关闭P0资源/复制、全测试发布或其他大功能要求。
不push，不启用GitHub Actions。

下列本机临时证据不是可移植发布产物，永久回归可从仓库直接执行
`python3 tests/build_cache_routing_test.py`；不要在真实数据目录手工复现旧行为。

- 旧/候选静态反例：`/tmp/dbms-r184-static-counterproof.t2yhl0hq/completed-static-receipt.json`，SHA256 `bd27cabbd95f8e01dbc88e5d0599a00f31e272bb3fe08a12722a87b4d08bfb49`。
- 两侧实际helper数据库验收：`/tmp/dbms-r184-fixed-helper-runtime.b4wwwhdj/completed-runtime-receipt.json`，SHA256 `da55869c91f984ad5d8a1332d517e038c71f9fd7ec954d7f77353bdcf3534ece`。
- 独立预提交证明：`/tmp/dbms-r184-independent-pre-proof._4vqh9d3/completed-proof.json`，SHA256 `b68e3a146ec83326302be81d8ae51508a5450c25a35b392530656ef43f793da2`。
- 新恢复副本：`/tmp/dbms-r184-root-preinstall-recovery.23jyjfzl/completed-recovery-receipt.json`，SHA256 `e6fbfee9ac8c00580d6c674192cb4f55ab865acce178f5a996ffb107d1f9f87f`。
- 精确合入：`/tmp/dbms-r184-root-preinstall-recovery.23jyjfzl/completed-install-receipt.json`，SHA256 `e534aad18c6ddc87e7dc3750f1e623374dbc19458d79c128bdb4dbd371f20814`。

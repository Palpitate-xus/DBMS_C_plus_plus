# R92：TOAST计数器缺失不能破坏已提交长值

状态：本范围已修复；30个不同完整native通过，协议邻居18/20通过，两个
普通DDL超时保留。新1384入口组合未全量运行，原273总清单未完成。

## 实际失败与修改方法

清洁ce557860上的生产代码与冻结332bcff4相同。先提交一个10KB长值，仅在
自有隔离fixture中把其toastmeta移为保存文件，再尝试插入另一个长值。
`r92-old-missing-counter.log` 实际退出1：INSERT返回IO_ERROR，却重新创建了
计数器，原值查询不再相同。后置输入/binary/全部58 receipt核对实际0。
永久测试的 `r92-old-permanent-native.log` 明确记录before_rows=1、after_rows=0，
同时合法ADD/DROP重写正向测试仍通过；不是把普通编译错误当作产品bug。

旧分配器在计数文件不存在时默认从1重新分配。结合源码调用链，已存在的
TOAST ID发生索引冲突后，prepareToastValues的失败清理又按该ID删除chunks，
使原已提交行的外部值无法读取。查询从1行变成0行不代表基表行已被物理删除。
本项不是R90区间大小/屏障错误：原更早的逐ID分配路径也允许缺失文件初始化。

修复将分配与初始化分开：已有关系缺失计数文件时立即清空线程保留区间、
返回0，不创建计数器、不写入或清理旧chunks。INSERT/UPDATE沿原IO_ERROR
路径返回；健康TOAST数据仍可读取。合法新代际由拥有者显式初始化：原CREATE、
TRUNCATE和ALTER TYPE已有初始化，补齐ADD/DROP COLUMN完整重写的两处调用，
检查空TOAST heap/index的open/flush及原有计数器原子发布结果。

不改24字节TID1/v1/checksum或8字节legacy格式、128-ID预留、flock、文件及目录
fsync、事务/WAL/恢复发布规则。也不承诺整条DDL的crash-atomic swap。
仅恢复计数器不能找回旧版本已经误删的chunks；本项防止新破坏，不是历史数据
自动修复工具。所有故障操作仅发生在隔离测试目录。

## 永久回归及历史对照

- `toast_missing_counter_test.cpp`：已提交长值/空表两组，缺失元数据时长值
  INSERT拒绝、计数器不重建、旧结果不变；UPDATE也拒绝且保留旧值；重复分配
  仍失败；恢复已知健康元数据后可正常写入，不以猜测上界“修复”元数据。
- `alter_column_toast_generation_test.cpp`：5次合法重写与最终5行逐列typed
  结果，覆盖保留长列、多个外部值、真实NULL、空串、文本NULL及__TOAST__1
  形用户文本；删除最后可变列再增加首个可变列仍可写入长值。

第一版永久失败源码保存在r92-first-permanent-missing-counter-test.cpp。
加强UPDATE控制后的相同最终源码另用冻结332bcff4的57份Root库复验：逐个
校验r91-full-cache-preparation.log中的原producer字节和相同公共头/构建脚本，
fresh driver/stubs、隔离执行。r92-historical-strengthened-native.log实际终1：
重写正控0，缺失计数器测试仍1；它是历史对照，不冒充当前Root验收。
旧库另存 `/tmp/dbms-review-332bcff4-libraries.8LgkJdNy/`，没有拷回当前构建。

## 当前构建与完整邻居结果

标准r92-build.log仅实际重编TableManage.cpp、链接并重复构建0；其它57份
生产对象保持匹配，不称全58份重编。当前binary SHA256为
`090e8124da1b4a71a17ce50b87bbbc1182c2316dc7e62a185e72593fedf47640`，
生产cache输入为`9d2d8e2b04c8fc0f91e5e7adabfc2f5c8d9213ef29aa08123574a5707584e7bb`。

r92-core-native.log五个完整入口0：缺失计数器、合法重写、R90持久化/并发/
两次实际SIGKILL、发布成本，以及缺失TOAST heap不自愈。修复后记录recreated=0、
unchanged=1、原值仍1行；UPDATE也unchanged=1。r92-neighbor-native.log另25个
不同完整native均0，覆盖原TOAST/bytea/vacuum/读取错误、共享owner、ADD/TYPE/
数组/TEMP/LOGGED重写、DDL失败回滚及重启、恢复图像、物理restore、
UNLOGGED和五个保存点映像/栈控制；两批全部58 receipt与生产输入核对0。

r92-protocols.log完整20入口实际终1：18通过、2超时，全部生产输入/binary/
58 receipt首尾一致。默认PG、完整COPY、TEMP/数组ALTER、CHECK、LATERAL/MV、
cold backup/restore状态、三序列协议、只读保存点、bound DML、stored-function
atomicity、完整SQRT46、DIV14及statement_timeout均0；不降低原断言/期限。

| 未解决入口 | 实际位置与后续方法 |
| --- | --- |
| copy_error_lock_release_protocol_e2e_test.py | line53准备CREATE TABLE（两个INT列）时socket TimeoutError，尚未进入COPY错误清理控制；生成的表名未打印，不能猜是target还是prior。按原完整驱动观测确切SQL和自有server失败线程，不能报告锁释放断言失败或提高期限 |
| transaction_isolation_protocol_e2e_test.py | line101 `ALTER TABLE transactional_ddl ADD COLUMN rolled_back INT` 等待响应超时；原表也仅id INT。保留完整事务/DDL/回滚断言并定位实际锁/I/O/目录路径，不把超时猜成TOAST错值或虚构同一根因 |

这些固定列语句本身不进入本项的可变列TOAST初始化分支；尚未取得失败线程栈，
不据此排除所有间接关系或宣布超时已解决。先前全量的SQRT准备超时和末尾DROP
失败见[冻结终态](issue-full-registered-332bcff4.md)，本次SQRT0不改写原失败。
本机日志与辅助脚本位于 `/tmp/dbms-review-r46.ifu6SWLp/`，不是发行依赖。

## 提交前独立核对

r92-test-cache-preparation.log仅复制同一当前Root的57份生产库并fresh编译
stubs，不是借用历史库或全57份重编。r92-terminal-fence.log独立核对全部58
receipt、生产/测试cache stamp、上述binary、源码及两个最终测试哈希、57份
测试库与生产库逐字节相同，实际0；随后重复构建up to date、实际0。
516份Markdown/922个本地内联目标、524 Python AST、13 shell语法、877 native
盘点、504唯一注册/486 standalone、文档/契约/版本及六个总账单测均通过。
r92-require-complete.log实际退出1，明确保留原273未完状态。

## 未完成范围

继续原完整普通DDL超时观测、并行布尔聚合表达式和其它未完要求；仅在确证
根因后逐项修复、回归、独立commit。用户暂缓范围不自行恢复；本项不关闭完整
STO-06、WAL或原273，仍22 complete/166 partial/70 unverified/15 deferred。
不push，Actions保持禁用，README保持长期入口。

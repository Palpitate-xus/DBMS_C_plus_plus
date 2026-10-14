# Catalog（系统目录）

系统目录采用项目自己的内存行模型和 `.cat` 持久化格式；字段使用 PostgreSQL 风格名称，不代表完整 schema、事务语义或磁盘格式兼容。

| 组件 | 职责 |
| --- | --- |
| `oid.h/.cpp` | 对象标识分配、范围预留及计数器持久化 |
| `systables.h/.cpp` | 系统表的项目内行结构 |
| `catalog.h/.cpp` | 对象查找、名称/OID索引、依赖及目录加载/发布 |
| `CatalogService.h/.cpp` | 每数据库目录缓存及其生命周期 |
| `type_registry.h/.cpp` | 类型登记和类型元数据查询 |

对象修改需同步真实存储元数据与目录，不得用内存修改成功代替持久化成功。加载和发布涉及缓存及文件版本检查；发生 peer 修改冲突时不能无条件覆盖另一方。

## 已知边界

[原代码审查报告](../../docs/code-review-2026-10-13.md)保留了损坏目录行、内部行指针及 owner 重试的原始失败。当前逐项状态以[修复清单](../../docs/review-fixes-2026-10-13.md)为准，不能把历史问题都当作当前未修复，或把局部修复当作整个目录族完成。CTAS／后台 checkpoint 专项仍按用户要求暂缓。

## Namespace 与目录对象生命周期

原生及 SQL CREATE/DROP SCHEMA 共用实际 engine/database 的 namespace owner，
包含目录身份、依赖和 statement rollback；`createTable` 的低层 storage-only
合约不因此改变。`public` 是可删除、可重新创建的 namespace，不是永远存在的
隐式别名。物理 marker 原语不提供独立的目录/依赖原子性。

`get()` 返回借用引用，不可跨 refresh/eviction/rollback 保留并当作 live model。
需要保留行或引用时使用 `acquire()` 的 generation lease；旧 lease 保持旧对象，
不会自动变成新提交的视图。发布是显式检查的操作，不从 retired generation 的
析构函数回写。干净 peer 更新换代，未发布修改冲突不能被静默覆盖。

事务BEGIN及延迟ownership晋升只用`persistAll(database)`发布目标缓存，不能因
另一个数据库的未发布模型或DDL owner阻塞本数据库；无参数`persistAll()`仍显式
发布全部缓存。此范围选择不会绕过目标数据库自己的generation冲突和I/O失败。

父事务拥有 DDL 物理图像时，已有 reader 使用 committed generation；无可用
generation 的冷 reader fail closed（55P03），不等于 PostgreSQL 完整目录 MVCC。
已验证范围和中间失败见 [R46](../../docs/issue-review-r46-native-schema-ownership.md)。

物理恢复staging尚未替换live目录，不提前淘汰仍有效的generation；实际publication
窗口的cold reader仍可报告55P03。staging消失也不代表后台job已终态，维护客户端
须等待实际状态，见[R77](../../docs/issue-review-r77-restore-catalog-staging.md)和
[R81](../../docs/issue-review-r81-restore-stage-completion.md)。

BIT/VARBIT目录修饰符按位数保存，不使用CHAR/VARCHAR的加4编码。显式最大容量
VARBIT与无界声明不能只用物理宽度区分；CREATE/ALTER使用实际声明，其他目录
同步保留已知声明。协议、数组及冷重载边界见[R50](../../docs/issue-review-r50-bit-type-modifiers.md)。

BYTEA的内部BLOB存储编解码名不代表schema下的新类型；目录必须保存真实内置
OID17。已有错误目录的明确ALTER修复与同名用户类型边界见[R55](../../docs/issue-review-r55-bytea-catalog-identity.md)。

原生BINARY/VARBINARY的raw-byte codec名字也不应阻断BYTEA overload身份匹配，
但不得把原始字节重新解释成BYTEA编码文本；签名及编码计数边界见
[R79](../../docs/issue-review-r79-native-binary-length.md)。

后续重点是完整字段校验、事务化目录与存储发布、值快照或明确的对象生命周期、依赖/回滚和真实错误传播。类型、对象及权限的完整范围见[总清单](../../docs/postgresql-18-gap-audit.md)；兼容声明见[兼容性契约](../../docs/compatibility-contract.md)。

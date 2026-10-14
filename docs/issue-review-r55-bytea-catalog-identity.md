# R55：BYTEA 的存储别名不能成为用户类型身份

## 根因与修复

原完整 legacy_sql_pattern_consumers 中 `b LIKE '__'::bytea` 错误报42883。
真实binder诊断显示左边被认成 `"public"."blob"`，右边才是内置BYTEA。
BYTEA的heap工厂保留旧BLOB编解码名称；DDL目录注册却把该内部名称创建成表
schema下的新类型。因此不是需要绕过BYTEA操作符检查，而是目录生产者OID错误。

与已有REAL/DOUBLE物理别名的处理一致，实际BLOB存储列现在注册为内置BYTEA
OID17。保持原heap编解码名称、TOAST格式和已有数据；不把SQL的BLOB扩展语法
加入PostgreSQL模式，不把已限定namespace的真实用户enum/composite映射成BYTEA。
临时binder基类型诊断已移除，操作符规则本身未被放宽。

历史目录不会被静默全库改写。原生回归重建原错误类型/OID并确认42883，再用
正常 `ALTER TABLE bytes ALTER COLUMN b TYPE BYTEA` 同步后恢复OID17与正常绑定；
不删除旧类型，因为它可能被其他对象引用。实际用户数据迁移前仍应备份并核验。

## 验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 结果 |
| --- | --- |
| 新28完整协议的实际PG18.6 | 全0，r55-reference28.log |
| 主分支未修正的新28控制 | 17失败，r55-root-original28.log |
| 原native目录断言 | 真正atttypid非17，exit134，r55-original-native.log |
| 修正后协议 | 新28、原完整legacy patterns、physical type catalog、缺参数54及三个pattern邻居，共七个完整入口全0 |
| 修正后native | 六个完整入口全0，r55-final-native.log；新增历史目录修复后完整7控制再次0，r55-legacy-repair-native.log |
| 标准构建 | DDL及移除诊断的binder真实O2重编/链接0，r55-first-standard-build.log |

新控制检查空/非空表、按字节匹配UTF8、零字节、NULL、空BYTEA、NOT LIKE、错误
TEXT/ILIKE签名和真实结果OID；原生直接检查目录OID、冷engine/eviction、ALTER、
原错误目录的明确修复及用户schema内同名enum身份。相邻TOAST/编解码测试不改。
全部当前58份生产对象receipts及原生结束输入检查通过；不冒另一轮fresh58。

真实异常栈、基类型诊断及原始失败全部保留。本项不关闭其他窗口、连接恢复、
目录事务MVCC或完整类型功能族，也不代表最终全注册验收通过。

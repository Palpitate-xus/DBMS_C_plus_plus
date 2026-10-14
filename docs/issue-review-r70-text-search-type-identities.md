# R70：全文类型缺少真实目录身份

状态：本项目录/协议身份已修复并通过主工作区定向门禁；不代表全文功能全部完成。

## 复现与原因

R62合法length重载检查发现：TypeRegistry已有tsvector/tsquery，但冷类型解析使用的
CatalogManager::builtinTypeRows及协议OID映射缺少这两个类型和数组。因此
`SELECT NULL::tsvector`报42704，不能因为回调或类型注册表存在就认为SQL类型可用。

实际PostgreSQL18.6确认如下物理身份，所有类型均在pg_catalog、变长、非byval、
alignment为i；本次不推断全文输入语法或二进制codec已经完整兼容。

| 类型 | OID | category | element | array | storage |
| --- | --- | --- | --- | --- | --- |
| tsvector | 3614 | U | 0 | 3643 | x |
| tsquery | 3615 | U | 0 | 3645 | p |
| _tsvector | 3643 | A | 3614 | 0 | x |
| _tsquery | 3645 | A | 3615 | 0 | x |

## 修改方法及范围

在真实builtin目录producer补齐四条记录与相互element/array链接，复用既有冷解析、
bootstrap及持久化路径；协议builtin类型名映射使用相同OID，不伪造返回文本类型。
没有增加执行时按值猜类型或创建不存在的用户类型，也未修改鉴权/权限专项。

新增native覆盖纯定义、独立声明、cold无目录/无cache副作用、warm、磁盘重载、
quoted namespace及数组；新增协议核对NULL真实OID和实际pg_type字段，支持实际
PG18.6对照。R62的length(tsvector)结果另由签名矩阵覆盖，不以本项替代全文功能验收。

本机证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`r70-reference-identities.log`、`r70-reference-matrix.log`为实际参考，10协议检查全0；
`r70-old-protocol.log`为修改前10项全失败，`r70-old-native-rerun.log`为67项中64失败。
首次未通过bash启动不可执行helper的exit126是命令前置错误，不计作产品测试。
这些临时路径不属于发行包。

修复版真实标准O2重编catalog/systables并链接；`r62-second-root-build.log`及
`r62-third-root-build.log`保留生产构建记录。主工作区同时包含尚未提交的R62候选，
这里如实记录实际组合，不把它冒充仅含R70的冻结提交或完整回归。
`r70-final-native.log`五个完整入口全0：新67检查、builtin_range_catalog、
type_registry、json_xml_catalog_type_identity、declared_type_cold_binding。
`r62-final-protocols.log`十一个完整协议驱动全0，包含本项10检查和R62合法length
重载。两组首尾核对当前生产输入、binary和58份对象receipt一致。

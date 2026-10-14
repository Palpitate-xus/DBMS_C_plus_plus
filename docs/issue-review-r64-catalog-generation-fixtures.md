# R64：目录回归必须区分当前代与已退休代

冻结主分支`18d85f4e`的1339完整注册回归中，alter_inherit、
alter_sequence_quoted_options、alter_statistics三个原生入口分别在DROP后的
relhassubclass、OWNED BY NONE后的依赖、SET STATISTICS后的目标断言失败。
原回归输入和失败日志不改写。

三个测试把`CatalogService::get()`的借用引用保留到替换目录代的DDL事务之后。
R46已明确该引用不是跨代owner；原对象可能已退休甚至释放，不代表当前目录。
只把每次目录检查改为重新取得当前模型的三个完整独立诊断，在未改产品的
主分支上全0。原值断言全部保留，没有忽略DROP、依赖或statistics检查。

最终夹具另持有`acquire()`的旧代lease，明确断言旧代仍保留DDL前的真实内容，
同时原断言检查新代实际改变，防止用改写/释放旧模型伪造当前状态。原有cold
目录重载、OID身份、列来源、失败DDL和真实storage检查不删减。产品无修改。

## 证据

- 原三失败仍在`/tmp/dbms-review-full-20261014-r56.O0AC4HsS/full-regression.log`。
- 未改产品主分支三个完整当前代诊断全0，
  `/tmp/dbms-review-orphan-controls.vQleNfjM/generation-probes.log`。
- 含旧代lease阳性控制的三个完整候选入口全0，
  `/tmp/dbms-review-r46.ifu6SWLp/r64-strengthened-native.log`；全部当前58份
  生产对象receipts与结束构建输入一致。
- 加强后的原三个完整入口又在冻结主分支的真实58份生产对象上独立重编、
  链接、隔离执行全0，`/tmp/dbms-review-r46.ifu6SWLp/r64-root-strengthened.log`。
  使用外部测试源而不修改冻结主分支的原测试，未改产品。

本项是夹具生命周期修正，不宣称修复了目录功能或整个完整回归。
主分支注册回归仍保持原输入，终态前不合入；未push，用户暂缓专项不重开。

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

## 后续：另三个完整目录入口

同一冻结完整回归继续在ddl_ast_bridge、enum_quoted_type_identity、enum_test
复现相同借用引用跨DDL后的过期读取。未改产品主分支的当前代完整诊断全0：
`/tmp/dbms-review-orphan-controls.vQleNfjM/enum-generation-followup-probes.log`及
`ddl-exact-generation-probe.log`。DDL、物理文件及原异常断言均未放宽。

六个夹具统一使用测试专用`CatalogGenerationView`：每次断言重新取得当前代，
同时持有所有已借用行的generation lease，防止旧namespace/row指针悬空。
额外断言旧代仍保留重命名前关系、CASCADE前enum及创建前schema，而当前代
确实发生改变；原cold reload、依赖、数据和故障出版控制不删减。helper仅在
tests目录，不改生产头或CatalogService契约。

加强后的全部六个原完整入口在冻结主分支真实58份生产对象上全0，证据
`/tmp/dbms-review-r46.ifu6SWLp/r64-root-six-current.log`；私树当前生产组合的
同六入口也全0，`r64-final-six.log`，各自核对58份receipts及结束构建输入。
这六个测试修正不是当前完整注册回归的终态，不会覆盖原六个失败。

构造首个外部DDL诊断时，机械替换曾误改字符串`pg_class.cat.tmp.`为
`pg_class.cat().tmp.`，使最后一个原有故障注入没有命中，日志
`generation-followup-probes.log`保留。逐一对照原477/63/262个C++字符串字面量后
恢复唯一误改，完整DDL重新通过；该失败不能归因于产品故障或临时文件API改变。
最终夹具保留原故障路径，不增加另一种伪造的成功/失败来源。

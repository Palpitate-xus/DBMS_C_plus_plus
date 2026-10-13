# R43：函数属性与 LANGUAGE/AS 的合法交错顺序

R24 的真实 volatile SQL 函数参数控制发现合法语句
`RETURNS BIGINT LANGUAGE SQL VOLATILE AS $$...$$` 被报 42601。
原 parser 先处理属性，再单独处理两轮 LANGUAGE/AS，导致中间及末尾属性
无法解析。不能通过改写回归 SQL 避开这个独立问题。

修复将 LANGUAGE、AS 和原属性放入同一 option loop，保留函数体为单个已
tokenize 的字面量；不搜索函数体内关键字。重复 LANGUAGE/AS 与缺失值拒绝，
未消费的尾部仍 fail closed。原有属性处理不变，没有公共头变化。

真实证据目录 `/tmp/dbms-review-root-conflict.Kdz38nRU/`：

- 原生52控制：四组 VOLATILE/STABLE/IMMUTABLE/STRICT 的全部六种排序，
  legacy/binding 两入口，加 body 关键字与 CALLED ON NULL INPUT。原实现
  36失败、exit 1（`r43-original-final-native.log`）；候选全通过、exit 0。
- 首次测试源码用了不存在的 parser API，编译 exit 1；保留
  `r43-original-native.log`，不当作产品失败。
- 标准真实重编 parser.cpp 并链接 exit 0（`r43-candidate-production-build.log`）；
  新驱动与 INSERT SELECT clause、stored scalar resolver、stored metadata
  namespace 三完整邻居全部 exit 0（`r43-candidate-native.log`）。
- R24 最终59协议控制在 PostgreSQL 18.6 全过；R24-only 候选仍有此函数
  CREATE 及后续两个控制失败。加入本修复后59全过
  （`r24-r43-final59-protocol.log`），包括函数产生的 nextval 仅执行一次。

52项是实际 AST 原生测试，不冒充52项独立 PG 比较。Root 集成需按当前主分支
头文件真实重新编译，不能使用私树旧头时代的生产对象。

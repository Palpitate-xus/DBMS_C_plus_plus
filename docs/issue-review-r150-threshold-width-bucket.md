# R150：双参数 WIDTH_BUCKET 阈值重载

状态：开放，未实现、未验收。不能用四参数 R149 的通过关闭本项。

## 实际观察与覆盖范围

在 PostgreSQL 18.6（实际版本号 180006）与 R150 记录轮的 Root 上串行运行 61 条观察：
59 条普通类型/错误场景，加 2 条在独立 schema 中创建的真实枚举场景。
两端均完成收集并清理所属 schema；这是观察日志，不是 61 项通过的测试报告。

例如：

~~~sql
SELECT width_bucket(3, ARRAY[1,2,4]);         -- PG: 2；当前: 42883
SELECT width_bucket(2, ARRAY[1,2,2,4]);       -- PG: 3；当前: 42883
SELECT width_bucket(3, ARRAY[]::integer[]);  -- PG: 0；当前: 42883
SELECT width_bucket(2, '[-10:-7]={1,2,2,4}'::integer[]);
-- PG: 3，返回阈值个数，不是带下界的数组下标；当前: 42883
~~~

[数学函数文档](https://www.postgresql.org/docs/18/functions-math.html) 定义了
anycompatible/anycompatiblearray 阈值重载。对照参考源 arrayfuncs.c 的
width_bucket_array 及三个搜索实现，重复阈值必须落在最右侧相等元素之后。

| 观察组 | 覆盖的内容 | 核实结果/后续约束 |
| --- | --- | --- |
| 1–6 | 普通、重复阈值、区间外、空数组、非 1 下界 | 有效结果为 0..N，数组下界不改变桶号 |
| 7–9 | 多维数组、任意位置的 NULL 阈值 | 分别为 2202E、22004，不能仅检查被二分搜索访问的元素 |
| 10–14 | NULL operand、NULL thresholds、UNKNOWN NULL | 严格 NULL 行为；NULL operand 不能误触发多维/NULL 阈值错误 |
| 15–18 | UNKNOWN 字符串、错误数组输入、TEXT 非数组、类型不兼容 | 有效 UNKNOWN 需要共同元素类型；错误输入和无签名要区分 |
| 19–25 | 混合 numeric/integer/bigint、超过 2^53、1e-400、NaN/Infinity | 不能统一转为 double；浮点 NaN 具有数据库排序语义 |
| 26–30 | 数字形文本、C collation、bpchar、name、boolean | 文本不能按数字比较；保留 padding、collation 和布尔排序 |
| 31–38 | date、微秒 timestamp、带偏移 timestamptz、time/timetz、interval、money | 必须使用实际类型的排序/共同类型规则，不用显示文本推断 |
| 39–48 | uuid、bytea、inet/cidr、macaddr、bit/varbit、oid/regtype、pg_lsn | 使用物理身份/值比较，不能普遍按字符串排序 |
| 49–54 | JSONB 标量/对象、int4range/numrange、tsvector/tsquery | 需要真实比较支持；不可把这个重载缩成仅 numeric/text |
| 55–58 | json、NULL json、point、box | 缺少比较函数的非 NULL 调用要 42883；严格 NULL 仍为 NULL |
| 59 | XML | 参考构建缺少 XML 支持，实际 0A000 是环境能力，不是通用 XML 排序契约 |
| 60–61 | 标签为 10、2、1 的真实命名枚举 | PG 为 2、3；原 Root 在调用前就因枚举数组目录缺失报 42704 |

当前[重载声明及回调](../src/expression/ExprEvaluator.cpp)只有四参数数值实现。
[枚举数组目录前置问题](issue-review-r151-enum-array-catalog.md)已另列；
其候选尚未验收，且已按用户要求暂缓；不能据此声称阈值重载可用。
本节 61 条结果为该轮历史观察，不能当作后续显示修复后的新结果。

## 修改计划与验收方法

1. R151 依赖当前由用户暂缓，普通继续不恢复。只有用户明确恢复后，才继续其真实
   枚举数组目录、稳定 OID、自动命名、持久化、依赖和生命周期及完整 341/103 门禁。
   R152 显示修复不替代 R151 验收，也不构成阈值重载实现。
2. 增加 anycompatible/anycompatiblearray 的声明级共同类型绑定。
   四参数重载、原生同名替换及真实 stored routine 的签名归属必须保留。
3. 接入实际排序支持，覆盖上述全部可比较类型。不要用一份 numeric 转换或字符串排序替代；
   缺少比较函数的类型、空数组与严格 NULL 的检查阶段也要按参考验证。
4. 校验维数及全部 NULL 阈值后做 upper-bound 二分搜索，正确处理重复项、非默认下界和 0..N。
   不替调用方排序阈值，也不声称未排序输入有额外保证。
5. 将整个观察范围转成永久原生/协议断言，补充参数、零行、真实表、错误恢复、
   OID/长度/typmod/原始 int4 binary datum；不删去非数字场景以取得通过。
6. 运行相邻完整入口及独立 accepted-base clean。只有全部规定控制通过、输入边界核对通过后
   才将实现逐项提交；共享头或 Main 改动仍要求相应的 fresh58/fresh Main 验证。

原始观察脚本和日志在本地 /tmp/dbms-review-r46.ifu6SWLp/r150-*。
本次没有新增本重载的生产实现或正式注册入口，也未将其标为已修复。
原 273 总账状态不变；不 push、不启用 Actions，用户已跳过的专项不重开。

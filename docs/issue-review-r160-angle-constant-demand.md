# R160：角度函数的常量规划错误时机（开放）

状态：已实际复现、未修复；与 R158 的回调数值修复分别验收。

~~~sql
CREATE TEMP TABLE r158_empty(x double precision);
SELECT degrees('1e308'::double precision) WHERE false;
SELECT radians('5e-324'::double precision) LIMIT 0;
~~~

PG 18.6 对上述纯常量先规划并报 22003，即使没有结果行。
旧 Root 和已接受 R157 返回零行、没有错误。
同样验证无 FROM 的 WHERE false / LIMIT 0、空表无过滤 / WHERE false / LIMIT 0：
两函数共十条观察，全部保留，不能称为十项通过。
这些不是带错误数据的变量列；变量没有被读取时不应执行回调。

原因方向：[PreparedQueryExecution](../src/expression/prepared_query_execution.cpp)
按真实调用归属决定可规划 builtin；当前角度调用没有进入此常量规划路径。
未来修改应使用已解析的实际 builtin 身份和不可变性，不按函数显示名触发回调，
保留 opaque 原生替换、真实 stored routine、参数阶段及 lazy CASE/COALESCE/子查询边界。
不能为取类型执行 SQL，也不能从一个空表样例推广到所有表达式的提前求值。
共享头未必需要修改；若涉及 AST/共享头，须另行完整构建及真实前端验证。

验收须保留十条已观察输入，并加入 Simple/Extended 的 Parse/Bind/Execute 具体阶段、
零行/NULL/变量/参数、嵌套与不可达分支、回调计数、事务恢复及相邻常量规划夹具。
原始完整 20 条 collector 及三端日志在 /tmp/dbms-review-r46.ifu6SWLp/r158-*。
它没有因本项另立而删除输入；后续仍需重新实测，不从修改代码推断通过。

R158范围修复（工作区报告`issue-review-r158-angle-conversion-range.md`）不关闭本项，
用户暂缓的 [R156 完整类型化常量](issue-review-r156-regtype-physical-constants.md)也不恢复。
原 273 项状态不改；不 push，Actions 禁用。

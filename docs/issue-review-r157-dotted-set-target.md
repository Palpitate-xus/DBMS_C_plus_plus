# R157：普通 SET 的 dotted 目标 AST 身份（开放）

状态：已复现、未修复。它是 R152 相邻 clean 门禁中的既存失败；
不恢复用户暂缓的 R140 TimeZone 值语法专项。

## 问题与证据

~~~sql
SET search_path.extra TO 'one';
SHOW search_path.extra;
SHOW search_path;
~~~

PG 18.6 设置/显示独立变量 search_path.extra 为 one，真正的 search_path 保持原值。
普通 SET AST 不能只留下首个 search_path token，否则 AST 消费者可能选择内建分支。

已接受 parser 的一般分支只执行一次 stmt->name = tokens[pos++]，
后面的点和名称会进入 values。
工作区旧候选多了一个完整名称拼接循环，所以 Root 通过不证明已提交版本正确。

| 输入 | 结果 |
| --- | --- |
| R152 Root 的完整搜索路径语法入口 | 通过 |
| R152 accepted-only clean，同一未改夹具 | 第 86 行名称断言退出 134，未完成到 footer |
| 冻结 R155 已接受版本，匹配旧头/flags/57 库 | 同一夹具、同一断言、同一 134 |
| R152 clean 完整相邻批次 | 29 通过、1 失败；完整 30 入口终态 1，未重试覆盖 |

此失败不是 R152 显示改动引入；也不因为它是旧缺陷而把 30 入口改称通过。
要求全绿的严格检查器仍退出 1，原断言与失败日志完整保留。

## 修改方法与验收

[parser.cpp](../src/parser/parser.cpp)应提取完整普通 var_name 点号链，
再按完整身份做内建分派，同时正确移动 values 起点，保留 scope 和引用边界。
不能通过比较首个 token、给相似名称别名或删除断言绕过。
补充 quoted/多级名称、内建近似前缀、缺失分量/分隔符及普通 parse/parseForBinding；
验证未知目标不改变真实搜索路径。不要借此接受 TimeZone 专项主体或其它暂缓代码。

[原夹具](../tests/search_path_values_grammar_test.cpp)必须完整保留。
代码修复应独立版本管理、独立 accepted-only clean 复现，并重跑相邻入口；
CPP-only 也要核对当前头/flags/生产对象来源和真实前端链接，不能用 Root 暂缓候选代替。

## PG 观察不能误读

本机 r157-reference-observations.log 的六条观察确认：
多参数 SET search_path_extra / search_path.extra 都报 22023（只接受一个参数），
不是语法 42601；原真正 search_path 不变。
r157-reference-single-observations.log 的另六条确认：
单参数无点未知名报 42704，单参数 dotted 变量成功、SHOW 返回 one，真正路径仍不变。
语法 AST 接受 var_list 与运行时 GUC 参数个数检查是不同阶段。

本项尚未提交生产修复，也未宣称项目已支持全部自定义 GUC。
问题、方法和本机 /tmp/dbms-review-r46.ifu6SWLp/r157-* 证据保留，
总清单未清零，不 push、不启用 Actions。

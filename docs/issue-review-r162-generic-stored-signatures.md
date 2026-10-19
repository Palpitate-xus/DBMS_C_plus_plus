# R162：普通命名存储函数的类型签名（有限修复）

状态：通用存储标量签名的有限修复已通过两端组合验收。以下为[R62](issue-review-r62-scalar-signatures.md)原先保留的
任意名字存储函数通用签名缺口，不能由少数 builtin 同名重载通过代替。

## 复现与错误

R161完整协议夹具实际创建 mix(t TEXT,n INTEGER) 严格 SQL 函数与真实表，
将 INTEGER列、TEXT列反序传入：PG18.6报42883；旧版及R161候选接受，
把两列的原始 bytes重新标成形式参数类型，错误返回例如2:alpha。
Simple/Extended text/binary × reached/WHEREfalse/LIMIT0 共九个执行控制均失败。
这是类型匹配阶段的错误，不应因为没有扫描到行而被允许。

下面是同一错误方向的独立最小复现写法；本节实测证据来自上述完整表/列控制，
不是声称已另跑此字面量脚本：

~~~sql
CREATE FUNCTION public.r162_mix(t TEXT,n INTEGER) RETURNS TEXT
LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT t || ':' || n::text$$;
SELECT public.r162_mix(1,'2'::text);
-- 正确：42883；INTEGER->TEXT 和 typed TEXT->INTEGER 不是这里允许的隐式转换。
~~~

## 原因与修改方法

[resolveScalarFunction](../src/expression/ExprEvaluator.cpp)只对 scalarSignatures
表列出的名字调用声明级候选选择。普通存储函数 fallback 仅核对 arity；
除 pg_typeof/部分多态 SRF 特例外，matches一直为true，返回的inputs为空。
bindScalarFunctions因此不插入已批准的隐式转换，BoundScalarRoutine丢掉来源类型，
[callUDF适配](../src/commands/TableManage.cpp)再以声明参数类型解释原始字符串。
ColumnRef的真实类型并未丢失，错误在选择和调用之间。

修复应复用通用声明候选的合法隐式转换、UNKNOWN验证、搜索路径及最佳候选规则，
给实际选中的存储函数保存形式输入并执行批准的转换；不能只拒绝所有非精确输入。
后置namespace中的精确匹配仍可胜过前置namespace中需要转换的候选；
相同签名仍由搜索路径先出现者遮蔽。不能为选择类型执行函数体。
依据：[PG函数类型解析](https://www.postgresql.org/docs/18/typeconv-func.html)。

原生未绑定列的元数据查询不能猜成TEXT；需在推断前识别hasUnboundScalarInput，
保持纯发现行为，并在实际typed RowContext可用时、丢弃类型前验证和转换。
直接接受raw strings的callUDF历史API不具备来源类型，不应在该API猜来源或改共享头绕过。
Opaque原生回调替换的参数/返回提示及调用归属必须保留。

## 必须补齐的验证

精确/合法隐式/非法类型、typedNULL/UNKNOWN NULL/错误UNKNOWN输入、显式CAST、
qualified/quoted/unqualified名字与路径竞争、SQL及PLpgSQL函数、真实列、
参数重执行、零行和不可达分支、纯metadata及effects计数、原生未绑定行/重绑定。
只使用现有内置数组身份；不恢复用户暂缓的自动枚举数组或完整类型化常量。
协议Parse/Describe的另九个错误由[R163](issue-review-r163-qualified-parse-validation.md)独立处理。

完整证据 /tmp/dbms-review-r46.ifu6SWLp/r161-v4-*-full637-protocol.log
与r161-v5-{root,clean}-protocol.log；R161 V5两端637各18失败：九个本项执行控制，
九个R163元数据控制。PG同一637项全0。没有删除任何原有输入。
以上为首次登记及修复前证据，不是最终候选的运行结论；总清单仍未完成，不push，Actions禁用。

## 最终候选的完整运行检查点

R169 V4工作区完整58入口实际全部通过；本项native343、协议1705及共享637均0失败。
已按上述声明规则选择并保存真实输入类型，批准的隐式转换在typed owner内执行；
UNKNOWN、NULL、未绑定原生列及opaque callback归属控制保留，不全局修改asDouble或共享头。
R164消除了原生32及328–333缺失声明的原因。原295控制仍在，按既有C/c规范化契约比较，
没有以删除输入或改函数匹配预期掩盖生产错误；此项不宣称修复collation生命周期。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合构建、依赖及原始日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)。

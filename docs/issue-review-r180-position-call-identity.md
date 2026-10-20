# R180：POSITION语法、实际callee与原生实参顺序

状态：Open，已观察；没有生产修改、注册测试或通过声明。与STRPOS签名R179分项处理。

## 最新：99722bd2上的完整24次双侧观察

同一冻结采集器在PG18.6及accepted-only R179版本各完整执行24次，均到ReadyI。
两侧采集进程外层0仅说明采集完成，没有兼容性验收oracle；8处诊断签名匹配、16处差异，
比较的是值/SQLSTATE/OID，不比较消息文本。不是16个独立缺陷，也不是8项候选通过。
R180目前仍没有生产修改或注册回归；R179的声明修复不解决这里的语法准入问题。

| # | 实际SQL | PG | 当前accepted产品 | 分类 |
| --- | --- | --- | --- | --- |
| 1 | `SELECT position('b' IN 'abc') AS v;` | `[["2"]]` | `[["2"]]` | 匹配控制 |
| 2 | `SELECT pg_catalog.position('abc'::text,'b'::text) AS v;` | `[["2"]]` | `[["0"]]` | 差异 |
| 3 | `SELECT "position"('abc'::text,'b'::text) AS v;` | `[["2"]]` | `[["0"]]` | 差异 |
| 4 | `SELECT pg_catalog.position('\x006100'::bytea,'\x61'::bytea) AS v;` | `[["2"]]` | `[["0"]]` | 差异 |
| 5 | `SELECT position('\x61'::bytea IN '\x006100'::bytea) AS v;` | `[["2"]]` | `[["2"]]` | 匹配控制 |
| 6 | `SELECT pg_catalog.position(B'1001',B'01') AS v;` | `[["3"]]` | `[["0"]]` | 差异 |
| 7 | `SELECT position(B'01' IN B'1001') AS v;` | `[["3"]]` | `[["3"]]` | 匹配控制 |
| 8 | `SELECT pg_catalog.position(B'1001'::bit varying,B'01'::bit varying) AS v;` | `[["3"]]` | `[["0"]]` | 差异 |
| 9 | `SELECT position(B'01'::bit varying IN B'1001'::bit varying) AS v;` | `[["3"]]` | `[["3"]]` | 匹配控制 |
| 10 | `SELECT position(1 IN 123) AS v;` | `ERROR 42883` | `[["1"]]` | 差异 |
| 11 | `SELECT "position"(1,2) AS v;` | `ERROR 42883` | `[["0"]]` | 差异 |
| 12 | `SELECT "position"('abc') AS v;` | `ERROR 42883` | `[[null]]` | 差异 |
| 13 | `SELECT "position"('abc','b','x') AS v;` | `ERROR 42883` | `[["0"]]` | 差异 |
| 14 | `SELECT position('abc','b') AS v;` | `ERROR 42601` | `[["0"]]` | 差异 |
| 15 | `SELECT position() AS v;` | `ERROR 42601` | `[[null]]` | 差异 |
| 16 | `SELECT position('abc') AS v;` | `ERROR 42601` | `[[null]]` | 差异 |
| 17 | `SELECT strpos('b' IN 'abc') AS v;` | `ERROR 42601` | `[["0"]]` | 差异 |
| 18 | `SELECT strpos('abc' FROM 'b') AS v;` | `ERROR 42601` | `[["2"]]` | 差异 |
| 19 | `SELECT position('b' FROM 'abc') AS v;` | `ERROR 42601` | `[["2"]]` | 差异 |
| 20 | `SELECT position('b' IN reverse('cba')) AS v;` | `[["2"]]` | `[["2"]]` | 匹配控制 |
| 21 | `SELECT position(position('b' IN 'abc')::text IN '123') AS v;` | `[["2"]]` | `[[null]]` | 差异 |
| 22 | `SELECT strpos(('b' IN ('abc','b'))::text,'true') AS v;` | `[["1"]]` | `[["1"]]` | 匹配控制 |
| 23 | `SELECT position(('b' IN ('abc','b'))::text IN 'true-false') AS v;` | `[["1"]]` | `[["1"]]` | 匹配控制 |
| 24 | `SELECT "POSITION"('abc','b') AS v;` | `ERROR 42883` | `ERROR 42883` | 匹配控制 |

除原有换参/声明问题，还确认：裸POSITION的逗号/空/单参数形式应42601却被接受；
非法STRPOS IN/FROM及POSITION FROM扩展应42601却成功；嵌套POSITION应2却返回NULL。
合法BIT/VARBIT/BYTEA IN形式、普通嵌套reverse、括号内真正IN表达式及quoted大写不存在的控制必须保留。

已定位[parser.cpp](../src/parser/parser.cpp)的全局tokenizer把POSITION/STRPOS顶层IN改成逗号，
丢掉原始语法角色；generic function parser还错误接受这些函数的FROM分隔符。
直接重排字符串或统一交换callback会破坏原生API和真正的IN表达式，不作为修复方案。

### 后续实现门禁

1. 仅在实际POSITION关键字语法的AST解析中处理其顶层IN边界，规范化为真实routine的haystack/needle顺序；
   直接quoted/qualified routine保留普通调用语法，非法扩展42601，真正嵌套IN表达式不重写。
2. 用明确的AST语义保留SQL调用与原生API参数约定，并维护真实callee的TEXT/BYTEA/BIT声明及namespace身份；
   不从显示文本或callee名字猜当前callback是否builtin，opaque替换不继承签名/换参规则。
3. 若增AST字段，两个实际手工clone路径及toString也必须维护语义：expr_helper.cpp、
   prepared_query_execution.cpp和FunctionCallExpr::toString。不能只改解析器。
4. 公共AST变化需两端genuine fresh58、fresh匹配native和三个真实Main；不能复用旧ABI对象。
   完整原生/三协议回归应包含全部24观察、错误优先级、NULL/空/typed参数/重绑定、namespace/同名覆盖及副作用。
5. 先PG完整对照及旧版反证，再候选/Root及原16相邻入口；通过后才按R180独立commit，不与R179混为已完成。

固定采集器`/tmp/dbms-review-r46.ifu6SWLp/r180-position-call-discovery-v1.py`。
参考日志`r180-position-call-reference24.log`：
`c3ea80dc0e2547efdf175b386261b8ca44d6fdedc360bc35abb3aafa4719e875`；
产品日志`r180-position-call-accepted24.log`：
`a8574e1e1f5e487ce41d7d847bbd59b61f1f20571e34d5cb41b8cd1e52d95a51`。
本地证据非可移植release依赖；目前未生成R180生产候选、对象或通过声明。

## 历史基础观察：1430b578

| 调用形式 | PG18.6 | 当前accepted R178产品 | 事实/修改方法 |
| --- | --- | --- | --- |
| `position('b'::text IN 'abc'::text)` | 2 | 2 | SQL关键字语法先写needle，后写haystack；保留正确控制 |
| `pg_catalog.position('abc'::text,'b'::text)` | 2 | 0 | 真实routine按haystack、needle顺序，不能沿用关键字表面顺序 |
| `"position"('abc'::text,'b'::text)` | 2 | 0 | quoted普通callee同样遵循真实声明，不是IN关键字语法 |
| `position(B'01' IN B'1001')` | 3 | 3 | BIT关键字形式当前正确；不是只支持TEXT的函数 |
| `pg_catalog.position(B'1001',B'01')` | 3 | 0 | BIT真实routine也需要正确的参数角色 |
| `position(1 IN 123)` | 42883 | 1 | 声明级签名缺失使整数被错误接受 |
| `position('\x61'::bytea IN '\x006100'::bytea)` | 2 | 2 | BYTEA的合法搜索必须保留，不能因STRPOS拒绝BYTEA而删除本重载 |

PG目录实查四个相关声明：STRPOS(TEXT,TEXT)，POSITION(TEXT,TEXT)、POSITION(BYTEA,BYTEA)、
POSITION(BIT,BIT)，结果均INTEGER。该反射语句在当前产品实际42P01；不将它称为本项通过或已实现目录查询。
已有原生`SelectExpr`及programmatic FunctionCallExpr的position测试使用needle/haystack顺序，
是独立的既有API约定。不能简单全局交换callback实参，使SQL修复破坏该API。

## 修改与完整验证计划

1. 在实际AST/绑定所有者中区分IN语法、SQL普通callee和已解码原生API；不以文本字符串包含IN猜测角色。
2. 依据真实声明规范化SQL实参角色，维护quoted/schema及stored/native callback身份，元数据不得执行输入。
3. 只给真实builtin绑定TEXT/TEXT、BYTEA/BYTEA、BIT/BIT及真实隐式转换；typed NULL、错误arity、
   错误类型/缺失列在空/false/LIMIT0仍需分析。原生opaque替换不继承builtin声明或换参规则。
4. 扩展为原生/三协议严格回归：多字节字符、零字节BYTEA、高bit、空needle、无匹配、NULL、
   UNKNOWN参数、重复绑定、同名重载及真实副作用；保留原生API和现有正控制。
5. PG完整对照、冻结旧版反证、候选/Root及相邻完整入口验证后才按本项独立commit。

以上为修改方法，不是实现或通过声明。R177及其他用户暂缓不恢复，R164和总清单继续未完成；不push，Actions禁用。

原始观察在`/tmp/dbms-review-r46.ifu6SWLp/r179-string-search-reference-discovery.log`、
`r179-string-search-old-discovery.log`及`r179-reference-search-declarations.log`、
`r179-product-search-declarations.log`；共同基线`1430b578`。

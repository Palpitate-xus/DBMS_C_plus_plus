# R180：POSITION语法、实际callee与原生实参顺序

状态：Open，已观察；没有生产修改、注册测试或通过声明。与STRPOS签名R179分项处理。

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

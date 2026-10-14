# R79：LENGTH签名丢失原生二进制codec身份

状态：本范围已修复并完成主工作区门禁。

冻结b5def9ba全注册回归中，原length_projection_semantics_test因42883异常退出。
保持原测试并逐项观察，九个原控制只有实际VARBINARY工厂列的length失败；
其他八项Unicode/text/NULL结果正确。诊断日志为
`/tmp/dbms-review-r46.ifu6SWLp/r79-length-discovery.log`；诊断工具虽退出0，
仍明确记录这个失败，不能将该退出码当作原测试通过。

R62声明签名只接受真实BYTEA输入，native BINARY/VARBINARY工厂仍保留旧raw-byte
codec名字，因而被拒绝。不能把这些原始字节直接标成BYTEA文本再解析：例如
原始六字节反斜杠x3132的长度为6，而BYTEA编码文本的同一字符串表示两个字节。

修改方法：只在签名身份比较中把两个明确的native存储别名映射为BYTEA，统一
声明解析、同名routine候选、绑定和runtime coercion的身份；不重写存储codec、
数据或真实限定用户类型，不增加PostgreSQL模式下的扩展DDL类型语法。执行时
保留raw-byte类型和值，编码计数重用既有转换器时也直接消费原始字节。

新增native_binary_length_signature_test共30控制：UTF8、hex-looking原始值、
嵌入NUL、空字节、非UTF8字节、显式UTF8字符计数、两处typed NULL、BYTEA/BLOB
真正编码值、TEXT/VARCHAR字符计数、upper/abs及错误arity严格拒绝，以及真实
VARBINARY表工厂/heap/native投影。修复前完整30控制有19失败，
`r79-old-native.log`，没有删减或在首个错误后结束。原九个控制保持不改。

ExprEvaluator实际标准O2重编、链接及repeat全0，`r79-standard-build.log`；
binary SHA256为eba80763b7b0168a72ca2dc5e6a98236dddb3fb58086d30673c2492c094abe2c。
新30控制19失败→0，原完整九控制及R80、100签名、42 ABS等十三完整native全0，
`r79-first-native.log`与`r79-neighbor-native.log`。后者包括BYTEA、真实目录身份、
TOAST、Unicode/pattern、encoding及ARRAY length，均使用当前58份生产receipt。
十个完整协议全0，`r79-verified-protocols.log`，包括原窗口44、132签名、67 ABS、
BYTEA目录、legacy patterns和排序；前后输入/binary/58份receipt一致。

实际PG18.6六组只读BYTEA/TEXT对照全0，`r79-reference.log`；没有把项目native
BINARY/VARBINARY类型语法说成PostgreSQL支持。原始字节长度、UTF8字符计数、
NUL/非UTF8及NULL的对照同时核对结果OID23。

额外串行批次中完整PG、真实取消协议、CLI、COPY、DIV14五个入口均0；随后
restore-stage夹具在staging消失、发布尚未结束时收到55P03，该批未到最后fence，
不能写成七项全部通过。此独立夹具竞态由R81继续处理，保留
`r79-final-serial-protocols.log`和独立driver日志。最初邻居协议批因调用者拼错
physical目录入口名在预检exit2，未执行测试，保留`r79-neighbor-protocols.log`。
上述证据均在`/tmp/dbms-review-r46.ifu6SWLp/`，不改写原冻结全量的两个FAILED。

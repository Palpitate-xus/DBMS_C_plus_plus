# Catalog（系统目录）

## 当前状态（Phase 2）

`src/catalog/` 目录已建立完整的 Catalog 框架：

### 核心组件

| 文件 | 职责 |
|------|------|
| `oid.h/cpp` | OID 分配器：单调递增、持久化到 `.oid_counter`、支持批量预留 |
| `systables.h/cpp` | 系统表行格式：pg_namespace、pg_class、pg_attribute、pg_type、pg_proc、pg_depend |
| `catalog.h/cpp` | CatalogManager：内存缓存、OID/名称索引、CRUD、CSV 持久化、依赖追踪 |
| `CatalogService.h/cpp` | 每数据库 catalog 缓存、当前格式加载与持久化 |

### 已实现功能

- ✅ **系统表定义**：6 张核心系统表的行结构（PG 兼容字段名）
- ✅ **OID 分配**：从 10000 开始单调递增，每 100 次分配自动持久化
- ✅ **内存索引**：按 OID 和名称的哈希索引，支持 O(1) 查询
- ✅ **CSV 持久化**：每表一个 `.cat` 文件，启动时加载、析构时保存
- ✅ **Bootstrap**：标准 namespace（pg_catalog=11, public=2200）+ 28 种系统类型（固定 OID）
- ✅ **依赖追踪**：
  - `planDrop()` 生成 CASCADE/RESTRICT 删除计划
  - 拓扑排序 + 循环检测 + pin 保护
- ✅ **当前格式边界**：catalog 只加载当前版本 `.cat` 文件；旧数据目录必须导出 SQL 后重建，不执行自动迁移

存储层同样采用 fail-closed 的当前格式边界：heap 文件头和 8 KiB 页面必须通过版本、布局和校验和验证；
损坏或截断文件不会继续向上层暴露，旧页格式不做兼容迁移。

### 使用方式

```cpp
#include "catalog/catalog.h"

dbms::CatalogManager cat("data/base/mydb");
cat.bootstrapSystemNamespaces();
cat.bootstrapSystemTypes();

// 创建 schema
Oid nspOid = cat.createNamespace("myschema", /*owner=*/10);

// 创建表（pg_class）
PgClassRow cls;
cls.relname = "mytable";
cls.relnamespace = nspOid;
cls.relkind = 'r';
cls.relnatts = 3;
Oid classOid = cat.createClass(cls);

// 添加列（pg_attribute）
PgAttributeRow attr;
attr.attrelid = classOid;
attr.attnum = 1;
attr.attname = "id";
attr.atttypid = 23; // int4
cat.addAttribute(attr);

// 检查依赖
auto plan = cat.planDrop(PgClassOid_Class, classOid,
                         CatalogManager::DropBehavior::Restrict);
if (!plan.ok()) std::cerr << plan.error;
```

### 待完成

- 临时 schema 与会话隔离（2.6）
- `pg_authid` / `pg_auth_members` catalog 结构与 CRUD 已存在（2.7）；网络与 DDL 账号路径已统一接入，仍需补全 ACL 与角色继承语义
- `COMMENT ON` 对象类型全集（2.8）
- 从内存 vector 迁移到真正的 HeapTuple/页面格式（Phase 3/4）

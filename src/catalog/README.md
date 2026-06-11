# Catalog（系统目录）

## 当前状态（Phase 2）

`src/catalog/` 目录已建立完整的 Catalog 框架：

### 核心组件

| 文件 | 职责 |
|------|------|
| `oid.h/cpp` | OID 分配器：单调递增、持久化到 `.oid_counter`、支持批量预留 |
| `systables.h/cpp` | 系统表行格式：pg_namespace、pg_class、pg_attribute、pg_type、pg_proc、pg_depend |
| `catalog.h/cpp` | CatalogManager：内存缓存、OID/名称索引、CRUD、CSV 持久化、依赖追踪 |
| `migrate.h/cpp` | 迁移工具：从现有 `.stc` 文件迁移到系统表 |

### 已实现功能

- ✅ **系统表定义**：6 张核心系统表的行结构（PG 兼容字段名）
- ✅ **OID 分配**：从 10000 开始单调递增，每 100 次分配自动持久化
- ✅ **内存索引**：按 OID 和名称的哈希索引，支持 O(1) 查询
- ✅ **CSV 持久化**：每表一个 `.cat` 文件，启动时加载、析构时保存
- ✅ **Bootstrap**：标准 namespace（pg_catalog=11, public=2200）+ 28 种系统类型（固定 OID）
- ✅ **依赖追踪**：
  - `planDrop()` 生成 CASCADE/RESTRICT 删除计划
  - 拓扑排序 + 循环检测 + pin 保护
- ✅ **迁移工具**：`migrateDatabaseToCatalog()` 遍历 `.stc` 文件，自动创建系统表条目

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
- `pg_authid` / `pg_auth_members` 替代 `user.dat`/`role.dat`（2.7）
- `COMMENT ON` 对象类型全集（2.8）
- 与 `main.cpp` / `StorageEngine` 的集成（在启动时自动初始化 Catalog）
- 从内存 vector 迁移到真正的 HeapTuple/页面格式（Phase 3/4）

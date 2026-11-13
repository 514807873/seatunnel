# Flink 增量：覆盖插入计数与自动更新时间戳拆开

适用版本：seatunnel 2.3.5-qinhuan，XJJDBC，`insertMode=increment` 且 `incrementMode=update`。

样例作业：设备开关机日志。Http 源，Oracle 目标 `ADB.QDSOS`，主键 `ID`。当前配置：`dontCompare=true`，`openDelete=false`，`recordOperate=false`，`autoTimestamp=false`，`zipperFlagValue=["I","U","D"]`。该作业源不是 JDBC，不会进极速模式，一直走 Flink 的 `MySinkWriterUpdate`。

## 目标

1. 修复覆盖插入的运行记录：目标表里已经存在的主键记到「修改（条）(包含upsert)」，只有目标表没有的主键记到「插入」。
2. 自动更新时间戳与另外两个开关拆开。只打开自动更新时间戳，就给本轮写过的行盖当前时间。

## 不改

- 极速模式（`pangu-batch-job`）
- 全量写入（`MySinkWriterComplete`）
- 拉链写入（`MySinkWriterZipper`）
- 操作类型的取值来源
- `getDataSql`、`insertDataCount` 里「排除操作类型已是删除值」的过滤条件

操作类型本次保持 Flink 现状：

| 动作 | 取值 |
| --- | --- |
| 插入 | `zipperFlagValue` 第 1 个，当前配置是 `I` |
| 更新 | 代码写死 `U`，不读配置第 2 个值 |
| 逻辑删除 | `zipperFlagValue` 第 3 个，当前配置是 `D` |

配置改成 `I/U/D` 后，更新行的 `U` 只是和写死的值相同。以后把第 2 个值改成别的字，更新行仍是 `U`。

## 1. 覆盖插入计数

现状在 `MySinkWriterUpdate` 收尾：

1. `dontCompare=true` 时先 `deleteDataInUc`，按主键删掉目标表里已有的行。
2. 再执行 `insertDataCount`，数「临时表有、目标表没有」的主键，结果写入 `insertCount`。
3. 对比被跳过，`updateCount` 保持 0。
4. 这次按主键删除的行不记入 `deleteCount`。

删除之后，交集主键在目标表里已经不存在，整批都被算成插入。读取条数等于插入条数，修改条数是 0。

调整后的顺序：

1. 在 `deleteDataInUc` 之前数两个数。
   - 临时表与目标表主键的交集 → `updateCount`（修改，含 upsert）
   - 只在临时表中的主键 → `insertCount`
2. 再按主键删除，再执行现有的插入。
3. 覆盖插入这条路不再用删除之后的 `insertDataCount` 覆盖 `insertCount`。
4. `openDelete=true` 时，只在目标表、不在临时表的行仍记删除。交集不记删除。

对比模式（`dontCompare=false`）的计数不改：有变化的行继续在 `compareTables` 里累加 `updateCount`，新主键继续走 `insertDataCount`。

计数 SQL 用主键 `EXISTS` / `NOT EXISTS`，不用 `NOT IN`，也不使用带「排除已删除行」条件的 `insertDataCount`。临时表有主键唯一索引，按主键计数即可。

## 2. 三个开关拆开

对齐极速模式增量的开关关系：盖时间戳只看 `autoTimestamp`，以及目标表是否真有该列。`recordOperate`、`openDelete` 不再作为盖时间戳的前提。

列是否存在只探测一次：`SELECT 列 FROM schema.table WHERE 1=0`（ClickHouse 用 `WHERE 1=2`）。必须 `executeQuery` 后再读元数据。列名为空或目标表没有该列时打告警，本轮不写时间戳，作业继续，计数仍按第 1 节。其他 SQL 异常让作业失败。标识符按各库方言加引号。

| 开关 | 结果 |
| --- | --- |
| 只开自动更新时间戳，列存在 | 本轮写过的行盖当前时间，不写操作列 |
| 自动更新时间戳和记录操作类型都开，操作列也存在 | 同一批行再写操作类型，取值见上表 |
| 只开记录操作类型 | 与现在一样，不写操作列 |
| 列名为空或列不存在 | 告警后跳过 |

盖哪些行：

- 覆盖插入：临时表里的全部主键。这些行都会先删再插。
- 对比模式：有变化的行、新插入的行。没变化的行不盖。
- 对比模式且 `openDelete=false`：上面两类，再加上目标表里这次没来的行。两个开关都开时，没来的行仍走现有 `deleteDataLogic`（写 `D` 并盖时间戳）。只开时间戳时，这些行只更新时间戳。
- 覆盖插入且 `openDelete=false`：没来的行不盖。覆盖插入没有删除事件。

时间在收尾开始时取一次，插入、更新、逻辑删除共用。增量不走原来的 `currentTimeString`（拉链和全量仍用它）。所有库先查列类型再拼字面量：字符列用带引号的 `yyyy-MM-dd HH:mm:ss`；Oracle、达梦用 `to_date`；MySQL 用 `str_to_date`；PostgreSQL、HexaDB 用 `to_timestamp`；SQL Server 用 `CONVERT`；ClickHouse 日期用 `toDate`，其它日期类型用 `toDateTime`；Trino 用 `TIMESTAMP '...'`。逻辑删除 SQL 也换成这个字面量，避免 Oracle 日期列 `ORA-01861`。

时间戳列如果已经在 `field_mapper` 里，插入列清单不要重复添加，最终值仍盖成当前时间。

## 3. 改哪些文件

只动 `connector-myconnect`。

| 文件 | 改动 |
| --- | --- |
| `sink/MySinkWriterUpdate.java` | 覆盖插入计数前移；合并前探测时间戳列；按第 2 节决定盖哪些行 |
| `dialect/JdbcDialect.java` | 拆开 `insertData`、`updateTableSql` 中「三个条件同时成立才拼时间戳」的判断。新增探测、字面量和只更新时间戳的 SQL。`getDataSql`、`insertDataCount` 的删除过滤条件不动 |
| `dialect/ClickHouse/ClickHouseDialect.java` | `updateTableSql` 同步拆开。没来的行用 `ALTER TABLE ... UPDATE`。逻辑删除的时间表达式改成探测后的字面量 |
| `dialect/oracle`、`dameng`、`pgsql`、`hexadb` 的 `deleteDataLogic` | 只替换时间表达式，删除条件不变 |

对比模式且只开时间戳时，没来的行走 `updateMissingAutoTimestamp`，不计入删除数。覆盖插入不调用逻辑删除。

`insertData`、`updateTableSql` 现在的判断是：

```text
!openDelete && recordOperate && autoTimestamp
```

拆开后：

- 拼时间戳：`autoTimestamp` 且探测通过
- 拼操作类型：`autoTimestamp` 且 `recordOperate` 且操作列探测通过
- 不再要求 `openDelete=false`

## 4. 验证

用设备开关机日志这条 Flink 增量作业，目标表 `ADB.QDSOS`。

计数：

- 覆盖插入，主键都已存在：修改 = 读取，插入 = 0
- 覆盖插入，目标表为空：插入 = 读取，修改 = 0
- 覆盖插入，只有一部分主键已存在：修改 = 交集，插入 = 差集
- `openDelete=true`：只在目标表的行仍记删除，交集记修改

时间戳：

- 只开自动更新时间戳，列存在：时间戳列变为当前时间，操作列不变
- 对比模式两个都开，且 `openDelete=false`：插入 `I`，更新 `U`，没来的行 `D`，并盖时间戳
- 覆盖插入两个都开：写出的行盖时间戳并写 `I`，没来的行不盖、不写 `D`
- 时间戳列不存在：作业成功，只打告警，计数仍正确
- 覆盖插入关着：没变化的行不盖时间戳，修改数只包含真正有变化的行

三个开关原本都开着的旧作业再跑一次，确认操作类型仍是 `I` / `U` / `D`，时间戳仍会写。

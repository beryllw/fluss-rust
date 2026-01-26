# Schema 管理

<cite>
**本文引用的文件**
- [table.rs](file://crates/fluss/src/metadata/table.rs)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs)
- [column.rs](file://crates/fluss/src/row/column.rs)
- [datum.rs](file://crates/fluss/src/row/datum.rs)
- [mod.rs](file://crates/fluss/src/row/mod.rs)
- [example_table.rs](file://crates/examples/src/example_table.rs)
</cite>

## 目录
1. [简介](#简介)
2. [项目结构](#项目结构)
3. [核心组件](#核心组件)
4. [架构总览](#架构总览)
5. [详细组件分析](#详细组件分析)
6. [依赖关系分析](#依赖关系分析)
7. [性能考量](#性能考量)
8. [故障排查指南](#故障排查指南)
9. [结论](#结论)
10. [附录](#附录)

## 简介
本文件围绕 Fluss 的 Schema 管理功能进行系统化文档化，重点解释 TableDescriptor 的设计与实现、字段定义与数据类型映射、约束条件、版本管理与兼容性检查、迁移策略，以及与 Arrow 格式的集成与性能优化。文档同时提供面向初学者的渐进式讲解与面向开发者的代码级细节，帮助读者快速理解并正确使用 Schema 管理能力。

## 项目结构
Schema 管理相关代码主要分布在以下模块：
- 元数据层：表结构定义、数据类型系统、JSON 序列化/反序列化
- 记录层：Arrow 集成、批记录构建与读取
- 行访问层：通用行与列式行接口及实现

```mermaid
graph TB
subgraph "元数据层"
TBL["table.rs<br/>Schema/TableDescriptor/PrimaryKey"]
DT["datatype.rs<br/>DataType/DataField/RowType"]
JSON["json_serde.rs<br/>JsonSerde 实现"]
end
subgraph "记录层"
ARW["arrow.rs<br/>Arrow 集成/批构建/读取"]
end
subgraph "行访问层"
ROWMOD["row/mod.rs<br/>InternalRow 接口"]
COL["column.rs<br/>ColumnarRow 实现"]
DAT["datum.rs<br/>Datum 类型族"]
end
TBL --> DT
JSON --> TBL
JSON --> DT
ARW --> DT
ARW --> ROWMOD
ROWMOD --> COL
ROWMOD --> DAT
```

图表来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L26-L144)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L21-L44)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L25-L464)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L402-L447)
- [mod.rs](file://crates/fluss/src/row/mod.rs#L26-L74)
- [column.rs](file://crates/fluss/src/row/column.rs#L25-L48)
- [datum.rs](file://crates/fluss/src/row/datum.rs#L37-L63)

章节来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L1-L921)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L1-L815)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L1-L465)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L1-L546)
- [mod.rs](file://crates/fluss/src/row/mod.rs#L1-L149)
- [column.rs](file://crates/fluss/src/row/column.rs#L1-L170)
- [datum.rs](file://crates/fluss/src/row/datum.rs#L1-L288)

## 核心组件
- Column：列定义，包含名称、数据类型、注释等
- Schema：表的逻辑模式，由列集合与主键约束组成，并派生出 Row 数据类型
- TableDescriptor：物理表描述，包含 Schema、分区键、分桶配置、属性等
- DataType/DataField/RowType：数据类型系统，覆盖标量、数组、映射、嵌套行等
- JsonSerde：Schema/TableDescriptor 的 JSON 序列化/反序列化
- Arrow 集成：将 Fluss 的 DataType 映射到 Arrow Schema/类型，支持批构建与读取

章节来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L26-L144)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L21-L815)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L25-L464)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L402-L447)

## 架构总览
Schema 管理从“定义”到“持久化/传输”的关键流程如下：
- 定义阶段：通过 SchemaBuilder/Column 定义列与主键
- 模型阶段：Schema 自动派生 RowType；TableDescriptorBuilder 组装表级属性
- 序列化阶段：JsonSerde 将 Schema/TableDescriptor 转换为 JSON
- 存储/传输阶段：Arrow 集成将 RowType 转换为 Arrow Schema，用于批构建与读取

```mermaid
sequenceDiagram
participant U as "用户代码"
participant SB as "SchemaBuilder"
participant S as "Schema"
participant TDB as "TableDescriptorBuilder"
participant TD as "TableDescriptor"
participant JS as "JsonSerde"
participant ARW as "Arrow集成"
U->>SB : 定义列与主键
SB->>S : build() 生成 Schema
U->>TDB : 设置 schema/分区/分桶/属性
TDB->>TD : build() 生成 TableDescriptor
TD->>JS : serialize_json()/deserialize_json()
S->>ARW : to_arrow_schema()/to_arrow_type()
ARW-->>U : RecordBatch/Reader 可用
```

图表来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L101-L215)
- [table.rs](file://crates/fluss/src/metadata/table.rs#L287-L374)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L232-L295)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L328-L464)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L402-L447)

## 详细组件分析

### TableDescriptor 设计与实现
- 角色定位：承载表的物理属性（Schema、分区键、分桶键/数量、属性、注释等）
- 关键点：
  - 分布规则校验：分桶键不可与分区键重叠；主键表的分桶键必须是主键去掉分区键后的子集
  - 默认分桶键：当主键存在且未显式指定分桶键时，默认使用“物理主键-分区键”
  - 属性管理：复制因子、KV/日志格式等以字符串键值形式存储
- 构建器：TableDescriptorBuilder 提供链式设置方法，最终 build() 校验并产出 TableDescriptor

```mermaid
classDiagram
class TableDescriptor {
+schema : Schema
+comment : Option<String>
+partition_keys : Vec<String>
+table_distribution : Option<TableDistribution>
+properties : HashMap<String,String>
+custom_properties : HashMap<String,String>
+builder() TableDescriptorBuilder
+replication_factor() Result<i32>
+is_partitioned() bool
+has_primary_key() bool
+is_default_bucket_key() Result<bool>
}
class TableDescriptorBuilder {
-schema : Option<Schema>
-properties : HashMap<String,String>
-custom_properties : HashMap<String,String>
-partition_keys : Vec<String>
-comment : Option<String>
-table_distribution : Option<TableDistribution>
+schema(Schema) Self
+partitioned_by(Vec<String>) Self
+distributed_by(Option<i32>,Vec<String>) Self
+property(key : String,value : T) Self
+properties(HashMap<String,String>) Self
+custom_property(key : String,value : String) Self
+custom_properties(HashMap<String,String>) Self
+comment(String) Self
+build() Result<TableDescriptor>
}
class TableDistribution {
+bucket_count : Option<i32>
+bucket_keys : Vec<String>
+bucket_count() Option<i32>
+bucket_keys() &[String]
}
TableDescriptorBuilder --> TableDescriptor : "构建"
TableDescriptor --> TableDistribution : "包含"
```

图表来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L287-L374)
- [table.rs](file://crates/fluss/src/metadata/table.rs#L376-L565)

章节来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L287-L565)

### Schema 与 Column
- Column：包含名称、数据类型、注释
- Schema：包含列集合、可选主键、以及派生的 RowType（DataType::Row）
- SchemaBuilder：提供 with_row_type/with_columns/column/primary_key 等方法，build() 时执行列名去重、主键完整性与可空性规范化

```mermaid
classDiagram
class Column {
-name : String
-data_type : DataType
-comment : Option<String>
+new(name,data_type) Column
+with_comment(comment) Column
+with_data_type(data_type) Column
+name() &str
+data_type() &DataType
+comment() Option<&str>
}
class PrimaryKey {
-constraint_name : String
-column_names : Vec<String>
+new(name,column_names) PrimaryKey
+constraint_name() &str
+column_names() &[String]
}
class Schema {
-columns : Vec<Column>
-primary_key : Option<PrimaryKey>
-row_type : DataType
+builder() SchemaBuilder
+empty() Result<Schema>
+columns() &[Column]
+primary_key() Option<&PrimaryKey>
+row_type() &DataType
+primary_key_indexes() Vec<usize>
+primary_key_column_names() Vec<&str>
+column_names() Vec<&str>
}
class SchemaBuilder {
-columns : Vec<Column>
-primary_key : Option<PrimaryKey>
+new() SchemaBuilder
+with_row_type(&DataType) Self
+column(name,data_type) Self
+with_columns(Vec<Column>) Self
+with_comment(str) Self
+primary_key(Vec<String>) Self
+primary_key_named(str,Vec<String>) Self
+build() Result<Schema>
-normalize_columns(&mut [Column],Option<&PrimaryKey>) Result<Vec<Column>>
}
SchemaBuilder --> Schema : "构建"
Schema --> Column : "包含"
Schema --> PrimaryKey : "可选"
```

图表来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L26-L144)
- [table.rs](file://crates/fluss/src/metadata/table.rs#L146-L268)

章节来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L26-L268)

### 数据类型系统（DataType/DataField/RowType）
- 基本类型：布尔、整数系列、浮点系列、字符、字符串、十进制、日期、时间、时间戳、字节/二进制等
- 复合类型：数组（元素类型）、映射（键值类型）、行（字段列表）
- 可空性：每个类型都支持可空标记，提供非空转换方法
- 工具类：DataTypes 提供便捷构造函数，DataField 用于行内字段定义

```mermaid
classDiagram
class DataType {
<<enum>>
+Boolean(BooleanType)
+TinyInt(TinyIntType)
+SmallInt(SmallIntType)
+Int(IntType)
+BigInt(BigIntType)
+Float(FloatType)
+Double(DoubleType)
+Char(CharType)
+String(StringType)
+Decimal(DecimalType)
+Date(DateType)
+Time(TimeType)
+Timestamp(TimestampType)
+TimestampLTz(TimestampLTzType)
+Bytes(BytesType)
+Binary(BinaryType)
+Array(ArrayType)
+Map(MapType)
+Row(RowType)
+is_nullable() bool
+as_non_nullable() DataType
}
class DataField {
+name : String
+data_type : DataType
+description : Option<String>
+new(name,data_type,desc) DataField
+name() &str
+data_type() &DataType
}
class RowType {
-nullable : bool
-fields : Vec<DataField>
+new(fields) RowType
+with_nullable(bool,fields) RowType
+as_non_nullable() RowType
+fields() &Vec<DataField>
}
class ArrayType {
-nullable : bool
-element_type : Box<DataType>
+new(element_type) ArrayType
+with_nullable(bool,element_type) ArrayType
+as_non_nullable() ArrayType
}
class MapType {
-nullable : bool
-key_type : Box<DataType>
-value_type : Box<DataType>
+new(key_type,value_type) MapType
+with_nullable(bool,key_type,value_type) MapType
+as_non_nullable() MapType
}
DataType --> RowType : "Row"
DataType --> ArrayType : "Array"
DataType --> MapType : "Map"
RowType --> DataField : "包含"
```

图表来源
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L21-L815)

章节来源
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L21-L815)

### JSON 序列化与版本管理
- JsonSerde trait：为 DataType/Column/Schema/TableDescriptor 提供统一的 JSON 序列化/反序列化
- 版本字段：Schema/TableDescriptor 在 JSON 中包含 version 字段，便于未来演进与兼容性检查
- 列定义：支持 name、data_type、comment
- 表定义：支持 schema、comment、partition_key、bucket_key、bucket_count、properties、custom_properties

```mermaid
flowchart TD
Start(["开始"]) --> CheckVersion["检查 JSON 中的 version 字段"]
CheckVersion --> ParseSchema["解析 schema 字段"]
ParseSchema --> ParsePK["解析 primary_key 字段"]
ParsePK --> ParseComment["解析 comment 字段"]
ParseComment --> ParsePartition["解析 partition_key 字段"]
ParsePartition --> ParseBucket["解析 bucket_key/bucket_count 字段"]
ParseBucket --> ParseProps["解析 properties/custom_properties 字段"]
ParseProps --> BuildTD["构建 TableDescriptor"]
BuildTD --> End(["结束"])
```

图表来源
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L225-L295)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L297-L464)

章节来源
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L25-L464)

### 与 Arrow 的集成与性能优化
- 类型映射：to_arrow_type 将 Fluss 的 DataType 映射到 Arrow 的基础类型；to_arrow_schema 将 RowType 映射为 Arrow Schema
- 批构建：MemoryLogRecordsArrowBuilder 使用 Arrow 的 ArrayBuilder 逐列构建 RecordBatch，并写入自定义批次头
- 读取：LogRecordBatch 解析批次头后，结合 Arrow Metadata 读取 RecordBatch，通过 ArrowReader/ColumnarRow 提供行访问
- 性能要点：
  - 列式存储减少序列化开销
  - 批量写入（默认最大记录数常量）提升吞吐
  - CRC 校验保证数据完整性
  - 支持不同 WriterId/BatchSequence 追踪幂等性（预留）

```mermaid
sequenceDiagram
participant APP as "应用"
participant ARW as "ArrowBuilder"
participant RB as "RecordBatch"
participant HDR as "批次头"
participant IO as "磁盘/网络"
APP->>ARW : append(GenericRow)
ARW->>RB : finish() 生成 RecordBatch
ARW->>HDR : 写入批次头(含 schema_id/crc/计数等)
HDR->>IO : 写出批次
IO-->>APP : 读取批次
APP->>ARW : 解析批次头/校验CRC
APP->>RB : 读取 RecordBatch
APP->>APP : ArrowReader/ColumnarRow 逐行访问
```

图表来源
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L104-L230)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L280-L400)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L528-L544)

章节来源
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L1-L546)

### 行访问层（ColumnarRow/GenericRow/Datum）
- InternalRow：统一的行访问接口，支持布尔、整数、浮点、字符串、二进制等读取
- ColumnarRow：基于 Arrow RecordBatch 的列式行实现，按列索引读取对应数组
- GenericRow：通用行，内部持有 Datum 列表，用于中间态或测试场景
- Datum：统一的值类型族，支持 Null、布尔、整数、浮点、字符串、Blob、Decimal、Date、Timestamp/TimestampTz 等

```mermaid
classDiagram
class InternalRow {
<<trait>>
+get_field_count() usize
+is_null_at(pos : usize) bool
+get_boolean(pos : usize) bool
+get_byte(pos : usize) i8
+get_short(pos : usize) i16
+get_int(pos : usize) i32
+get_long(pos : usize) i64
+get_float(pos : usize) f32
+get_double(pos : usize) f64
+get_char(pos : usize,length : usize) String
+get_string(pos : usize) &str
+get_binary(pos : usize,length : usize) Vec<u8>
+get_bytes(pos : usize) Vec<u8>
}
class ColumnarRow {
-record_batch : Arc<RecordBatch>
-row_id : usize
+new(batch : Arc<RecordBatch>) ColumnarRow
+new_with_row_id(batch : Arc<RecordBatch>,row_id : usize) ColumnarRow
+set_row_id(row_id : usize) void
}
class GenericRow {
+values : Vec<Datum>
+new() GenericRow
+set_field(pos : usize,value : impl Into<Datum>) void
}
class Datum {
<<enum>>
+Null
+Bool(bool)
+Int16(i16)
+Int32(i32)
+Int64(i64)
+Float64(F64)
+String(&str)
+Blob(Blob)
+Decimal(Decimal)
+Date(Date)
+Timestamp(Timestamp)
+TimestampTz(TimestampLtz)
+is_null() bool
+as_str() &str
}
ColumnarRow ..|> InternalRow
GenericRow ..|> InternalRow
```

图表来源
- [mod.rs](file://crates/fluss/src/row/mod.rs#L26-L74)
- [column.rs](file://crates/fluss/src/row/column.rs#L25-L170)
- [datum.rs](file://crates/fluss/src/row/datum.rs#L37-L288)

章节来源
- [mod.rs](file://crates/fluss/src/row/mod.rs#L1-L149)
- [column.rs](file://crates/fluss/src/row/column.rs#L1-L170)
- [datum.rs](file://crates/fluss/src/row/datum.rs#L1-L288)

## 依赖关系分析
- 元数据层依赖：Schema/DataType 作为核心模型，TableDescriptor 依赖 Schema 与分布策略
- 序列化层：JsonSerde 依赖 DataType/Column/Schema/TableDescriptor 的具体实现
- 记录层：Arrow 集成依赖 DataType/RowType 映射，以及行访问层的实现
- 行访问层：ColumnarRow/GenericRow 依赖 Arrow 的 RecordBatch 与 Datum

```mermaid
graph LR
DT["DataType/DataField/RowType"] --> TBL["Schema/TableDescriptor"]
JSON["JsonSerde"] --> TBL
ARW["Arrow 集成"] --> DT
ARW --> ROW["InternalRow 实现"]
ROW --> COL["ColumnarRow"]
ROW --> DAT["Datum"]
```

图表来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L26-L144)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L21-L815)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L25-L464)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L402-L447)
- [mod.rs](file://crates/fluss/src/row/mod.rs#L26-L74)
- [column.rs](file://crates/fluss/src/row/column.rs#L25-L170)
- [datum.rs](file://crates/fluss/src/row/datum.rs#L37-L288)

章节来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L1-L921)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L1-L815)
- [json_serde.rs](file://crates/fluss/src/metadata/json_serde.rs#L1-L465)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L1-L546)
- [mod.rs](file://crates/fluss/src/row/mod.rs#L1-L149)
- [column.rs](file://crates/fluss/src/row/column.rs#L1-L170)
- [datum.rs](file://crates/fluss/src/row/datum.rs#L1-L288)

## 性能考量
- 列式存储：Arrow 的列式布局在扫描与聚合场景下具有显著优势
- 批处理：MemoryLogRecordsArrowBuilder 使用固定上限的记录数批量写入，降低系统调用次数
- 类型映射：精确的 Arrow 类型映射避免不必要的装箱与转换
- 校验与幂等：批次头包含 CRC、WriterId、BatchSequence 等字段，便于快速校验与幂等控制
- 可空性：合理设置可空性可减少冗余存储与比较成本

[本节为通用性能讨论，不直接分析特定文件]

## 故障排查指南
- 主键约束错误
  - 现象：构建 TableDescriptor 时报错，提示分桶键包含分区键或分桶键不是主键的子集
  - 处理：确认分区键与分桶键无交集；主键表的分桶键应为主键去除分区键后的子集
- 列名重复
  - 现象：SchemaBuilder.build() 抛出重复列名错误
  - 处理：确保列名唯一
- 可空性违规
  - 现象：主键列被设置为可空
  - 处理：自动规范化为主键列强制非空
- Arrow 类型不支持
  - 现象：to_arrow_type 对某些 Fluss 类型未实现映射
  - 处理：扩展映射或在上层转换为已支持类型

章节来源
- [table.rs](file://crates/fluss/src/metadata/table.rs#L217-L268)
- [table.rs](file://crates/fluss/src/metadata/table.rs#L510-L564)
- [arrow.rs](file://crates/fluss/src/record/arrow.rs#L425-L447)

## 结论
Fluss 的 Schema 管理以 DataType/DataField/RowType 为核心数据模型，配合 Schema/TableDescriptor 的构建与校验机制，实现了从逻辑模式到物理表的完整闭环。通过 JsonSerde 支持版本化与跨系统传输，借助 Arrow 的高性能列式存储与批处理能力，整体方案兼顾了表达力、可维护性与运行效率。建议在实际使用中：
- 明确主键与分区键，避免冲突
- 合理选择数据类型与可空性
- 使用 TableDescriptorBuilder 的链式 API 逐步构建
- 在需要复杂嵌套或数组时，优先采用 RowType/Map/Array 并配合 JSON 序列化

[本节为总结性内容，不直接分析特定文件]

## 附录

### 示例：定义复杂 Schema（嵌套结构、数组字段）
- 嵌套结构：使用 DataTypes::row 或 DataTypes::row_from_types 定义行字段
- 数组字段：使用 DataTypes::array 包裹元素类型
- 示例参考路径：[示例程序](file://crates/examples/src/example_table.rs#L34-L41)

章节来源
- [example_table.rs](file://crates/examples/src/example_table.rs#L34-L41)
- [datatype.rs](file://crates/fluss/src/metadata/datatype.rs#L773-L787)
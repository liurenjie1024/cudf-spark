# RAPIDS Accelerator for Apache Spark Iceberg Support

The Iceberg support is organized into multiple Maven projects, one per Iceberg minor
version that is supported. This allows each submodule to build against the Iceberg minor
version it supports.

# Iceberg Submodules

The following table shows the mapping of Iceberg versions to their supported Spark version
and the directory that contains the corresponding support code.

| Iceberg Version | Spark Version              | Directory         |
|-----------------|----------------------------|-------------------|
| 1.6.x           | Spark 3.5.0-3.5.3          | `iceberg-1-6-x`  |
| 1.9.x           | Spark 3.5.4-3.5.8          | `iceberg-1-9-x`  |
| 1.10.x          | Spark 3.5.4-3.5.8, 4.0.x  | `iceberg-1-10-x` |

Iceberg GPU acceleration is currently supported on Spark 3.5.x and 4.0.x.

For Spark 3.5.4+, both `iceberg-1-9-x` and `iceberg-1-10-x` modules are compiled into the
build. The correct version-specific implementation is selected at runtime by probing the
`iceberg-spark-runtime` jar on the classpath. Version-specific code lives in distinct
sub-packages (`iceberg19x`, `iceberg110x`) to avoid class conflicts, and the common
`ShimUtils` dispatcher delegates to the appropriate implementation.

For Spark 4.0.x, only `iceberg-1-10-x` is compiled during the build.

## Code Shared Between Modules

The `common` directory contains code that is shared across some or all of the Iceberg
submodules. It is not built directly as a Maven submodule but simply houses common code
that is picked up by the Iceberg submodules via the Maven build helper plugin.

| Directory                         | Description                              |
|-----------------------------------|------------------------------------------|
| `common/src/main/scala`           | Scala code shared across all versions    |
| `common/src/main/java`            | Java code shared across all versions     |
| `common/src/main/spark35x/java`   | Java code for Spark 3.5.x only           |

## Per-Session Read Overrides

Spark RAPIDS prefers larger Parquet read splits than CPU Spark. For Iceberg tables, the split
size is governed by the table properties `read.split.target-size` and
`read.split.planning-lookback`, not by `spark.sql.files.maxPartitionBytes`. To override these
at the session level — without editing the iceberg table metadata — register
`RapidsIcebergSparkSessionCatalog` in place of Iceberg's `SparkSessionCatalog`:

```
spark.sql.catalog.spark_catalog = com.nvidia.spark.rapids.iceberg.spark.RapidsIcebergSparkSessionCatalog
spark.sql.catalog.spark_catalog.type = hive
```

Then provide per-table overrides keyed by the fully qualified table name
(`<catalog>.<namespace>.<table>`):

```
spark.rapids.iceberg.tables.spark_catalog.db.events.split-size     = 2147483648
spark.rapids.iceberg.tables.spark_catalog.db.events.lookback       = 1000
spark.rapids.iceberg.tables.spark_catalog.db.events.file-open-cost = 4194304
```

Recognized option suffixes (matching Iceberg's `SparkReadOptions`):

| Suffix           | Iceberg table property         |
|------------------|--------------------------------|
| `split-size`     | `read.split.target-size`       |
| `lookback`       | `read.split.planning-lookback` |
| `file-open-cost` | `read.split.open-file-cost`   |

Each table that needs an override must list it explicitly — there is no implicit global default.
Options supplied directly via `DataFrameReader.option(...)` always take precedence over the
session-level overrides.

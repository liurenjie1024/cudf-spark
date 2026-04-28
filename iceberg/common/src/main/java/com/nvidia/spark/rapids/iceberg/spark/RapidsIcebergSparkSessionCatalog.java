/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.iceberg.spark;

import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

/**
 * A drop-in replacement for {@link SparkSessionCatalog} that lets users override Iceberg
 * read-planning options on a per-table basis through Spark session conf. This avoids editing
 * iceberg table metadata just to give RAPIDS larger split sizes than the iceberg defaults.
 *
 * <p>Register it in place of Iceberg's {@code SparkSessionCatalog}:
 * <pre>
 *   spark.sql.catalog.spark_catalog =
 *     com.nvidia.spark.rapids.iceberg.spark.RapidsIcebergSparkSessionCatalog
 *   spark.sql.catalog.spark_catalog.type = hive
 * </pre>
 *
 * <p>Then provide per-table overrides as session conf, with the table identified by its fully
 * qualified name {@code <catalog>.<namespace>.<table>}:
 * <pre>
 *   spark.rapids.iceberg.tables.spark_catalog.db.events.split-size     = 2147483648
 *   spark.rapids.iceberg.tables.spark_catalog.db.events.lookback       = 1000
 *   spark.rapids.iceberg.tables.spark_catalog.db.events.file-open-cost = 4194304
 * </pre>
 *
 * <p>The class extends {@code SparkSessionCatalog} as a raw type because the type-parameter
 * intersection bound for {@code T} differs across Iceberg versions (1.6.x lacks
 * {@code ViewCatalog}, 1.9.x and 1.10.x include it). Raw-type extension compiles against all
 * three. We don't override any method that uses {@code T} directly, so the lost type information
 * has no behavioral effect.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RapidsIcebergSparkSessionCatalog extends SparkSessionCatalog {

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return RapidsIcebergSparkTable.wrapIfIceberg(super.loadTable(ident), name(), ident);
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return RapidsIcebergSparkTable.wrapIfIceberg(super.loadTable(ident, version), name(), ident);
  }

  @Override
  public Table loadTable(Identifier ident, long timestampMicros) throws NoSuchTableException {
    return RapidsIcebergSparkTable.wrapIfIceberg(
        super.loadTable(ident, timestampMicros), name(), ident);
  }
}

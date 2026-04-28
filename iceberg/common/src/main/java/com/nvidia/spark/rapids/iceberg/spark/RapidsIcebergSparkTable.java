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

import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subclass of Iceberg's {@link SparkTable} that injects per-table read overrides from session
 * conf into the {@link CaseInsensitiveStringMap} passed to {@link #newScanBuilder} during
 * planning.
 *
 * <p>Subclassing (rather than wrapping) preserves {@code instanceof SparkTable} for Iceberg
 * internals — procedures (e.g. {@code rewrite_data_files}), {@code MERGE INTO} rewrites, and
 * {@code Spark3Util.toIcebergTable} all rely on that cast.
 *
 * <p>Recognized session-conf keys (no global default; each table must list its overrides):
 * <pre>
 *   spark.rapids.iceberg.tables.&lt;catalog&gt;.&lt;namespace&gt;.&lt;table&gt;.split-size
 *   spark.rapids.iceberg.tables.&lt;catalog&gt;.&lt;namespace&gt;.&lt;table&gt;.lookback
 *   spark.rapids.iceberg.tables.&lt;catalog&gt;.&lt;namespace&gt;.&lt;table&gt;.file-open-cost
 * </pre>
 *
 * <p>Precedence (matches Iceberg's {@code SparkConfParser}): user-supplied
 * {@code DataFrameReader.option(...)} &gt; this catalog's session-conf injection &gt;
 * table property &gt; Iceberg session conf &gt; default.
 */
public class RapidsIcebergSparkTable extends SparkTable {
  private static final Logger LOG = LoggerFactory.getLogger(RapidsIcebergSparkTable.class);

  static final String CONF_PREFIX = "spark.rapids.iceberg.tables.";

  static final Set<String> KNOWN_OPTIONS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      SparkReadOptions.SPLIT_SIZE,
      SparkReadOptions.LOOKBACK,
      SparkReadOptions.FILE_OPEN_COST)));

  // Tracks suffixes already warned about so we don't spam the log on every table load.
  private static final Set<String> WARNED_UNKNOWN_KEYS = ConcurrentHashMap.newKeySet();

  private final String fqTableKey;

  private RapidsIcebergSparkTable(SparkTable src, String fqTableKey, boolean refreshEagerly) {
    super(src.table(), refreshEagerly);
    this.fqTableKey = fqTableKey;
  }

  private RapidsIcebergSparkTable(
      SparkTable src, String fqTableKey, String branch, boolean refreshEagerly) {
    super(src.table(), branch, refreshEagerly);
    this.fqTableKey = fqTableKey;
  }

  private RapidsIcebergSparkTable(
      SparkTable src, String fqTableKey, Long snapshotId, boolean refreshEagerly) {
    super(src.table(), snapshotId, refreshEagerly);
    this.fqTableKey = fqTableKey;
  }

  /**
   * Wrap an Iceberg {@link SparkTable} so its scan-builder reads pick up session-level overrides.
   * Non-Iceberg tables (e.g. V1 fallbacks returned by {@code SparkSessionCatalog} for non-Iceberg
   * session tables) are returned unchanged.
   */
  public static Table wrapIfIceberg(Table table, String catalogName, Identifier ident) {
    if (!(table instanceof SparkTable)) {
      return table;
    }
    SparkTable src = (SparkTable) table;
    String key = fqTableKey(catalogName, ident);
    String branch = src.branch();
    Long snapshotId = src.snapshotId();
    if (branch != null) {
      return new RapidsIcebergSparkTable(src, key, branch, /*refreshEagerly=*/ false);
    } else if (snapshotId != null) {
      return new RapidsIcebergSparkTable(src, key, snapshotId, /*refreshEagerly=*/ false);
    } else {
      return new RapidsIcebergSparkTable(src, key, /*refreshEagerly=*/ false);
    }
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap userOptions) {
    return super.newScanBuilder(mergeReadOverrides(currentSessionConf(), fqTableKey, userOptions));
  }

  /**
   * Build the merged read options. Pure (no SparkSession dependency) so the merge logic can be
   * exercised directly in tests.
   */
  static CaseInsensitiveStringMap mergeReadOverrides(
      Map<String, String> sessionConf, String fqTableKey, CaseInsensitiveStringMap userOptions) {
    String prefix = CONF_PREFIX + fqTableKey + ".";
    Map<String, String> merged = new HashMap<>();
    for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(prefix)) {
        continue;
      }
      String suffix = key.substring(prefix.length());
      if (KNOWN_OPTIONS.contains(suffix)) {
        merged.put(suffix, entry.getValue());
      } else if (WARNED_UNKNOWN_KEYS.add(key)) {
        LOG.warn("Ignoring unknown Iceberg read-option override '{}' (suffix '{}'). "
            + "Recognized suffixes: {}", key, suffix, KNOWN_OPTIONS);
      }
    }
    if (userOptions != null) {
      // User-supplied options always win — match Iceberg's option > session-conf precedence.
      merged.putAll(userOptions.asCaseSensitiveMap());
    }
    return new CaseInsensitiveStringMap(merged);
  }

  static String fqTableKey(String catalogName, Identifier ident) {
    StringBuilder sb = new StringBuilder(catalogName);
    for (String n : ident.namespace()) {
      sb.append('.').append(n);
    }
    return sb.append('.').append(ident.name()).toString();
  }

  private static Map<String, String> currentSessionConf() {
    return JavaConverters.mapAsJavaMapConverter(SparkSession.active().conf().getAll()).asJava();
  }
}

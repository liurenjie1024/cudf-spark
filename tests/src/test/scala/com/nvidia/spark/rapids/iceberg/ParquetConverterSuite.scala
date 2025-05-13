package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded.unshade
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite

class ParquetConverterSuite extends AnyFunSuite {
  test("Bench schema conversion") {
    val schemaString =
      """ message Record {
        |  required binary id (UTF8);
        |  optional int32 age;
        | }""".stripMargin

    val messageTypeShaded = MessageTypeParser.parseMessageType(schemaString)

    val unshaded = unshade(messageTypeShaded)

    println(s"Unshaded: $unshaded")
  }
}

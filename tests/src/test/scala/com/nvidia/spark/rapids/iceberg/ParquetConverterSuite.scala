package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded.unshade
import org.apache.hadoop.shaded.org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite

class ParquetConverterSuite extends AnyFunSuite {
  test("Bench schema conversion") {
    val schemaString = generateSchemaString(200)
    val messageTypeShaded = MessageTypeParser.parseMessageType(schemaString)

    val ret = timeDist(1000, () => {
      val messageType = unshade(messageTypeShaded)
      assert(messageType.getFields.size() == 202)
    })

    println(s"Unshade nano time: $ret")
  }

  def generateSchemaString(col: Int): String = {
    val schemaString =
      s""" message Record {
         |  ${(0 until col).map(i => genCol(i % 6, s"col_$i")).mkString("\n  ")}
         |  optional int32 age;
         |  required int64 c1;
         | }""".stripMargin
    schemaString
  }

  def genCol(typ: Int, colName: String): String = {
    typ match {
      case 0 => s"required int32 $colName;"
      case 1 => s"optional int32 $colName;"
      case 2 => s"required int64 $colName;"
      case 3 => s"optional int64 $colName;"
      case 4 => s"required binary $colName (UTF8);"
      case 5 => s"optional binary $colName (UTF8);"
      case _ => throw new IllegalArgumentException(s"Unknown type $typ")
    }
  }

  def time(code: () => Unit): Long = {
    val start = System.nanoTime()
    code()
    System.nanoTime() - start
  }

  def timeDist(times: Int = 1000, code: () => Unit): String = {
    val duration = new Array[Double](times)
    for (i <- 0 until times) {
      duration(i) = time(code)
    }

    val p = new Percentile()
    p.setData(duration)

    s"""Max: ${p.evaluate(100)}, p99: ${p.evaluate(99)}, p95: ${p.evaluate(95)},
       |mean: ${p.evaluate(50)}""".stripMargin
  }
}

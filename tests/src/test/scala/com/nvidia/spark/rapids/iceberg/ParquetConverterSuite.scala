package com.nvidia.spark.rapids.iceberg

import scala.util.Using

import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded.unshade
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.shaded.org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.JavaConverters._

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

  test("Bench blob metadata conversion") {
    val filename = "/home/ubuntu/Workspace/kudo-bench-datagen/gen/" +
      "5000000/part-00000-0c7e9b7c-b6b6-4b20-9f3f-f567ef1a77ec-c000.snappy.parquet"


    val conf = new Configuration()
    val inputFile = HadoopInputFile.fromPath(new Path(filename), conf)

    Using.resource(ParquetFileReader.open(inputFile)) { reader =>
      val footer = reader.getFooter
      val blocks = footer.getBlocks
      println(s"Blocks count: ${blocks.size()}")

      val ret = timeDist(1000, () => {
        val unshaded =  blocks.asScala.map(unshade)
        assert(unshaded.length == blocks.size())
      })

      println(s"Unshade block metadata: $ret")
    }
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

package com.nvidia.spark.rapids

import HadoopFileIOSuite._
import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.FileRangeWithOffset
import com.nvidia.spark.rapids.fileio.hadoop.{HadoopFileIO, HadoopFileIOConfig, HadoopInputFile}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.JavaConverters._



class HadoopFileIOSuite extends AnyFunSuite {
  test("HadoopFileIO vector read") {
    testWithHadoopFileIOConfig(new HadoopFileIOConfig(true, 64, 4 * 1024 * 1024))
  }

  test("HadoopFileIO non vector read") {
    testWithHadoopFileIOConfig(new HadoopFileIOConfig(false, 64, 4 * 1024 * 1024))
  }

  private def testWithHadoopFileIOConfig(config: => HadoopFileIOConfig): Unit = {
    // Construct a HadoopFileIO instance
    val hadoopConf = new Configuration()
    val hadoopFileIO = new HadoopFileIO(hadoopConf, config)

    // Assume tempFile already exists on disk with the required data
    // Use a file on HDFS instead of a local temp file

    // Create an InputFile
    val inputFile = hadoopFileIO.newInputFile(hdfsPath).asInstanceOf[HadoopInputFile]

    // Allocate Host memory of 400MB

    // Call vectorRead API to read 10 40MB chunks into the memory buffer
    // Note: vectorRead is not a standard HadoopFileIO API, but is available in Iceberg InputFile
    // We'll use InputFile#vectorRead if available, otherwise simulate chunked reads
    // The following assumes Iceberg 1.3.0+ with vectorRead API

    val chunks = (0 until numChunks)
      .map(i => new FileRangeWithOffset(i * (chunkSize +
        holeSize).toLong, chunkSize, i*chunkSize.toLong) ).asJava

    withResource(HostMemoryBuffer.allocate(bufferSize)) { buf =>
      HadoopFileIOSuite.measureAndPrintStats("non vectorRead") {
        val bytesRead = inputFile.vectorRead(buf, chunks)
        assert(bytesRead == numChunks * chunkSize.toLong)
      }
    }
  }
}

object HadoopFileIOSuite {
  // Path to a file on HDFS with sufficient size for testing
  val hdfsPath: String = "hdfs://rl-r7525-d32-u38.raplab.nvidia.com:9000/data/nds2.0/" +
    "sf200_big_files/parquet/store_sales/" +
    "part-00000-fb1c5b06-fd0b-4b4b-8a52-ae24d96ad52a-c000.snappy.parquet"
  val chunkSize: Int = 4 * 1024 * 1024 // 4MB
  val numChunks: Int = 1000
  val holeSize: Int = 1 * 1024 * 1024 // 1MB
  val bufferSize: Long = chunkSize.toLong * numChunks

  def measureAndPrintStats(name: String)(block: => Unit): Unit = {
    val timings = new Array[Long](100)
    for (i <- 0 until 100) {
      val start = System.nanoTime()
      block
      val end = System.nanoTime()
      timings(i) = end - start
    }
    val sorted = timings.sorted
    val min = sorted.head / 1e6
    val max = sorted.last / 1e6
    val mean = sorted.sum.toDouble / sorted.length / 1e6
    val median = if (sorted.length % 2 == 0) {
      (sorted(sorted.length/2 - 1) + sorted(sorted.length/2)).toDouble / 2 / 1e6
    } else {
      sorted(sorted.length/2).toDouble / 1e6
    }
    val p95 = sorted((sorted.length * 0.95).toInt) / 1e6
    println(f"$name Timings (ms): min=$min%.2f, max=$max%.2f, mean=$mean%.2f, " +
      f"median=$median%.2f, " +
      f"p95=$p95%.2f")
  }
}
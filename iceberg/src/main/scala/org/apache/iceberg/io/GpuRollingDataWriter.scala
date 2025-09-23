package org.apache.iceberg.io

import java.util.{List => JList}

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, PartitionSpec, StructLike}
import org.apache.iceberg.encryption.EncryptedOutputFile
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory

class GpuRollingDataWriter(
  val gpuSparkFileWriterFactory: GpuSparkFileWriterFactory,
  val fileFactory: OutputFileFactory,
  val io: FileIO,
  val targetFileSize: Long,
  val spec: PartitionSpec,
  val partition: StructLike) extends
  GpuRollingFileWriter[DataWriter[SpillableColumnarBatch], DataWriteResult] {

  private val dataFiles: JList[DataFile] = Lists.newArrayList[DataFile]()
  openCurrentWriter()

  protected override def newWriter(file: EncryptedOutputFile): DataWriter[SpillableColumnarBatch] =
  {
    gpuSparkFileWriterFactory.newDataWriter(file, spec, partition)
  }

  protected override def addResult(result: DataWriteResult): Unit = {
    dataFiles.addAll(result.dataFiles())
  }

  protected override def aggregatedResult(): DataWriteResult = {
    new DataWriteResult(dataFiles)
  }
}

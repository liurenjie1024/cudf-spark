package org.apache.iceberg.io

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{PartitionSpec, StructLike}
import org.apache.iceberg.encryption.EncryptedOutputFile


trait GpuRollingFileWriter[W <: FileWriter[SpillableColumnarBatch, R], R] extends
  FileWriter[SpillableColumnarBatch, R] {

  val fileFactory: OutputFileFactory
  val io: FileIO
  val targetFileSize: Long
  val spec: PartitionSpec
  val partition: StructLike

  private var currentFile: EncryptedOutputFile = _
  private var currentFileRows: Long = 0L
  private var currentWriter: W = _

  private var closed: Boolean = false

  protected def newWriter(file: EncryptedOutputFile): W
  protected def addResult(result: R): Unit
  protected def aggregatedResult(): R

  override def length(): Long = {
    throw new UnsupportedOperationException("length is not supported" +
      " in GpuRollingFileWriter")
  }

  override def write(batch: SpillableColumnarBatch): Unit = {
    if (closed) {
      throw new IllegalStateException("Cannot write to a closed writer")
    }

    currentWriter.write(batch)
    currentFileRows += batch.numRows()

    if (currentFileRows >= targetFileSize) {
      closeCurrentWriter()
      openCurrentWriter()
    }
  }

  protected def openCurrentWriter(): Unit = {
    require(currentWriter == null,
      "Current writer should be null when opening a new writer")

    currentFile = newFile()
    currentWriter = newWriter(currentFile)
    currentFileRows = 0L
  }

  private def newFile(): EncryptedOutputFile = {
    if (spec.isUnpartitioned || partition == null) {
      fileFactory.newOutputFile
    } else {
      fileFactory.newOutputFile(spec, partition)
    }
  }

  private def closeCurrentWriter(): Unit = {
    if (currentWriter != null) {
        currentWriter.close()

      if (currentFileRows == 0L) {
        io.deleteFile(currentFile.encryptingOutputFile)
      } else {
        addResult(currentWriter.result())
      }
      this.currentFile = null
      this.currentFileRows = 0
      this.currentWriter = null
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closeCurrentWriter()
      this.closed = true
    }
  }

  override def result: R = {
    require(closed, "Cannot get result from unclosed writer")
    aggregatedResult()
  }
}

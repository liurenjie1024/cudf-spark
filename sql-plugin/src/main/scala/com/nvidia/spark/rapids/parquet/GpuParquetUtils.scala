/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.parquet


import java.util.Locale

import scala.collection.JavaConverters._

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.HostMemoryOutputStream
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import org.apache.hadoop.io.IOUtils
import org.apache.parquet.bytes.BytesUtils.readIntLittleEndian
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging

object GpuParquetUtils extends Logging {
  private val FOOTER_LENGTH_SIZE = 4

  /**
   * Read the parquet footer buffer from a file.
   * The buffer includes MAGIC + footer + footerLength + MAGIC.
   *
   * @param inputFile the input file to read from
   * @param filePath  the file path string for error messages
   * @return the footer buffer as a HostMemoryBuffer
   */
  def readFooterBuffer(inputFile: RapidsInputFile, filePath: String): HostMemoryBuffer = {
    // Much of this code came from the parquet_mr projects ParquetFileReader, and was modified
    // to match our needs
    val fileLen = inputFile.getLength
    // MAGIC + data + footer + footerIndex + MAGIC
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen )")
    }
    val footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length
    withResource(inputFile.open()) { inputStream =>
      inputStream.seek(footerLengthIndex)
      val footerLength = readIntLittleEndian(inputStream)
      val magic = new Array[Byte](MAGIC.length)
      IOUtils.readFully(inputStream, magic, 0, magic.length)
      val footerIndex = footerLengthIndex - footerLength
      verifyParquetMagic(filePath, magic)
      if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
        throw new RuntimeException(s"corrupted file: the footer index is not within " +
          s"the file: $footerIndex")
      }
      val hmbLength = (fileLen - footerIndex).toInt
      closeOnExcept(HostMemoryBuffer.allocate(hmbLength + MAGIC.length, false)) { outBuffer =>
        val out = new HostMemoryOutputStream(outBuffer)
        out.write(MAGIC)
        inputStream.seek(footerIndex)
        // read the footer til the end of the file
        val tmpBuffer = new Array[Byte](4096)
        var bytesLeft = hmbLength
        while (bytesLeft > 0) {
          val readLength = Math.min(bytesLeft, tmpBuffer.length)
          IOUtils.readFully(inputStream, tmpBuffer, 0, readLength)
          out.write(tmpBuffer, 0, readLength)
          bytesLeft -= readLength
        }
        outBuffer
      }
    }
  }

  private val PARQUET_MAGIC_ENCRYPTED = "PARE".getBytes(java.nio.charset.StandardCharsets.US_ASCII)

  /**
   * Verify the parquet magic bytes.
   *
   * @param filePath the file path string for error messages
   * @param magic    the magic bytes to verify
   */
  def verifyParquetMagic(filePath: String, magic: Array[Byte]): Unit = {
    if (!java.util.Arrays.equals(MAGIC, magic)) {
      if (java.util.Arrays.equals(PARQUET_MAGIC_ENCRYPTED, magic)) {
        throw new RuntimeException("The GPU does not support reading encrypted Parquet " +
          "files. To read encrypted or columnar encrypted files, disable the GPU Parquet " +
          "reader.")
      } else {
        throw new RuntimeException(s"$filePath is not a Parquet file. " +
          s"Expected magic number at tail ${java.util.Arrays.toString(MAGIC)} " +
          s"but found ${java.util.Arrays.toString(magic)}")
      }
    }
  }
  /**
   * Trim block metadata to contain only the column chunks that occur in the specified schema.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param readSchema the schema to preserve
   * @param blocks the block metadata from the original Parquet file
   * @param isCaseSensitive indicate if it is case sensitive
   * @return the updated block metadata with undesired column chunks removed
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  def clipBlocksToSchema(
      readSchema: MessageType,
      blocks: java.util.List[BlockMetaData],
      isCaseSensitive: Boolean): Seq[BlockMetaData] = {
    val columnPaths = readSchema.getPaths.asScala.map(x => ColumnPath.get(x: _*))
    val pathSet = if (isCaseSensitive) {
      columnPaths.map(cp => cp.toDotString).toSet
    } else {
      columnPaths.map(cp => cp.toDotString.toLowerCase(Locale.ROOT)).toSet
    }
    blocks.asScala.toSeq.map { oldBlock =>
      //noinspection ScalaDeprecation
      val newColumns = if (isCaseSensitive) {
        oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath.toDotString))
      } else {
        oldBlock.getColumns.asScala.filter(c =>
          pathSet.contains(c.getPath.toDotString.toLowerCase(Locale.ROOT)))
      }
      newBlockMeta(oldBlock.getRowCount, newColumns.toSeq)
    }
  }

  /**
   * Build a new BlockMetaData
   *
   * @param rowCount the number of rows in this block
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  def newBlockMeta(
      rowCount: Long,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(rowCount)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }
}

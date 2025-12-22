/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet.converter

import java.nio.ByteBuffer

import org.apache.iceberg.shaded.org.apache.parquet.io.{InputFile => ShadedInputFile, SeekableInputStream => ShadedSeekableInputStream}
import org.apache.parquet.io.{InputFile, SeekableInputStream}


/**
 * Converters to wrap standard Parquet types into Iceberg shaded Parquet types.
 */
object ToIcebergShaded {

  /**
   * Wrap a standard Parquet InputFile into a shaded InputFile.
   */
  def shade(inputFile: InputFile): ShadedInputFile = new ShadedInputFileWrapper(inputFile)

  /**
   * Wrap a standard Parquet SeekableInputStream into a shaded SeekableInputStream.
   */
  def shade(stream: SeekableInputStream): ShadedSeekableInputStream =
    new ShadedSeekableInputStreamWrapper(stream)
}

/**
 * Wrapper that converts a standard Parquet InputFile to a shaded InputFile.
 */
class ShadedInputFileWrapper(delegate: InputFile) extends ShadedInputFile {
  override def getLength: Long = delegate.getLength

  override def newStream(): ShadedSeekableInputStream =
    ToIcebergShaded.shade(delegate.newStream())
}

/**
 * Wrapper that converts a standard Parquet SeekableInputStream to a shaded SeekableInputStream.
 */
class ShadedSeekableInputStreamWrapper(delegate: SeekableInputStream)
    extends ShadedSeekableInputStream {

  override def getPos: Long = delegate.getPos

  override def seek(newPos: Long): Unit = delegate.seek(newPos)

  override def readFully(bytes: Array[Byte]): Unit = delegate.readFully(bytes)

  override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit =
    delegate.readFully(bytes, start, len)

  override def read(buf: ByteBuffer): Int = delegate.read(buf)

  override def readFully(buf: ByteBuffer): Unit = delegate.readFully(buf)

  override def read(): Int = delegate.read()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def skip(n: Long): Long = delegate.skip(n)

  override def available(): Int = delegate.available()

  override def close(): Unit = delegate.close()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def reset(): Unit = delegate.reset()

  override def markSupported(): Boolean = delegate.markSupported()
}


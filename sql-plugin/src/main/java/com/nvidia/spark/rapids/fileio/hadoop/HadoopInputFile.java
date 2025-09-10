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

package com.nvidia.spark.rapids.fileio.hadoop;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.fileio.FileRangeWithOffset;
import com.nvidia.spark.rapids.fileio.RapidsFileIOExecutor;
import com.nvidia.spark.rapids.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.fileio.SeekableInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of {@link RapidsInputFile} using the Hadoop file system.
 * <br/>
 * This class provides methods to get the length of the file and to open a seekable input stream
 * for reading the file.
 */
public class HadoopInputFile implements RapidsInputFile {

  private static final int THREAD_LOCAL_BUFFER_LEN = 8 * 1024 * 1024;
  private static final ThreadLocal<byte[]> bufferHolder =
      ThreadLocal.withInitial(() -> new byte[THREAD_LOCAL_BUFFER_LEN]);

  private final Path filePath;
  private final FileSystem fs;
  private final long length;
  private final HadoopFileIOConfig config;

  public static HadoopInputFile create(Path filePath, Configuration conf,
      HadoopFileIOConfig config) throws IOException {
    Objects.requireNonNull(filePath, "filePath can't be null!");
    Objects.requireNonNull(conf, "Hadoop conf can't be null");
    Objects.requireNonNull(config, "HadoopFileIOConfig can't be null");
    FileSystem fs = filePath.getFileSystem(conf);
    return new HadoopInputFile(filePath, fs, config);
  }

  private HadoopInputFile(Path filePath, FileSystem fs, HadoopFileIOConfig config) throws IOException {
    Objects.requireNonNull(filePath, "filePath can't be null!");
    Objects.requireNonNull(fs, "FileSystem can't be null");
    Objects.requireNonNull(config, "HadoopFileIOConfig can't be null");
    this.filePath = filePath;
    this.fs = fs;
    this.length = fs.getFileStatus(filePath).getLen();
    this.config = config;
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public SeekableInputStream open() throws IOException {
    return new HadoopInputStream(fs.open(filePath));
  }

  @Override
  public HostMemoryBuffer tailRead(long length) throws IOException {
    checkArgument(length > 0, "Length must be positive");

    long fileLen = getLength();

    long startPos = fileLen - length;
    if (startPos < 0) {
      throw new IllegalArgumentException(
          "Length exceeds file size: " + length + " > " + fileLen);
    }

    HostMemoryBuffer buf = null;
    try (FSDataInputStream fin = fs.open(filePath)) {
      buf = HostMemoryBuffer.allocate(length);
      fin.readFully(startPos, buf.asByteBuffer());
      return buf;
    } catch (Throwable e) {
      if (buf != null) {
        buf.close();
      }
      throw e;
    }
  }

  @Override
  public long vectorRead(HostMemoryBuffer dest, List<FileRangeWithOffset> ranges) throws IOException {
    Objects.requireNonNull(dest, "Destination buffer cannot be null");
    Objects.requireNonNull(ranges, "Ranges cannot be null");
    checkArgument(!ranges.isEmpty(), "Ranges cannot be empty");

    if (config.isParallelVectorReadEnabled()) {
      return parallelVectorRead(dest, ranges);
    } else {
      return serialVectorRead(dest, ranges);
    }
  }

  private long parallelVectorRead(HostMemoryBuffer dest, List<FileRangeWithOffset> ranges) throws IOException {

    List<FileRangeWithOffset> coalescedRanges = FileRangeWithOffset.coalesce(ranges,
        config.getParallelVectorReadMaxChunkSize());
    List<Future<Integer>> tasks = new ArrayList<>(coalescedRanges.size());

    ExecutorService service = RapidsFileIOExecutor.getExecutor(config.getParallelVectorReadThreads());

    long bytesCopied = 0L;

    for (FileRangeWithOffset range : coalescedRanges) {
      tasks.add(service.submit(() -> {
        try (FSDataInputStream fin = fs.open(filePath)) {
          // Read the data into the destination buffer
          readFully(fin, range.getStartPos(), range.getLength(), dest, range.getDestOffset());
          return range.getLength();
        } catch (IOException e) {
          throw new RuntimeException("Error reading from file: " + filePath, e);
        }
      }));
    }

    for (Future<Integer> task : tasks) {
      try {
        int bytesRead = task.get();
        bytesCopied += bytesRead;
      } catch (Exception e) {
        throw new IOException("Error reading from file: " + filePath, e);
      }
    }

    return bytesCopied;
  }

  private long serialVectorRead(HostMemoryBuffer dest, List<FileRangeWithOffset> ranges) throws IOException {
    Objects.requireNonNull(dest, "Destination buffer cannot be null");
    Objects.requireNonNull(ranges, "Ranges cannot be null");
    checkArgument(!ranges.isEmpty(), "Ranges cannot be empty");

    // Coalesce the ranges to avoid redundant reads
    List<FileRangeWithOffset> coalescedRanges = FileRangeWithOffset.coalesce(ranges, Integer.MAX_VALUE);

    long bytesCopied = 0L;

    try (FSDataInputStream fin = fs.open(filePath)) {
      for (FileRangeWithOffset range : coalescedRanges) {
        // Read the data into the destination buffer
        readFully(fin, range.getStartPos(), range.getLength(), dest, range.getDestOffset());
        bytesCopied += range.getLength();
      }

      return bytesCopied;
    }
  }

  private static void readFully(FSDataInputStream fin, long srcPos,
      int len, HostMemoryBuffer dest, long destPos) throws IOException {
    int remaining = len;
    byte[] buffer = bufferHolder.get();
    while (remaining > 0) {
      int alreadyRead = (len - remaining);
      int toRead = Math.min(remaining, buffer.length);
      fin.readFully(srcPos + alreadyRead, buffer, 0, toRead);
      dest.setBytes(destPos + alreadyRead, buffer, 0, toRead);
      remaining -= toRead;
    }
  }
}

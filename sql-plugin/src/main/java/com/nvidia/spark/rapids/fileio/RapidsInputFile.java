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

package com.nvidia.spark.rapids.fileio;

import ai.rapids.cudf.HostMemoryBuffer;

import java.io.IOException;
import java.util.List;

/**
 * Represents an input file that can be read from.
 * <br/>
 * The implementation of this interface should be thread-safe.
 */
public interface RapidsInputFile {
    /**
     * Get the length of the file in bytes.
     * @return the length of the file in bytes
     * @throws IOException if an I/O error occurs while getting the length
     */
    long getLength() throws IOException;

    /**
     * Open the file for reading.
     * @return a {@link SeekableInputStream } to read from the file
     * @throws IOException if an I/O error occurs while opening the file
     */
    SeekableInputStream open() throws IOException;

    /**
     * Read the last `length` bytes from the file into a {@link HostMemoryBuffer}.
     * <br/>
     * This method blocks until the end of the stream is reached.
     * <br/>
     *
     * @param length the number of bytes to read from the end of the file, must be positive.
     * @return a {@link HostMemoryBuffer} containing the last `length` bytes
     * @throws IOException if an I/O error occurs while reading the file
     */
    HostMemoryBuffer tailRead(long length) throws IOException;

    /**
     * Read the specified ranges from the file into a {@link HostMemoryBuffer}.
     * <br/>
     * This method blocks until all ranges are read.
     * <br/>
     *
     * @param dest the destination buffer to read data into
     * @param ranges a list of {@link FileRangeWithOffset} specifying the ranges to read
     * @throws IOException if an I/O error occurs while reading the file
     */
    long vectorRead(HostMemoryBuffer dest, List<FileRangeWithOffset> ranges) throws IOException;
}

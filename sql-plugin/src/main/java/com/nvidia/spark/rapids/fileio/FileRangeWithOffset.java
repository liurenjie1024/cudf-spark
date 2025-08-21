package com.nvidia.spark.rapids.fileio;


import ai.rapids.cudf.HostMemoryBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;

/**
 * Represents a range of bytes in a file with an associated destination offset.
 * This is used to specify a portion of a file to be read, along with
 * the offset where the data should be placed in the destination buffer.
 * <br/>
 * See {@link RapidsInputFile#vectorRead(HostMemoryBuffer, List)} for usage.
 */
public class FileRangeWithOffset {
  private final long startPos;
  private final int length;
  private final long destOffset;

  public FileRangeWithOffset(long startPos, int length, long destOffset) {
    checkArgument(startPos >= 0, "startPos must be non-negative");
    checkArgument(length > 0, "length must be positive");
    checkArgument(destOffset >= 0, "destOffset must be non-negative");
    this.startPos = startPos;
    this.length = length;
    this.destOffset = destOffset;
  }

  /**
   * Get the starting position of the range in the file.
   * @return the starting position in bytes
   */
  public long getStartPos() {
    return startPos;
  }

  /**
   * Get the length of the range in bytes.
   * @return the length of the range
   */
  public int getLength() {
    return length;
  }

  /**
   * Get the offset in the destination buffer where the data should be placed.
   * @return the destination offset in bytes
   */
  public long getDestOffset() {
    return destOffset;
  }

  /**
   * Get the end position of the range in the input file.
   * @return the end position in bytes
   */
  public long endOfRead() {
    return startPos + length;
  }

  /**
   * Get the end position of the range in the destination buffer.
   * @return the end position in bytes
   */
  public long endOfWrite() {
    return destOffset + length;
  }



  /**
   * Coalesce a list of {@link FileRangeWithOffset} ranges into a new list.
   * <br/>
   * This method combines adjacent ranges into a single range.
   *
   * @param ranges the list of ranges to coalesce
   * @return a new list containing the coalesced ranges
   */
  public static List<FileRangeWithOffset> coalesce(List<FileRangeWithOffset> ranges,
      int maxChunkSize) {
    Objects.requireNonNull(ranges, "ranges cannot be null");
    checkArgument(!ranges.isEmpty(), "ranges cannot be empty");

    List<FileRangeWithOffset> coalesced = new ArrayList<>(ranges.size());

    FileRangeWithOffset current = null;
    for (FileRangeWithOffset range : ranges) {
      if (current == null) {
        current = range;
      } else if (current.endOfRead() == range.getStartPos() &&
                 current.endOfWrite() == range.getDestOffset()) {

        int newLen = toIntExact(current.getLength() + range.getLength());
        if (newLen > maxChunkSize) {
          coalesced.add(current);
          current = range;
        } else {
          // Coalesce adjacent ranges
          current = new FileRangeWithOffset(current.getStartPos(),
              newLen,
              current.getDestOffset());
        }
      } else {
        // Add the current range to the coalesced list and move to the next
        coalesced.add(current);
        current = range;
      }
    }

    coalesced.add(current);
    return coalesced;
  }
}

package com.nvidia.spark.rapids.fileio.hadoop;

import java.io.Serializable;

public class HadoopFileIOConfig implements Serializable {
  private final boolean parallelVectorReadEnabled;
  private final int parallelVectorReadThreads;
  private final int parallelVectorReadMaxChunkSize;

  public HadoopFileIOConfig(boolean parallelVectorReadEnabled, int parallelVectorReadThreads,
      int parallelVectorReadMaxChunkSize) {
    this.parallelVectorReadEnabled = parallelVectorReadEnabled;
    this.parallelVectorReadThreads = parallelVectorReadThreads;
    this.parallelVectorReadMaxChunkSize = parallelVectorReadMaxChunkSize;
  }

  public boolean isParallelVectorReadEnabled() {
    return parallelVectorReadEnabled;
  }

  public int getParallelVectorReadThreads() {
    return parallelVectorReadThreads;
  }

  public int getParallelVectorReadMaxChunkSize() {
    return parallelVectorReadMaxChunkSize;
  }
}

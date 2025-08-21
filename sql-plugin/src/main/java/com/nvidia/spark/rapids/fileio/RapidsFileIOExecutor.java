package com.nvidia.spark.rapids.fileio;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RapidsFileIOExecutor {
  private static volatile ExecutorService executor = null;

  public static ExecutorService getExecutor(int parallelism) {
    if (executor == null) {
      synchronized (RapidsFileIOExecutor.class) {
        if (executor == null) {
          executor = Executors.newFixedThreadPool(parallelism);
        }
      }
    }
    return executor;
  }
}

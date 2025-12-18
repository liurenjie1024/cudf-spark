package org.apache.spark.deploy

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging

object Conn extends Logging {
  def conn(): Unit = {
    val hadoopConf = org.apache.spark.deploy.SparkHadoopUtil.get.conf
    val path = new Path("hdfs://rl-r7525-d32-u38.raplab.nvidia.com:9000/data/")

    try {
      val fs = FileSystem.get(path.toUri, hadoopConf)
      logError(s"$path is dir: ${fs.getFileStatus(path).isDirectory}")
    } catch {
      case t: Exception =>
        logError("Failed to get status", t)
    }
  }
}

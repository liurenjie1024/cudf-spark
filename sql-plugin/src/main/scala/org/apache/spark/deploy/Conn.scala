package org.apache.spark.deploy

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging

object Conn extends Logging {
  def conn(): Unit = {
    val hadoopConf = org.apache.spark.deploy.SparkHadoopUtil.get.conf
    val path = new Path("hdfs://data/nds2.0")

    val fs = FileSystem.get(path.toUri, hadoopConf)
    logError(s"$path is dir: ${fs.getFileStatus(path).isDirectory}")
  }
}

package com.daniel.scala.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by daniel on 2020-04-14.
  **/
object HdfsUtils {

  private val conf: Configuration = new Configuration()

  conf.addResource("hdfs-site.xml")
  conf.addResource("core-site.xml" )

  private val fileSystem: FileSystem = FileSystem.get(conf)
  def fileExists(path:Path):Boolean = {
    fileSystem.exists(path)
  }

  def main(args: Array[String]): Unit = {
    val path = new Path("/")

//    println(fileSystem.getTrashRoot(path).toString)
  }

  fileSystem.close()
}

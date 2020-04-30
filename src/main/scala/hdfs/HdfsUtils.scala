package hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by daniel on 2020/4/27.
 **/


case class HdfsUtils()  {
  private val logger: Logger = LoggerFactory.getLogger(classOf[HdfsUtils])
  private val conf = new Configuration()
  conf.addResource("hdfs-site.xml")
  conf.addResource("core-site.xml")
  logger.info(conf.get("fs.defaultFS"))
  val keyTab = "/Users/daniel/IdeaProjects/FrommyMind/BigdataDemoCode/src/main/resources/daniel.keytab"
  val principal = "daniel@DANIEL.COM"
  if (conf.get("hadoop.security.authentication") == "kerberos"){
    UserGroupInformation.loginUserFromKeytab(principal, keyTab)
  }
  val fs: FileSystem = FileSystem.get(conf)

  /**
   * create file on hdfs
   * @param path the file will create
   * @param overWrite overwrite if file exists, default value is true
   */
  def createPath(path: String, overWrite: Boolean = true): Unit = {
    if (!overWrite && existPath(path)) {
      logger.info("File {} is exists. will not overwrite ", path)
    } else {
      fs.create(new Path(path), overWrite)
    }
  }

  /**
   * check file exist or not
   * @param path the file on HDFS
   * @return true if file exists on HDFS
   */
  def existPath(path: String): Boolean = {
    fs.exists(new Path(path))
  }


  /**
   * delete file if exists on HDFS
   * @param path the file path on HDFS
   * @param recursive default value is true
   */
  def deletePath(path: String, recursive:Boolean = true): Unit ={
    if(existPath(path)) {
      fs.delete(new Path(path), recursive)
    }else {
      logger.info("File {} does not exist",path)
    }
  }

  /**
   * create dir on HDFS
   * @param path the dir path
   */
  def mkdir(path: String): Unit = {
    if (!existPath(path)) {
      fs.mkdirs(new Path(path))
    }else {
      logger.info("Path {} already exist",path)
    }
  }

  def isFile(path:String): Boolean ={
    fs.isFile(new Path(path))
  }

  def isDirectory(path:String):Boolean ={
    fs.isDirectory(new Path(path))
  }
}
object HdfsUtils {

  def main(args: Array[String]): Unit = {
    HdfsUtils.apply().createPath("/user/daniel/test1")
    HdfsUtils.apply().createPath("/user/daniel/test1",false)
    HdfsUtils.apply().mkdir("/user/daniel/testFolder")
  }
}

package hdfs_hw

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object hdfs_hw {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def saveFile(in_filepath: String, out_filepath: String): Unit = {
    val in_file =  getFile(in_filepath)

    val out_file = new File(out_filepath)
    val out_path = new Path(out_file.getPath)

    val out = if (fileSystem.exists(out_path).equals(true)) {
      fileSystem.append(out_path)
    } else {
      fileSystem.create(out_path)
    }

    val in = new BufferedInputStream(in_file)
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)

    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

}

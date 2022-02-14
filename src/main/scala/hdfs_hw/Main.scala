package hdfs_hw

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.io.BufferedInputStream
import java.net.URI

object Main extends App {
  // конфигурация fs
  val uri = "hdfs://namenode:9000"
  val conf = new Configuration()
  conf.set("fs.defaultFS", uri)
  conf.set("dfs.replication", "1")
  conf.set("dfs.support.append", "true")
  val fs = FileSystem.get(new URI(uri), conf)

  // имена входных и выходных директорий
  val input_dir_name = "/stage/"
  val output_dir_name = "/ods/"
  val output_file_name = "part-0000.csv"

  //  получаем список всех поддиректорий в директориии /stage
  val input_subdir_list = fs.listStatus(new Path(input_dir_name)).map{b => b.getPath.getName}.toList

  // удаляем директорию /ods, чтобы не аппендить лишнее при перезапуске
  hdfs_hw.removeFile(output_dir_name)

  input_subdir_list.foreach {
    subdir => {
      val id = input_dir_name + subdir
      val od = output_dir_name + subdir

      // создаем директорию /ods
      hdfs_hw.createFolder(od)

      // получаем список csv-файлов в поддиректориях /stage
      val input_files = fs.listStatus(new Path(id)).flatMap { b =>
        if (b.getPath.getName.endsWith(".csv")) List(b.getPath.getName)
        else Nil
      }.toList

      // аппендим файлы в одну партицию в /ods
      input_files.foreach {
        input_filename => {
          hdfs_hw.saveFile(id + "/" + input_filename, od + "/" + output_file_name)
        }
      }

      // удаляем директорию /stage
      hdfs_hw.removeFile(input_dir_name)

    }
  }

}

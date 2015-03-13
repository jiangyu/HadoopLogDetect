package com.dj

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{RDD, NewHadoopRDD}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.Text
import java.util.Calendar
import java.text.SimpleDateFormat


/**
 * Created by jiangyu on 3/11/15.
 */
object Detecter {
  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      println("Wrong parameter!")
      System.exit(-2)
    }
    val reg = args(0)
    val dir = args(1).toString

    val conf =  new SparkConf().setAppName("Shuffle killer!")
    val sc = new SparkContext(conf)

    val inputFile = "hdfs://ns1/logs/*/logs/"+reg+"*"
    val files  =  sc.newAPIHadoopFile[LongWritable,Text,TextInputFormat](inputFile)
    val hadoopRDD: NewHadoopRDD[LongWritable, Text] = files.asInstanceOf[NewHadoopRDD[LongWritable,Text]]
    val fileAndLine = hadoopRDD.mapPartitionsWithInputSplit{ (split, iter) =>
      val file = split.asInstanceOf[FileSplit]
      iter.map(tpl => (file.getPath.toString,tpl._2.toString))
    }

//    implicit val caseInsensitiveOrdering = new Ordering[String] {
//      override def compare(x: String, y: String) = y.split("/")(6).split("_")(2).toLong.
//        compareTo(x.split("/")(6).split("_")(2).toLong)
//    }


    val calToday = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val lastFileName = format.format(calToday)

    val calYes = Calendar.getInstance()
    calYes.add(Calendar.DATE,-1)
    val yesterday = format.format(calYes.getTime)


    val pattern = ("^"+yesterday+".*(java.lang.OutOfMemoryError|ShuffleError).*").r.pattern
    val all = fileAndLine.filter{case(name,line) => pattern.matcher(line).matches}.repartition(1)
    all.saveAsTextFile("hdfs://ns1/user/mapred/error/"+lastFileName)
  }
}

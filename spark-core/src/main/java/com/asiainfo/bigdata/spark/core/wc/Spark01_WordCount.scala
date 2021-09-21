package com.asiainfo.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
    val lines: RDD[String] = sc.textFile("data")
    val words = lines.flatMap(_.split(" ")).map(x => (x, 1))
    //    val wordGroup = words.groupBy(x => x)
    //    val value = wordGroup.map {
    //      case (str, strings) => {
    //        (str, strings.size)
    //      }
    //    }
    val value: RDD[(String, Int)] = words.reduceByKey(_ + _);
    value.foreach(x => println(x))

    sc.stop()
  }

}

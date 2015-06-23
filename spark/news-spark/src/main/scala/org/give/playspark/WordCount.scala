package org.give.playspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 14-10-4.
 * this file is just for test
 */
object WordCount {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
        val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))
        val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
        System.out.println(wordCounts.collect().mkString(", "))
    }
}

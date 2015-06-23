package org.give.altc.features

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace

/**
 * Created by zjh on 15-4-13.
 */
object GenerateLRData {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))

        val offlinetrainfeaturedata = sc.textFile(PathNamespace.prefix + "offlinetrainfeatures")
        val offlinetestfeaturedata = sc.textFile(PathNamespace.prefix + "offlinetestfeatures")
        val onlinetrainfeaturedata = sc.textFile(PathNamespace.prefix + "onlinetrainfeatures")

        def generateLRData(data: org.apache.spark.rdd.RDD[String]): Unit = {
            data.map {
                record =>
                    val items = record.split(",")
                    val (useritemid, features) = (items(0), items(1))
                    
            }
        }
    }
}

package org.give.altc.subset

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.give.altc.PathNamespace

/**
 * Created by zjh on 15-4-16.
 */

//合并useritem user item
object MergeFeature {
    def merge(sc:org.apache.spark.SparkContext, useritemfeaturepath: String, userfeaturepath: String, itemfeaturepath: String): org.apache.spark.rdd.RDD[String] = {
        //21951546-127193531
        val useritemfeatureKeyByUser = sc.textFile(useritemfeaturepath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0).split("-")
                val (userid, itemid) = (useritemid(0), useritemid(1))
                val features = items(1)
                (userid, (itemid, features))
        }
        val userfeatureByUser = sc.textFile(userfeaturepath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val features = items(1)
                (userid, features)
        }
        val itemfeatureByItem = sc.textFile(itemfeaturepath).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                val features = items(1)
                (itemid, features)
        }

        useritemfeatureKeyByUser.join(userfeatureByUser).map {
            record =>
                val userid = record._1
                val itemid = record._2._1._1
                val useritemfeatures = record._2._1._2
                val userfeatures = record._2._2
                (itemid, (userid, useritemfeatures + "-" + userfeatures))
        }.join(itemfeatureByItem).map {
            record =>
                val itemid = record._1
                val userid = record._2._1._1
                val useritemuserfeatures = record._2._1._2
                val itemfeatures = record._2._2
                //(userid + "-" + itemid, useritemuserfeatures + "-" + itemfeatures)
                userid + "-" + itemid + "," + useritemuserfeatures + "-" + itemfeatures
        }
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        merge(sc, PathNamespace.offlineuseritemtrainfeaturesubdata, PathNamespace.offlineusertrainfeaturesubdata, PathNamespace.offlineitemtrainfeaturesubdata).coalesce(1).saveAsTextFile(PathNamespace.offlinealltrainfeaturesubdata)
        merge(sc, PathNamespace.offlineuseritemtestfeaturesubdata, PathNamespace.offlineusertestfeaturesubdata, PathNamespace.offlineitemtestfeaturesubdata).coalesce(1).saveAsTextFile(PathNamespace.offlinealltestfeaturesubdata)
        merge(sc, PathNamespace.onlineuseritemtrainfeaturesubdata, PathNamespace.onlineusertrainfeaturesubdata, PathNamespace.onlineitemtrainfeaturesubdata).coalesce(1).saveAsTextFile(PathNamespace.onlinealltrainfeaturesubdata)
    }
}

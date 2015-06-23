package org.give.altc.ultimate

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace

/**
 * Created by zjh on 15-4-16.
 */

//合并useritem user item
object MergeFeature {
    def merge(sc: org.apache.spark.SparkContext, useritemfeaturepath: String, userfeaturepath: String, itemfeaturepath: String): org.apache.spark.rdd.RDD[String] = {
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

    def mergeFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        val prefix: String = "hdfs://namenode:9000/givedata/altc/"
        val metadataprefix: String = prefix + "metadata/"

        val offlinetrainfeaturesubdata: String = metadataprefix + "offlinetrainfeaturesubdata"
        val offlinetestfeaturesubdata: String = metadataprefix + "offlinetestfeaturesubdata"
        val onlinetrainfeaturesubdata: String = metadataprefix + "onlinetrainfeaturesubdata"

        //useritem的特征数据
        val offlineuseritemtrainfeaturesubdata: String = prefix + "subsetold/offlinetrainfeatures"
        val offlineuseritemtestfeaturesubdata: String = prefix + "subsetold/offlinetestfeatures"
        val onlineuseritemtrainfeaturesubdata: String = prefix + "subsetold/onlinetrainfeatures"

        //user的特征数据
        val offlineusertrainfeaturesubdata: String = prefix + "subset/offlineusertrainfeaturesubdata"
        val offlineusertestfeaturesubdata: String = prefix + "subset/offlineusertestfeaturesubdata"
        val onlineusertrainfeaturesubdata: String = prefix + "subset/onlineusertrainfeaturesubdata"

        //item的特征数据
        val offlineitemtrainfeaturesubdata: String = prefix + "subset/offlineitemtrainfeaturesubdata"
        val offlineitemtestfeaturesubdata: String = prefix + "subset/offlineitemtestfeaturesubdata"
        val onlineitemtrainfeaturesubdata: String = prefix + "subset/onlineitemtrainfeaturesubdata"

        //useritem user item join在一起的所有的特征数据
        val offlinealltrainfeaturesubdata: String = prefix + "subset/offlinealltrainfeatures"
        val offlinealltestfeaturesubdata: String = prefix + "subset/offlinealltestfeatures"
        val onlinealltrainfeaturesubdata: String = prefix + "subset/onlinealltrainfeatures"

        merge(sc, offlineuseritemtrainfeaturesubdata, offlineusertrainfeaturesubdata, offlineitemtrainfeaturesubdata).coalesce(1).saveAsTextFile(offlinealltrainfeaturesubdata)
        merge(sc, offlineuseritemtestfeaturesubdata, offlineusertestfeaturesubdata, offlineitemtestfeaturesubdata).coalesce(1).saveAsTextFile(offlinealltestfeaturesubdata)
        merge(sc, onlineuseritemtrainfeaturesubdata, onlineusertrainfeaturesubdata, onlineitemtrainfeaturesubdata).coalesce(1).saveAsTextFile(onlinealltrainfeaturesubdata)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        mergeFeatureData(sc)
    }
}

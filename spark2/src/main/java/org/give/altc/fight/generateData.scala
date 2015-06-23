package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-18.
 */
object generateData {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"
    val metadataprefix: String = prefix + "metadata/"

    //useritem user item join在一起的所有的特征数据
    val offlinealltrainfeaturesubdata: String = prefix + "subset/offlinealltrainfeatures"
    val offlinealltestfeaturesubdata: String = prefix + "subset/offlinealltestfeatures"
    val onlinealltrainfeaturesubdata: String = prefix + "subset/onlinealltrainfeatures"

    //12.17的target数据
    val offlinetrainlabelsubdata: String = metadataprefix + "offlinetrainlabelsubdata"
    //12.18的target数据
    val offlinetestlabelsubdata: String = metadataprefix + "offlinetestlabelsubdata"

    val offlinetrainfeature = prefix + "subset/offlinetrainfeature"
    val offlinetestfeature = prefix + "subset/offlinetestfeature"

    val onlinetrainfeature = prefix + "subset/onlinetrainfeature"
    val onlinetestfeature = prefix + "subset/onlinetestfeature"

    def generateTrainData(sc: org.apache.spark.SparkContext, trainfeaturepath: String, labelpath: String, output: String): Unit = {
        //train
        val allfeatures = sc.textFile(trainfeaturepath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")
                (useritemid, features)
        }

        //3369
        val traindata = sc.textFile(labelpath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = items(2).toInt
                (userid + "-" + itemid, label)
        }.filter(_._2 == 4).distinct

        val b = allfeatures.leftOuterJoin(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2 match {
                    case Some(behavior) => "1"
                    case None => "0"
                }
                label + "," + useritem + "," + features
        }
        b.coalesce(1).saveAsTextFile(output)
    }

    def generateTestData(sc: org.apache.spark.SparkContext, testfeaturepath: String, output: String): Unit = {
        val d = sc.textFile(testfeaturepath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                useritemid + "," + features
        }

        d.coalesce(1).saveAsTextFile(output)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateTrainData(sc, offlinealltrainfeaturesubdata, offlinetrainlabelsubdata, offlinetrainfeature)
        //generateTrainData(sc, offlinealltrainfeaturesubdata, offlinetestlabelsubdata, offlinetrainfeature)
        generateTestData(sc, offlinealltestfeaturesubdata, offlinetestfeature)

        generateTrainData(sc, offlinealltestfeaturesubdata, offlinetestlabelsubdata, onlinetrainfeature)
        generateTestData(sc, onlinealltrainfeaturesubdata, onlinetestfeature)
    }
}

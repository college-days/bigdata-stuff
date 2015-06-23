package org.give.altc.observedata

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.give.altc.features.MergeFeatures
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-13.
 */
object ExportUserItemFeatureData {
    val userItemTrainFeaturePath = PathNamespace.prefix + "offlinetrainfeatures"
    val userItemTestFeaturePath =  PathNamespace.prefix + "offlinetestfeatures"

    val exporttraindataPath = PathNamespace.prefix + "/export/exportuseritemtrainlabel"
    val exporttestdataPath = PathNamespace.prefix + "/export/exportuseritemtestlabel"

    def generateLabeledDataNotSparse(sc: org.apache.spark.SparkContext, allfeaturedataPath: String, traindataPath: String, output: String): Unit = {
        val allfeatures = sc.textFile(allfeaturedataPath).map{
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1)
                (useritemid, features)
        }

        val traindata = sc.textFile(traindataPath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = MergeFeatures.getLabel(items(2).toInt)
                (userid + "-" + itemid, label)
        }

        allfeatures.join(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2
                label + "," + useritem + "," + features
        }.distinct().coalesce(1).saveAsTextFile(output)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateLabeledDataNotSparse(sc, userItemTrainFeaturePath, PathNamespace.offlinetrainlabeldata, exporttraindataPath)
        generateLabeledDataNotSparse(sc, userItemTestFeaturePath, PathNamespace.offlinetestlabeldata, exporttestdataPath)
    }
}

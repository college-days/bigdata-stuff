package org.give.altc.yaonline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{Cota, CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures
import org.apache.spark.SparkContext._

/**
  * Created by zjh on 15-4-13.
  */
object LRTrainPredict {
     val prefix: String = PathNamespace.prefix + "lrcleantha/"

     val useritemlabeledtraindataPath = prefix + "useritemlabeledtraindata"
     val useritemlabeledtestdataPath = prefix + "useritemlabeledtestdata"

     val offlinetestfeaturedataPath = PathNamespace.prefix + "offlinetestfeaturespart3"
     val onlinetestfeaturedataPath = PathNamespace.prefix + "onlinetestfeaturespart3"

    def generateLabeledDataWithSpecificFeature(sc: org.apache.spark.SparkContext, featureinputpath: String, traindatainputpath: String, output: String): Unit = {
        val allfeatures = sc.textFile(featureinputpath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                (useritemid, features)
        }.filter(_._2 != "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0")

        val traindata = sc.textFile(traindatainputpath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = items(2).toInt
                (userid + "-" + itemid, label)
        }.filter(_._2 == 4).distinct

        allfeatures.leftOuterJoin(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2 match {
                    case Some(behavior) => "1"
                    case None => "0"
                }
                label + "," + features
        }.coalesce(1).saveAsTextFile(output)
    }

     def generateTestFeatureDataWithSpecificFeature(sc: org.apache.spark.SparkContext, useritemFeaturePath: String, outputpath: String): Unit = {
         sc.textFile(useritemFeaturePath).map {
             record =>
                 val items = record.split(",")
                 val useritemid = items(0)
                 val features = items(1).replace("-", " ")

                 useritemid + "," + features
         }.coalesce(1).saveAsTextFile(outputpath)
     }

     def generateData(sc: org.apache.spark.SparkContext): Unit = {
         generateLabeledDataWithSpecificFeature(sc, offlinetestfeaturedataPath, PathNamespace.offlinetrainlabeldata, useritemlabeledtraindataPath)
         generateTestFeatureDataWithSpecificFeature(sc, onlinetestfeaturedataPath, useritemlabeledtestdataPath)
     }

     def trainAndPredict(sc: org.apache.spark.SparkContext): Unit = {
         val offlinelabeledtraindata = sc.textFile(useritemlabeledtraindataPath)

         //获取正样本数量 后面才能进行正负样本按比例采样
         val allcount = offlinelabeledtraindata.count
         val positivedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 1)
         val negativedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 0)
         val positivecount = positivedata.count
         val negativecount = negativedata.count

         val i = 10;
         val negativesamplecount = positivecount * i
         val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

         val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
         val finaltraindata = positivedata.union(samplenegativedata)

         val finaltraindatapath = prefix + "onlinefinaltraindatauseritem"
         finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)

         val joinpredictresultpath = prefix + "onlinepredictjoin"
         val nojoinpredictresultpath = prefix + "onlinepredictnojoin"
         //CommonAlgo.lrClassifierPredict(sc, finaltraindatapath, useritemlabeledtestdataPath, Cota.SUBMIT_COUNT, joinpredictresultpath, nojoinpredictresultpath)
     }

     def main(args: Array[String]): Unit = {
         val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
         generateData(sc)
         trainAndPredict(sc)
     }
 }

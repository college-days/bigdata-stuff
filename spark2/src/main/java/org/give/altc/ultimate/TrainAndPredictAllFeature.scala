package org.give.altc.ultimate

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.give.altc.ultimate.Algo._
import GenerateFeatures._
import MergeFeature._
import GenerateLRFeatures._

/**
 * Created by zjh on 15-4-17.
 */
object TrainAndPredictAllFeature {
    val prefix: String = PathNamespace.prefix + "ultimatelr/"
    val yaprefix: String = PathNamespace.prefix + "yaultimatelr/"

    val lrtraindatapath = prefix + "offlinelrfeature"
    val lrtestdatapath = prefix + "offlinelrtest"

    val targetdataPath = PathNamespace.prefix + "offlinetargetdatajoinitem"

    def generateTrainAndTestData(sc: org.apache.spark.SparkContext): Unit = {
        generateFeatureData(sc)
        mergeFeatureData(sc)
        //generateLRTrainFeature(sc, PathNamespace.offlinealltrainfeaturesubdata, PathNamespace.offlinetestlabelsubdata, lrtraindatapath)
        generateLRTrainFeature(sc, PathNamespace.offlinealltrainfeaturesubdata, PathNamespace.offlinetrainlabelsubdata, lrtraindatapath)
        generateLRTestFeature(sc, PathNamespace.offlinealltestfeaturesubdata, lrtestdatapath)
    }

    def trainAndPredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(lrtraindatapath)

        val targetresult = sc.textFile(targetdataPath)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        var offlinetrainjoinresult = List[String]()
        var offlinetrainnojoinresult = List[String]()
        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 5 to 20 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)

            val predictresult = offlineRFTest(sc, finaltraindatapath, lrtestdatapath, targetresult.count.toInt)

            predictresult.saveAsTextFile(yaprefix + "offlinepredictresult-" + i)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / targetresult.count.toFloat
            val recall = hitcount / predictresult.count.toFloat
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(yaprefix + "traintestresultuseritem")
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateTrainAndTestData(sc)
        //trainAndPredict(sc)
    }
}

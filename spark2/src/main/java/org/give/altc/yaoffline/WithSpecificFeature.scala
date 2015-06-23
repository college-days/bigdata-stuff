package org.give.altc.yaoffline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures

/**
 * Created by zjh on 15-4-11.
 */

//先不把所有特征都join到一起 先只使用useritem的特征试试看离线成绩
object WithSpecificFeature {
    //val prefix: String = PathNamespace.prefix + "jiayoujiayou/"
    //val yaprefix: String = PathNamespace.prefix + "yajiayoujiayou/"

    val prefix: String = PathNamespace.prefix + "jiayoujiayou3/"
    val yaprefix: String = PathNamespace.prefix + "yajiayoujiayou3/"

    //val userItemTrainFeaturePath = PathNamespace.prefix + "offlinetrainfeatures"
    //val userItemTestFeaturePath = PathNamespace.prefix + "offlinetestfeatures"

    val userItemTrainFeaturePath = PathNamespace.prefix + "offlinetrainfeaturespart3"
    val userItemTestFeaturePath =  PathNamespace.prefix + "offlinetestfeaturespart3"

    val useritemlabeledtraindataPath = prefix + "useritemlabeledtraindata"
    val useritemlabeledtestdataPath = prefix + "useritemlabeledtestdata"

    def generateFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        //分别生成带有label的训练数据和带有userid itemid标识的测试数据
        MergeFeatures.generateLabeledDataWithSpecificFeature(sc, userItemTrainFeaturePath, PathNamespace.offlinetrainlabeldata, useritemlabeledtraindataPath)
        MergeFeatures.generateTestFeatureDataWithSpecificFeature(sc, userItemTestFeaturePath, useritemlabeledtestdataPath)
    }

    def trainandpredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(useritemlabeledtraindataPath)

        val targetresult = TrainPredict.getOfflineTrainDataAfterJoinItem(sc)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 5 to 20 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
            val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt)
            predictresult.coalesce(1).saveAsTextFile(yaprefix + "redictresult" + i)

            //val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, labeledtestdataPath, 1000)
            //val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, labeledtestdataPath, 1000)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / (targetresult.count.toFloat)
            val recall = hitcount / (predictresult.count.toFloat)
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(yaprefix + "traintestresultuseritem")
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateFeatureData(sc)
        trainandpredict(sc)
    }
}

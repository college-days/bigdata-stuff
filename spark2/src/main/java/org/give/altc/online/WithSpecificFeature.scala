package org.give.altc.online

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{Cota, CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures

/**
 * Created by zjh on 15-4-12.
 */
object WithSpecificFeature {
    val userItemTrainFeaturePath = PathNamespace.prefix + "offlinetestfeatures"
    val userItemTestFeaturePath = PathNamespace.prefix + "onlinetrainfeatures"

    val finaltraindataPath = PathNamespace.cleanthaprefix + "finaltraindata"

    val useritemlabeledtraindataPath = PathNamespace.cleanthaprefix + "useritemlabeledtraindata"
    val useritemlabeledtestdataPath = PathNamespace.cleanthaprefix + "useritemlabeledtestdata"
    val submitdataPath = PathNamespace.cleanthaprefix + "cleantha"

    def generateFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        //分别生成带有label的训练数据和带有userid itemid标识的测试数据
        MergeFeatures.generateLabeledDataWithSpecificFeature(sc, userItemTrainFeaturePath, PathNamespace.offlinetestlabeldata, useritemlabeledtraindataPath)
        MergeFeatures.generateTestFeatureDataWithSpecificFeature(sc, userItemTestFeaturePath, useritemlabeledtestdataPath)
    }

    def trainandpredict(sc: org.apache.spark.SparkContext): Unit = {
        val labeledtraindata = sc.textFile(useritemlabeledtraindataPath)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = labeledtraindata.count
        val positivedata = labeledtraindata.filter(_.split(" ")(0).toInt == 1)
        val negativedata = labeledtraindata.filter(_.split(" ")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        val samplevalue = 10
        val negativesamplecount = positivecount * samplevalue
        val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

        val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
        val finaltraindata = positivedata.union(samplenegativedata)

        finaltraindata.coalesce(1).saveAsTextFile(finaltraindataPath)
    }

    def trainAndPredict(sc: org.apache.spark.SparkContext): Unit = {
        val predictresult = CommonAlgo.rfClassifierPredict(sc, finaltraindataPath, useritemlabeledtestdataPath, Cota.SUBMIT_COUNT, submitdataPath)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateFeatureData(sc)
        trainandpredict(sc)
        trainAndPredict(sc)
    }
}

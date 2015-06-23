package org.give.altc.online

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{Cota, CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-10.
 */
object Cleantha {
    val trainorigindataPath = PathNamespace.offlinefeaturedata
    val userItemFeatureTrainPath = PathNamespace.cleanthaprefix + "useritemfeaturestrain"
    val userFeatureTrainPath = PathNamespace.cleanthaprefix + "userfeaturestrain"
    val itemFeatureTrainPath = PathNamespace.cleanthaprefix + "itemfeaturestrain"

    val labeledtraindataPath = PathNamespace.cleanthaprefix + "labeledtraindata"
    val labeleddatajoinitemdataPath = PathNamespace.cleanthaprefix + "labeleddatajoinitemdata"
    val finaltraindataPath = PathNamespace.cleanthaprefix + "finaltraindata"
    val finaltraindatajoinitemdataPath = PathNamespace.cleanthaprefix + "finaltraindatajoinitemdata"

    val alltrainfeaturePath = PathNamespace.cleanthaprefix + "alltrainfeature"

    val testorigindataPath = PathNamespace.onlinefeaturedata
    val userItemFeatureTestPath = PathNamespace.cleanthaprefix + "useritemfeaturestest"
    val userFeatureTestPath = PathNamespace.cleanthaprefix + "userfeaturestest"
    val itemFeatureTestPath = PathNamespace.cleanthaprefix + "itemfeaturestest"

    val predictdataPath = PathNamespace.cleanthaprefix + "predictdata"
    val predictdatajoinitemdataPath = PathNamespace.cleanthaprefix + "predictdatajoinitemdata"

    val submitdataPath = PathNamespace.cleanthaprefix + "cleantha"
    val joinitemdatasubmitdataPath = PathNamespace.cleanthaprefix + "joinitemdatacleantha"

    //用1118-1217生成训练特征数据
    //用1119-1218生成预测特征数据
    //训练特征数据还需要以1:10的比例来进行正负样本个数采样生成最终的训练数据集
    def generateData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateRawFeatures(sc, trainorigindataPath, userItemFeatureTrainPath, userFeatureTrainPath, itemFeatureTrainPath)
        MergeFeatures.generateLabeledData(sc, userItemFeatureTrainPath, userFeatureTrainPath, itemFeatureTrainPath, PathNamespace.offlinetraindata, labeledtraindataPath)

        MergeFeatures.generateRawFeatures(sc, testorigindataPath, userItemFeatureTestPath, userFeatureTestPath, itemFeatureTestPath)
        MergeFeatures.generateTestFeatureData(sc, userItemFeatureTestPath, userFeatureTestPath, itemFeatureTestPath, predictdataPath)

        val labeledtraindata = sc.textFile(labeledtraindataPath)

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
        val predictresult = CommonAlgo.rfClassifierPredict(sc, finaltraindataPath, predictdataPath, Cota.SUBMIT_COUNT, submitdataPath)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //generateData(sc)
        trainAndPredict(sc)
    }
}

package org.give.altc.online

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.features.MergeFeatures
import org.give.altc.{Cota, PathNamespace, CommonAlgo, CommonOP}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-7.
 */

//提交数据需要和itemdatajoin一下 来提交子集 因为最终预测的itemid都是只出现在itemdata数据表中的
object TrainPredict {
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

    def generateData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateRawFeatures(sc, trainorigindataPath, userItemFeatureTrainPath, userFeatureTrainPath, itemFeatureTrainPath)
        MergeFeatures.generateLabeledData(sc, userItemFeatureTrainPath, userFeatureTrainPath, itemFeatureTrainPath, PathNamespace.offlinetraindata, labeledtraindataPath)

        MergeFeatures.generateRawFeatures(sc, testorigindataPath, userItemFeatureTestPath, userFeatureTestPath, itemFeatureTestPath)
        MergeFeatures.generateTestFeatureData(sc, userItemFeatureTestPath, userFeatureTestPath, itemFeatureTestPath, predictdataPath)

        val labeledtraindata = sc.textFile(labeledtraindataPath)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = labeledtraindata.count
        val positivedata = labeledtraindata.filter(_.split(" ")(0).toInt == 1)
        val positivecount = positivedata.count

        val samplevalue = 10
        val negativecount = positivecount * samplevalue
        val sampleproportion = negativecount.toFloat / allcount.toFloat

        val negativedata = labeledtraindata.sample(false, sampleproportion, 11L)
        val finaltraindata = positivedata.union(negativedata)

        finaltraindata.coalesce(1).saveAsTextFile(finaltraindataPath)
    }

    def trainAndPredict(sc: org.apache.spark.SparkContext): Unit = {
        val predictresult = CommonAlgo.rfClassifierPredict(sc, finaltraindataPath, predictdataPath, Cota.SUBMIT_COUNT, submitdataPath)
    }

    //用11.18-12.17的数据训练完特征之后
    //这些特征中提取与itemdata中的itemid相交的部分来做训练
    //特征数据信息不丢失 但是训练集减小范围 去掉噪音
    def getAllTrainFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateTestFeatureData(sc, userItemFeatureTrainPath, userFeatureTrainPath, itemFeatureTrainPath, alltrainfeaturePath)
    }

    def joinItemData(sc: org.apache.spark.SparkContext, origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] ={
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val origindataByKey = origindata.map {
            record =>
                val itemid = record.split(" ")(0).split("-")(1)
                (itemid, record)
        }

        origindataByKey.join(itemdata).map(_._2._1)
    }

    def joinAllTrainDataWithItemData(sc: org.apache.spark.SparkContext): Unit = {
        val alltrainfeatureData = joinItemData(sc, sc.textFile(alltrainfeaturePath)).map {
            record =>
                val useritemid = record.split(" ")(0)
                val features = record.split(" ").drop(1).mkString(" ")
                (useritemid, features)
        }

        val traindata = sc.textFile(PathNamespace.offlinetraindata).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = MergeFeatures.getLabel(items(2).toInt)
                (userid + "-" + itemid, label)
        }

        alltrainfeatureData.join(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2
                label + " " + features
        }.coalesce(1).saveAsTextFile(labeleddatajoinitemdataPath)

        val labeledtraindata = sc.textFile(labeleddatajoinitemdataPath)

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

        finaltraindata.coalesce(1).saveAsTextFile(finaltraindatajoinitemdataPath)
    }

    def joinPredictDataWithItemData(sc: org.apache.spark.SparkContext): Unit = {
        val predictDataJoinItemData = joinItemData(sc, sc.textFile(predictdataPath))
        predictDataJoinItemData.coalesce(1).saveAsTextFile(predictdatajoinitemdataPath)
    }

    def predictJoinItemData(sc: org.apache.spark.SparkContext): Unit = {
        CommonAlgo.simplePredict(sc, finaltraindatajoinitemdataPath, predictdatajoinitemdataPath, joinitemdatasubmitdataPath)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //generateData(sc)
        //trainAndPredict(sc)
        //getAllTrainFeatureData(sc)
        //joinAllTrainDataWithItemData(sc)
        //joinPredictDataWithItemData(sc)
        predictJoinItemData(sc)
    }
}

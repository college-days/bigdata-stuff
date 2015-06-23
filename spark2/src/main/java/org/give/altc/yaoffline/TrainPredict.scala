package org.give.altc.yaoffline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-11.
 */
object TrainPredict {
    val prefix: String = PathNamespace.prefix + "jiayoujiayou/"
    val yaprefix: String = PathNamespace.prefix + "yajiayoujiayou/"

    val userItemTrainFeaturePath = prefix + "useritemtrainfeatures"
    val userTrainFeaturePath = prefix + "usertrainfeatures"
    val itemTrainFeaturePath = prefix + "itemtrainfeatures"
    val userItemTestFeaturePath = prefix + "useritemtestfeatures"
    val userTestFeaturePath = prefix + "usertestfeatures"
    val itemTestFeaturePath = prefix + "itemtestfeatures"

    val labeledtraindataPath = prefix + "labeledtraindata"
    val labeledtestdataPath = prefix + "labeledtestdata"

    def getOfflineTrainDataAfterJoinItem(sc: org.apache.spark.SparkContext): org.apache.spark.rdd.RDD[String] = {
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val targetresult = sc.textFile(PathNamespace.offlinetestlabeldata).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior) = (items(0), items(1), items(2))
                (itemid, userid + "," + itemid + "-" + behavior.toInt)
        }

        //473个结果 1218号与item表join以后会购买的pair对为473 这个是离线进行f1计算的target数据集合
        val result = targetresult.join(itemdata).map {
            record =>
                val items = record._2._1.split("-")
                val (useritemid, behavior) = (items(0), items(1))
                (useritemid, behavior)
        }.filter(_._2.toInt == 4).map(_._1).distinct

        //count -> 473
        result
    }

    def generateFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateRawFeatures(sc, PathNamespace.offlinetrainfeaturedata, userItemTrainFeaturePath, userTrainFeaturePath, itemTrainFeaturePath)
        MergeFeatures.generateRawFeatures(sc, PathNamespace.offlinetestfeaturedata, userItemTestFeaturePath, userTestFeaturePath, itemTestFeaturePath)

        //分别生成带有label的训练数据和带有userid itemid标识的测试数据
        MergeFeatures.generateLabeledData(sc, userItemTrainFeaturePath, userTrainFeaturePath, itemTrainFeaturePath, PathNamespace.offlinetrainlabeldata, labeledtraindataPath)
        MergeFeatures.generateTestFeatureData(sc, userItemTestFeaturePath, userTestFeaturePath, itemTestFeaturePath, labeledtestdataPath)
    }

    def trainandpredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(labeledtraindataPath)

        val targetresult = getOfflineTrainDataAfterJoinItem(sc)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 5 to 20 by 3) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindatagbrt1000" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
            //val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, labeledtestdataPath, targetresult.count.toInt)
            //val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, labeledtestdataPath, 1000)
            val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, labeledtestdataPath, 1000)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / (targetresult.count.toFloat)
            val recall = hitcount / (predictresult.count.toFloat)
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(yaprefix + "traintestresultgbrt1000")
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //generateFeatureData(sc)
        trainandpredict(sc)
    }
}

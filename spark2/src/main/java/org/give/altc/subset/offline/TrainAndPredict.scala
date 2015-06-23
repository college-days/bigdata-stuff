package org.give.altc.subset.offline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures
import org.give.altc.yaoffline.TrainPredict
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-15.
 */
object TrainAndPredict {
    val prefix: String = PathNamespace.prefix + "offlinesubset/"
    val yaprefix: String = PathNamespace.prefix + "yaofflinesubset/"

    val userItemTrainFeaturePath = PathNamespace.prefix + "subset/offlinetrainfeatures"
    val userItemTestFeaturePath =  PathNamespace.prefix + "subset/offlinetestfeatures"

    val useritemlabeledtraindataPath = prefix + "useritemlabeledtraindata"
    val useritemlabeledtestdataPath = prefix + "useritemlabeledtestdata"

    val targetdataPath = PathNamespace.prefix + "offlinetargetdatajoinitem"

    def getOfflineTrainDataAfterJoinItem(sc: org.apache.spark.SparkContext): Unit = {
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val targetresult = sc.textFile(PathNamespace.offlinetestlabelsubdata).map {
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
        result.saveAsTextFile(targetdataPath)
    }

    def generateLabeledDataWithSpecificFeature(sc: org.apache.spark.SparkContext, featureinputpath: String, traindatainputpath: String, output: String): Unit = {
        val allfeatures = sc.textFile(featureinputpath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                (useritemid, features)
        }

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
                (label, features)
        }.map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(output)

        /*.filter(e => !(e._1.toInt == 1 && e._2 == "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"))*/
    }

    def generateLabeledDataWithSpecificFeatureWithUserItem(sc: org.apache.spark.SparkContext, featureinputpath: String, traindatainputpath: String): org.apache.spark.rdd.RDD[String] = {
        val allfeatures = sc.textFile(featureinputpath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                (useritemid, features)
        }
        //.filter(_._2 != "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0")

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
                label + "," + useritem + "," + features
        }//.filter(e => !(e.split(",")(0).toInt == 1 && e.split(",")(2) == "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"))//.coalesce(1).saveAsTextFile(output)
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

    def generateFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        generateLabeledDataWithSpecificFeature(sc, userItemTrainFeaturePath, PathNamespace.offlinetrainlabelsubdata, useritemlabeledtraindataPath)
        generateTestFeatureDataWithSpecificFeature(sc, userItemTestFeaturePath, useritemlabeledtestdataPath)
    }

    def trainandpredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(useritemlabeledtraindataPath)

        val targetresult = TrainPredict.getOfflineTrainDataAfterJoinItem(sc)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 10 to 10 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
            //val predictresult = CommonAlgo.offlineLRTest(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt)
            val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt)
            predictresult.coalesce(1).saveAsTextFile(yaprefix + "predictresult" + i)

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
        //generateFeatureData(sc)
        trainandpredict(sc)
    }
}

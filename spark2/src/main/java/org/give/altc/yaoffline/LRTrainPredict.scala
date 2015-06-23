package org.give.altc.yaoffline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-13.
 */
object LRTrainPredict {
    val prefix: String = PathNamespace.prefix + "lrjiayoujiayou3/"
    val yaprefix: String = PathNamespace.prefix + "lryajiayoujiayou3/"

    /*val userItemTrainFeaturePath = PathNamespace.prefix + "offlinetrainfeaturespart3"
    val userItemTestFeaturePath =  PathNamespace.prefix + "offlinetestfeaturespart3"*/

    val useritemlabeledtraindataPath = prefix + "useritemlabeledtraindata"
    //val useritemlabeledtraindatawithuseritemPath = prefix + "useritemlabeledtraindatawithuseritem"
    val useritemlabeledtestdataPath = prefix + "useritemlabeledtestdata"

    val useritemlabeledtraindatawithuseritemPath = prefix + "useritemlabeledtraindatawithuseritempart3"
    val useritemlabeledtraindatawithuseritemPositivePath = prefix + "useritemlabeledtraindatawithuseritempart3positive"
    val useritemlabeledtraindatawithuseritemNegativePath = prefix + "useritemlabeledtraindatawithuseritempart3negative"

    val offlinetrainfeaturedataPath = PathNamespace.prefix + "offlinetrainfeaturespart3"
    val offlinetestfeaturedataPath = PathNamespace.prefix + "offlinetestfeaturespart3"

    //val offlinetrainfeaturedataPath = PathNamespace.prefix + "offlinetrainfeatures"
    //val offlinetestfeaturedataPath = PathNamespace.prefix + "offlinetestfeatures"

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
        }.filter(e => !(e._1.toInt == 1 && e._2 == "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"))
            .map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(output)
    }

    def generateLabeledDataWithSpecificFeatureWithUserItem(sc: org.apache.spark.SparkContext, featureinputpath: String, traindatainputpath: String): org.apache.spark.rdd.RDD[String] = {
        val allfeatures = sc.textFile(featureinputpath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                (useritemid, features)
        }//.filter(_._2 != "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0")

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
        }.filter(e => !(e.split(",")(0).toInt == 1 && e.split(",")(2) == "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"))//.coalesce(1).saveAsTextFile(output)
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
        generateLabeledDataWithSpecificFeature(sc, offlinetrainfeaturedataPath, PathNamespace.offlinetrainlabeldata, useritemlabeledtraindataPath)
        generateTestFeatureDataWithSpecificFeature(sc, offlinetestfeaturedataPath, useritemlabeledtestdataPath)
        val data = generateLabeledDataWithSpecificFeatureWithUserItem(sc, offlinetrainfeaturedataPath, PathNamespace.offlinetrainlabeldata)
        data.filter(_.split(",")(0).toInt == 1).coalesce(1).saveAsTextFile(useritemlabeledtraindatawithuseritemPositivePath)
        data.filter(_.split(",")(0).toInt == 0).coalesce(1).saveAsTextFile(useritemlabeledtraindatawithuseritemNegativePath)
    }

    def trainAndPredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(useritemlabeledtraindataPath)

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
        for (i <- 10 to 10 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)

            /*val joinpredictresultpath = yaprefix + "offlinepredictjoin" + "-" + i
            val nojoinpredictresultpath = yaprefix + "offlinepredictnojoin" + "-" + i
            CommonAlgo.lrClassifierPredict(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt, joinpredictresultpath, nojoinpredictresultpath)

            def calcf1(path: String): String = {
                val data = sc.textFile(path)

                val hitcount = targetresult.intersection(data).count.toFloat
                val precision = hitcount / (targetresult.count.toFloat)
                val recall = hitcount / (data.count.toFloat)
                val f1 = 2 * precision * recall / (precision + recall)

                "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
            }
            offlinetrainjoinresult = offlinetrainjoinresult :+ calcf1(joinpredictresultpath)
            offlinetrainnojoinresult = offlinetrainnojoinresult :+ calcf1(nojoinpredictresultpath)*/

            //val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt).distinct
            //val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, offlineprefix + "offlinesubmitdata" + trainweight)

            /*val predictresult = CommonAlgo.offlineLRTest(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt)

            predictresult.saveAsTextFile(yaprefix + "offlinepredictresult-" + i)

            //val targetresult = getOfflineTrainDataAfterJoinItem(sc)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / targetresult.count.toFloat
            val recall = hitcount / predictresult.count.toFloat
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1*/
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(yaprefix + "traintestresultuseritem")
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //getOfflineTrainDataAfterJoinItem(sc)
        generateData(sc)
        trainAndPredict(sc)
    }
}

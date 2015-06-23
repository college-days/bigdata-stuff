package org.give.altc.offline

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.features.MergeFeatures
import org.give.altc.{CommonAlgo, PathNamespace}

/**
 * Created by zjh on 15-4-10.
 */

//离线再来一遍
object Cleantha {
    val prefix: String = PathNamespace.prefix + "jiayoujiayou/"
    val yaprefix: String = PathNamespace.prefix + "yajiayoujiayou/"
    val trainweight: Double = 0.7

    val origindataPath = PathNamespace.offlinefeaturedata
    val userItemFeaturePath = prefix + "useritemfeatures"
    val userFeaturePath = prefix + "userfeatures"
    val itemFeaturePath = prefix + "itemfeatures"

    val offlinetraindataPath = prefix + "traindata" + trainweight
    val offlinetestdataPath = prefix + "testdata" + trainweight

    val labeledtraindataPath = prefix + "labeledtraindata" + trainweight
    val labeledtestdataPath = prefix + "labeledtestdata" + trainweight
    val yalabeledtestdataPath = prefix + "yalabeledtestdata" + trainweight
    val yalabeledtraindataPath = prefix + "yalabeledtraindata" + trainweight

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

        val targetresult = sc.textFile(PathNamespace.offlinetraindata).map {
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
        MergeFeatures.generateRawFeatures(sc, origindataPath, userItemFeaturePath, userFeaturePath, itemFeaturePath)

        val offlinefeaturedata = sc.textFile(userItemFeaturePath)

        //7:3将11.18-12.17的数据中的useriditemid对分为两部分
        val testweight = 1 - trainweight
        val splitdata = offlinefeaturedata.randomSplit(Array(trainweight, testweight))
        val (traindata, testdata) = (splitdata(0), splitdata(1))

        traindata.coalesce(1).saveAsTextFile(offlinetraindataPath)
        testdata.coalesce(1).saveAsTextFile(offlinetestdataPath)

        //分别生成带有label的训练数据和带有userid itemid标识的测试数据
        MergeFeatures.generateLabeledData(sc, offlinetraindataPath, userFeaturePath, itemFeaturePath, PathNamespace.offlinetraindata, labeledtraindataPath)
        MergeFeatures.generateTestFeatureData(sc, offlinetestdataPath, userFeaturePath, itemFeaturePath, labeledtestdataPath)
    }

    //7 3分的3的testdata和itemdata相交和offlinetraindata相交得到训练预测理想的预测值
    def generateTestLabeledData(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
        //貌似应该和itemdata的子集相交一下
        //而且还是要加上userid itemid的好么 不然没法愉快的intersection啊 交出来的都是0
        val allfeatures = sc.textFile(input).map {
            record =>
                val itemid = record.split(",")(0).split("-")(1)
                (itemid, record)
        }

        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val finalAllFeatures = allfeatures.join(itemdata).map(_._2._1).map {
            record =>
                val items = record.split(",")
                val (useritemid, features) = (items(0), items(1))
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

        val result = finalAllFeatures.join(traindata).map {
            record =>
                val useritem = record._1.replace("-", ",")
                val features = record._2._1
                val label = record._2._2
                (label, useritem)
        }.filter(_._1.toInt == 1).map(_._2)

        result.coalesce(1).saveAsTextFile(output)
    }

    def trainandpredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(labeledtraindataPath)
        val yaofflinelabledtestdata = sc.textFile(yalabeledtestdataPath)

        val targetresult = yaofflinelabledtestdata

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 5 to 20) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindata" + trainweight + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
            val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, labeledtestdataPath, targetresult.count.toInt)
            //val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, offlineprefix + "offlinesubmitdata" + trainweight)

            //val targetresult = getOfflineTrainDataAfterJoinItem(sc)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / (targetresult.count.toFloat)
            val recall = hitcount / (predictresult.count.toFloat)
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ trainweight + "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(yaprefix + "traintestresult" + trainweight)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //generateFeatureData(sc)
        //generateTestLabeledData(sc, offlinetestdataPath, yalabeledtestdataPath)
        //generateTestLabeledData(sc, offlinetraindataPath, yalabeledtraindataPath)
        trainandpredict(sc)
    }
}

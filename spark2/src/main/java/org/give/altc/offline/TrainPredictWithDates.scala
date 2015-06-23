package org.give.altc.offline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.CommonOP._
import org.give.altc.{CommonAlgo, PathNamespace}
import org.give.altc.features.MergeFeatures

/**
 * Created by zjh on 15-4-9.
 */

//之前训练数据都是从1118-1217或者从1119-1218时间跨度很大 噪音也大 有些较早的数据其实对最后一天的预测影响不大所以可以缩短训练数据的时间范围
object TrainPredictWithDates {
    val startdate: Long = 20141204
    val enddate: Long = 20141217
    val trainweight: Double = 0.7
    //val pathprefix: String = PathNamespace.prefix + "offline" + trainweight + "@" + startdate + "-" + enddate + "/"
    //val yapathprefix: String = PathNamespace.prefix + "yaoffline" + trainweight + "@" + startdate + "-" + enddate + "/"
    val pathprefix: String = PathNamespace.prefix + "offline" + trainweight + "/"
    val yapathprefix: String = PathNamespace.prefix + "yaoffline" + trainweight + "/"

    val offlinedataPath: String = pathprefix + "offlinefeaturedata"

    val userItemFeaturePath = pathprefix + "useritemfeatures"
    val userFeaturePath = pathprefix + "userfeatures"
    val itemFeaturePath = pathprefix + "itemfeatures"

    val offlinetraindataPath = pathprefix + "traindata"
    val offlinetestdataPath = pathprefix + "testdata"

    //带有flag的训练数据
    val labeledTrainDataPath = pathprefix + "labeledtraindata"
    val labeledTestDataPath = pathprefix + "labeledtestdata"

    val traintestResultPath = yapathprefix + "traintestresult"

    //设置开始和截止日期
    def generateOfflineFeatureDataWithDates(sc: org.apache.spark.SparkContext): Unit = {
        //665836
        //cutDataWithDaysJoinItemData(sc, PathNamespace.offlinefeaturedata, offlinedataPath, startdate, enddate)
        //cutDataWithDatesWriteToHDFS(sc, PathNamespace.offlinefeaturedata, offlinedataPath, startdate, enddate)
        getDataJoinWithItemData(sc, sc.textFile(PathNamespace.offlinefeaturedata), offlinedataPath)
    }

    //正确的做法应该是先提取特征再按7:3切分数据划分出离线训练数据和离线测试数据
    def splitData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateRawFeatures(sc, offlinedataPath, userItemFeaturePath, userFeaturePath, itemFeaturePath)

        val offlinefeaturedata = sc.textFile(userItemFeaturePath)

        //7:3将11.18-12.17的数据中的useriditemid对分为两部分
        val testweight = 1 - trainweight
        val splitdata = offlinefeaturedata.randomSplit(Array(trainweight, testweight))
        val (traindata, testdata) = (splitdata(0), splitdata(1))

        traindata.coalesce(1).saveAsTextFile(offlinetraindataPath)
        testdata.coalesce(1).saveAsTextFile(offlinetestdataPath)

        //分别生成带有label的训练数据和带有userid itemid标识的测试数据
        MergeFeatures.generateLabeledData(sc, offlinetraindataPath, userFeaturePath, itemFeaturePath, PathNamespace.offlinetraindata, labeledTrainDataPath)
        MergeFeatures.generateTestFeatureData(sc, offlinetestdataPath, userFeaturePath, itemFeaturePath, labeledTestDataPath)
    }

    def offlinetrainandtest(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(labeledTrainDataPath)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(" ")(0).toInt == 1)
        val positivecount = positivedata.count

        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 5 to 20) {
            val negativecount = positivecount * i
            val sampleproportion = negativecount.toFloat / allcount.toFloat

            val negativedata = offlinelabeledtraindata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(negativedata)

            val finaltraindatapath = yapathprefix + "offlinefinaltraindata" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
            //val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, labeledTestDataPath).distinct
            val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, labeledTestDataPath, 1000).distinct

            val targetresult = TrainPredict.getOfflineTrainDataAfterJoinItem(sc)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / targetresult.count.toFloat
            val recall = hitcount / predictresult.count.toFloat
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ trainweight + "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(traintestResultPath)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateOfflineFeatureDataWithDates(sc)
        splitData(sc)
        offlinetrainandtest(sc)
    }
}

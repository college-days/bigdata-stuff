package org.give.altc.offline

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.features.MergeFeatures
import org.give.altc.{PathNamespace, CommonAlgo, CommonOP}

/**
 * Created by zjh on 15-4-6.
 */

//购买记录中会出现重复
//3767
//3470
//离线数据 7 3分 分完之后 生成完特征之后就不需要再变了 不然就太慢了
object TrainPredict {
    val trainweight: Double = 0.7

    //最终预测的数据与离线测试target的数据都需要根据itemid与item那张数据表进行join
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

        result
    }

    val offlinedataPath = PathNamespace.offlinefeaturedata
    val userItemFeaturePath = PathNamespace.offlineprefix + "useritemfeatures" + trainweight
    val userFeaturePath = PathNamespace.offlineprefix + "userfeatures" + trainweight
    val itemFeaturePath = PathNamespace.offlineprefix + "itemfeatures" + trainweight

    val offlinetraindataPath = PathNamespace.offlineprefix + "traindata" + trainweight
    val offlinetestdataPath = PathNamespace.offlineprefix + "testdata" + trainweight

    val labeledtraindataPath = PathNamespace.offlineprefix + "labeledtraindata" + trainweight
    val labeledtestdataPath = PathNamespace.offlineprefix + "labeledtestdata" + trainweight
    /**
     * 将11.18-12.17之间的所有数据按照7:3 6:4分为两份 一份作为训练数据一份作为测试数据
     * 训练数据与12.18的真实数据一起用来训练模型
     * 训练数据用按照1:5 - 1:20的正负样本比例来进行训练
     * 测试数据用来放入到训练好的模型中来预测12.18的购买行为计算F1
     * 所以训练数据需要产生2*16 -> 32份训练数据 与对应的两份测试数据
     */
    //正确的做法应该是先提取特征再按7:3切分数据划分出离线训练数据和离线测试数据
    def splitData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateRawFeatures(sc, offlinedataPath, userItemFeaturePath, userFeaturePath, itemFeaturePath)

        //val offlinefeaturedata = sc.textFile(userItemFeaturePath)
        //join一下itemdata所辖训练集范围 减小噪声
        val offlinefeaturedata = org.give.altc.online.TrainPredict.joinItemData(sc, sc.textFile(userItemFeaturePath))

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

    def offlinetrainandtest(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(labeledtraindataPath)

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

            val finaltraindatapath = PathNamespace.yaofflineprefix + "offlinefinaltraindata" + trainweight + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
            val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, labeledtestdataPath, 473).distinct
            //val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, offlineprefix + "offlinesubmitdata" + trainweight)

            val targetresult = getOfflineTrainDataAfterJoinItem(sc)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / targetresult.count.toFloat
            val recall = hitcount / predictresult.count.toFloat
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ trainweight + "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
            //println(negativedata.count)
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(PathNamespace.yaofflineprefix + "traintestresult" + trainweight)
        //println(positivecount)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        splitData(sc)
        offlinetrainandtest(sc)
    }
}

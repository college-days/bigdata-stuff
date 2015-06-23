package org.give.altc.observedata

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.give.altc.features.MergeFeatures

/**
 * Created by zjh on 15-4-8.
 */
object ExportFeatureData {
    val exportprefix: String = PathNamespace.prefix + "export/"
    val trainweight: Double = 0.7

    val origindataPath = PathNamespace.offlinefeaturedata
    val userItemFeaturePath = exportprefix + "useritemfeatures"
    val userFeaturePath = exportprefix + "userfeatures"
    val itemFeaturePath = exportprefix + "itemfeatures"

    val offlinetraindataPath = exportprefix + "traindata" + trainweight
    val offlinetestdataPath = exportprefix + "testdata" + trainweight

    val traindataoutputPath = exportprefix + "trainfeaturedata"
    val testdataoutputPath = exportprefix + "testfeaturedata"

    val finaltraindataPath = exportprefix + "finaltraindata"
    val ultimatetraindataPath = exportprefix + "ultimatetraindata"

    def getFeatures(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateRawFeatures(sc, origindataPath, userItemFeaturePath, userFeaturePath, itemFeaturePath)
        //MergeFeatures.generateTestFeatureData(sc, userItemFeaturePath, userFeaturePath, itemFeaturePath, exportprefix + "1118-1217featuredata")
    }

    def exportTrainTestFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        val useritemFeatureData = sc.textFile(userItemFeaturePath)
        val testweight = 1 - trainweight
        val splitdata = useritemFeatureData.randomSplit(Array(trainweight, testweight))
        val (traindata, testdata) = (splitdata(0), splitdata(1))

        traindata.coalesce(1).saveAsTextFile(offlinetraindataPath)
        testdata.coalesce(1).saveAsTextFile(offlinetestdataPath)

        MergeFeatures.joinUserItemFeaturesNotSparse(sc, offlinetraindataPath, userFeaturePath, itemFeaturePath, traindataoutputPath)
        MergeFeatures.joinUserItemFeaturesNotSparse(sc, offlinetestdataPath, userFeaturePath, itemFeaturePath, testdataoutputPath)
    }

    def exportSampledTrainData(sc: org.apache.spark.SparkContext): Unit = {
        MergeFeatures.generateLabeledDataNotSparse(sc, traindataoutputPath, PathNamespace.offlinetraindata, finaltraindataPath)

        val labeledtraindata = sc.textFile(finaltraindataPath)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = labeledtraindata.count
        val positivedata = labeledtraindata.filter(_.split(",")(0).toInt == 1)
        val negativedata = labeledtraindata.filter(_.split(",")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        val samplevalue = 10
        val negativesamplecount = positivecount * samplevalue
        val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

        val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
        val finaltraindata = positivedata.union(samplenegativedata)

        finaltraindata.coalesce(1).saveAsTextFile(ultimatetraindataPath)
    }

    def exportLuoTrainData(sc: org.apache.spark.SparkContext): Unit = {
        val luotraindatapath = "hdfs://namenode:9000/givedata/altc/newdata/luofeature/train1_featureLabel"
        val luotrainfeaturedata = sc.textFile(luotraindatapath).map {
            record =>
                val items = record.split(" ")
                val label = items.last
                val useritemid = items(0).replace(",", " ")
                val features = items.drop(1).dropRight(1).mkString(" ")
                //items.drop(1).dropRight(1).size
                (label, useritemid + " " + features)
        }

        //获取正样本数量 后面才能进行正负样本按比例采样
        val positivedata = luotrainfeaturedata.filter(_._1.toInt == 1)
        val negativedata = luotrainfeaturedata.filter(_._1.toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        val i = 10;
        val negativesamplecount = positivecount * i
        val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

        val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
        val finaltraindata = positivedata.union(samplenegativedata).map(e => e._1 + " " + e._2)

        val finaltraindatapath = "hdfs://namenode:9000/givedata/altc/newdata/luofeature/sampletraindata"
        finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)

        val luotestdatapath = "hdfs://namenode:9000/givedata/altc/newdata/luofeature/test_featureLabel"
        sc.textFile(luotestdatapath).map {
            record =>
                val items = record.split(" ")
                val label = items.last
                val useritemid = items(0).replace(",", " ")
                val features = items.drop(1).dropRight(1).mkString(" ")
                label + " " + useritemid + " " + features
        }.coalesce(1).saveAsTextFile("hdfs://namenode:9000/givedata/altc/newdata/luofeature/testdata")
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        getFeatures(sc)
        exportTrainTestFeatureData(sc)
        exportSampledTrainData(sc)
    }
}

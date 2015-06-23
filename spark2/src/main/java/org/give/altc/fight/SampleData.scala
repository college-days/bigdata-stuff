package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 15-4-18.
 */
object SampleData {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"

    val offlinetrainfeature = prefix + "subset/offlinetrainfeature"
    val onlinetrainfeature = prefix + "subset/onlinetrainfeature"

    def sampleOfflineData(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(offlinetrainfeature)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        //1:5 - 1:20的正负样本比例
        for (i <- 15 to 15 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = prefix + "subsetcleantha/offlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
        }
    }

    def sampleOnlineData(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(onlinetrainfeature)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        for (i <- 15 to 15 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = prefix + "subsetcleantha/onlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)
        }
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        sampleOfflineData(sc)
        sampleOnlineData(sc)
    }
}

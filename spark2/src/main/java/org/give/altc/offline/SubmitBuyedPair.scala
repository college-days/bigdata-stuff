package org.give.altc.offline

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{Cota, PathNamespace}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-9.
 */

//提交历史购买过的pair对 看小效果
//购买过一次 两次 三次的
//一周内 两周内 半个月内购买的
object SubmitBuyedPair {
    def joinItemData(sc: org.apache.spark.SparkContext, inputData: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val targetresult = inputData.map {
            record =>
                val items = record.split(",")
                val itemid = items(1)
                (itemid, record)
        }

        //473个结果 1218号与item表join以后会购买的pair对为473 这个是离线进行f1计算的target数据集合
        val result = targetresult.join(itemdata).map(_._2._1)

        result
    }

    def getBuyedPairs(sc: org.apache.spark.SparkContext, behavior: Int, pred: Int => Boolean): org.apache.spark.rdd.RDD[String] = {
        val data = sc.textFile(PathNamespace.offlinefeaturedata).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior) = (items(0), items(1), items(2))
                (userid + "," + itemid, behavior)
        }.groupByKey.map {
            record =>
                (record._1, record._2.toList)
        }.filter(_._2.contains("4")).map {
            record =>
                (record._1, record._2.count(_.toInt == behavior))
        }.filter(e => pred(e._2)).map(_._1)

        val result = joinItemData(sc, data)
        //result.coalesce(1).saveAsTextFile(PathNamespace.prefix + "submitbuydata/buycountgtzero")
        result
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val targetresult = TrainPredict.getOfflineTrainDataAfterJoinItem(sc)

        var offlinetrainresult = List[String]()
        for (i <- 0 to 3) {
            val pred = (x: Int) => x > i
            val predictresult = getBuyedPairs(sc, Cota.CART, pred)
            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / targetresult.count.toFloat
            val recall = hitcount / predictresult.count.toFloat
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(PathNamespace.prefix + "submitbuydata/submitbuyedresult")
    }
}


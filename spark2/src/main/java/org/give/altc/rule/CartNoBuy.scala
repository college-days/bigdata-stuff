package org.give.altc.rule

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-15.
 */

//预测日期前一天有购物车行为但是无购买的useritempair去预测试试
object CartNoBuy {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //val a = PathNamespace.tianchi_mobile_recommend_train_user
        //val b = PathNamespace.tianchi_mobile_recommend_train_user
        val featuredata = sc.textFile(PathNamespace.prefix + "subset/offlinetestfeatures").map {
            record =>
                val items = record.split(",")
                val useritemid = items(0).replace("-", ",")
                //val features = items(1)
                val features = items(1).split("-")
                //val score = features(0).toInt * 1 + features(2).toInt * 2 + features(3).toInt * 3
                //val score = features(3).toInt
                //val score = items(1).split("-").toList.map(_.toInt).sum
                val score = features.take(8).map(_.toInt).sum + features.drop(9).map(_.toInt).sum
                (useritemid, score)
        }

        /*val a = PathNamespace.offlinetestfeaturesubdata
        val b = PathNamespace.onlinetrainfeaturesubdata*/
        val a = "hdfs://namenode:9000/givedata/altc/metadata/offlinetestfeaturesubdata"
        val b = "hdfs://namenode:9000/givedata/altc/metadata/onlinetrainfeaturesubdata"
        /*val predictdata = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-17").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && e._2.contains("1") && !e._2.contains("4")).map(_._1).distinct//.map (e => (e, 1))*/

        //hit44
        val predictdata = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-17").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.filter(_ == "3").size == 1 && e._2.contains("1") && !e._2.contains("4")).map(_._1).distinct//.map (e => (e, 1))
        //.filter(e => e._2.filter(_ == "2").size == 1).map(_._1).distinct

        val cartnotbuy3 = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(e => e._1 == "2014-12-17" || e._1 == "2014-12-16" || e._1 == "2014-12-15").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.filter(_ == "3").size == 1 && e._2.contains("1") && !e._2.contains("4")).map(_._1).distinct//.map (e => (e, 1))

        val buyedlastday = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-17").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("4")).map(_._1).distinct

        val lastdaycart = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-17").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.filter(_ == "3").size == 1 && e._2.contains("1") && !e._2.contains("4")).map(_._1).distinct//.map (e => (e, 1))

        val double12predictdata = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-12").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => !e._2.contains("4")).map(_._1).distinct

        /*val cleantha = predictdata.join(featuredata).map {
            record =>
                val useritemid = record._1
                val score = record._2._2
                (score, useritemid)
        }.sortByKey(false).map(_._2)*/

        val targetdata = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).filter(_._2.toInt == 4).map(_._1).distinct

        val predictcount = predictdata.count.toFloat
        val targetcount = targetdata.count.toFloat
        val hitcount = predictdata.intersection(targetdata).count.toFloat
        //val hitcount = sc.parallelize(cleantha.take(30)).intersection(targetdata).count.toFloat

        //val hitrecord = predictdata.intersection(targetdata).map (e => (e, 1))

        /*val cleantha = hitrecord.join(featuredata).map {
            record =>
                val useritemid = record._1
                val score = record._2._2
                (score, useritemid)
        }.sortByKey(false)*/

        //hit 44
        val p = hitcount / targetcount
        val r = hitcount / predictcount
        val f1 = 2 * p * r / (p + r)
        println("p -> " + p + " r -> " + r + " f1 -> " + f1)
        //p -> 0.036531847 r -> 0.036531847 f1 -> 0.036531847
    }
}

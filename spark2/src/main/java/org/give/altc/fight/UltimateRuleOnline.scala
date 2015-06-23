package org.give.altc.fight

import java.text.DecimalFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.give.altc.CommonOP._
import org.give.altc.Cota

/**
 * Created by zjh on 15-4-20.
 */

//f1 -> 7.462687% p -> 7.500000% r -> 7.425743%
object UltimateRuleOnline {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"

    val itemdatapath = prefix + "newdata/tianchi_mobile_recommend_train_item.csv"
    val userdatapath = prefix + "newdata/tianchi_mobile_recommend_train_user.csv"

    val joinitemuserdata = prefix + "newdata/joinitemiduserdata"

    //转化率数据
    val useritemtranspath = prefix + "newdata/useritemtrans"

    def generateData(sc: org.apache.spark.SparkContext): Unit = {
        val userdataByItemid = sc.textFile(userdatapath).map {
            record =>
                val itemid = record.split(",")(1)
                (itemid, record)
        }

        val itemdataByItemid = sc.textFile(itemdatapath).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                val category = items(2)
                (itemid, Nil)
        }.distinct

        userdataByItemid.join(itemdataByItemid).map {
            record =>
                val itemid = record._1
                val userdata = record._2._1
                userdata
        }.coalesce(1).saveAsTextFile(joinitemuserdata)
    }

    def getSubmitData(origindata: org.apache.spark.rdd.RDD[String], date: String): org.apache.spark.rdd.RDD[String] = {
        val category_buy = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, category, behavior, date) = (items(0), items(4), items(2), items(5).split(" ")(0))
                (behavior + "-" + date, userid + "," + category)
        }.filter(e => e._1 == "4-" + date).map(_._2).distinct.collect()

        val onlybuyondouble12 = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (behavior, (userid, date))
        }.filter(_._1.toInt == 4).map(_._2).groupByKey.filter {
            record =>
                val userid = record._1
                val datelist = record._2.toList
                datelist.filter(_ == "2014-12-12").size == datelist.size
        }.map(_._1).collect

        val submitdata = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == date).map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !category_buy.contains(e._1.split(",")(0) + "," + e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1))//.distinct

        val submitonlybuyondouble12 = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                //(date, (userid + "," + itemid, behavior))
                (behavior, (userid, date))
        }.filter(_._1.toInt == 4).map(_._2).groupByKey.filter {
            record =>
                val userid = record._1
                val datelist = record._2.toList
                datelist.filter(_ == "2014-12-12").size == datelist.size
        }.map(_._1).collect

        val submitdatahehe = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == date).map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !submitonlybuyondouble12.contains(e._1.split(",")(0)) && !category_buy.contains(e._1.split(",")(0) + "," + e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1))//.distinct

        val submitdatanoexcludedouble12 = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == date).map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !category_buy.contains(e._1.split(",")(0) + "," + e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1))//.distinct

        submitdatahehe.distinct()
        //submitdatanoexcludedouble12.distinct()
    }

    def getOfflineTargetData(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
        val targetdata = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).filter(_._2.toInt == 4).map(_._1).distinct
        targetdata
    }

    //获得转化率来进行倒排序
    def getUserItemTrans(origindata: org.apache.spark.rdd.RDD[String]): Unit ={
        val formatter = new DecimalFormat("#.#########")

        def getSpecificBehaviorData(origindata: org.apache.spark.rdd.RDD[String], behavior: Int): org.apache.spark.rdd.RDD[String] = {
            origindata.filter(_.split(",")(2).toInt == behavior)
        }

        def getBehaviorTotalCount(behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val behaviorData = getSpecificBehaviorData(origindata, behavior)

            behaviorData.map {
                record =>
                    (record.split(",")(0) + "-" + record.split(",")(1), 1)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.sum.toString)
            }
        }

        val userItemTotalClickCount = getBehaviorTotalCount(1)
        val userItemTotalBuyCount = getBehaviorTotalCount(4)

        //购买次数/点击次数 也就是购买的转化率
        //4570440
        val userItemTrans = userItemTotalClickCount.leftOuterJoin(userItemTotalBuyCount).map {
            record =>
                val useritem = record._1
                val clickcount = record._2._1
                val buycount = record._2._2 match {
                    case Some(value) => value
                    case None => "0"
                }
                (useritem, clickcount + "-" + buycount)
        }.map {
            record =>
                val items = record._2.split("-")
                var (clickcount, buycount) = (items(0).toFloat, items(1).toFloat)

                if (clickcount == 0) {
                    clickcount = 1.0f
                }

                val useritem = record._1.replace("-", ",")
                val transform = formatter.format(buycount / clickcount)
                //(useritem, transform)
                useritem + "-" + transform
        }.coalesce(1).saveAsTextFile(useritemtranspath)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val origindata = sc.textFile(joinitemuserdata)
        //getUserItemTrans(origindata)

        val targetdata = getOfflineTargetData(origindata)
        val offline = getSubmitData(origindata, "2014-12-17")
        val online = getSubmitData(origindata, "2014-12-18")

        val useritemtrans = sc.textFile(useritemtranspath).map(e => (e.split("-")(0), e.split("-")(1)))

        val result = offline.map(e => (e, 1)).join(useritemtrans).map(e => (e._2._2, e._1))
        result.filter(_._1.toFloat != 0)
        sc.parallelize(result.sortByKey(false).map(_._2).take(500)).intersection(targetdata).count
        //sc.parallelize(result.sortByKey(false).map(_._2).take(400)).coalesce(1).saveAsTextFile(prefix + "offlinecleantharule")

        val submitresult = online.map(e => (e, 1)).join(useritemtrans).map(e => (e._2._2, e._1))
        sc.parallelize(submitresult.sortByKey(false).map(_._2).take(500)).coalesce(1).saveAsTextFile(prefix + "cleantharule0422")
    }
}

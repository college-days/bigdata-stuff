package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-20.
 */
object NewNewRule {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))

        val prefix: String = "hdfs://namenode:9000/givedata/altc/"
        val origindatapath = prefix + "log_o2o.csv"
        val cartnobuy18path = prefix + "cart_notbuy_18.csv"
        val categorybuy18path = prefix + "category_buy_18.csv"

        val userdatapath = prefix + "tianchi_mobile_recommend_train_user.csv"

        val origindata = sc.textFile(origindatapath)
        val cartnobuy18data = sc.textFile(cartnobuy18path).map(e => e.split(",")(0) + "," + e.split(",")(1)).distinct
        val categorybuy18data = sc.textFile(categorybuy18path)

        val onlinetrainfeaturesubdata: String = prefix + "metadata/onlinetrainfeaturesubdata"

        val cart_18 = origindata.map {
            record =>
                val items = record.split(",")
                val (behavior, date) = (items(2), items(5).split(" ")(0))
                (behavior + "-" + date, record)
        }.filter(e => e._1 == "3-2014-12-18").map(_._2)

        val buy_18 = origindata.map {
            record =>
                val items = record.split(",")
                val (behavior, date) = (items(2), items(5).split(" ")(0))
                (behavior + "-" + date, record)
        }.filter(e => e._1 == "4-2014-12-18").map(_._2)

        val category_buy_wrong = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, behavior, category, date) = (items(0), items(2), items(4), items(5).split(" ")(0))
                (behavior + "-" + date, (userid, category))
        }.filter(_._1 == "4-2014-12-18").map(_._2).groupByKey.map(e => (e._1, e._2.toList)).collect().toMap

        val category_buy = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, category, behavior, date) = (items(0), items(4), items(2), items(5).split(" ")(0))
                (behavior + "-" + date, userid + "," + category)
        }.filter(e => e._1 == "4-2014-12-18").map(_._2).distinct.collect()

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

        val submitonlybuyondouble12 = sc.textFile(onlinetrainfeaturesubdata).map {
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

        val submitdata = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !category_buy.contains(e._1.split(",")(0) + "," + e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1))//.distinct

        val submitdatahehe = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !submitonlybuyondouble12.contains(e._1.split(",")(0)) && !category_buy.contains(e._1.split(",")(0) + "," + e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1))//.distinct

        val submitdatawrong = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4")).map(_._1).filter {
            record =>
                val items = record.split(",")
                val (userid, category) = (items(0), items(2))
                val categoryList = category_buy_wrong.get(userid) match {
                    case Some(lst) => lst
                    case None => List[String]()
                }
            !categoryList.contains(category)
        }
    }
}

package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-19.
 */
object NewRule {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"
    val targetdataPath = prefix + "offlinetargetdatajoinitem"

    val itemdatapath = prefix + "tianchi_mobile_recommend_train_item.csv"
    val userdatapath = prefix + "tianchi_mobile_recommend_train_user.csv"

    val a = prefix + "metadata/offlinetestfeaturesubdata"
    val b = prefix + "metadata/onlinetrainfeaturesubdata"

    val onlinetrainfeaturesubdata: String = prefix + "metadata/onlinetrainfeaturesubdata"

    val joinitemuserdata = prefix + "joinitemiduserdata"

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

    def generateOnlyBuyOnDouble12(sc: org.apache.spark.SparkContext): Unit = {
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
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !submitonlybuyondouble12.contains(e._1.split(",")(0))).map(_._1).distinct//.map (e => (e, 1))

        val data = sc.textFile(onlinetrainfeaturesubdata).map {
            record =>
                val items = record.split(",")
                val (userid, behavior, category, date) = (items(0), items(2), items(4), items(5).split(" ")(0))
                (behavior + "-" + date, (userid, category))
        }.filter(_._1 == "4-2014-12-18").map(_._2).groupByKey.map(e => (e._1, e._2.toList)).collect().toMap

        //
        val submitdata = sc.textFile(onlinetrainfeaturesubdata).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !submitonlybuyondouble12.contains(e._1.split(",")(0)) && data.get(e._1.split(",")(0)).toList.contains(e._1.split(",")(1))).map(_._1).distinct//.map (e => (e, 1))

        //
    }

    def generateUserBuyedCategory(sc: org.apache.spark.SparkContext): Unit = {
        val data = sc.textFile(onlinetrainfeaturesubdata).map {
            record =>
                val items = record.split(",")
                val (userid, behavior, category, date) = (items(0), items(2), items(4), items(5).split(" ")(0))
                (behavior + "-" + date, (userid, category))
        }.filter(_._1 == "4-2014-12-18").map(_._2).groupByKey.map(e => (e._1, e._2.toList)).collect().toMap
    }

    def predict(sc: org.apache.spark.SparkContext): Unit = {
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

        val data = sc.textFile(onlinetrainfeaturesubdata).map {
            record =>
                val items = record.split(",")
                val (userid, behavior, category, date) = (items(0), items(2), items(4), items(5).split(" ")(0))
                (behavior + "-" + date, (userid, category))
        }.filter(_._1 == "4-2014-12-18").map(_._2).groupByKey.map(e => (e._1, e._2.toList)).collect().toMap

        val submitdata = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !submitonlybuyondouble12.contains(e._1.split(",")(0)) && !data.get(e._1.split(",")(0)).toList.contains(e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1)).distinct//.map (e => (e, 1))
    }

    def main(args: Array[String]): Unit ={
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateData(sc)
        val subsetdata = sc.textFile(joinitemuserdata)
    }
}

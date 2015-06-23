package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-20.
 */
object UltimateRule {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"
    val origindatapath = prefix + "log_o2o.csv"
    val joinitemuserdata = prefix + "joinitemiduserdata"
    val onlybuyondouble12luopath = prefix + "onlybuyondouble12.label"
    val olddouble12luopath = prefix + "newdata/olddataonlybuyondouble12.label"

    def getSubmitData(sc: org.apache.spark.SparkContext, origindata: org.apache.spark.rdd.RDD[String], date: String): org.apache.spark.rdd.RDD[String] = {
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

        val olddouble12luodata = sc.textFile(olddouble12luopath).distinct.collect()

        val submitdatahaha = origindata.map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date, category) = (items(0), items(1), items(2), items(5).split(" ")(0), items(4))
                (date, (userid + "," + itemid + "," + category, behavior))
        }.filter(_._1 == date).map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !olddouble12luodata.contains(e._1.split(",")(0)) && !category_buy.contains(e._1.split(",")(0) + "," + e._1.split(",")(2))).map(e => e._1.split(",")(0) + "," + e._1.split(",")(1))

        submitdatahaha.distinct()
    }

    def main(arg: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val origindata = sc.textFile(origindatapath)
        val onlybuyondouble12luo = sc.textFile(onlybuyondouble12luopath).collect()
        val olddouble12luodata = sc.textFile(olddouble12luopath).distinct.collect()

        val b = "hdfs://namenode:9000/givedata/altc/metadata/onlinetrainfeaturesubdata"

        val targetdata = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).filter(_._2.toInt == 4).map(_._1).distinct

        val offline = getSubmitData(sc, origindata, "2014-12-17")
        val online = getSubmitData(sc, origindata, "2014-12-18")
        val subsetdata = sc.textFile(joinitemuserdata)
        val offlinesubset = getSubmitData(sc, subsetdata, "2014-12-17")
        val onlinesubset = getSubmitData(sc, subsetdata, "2014-12-18")
    }
}

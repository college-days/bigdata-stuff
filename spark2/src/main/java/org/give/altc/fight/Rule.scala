package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.give.altc.PathNamespace

/**
 * Created by zjh on 15-4-19.
 */
object Rule {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val prefix: String = "hdfs://namenode:9000/givedata/altc/"
        val targetdataPath = prefix + "offlinetargetdatajoinitem"

        val a = prefix + "metadata/offlinetestfeaturesubdata"
        val b = prefix + "metadata/onlinetrainfeaturesubdata"

        val c = prefix + "tianchi_mobile_recommend_train_user.csv"
        val d = prefix + "tianchi_mobile_recommend_train_item.csv"

        val onlybuyondouble12 = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                //(date, (userid + "," + itemid, behavior))
                (userid, behavior + "," + date)
        }.groupByKey.filter {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                //behaviorlist.filter(_ == "4,2014-12-12").size == behaviorlist.size
                behaviorlist.contains("4,2014-12-12")
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
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !onlybuyondouble12.contains(e._1.split(",")(0))).map(_._1).distinct//.map (e => (e, 1))

        //

        val submitonlybuyondouble12 = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                //(date, (userid + "," + itemid, behavior))
                (userid, behavior + "," + date)
        }.groupByKey.filter {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                //behaviorlist.filter(_ == "4,2014-12-12").size == behaviorlist.size
                behaviorlist.contains("4,2014-12-12")
        }.map(_._1).collect

        val submitdata = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4") && !submitonlybuyondouble12.contains(e._1.split(",")(0))).map(_._1).distinct//.map (e => (e, 1))

        //
        val submitdataByItem = submitdata.map {
            record =>
                val itemid = record.split(",")(1)
                (itemid, record)
        }

        val itemcategoryByItem = sc.textFile(d).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                val category = items(2)
                (itemid, category)
        }

        val submitdatacategory = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }

        val targetdata = sc.textFile(b).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-18").map(_._2).filter(_._2.toInt == 4).map(_._1).distinct

    }
}

package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-20.
 */
object CheckOnlyBuyOnDouble12 {
    val sc = new SparkContext(new SparkConf().setAppName("cleantha"))

    val prefix: String = "hdfs://namenode:9000/givedata/altc/"

    val neworigindatapath = prefix + "newdata/joinitemiduserdata"
    val oldorigindatapath = prefix + "joinitemiduserdata"

    val newdouble12luopath = prefix + "newdata/newdataonlybuyondouble12.label"
    val olddouble12luopath = prefix + "newdata/olddataonlybuyondouble12.label"

    val neworigindata = sc.textFile(neworigindatapath)
    val oldorigindata = sc.textFile(oldorigindatapath)

    val newdouble12luodata = sc.textFile(newdouble12luopath).distinct
    val olddouble12luodata = sc.textFile(olddouble12luopath).distinct

    val newdouble12data = neworigindata.map {
        record =>
            val items = record.split(",")
            val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
            (behavior, (userid, date))
    }.filter(_._1.toInt == 4).map(_._2).groupByKey.filter {
        record =>
            val userid = record._1
            val datelist = record._2.toList
            datelist.filter(_ == "2014-12-12").size == datelist.size
    }.map(_._1)

    val olddouble12data = oldorigindata.map {
        record =>
            val items = record.split(",")
            val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
            (behavior, (userid, date))
    }.filter(_._1.toInt == 4).map(_._2).groupByKey.filter {
        record =>
            val userid = record._1
            val datelist = record._2.toList
            datelist.filter(_ == "2014-12-12").size == datelist.size
    }.map(_._1)
}

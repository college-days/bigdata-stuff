package org.give.altc.fightfight.features

import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-22.
 */
object GeoFeatures {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"

    val itemdatapath = prefix + "newdata/tianchi_mobile_recommend_train_item.csv"
    val joinitemuserdatapath = prefix + "newdata/joinitemiduserdata"

    def longestCommonPrefix(strs: List[String]): String = {
        if (strs.size == 0) {
            return ""
        }

        val num: Int = strs.size
        val len: Int = strs(0).size

        for (i <- 0 to len - 1) {
            for (j <- 1 to num - 1) {
                if (i > strs(j).size || strs(j)(i) != strs(0)(i)) {
                    return strs(0).substring(0, i)
                }
            }
        }
        strs(0)
    }

    def findLongestCommonPrefix(strs: List[String]): String = {
        val notEmpty = strs.filter(_ != "")

        if (notEmpty.size == 0) {
            return ""
        }

        if (notEmpty.size == 1) {
            return notEmpty(0)
        }

        //a.combinations(2).foreach(e => println(e(0)))
        val prefixs = (2 to notEmpty.size).map {
            i =>
                val localPrefixs = notEmpty.combinations(i).map(e => longestCommonPrefix(e))
                if (!localPrefixs.isEmpty) {
                    localPrefixs.maxBy(_.size)
                } else {
                    ""
                }
        }
        if (!prefix.isEmpty) {
            prefixs.maxBy(_.size)
        } else {
            ""
        }
    }

    //利用关联的user的地理信息来填充缺失地理信息的item的地理信息
    def fillItemGeoInfo(sc: org.apache.spark.SparkContext): Unit = {
        val itemdata = sc.textFile(itemdatapath).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                val geoinfo = items(1)
                (itemid, geoinfo)
        }

        val origindata = sc.textFile(joinitemuserdatapath).map {
            record =>
                val items = record.split(",")
                val itemid = items(1)
                val geoinfo = items(3)
                (itemid, geoinfo)
        }.groupByKey.map {
            record =>
                val itemid = record._1
                val prefix = findLongestCommonPrefix(record._2.toList)
                (itemid, prefix)
        }

        origindata.coalesce(1).saveAsTextFile(prefix + "newdata/hehe")

        itemdata.join(origindata).map {
            record =>
                val itemid = record._1
                val geoinfo = record._2._1
                val prefix = record._2._2
                if (geoinfo.isEmpty) {
                    prefix
                } else {
                    geoinfo
                }
        }.take(10)
    }
}

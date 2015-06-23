package org.give.altc.fight

import java.text.DecimalFormat

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 15-3-21.
 */
object UserFeatures {
    val formatter = new DecimalFormat("#.#########")

    def incIfZero[T](num: T, inc: T): T = {
        if (num == 0) {
            inc
        } else {
            num
        }
    }

    def operatorAfterJoinGetSecond(record: (String, (String, Option[String])), noneValue: String): (String, String) = {
        val id = record._1
        val secondbehavior = record._2._2 match {
            case Some(behavior) => behavior
            case None => noneValue
        }
        (id, secondbehavior)
    }

    def operatorAfterJoin(record: (String, (String, Option[String])), noneValue: String): (String, String) = {
        val id = record._1
        val firstbehavior = record._2._1
        val secondbehavior = record._2._2 match {
            case Some(behavior) => behavior
            case None => noneValue
        }
        (id, firstbehavior + "-" + secondbehavior)
    }

    def operatorAfterInnerJoin(record: (String, (String, String))): (String, String) = {
        (record._1, record._2._1 + "-" + record._2._2)
    }

    def joinWithOriginData(origindata: org.apache.spark.rdd.RDD[String], data: org.apache.spark.rdd.RDD[(String, String)]): org.apache.spark.rdd.RDD[(String, String)] = {
        origindata.map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                (userid, record)
        }.leftOuterJoin(data).map(operatorAfterJoinGetSecond(_, "0"))
    }

    def getUserFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        //点击总商品数 购买总商品数 收藏总商品数 购物车总商品数
        def generateUserItemBehavior(behaviorid: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            origindata.map {
                record =>
                    (record.split(",")(0) + "-" + record.split(",")(1), record.split(",")(2))
            }.filter(_._2.toInt == behaviorid).map {
                record =>
                    val (userid, itemid) = (record._1.split("-")(0), record._1.split("-")(1))
                    (userid, itemid)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.distinct.size.toString)
            }
        }

        val uservisititemcount = generateUserItemBehavior(1)
        val userbuyitemcount = generateUserItemBehavior(4)
        val userfavitemcount = generateUserItemBehavior(2)
        val usercartitemcount = generateUserItemBehavior(3)

        //点击总次数 收藏总次数 加入购物车总次数 购买总次数 购买数/点击数 购买数/收藏数 购买数/购物车数
        //9992
        val userbehaviors = origindata.map {
            record =>
                (record.split(",")(0), record.split(",")(2))
        }.groupByKey.map {
            record =>
                (record._1, record._2.toList)
        }.map {
            record =>
                val visitcount = record._2.count(_.toInt == 1).toFloat
                val favcount = record._2.count(_.toInt == 2).toFloat
                val cartcount = record._2.count(_.toInt == 3).toFloat
                val buycount = record._2.count(_.toInt == 4).toFloat

                (record._1, List(visitcount, favcount, cartcount, buycount, formatter.format(buycount / incIfZero[Float](visitcount, 1.0f)), formatter.format(buycount / incIfZero[Float](favcount, 1.0f)), formatter.format(buycount / incIfZero[Float](cartcount, 1.0f))).mkString("-"))
        }

        val result = joinWithOriginData(origindata, uservisititemcount).leftOuterJoin(userbuyitemcount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userfavitemcount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(usercartitemcount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userbehaviors)
            .map(operatorAfterJoin(_, "0-0-0-0-0-0-0")).distinct()

        //userbehaviors useritembehaviors两个大小是一样的所以可以join
        //到底这里是要join还是leftouterjoin还需要根据具体数据斟酌一下
        //9992
        //result.map(e => e._1 + "," + e._2)
        result
    }

    /**
     * 近7天的购买数（遇到双十二去除）
        近7天的点击数（遇到双十二去除）
        近7天收藏数（遇到双十二去除）
        近7天的购物车数（遇到双十二去除）
        近8-14天购买数
        近8-14天点击数
        近8-14天收藏数
        近8-14天购物车数
        近15-21天购买数
        近15-21天点击数
        近15-21天收藏数
        近15-21天购物车数
        双十二的购买数
        双十二的点击数
        双十二的收藏数
        双十二的购物车数
     * @param origindata
     * @return
     */
    def yaGetUserFeatures(origindata: org.apache.spark.rdd.RDD[String], targetDate: String): org.apache.spark.rdd.RDD[(String, String)] = {
        val targetDateNum = targetDate.split("-").mkString("").toInt
        val excludeDateNum: Long = 20141212

        def getYearMonthDay(origin: String): Long = {
            val timeItems = origin.split(",").last.split(" ")(0).split("-")
            val (year, month, day) = (timeItems(0), timeItems(1), timeItems(2))
            (year + month + day).toLong
        }

        def getFeatureDataByDate(data: org.apache.spark.rdd.RDD[String], startdate: Long, enddate: Long): org.apache.spark.rdd.RDD[String] = {
            data.filter {
                record =>
                    val date = getYearMonthDay(record)
                    date >= startdate && date <= enddate && date != excludeDateNum
            }
        }

        def getDataByIntervalByBehavior(start: Int, end: Int, behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val startDateNum = targetDateNum - start
            val endDateNum = targetDateNum - end
            val intervalData = getFeatureDataByDate(origindata, startDateNum, endDateNum)
            intervalData.filter(_.split(",")(2).toInt == behavior).map {
                record =>
                    val items = record.split(",")
                    val userid = items(0)
                    val itemid = items(1)
                    (userid, 1)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.size.toString)
            }
        }

        def getResultByBehavior(behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val intervaloneweek = getDataByIntervalByBehavior(7, 1, behavior)
            val intervaltwoweek = getDataByIntervalByBehavior(14, 8, behavior)
            val intervalthreeweek = getDataByIntervalByBehavior(21, 15, behavior)

            joinWithOriginData(origindata, intervaloneweek).leftOuterJoin(intervaltwoweek).map(operatorAfterJoin(_, "0")).leftOuterJoin(intervalthreeweek).map(operatorAfterJoin(_, "0")).distinct()
        }

        val useritemClickData = getResultByBehavior(1)
        val useritemBuyData = getResultByBehavior(4)
        val useritemFavData = getResultByBehavior(2)
        val useritemCartData = getResultByBehavior(3)

        val result = useritemClickData.join(useritemBuyData).map(operatorAfterInnerJoin).join(useritemFavData).map(operatorAfterInnerJoin).join(useritemCartData).map(operatorAfterInnerJoin)

        result
    }

    /**
     * 双十二的购买数
        双十二的点击数
        双十二的收藏数
        双十二的购物车数
     */
    def yayaGetUserFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        val includedate: Long = 20141212

        def getYearMonthDay(origin: String): Long = {
            val timeItems = origin.split(",").last.split(" ")(0).split("-")
            val (year, month, day) = (timeItems(0), timeItems(1), timeItems(2))
            (year + month + day).toLong
        }

        def getFeatureDataByDate(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
            data.filter {
                record =>
                    val date = getYearMonthDay(record)
                    date == includedate
            }
        }

        def getDataByBehavior(behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val intervalData = getFeatureDataByDate(origindata)
            intervalData.filter(_.split(",")(2).toInt == behavior).map {
                record =>
                    val items = record.split(",")
                    val userid = items(0)
                    val itemid = items(1)
                    (userid, 1)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.size.toString)
            }
        }

        val useritemClickData = getDataByBehavior(1)
        val useritemBuyData = getDataByBehavior(4)
        val useritemFavData = getDataByBehavior(2)
        val useritemCartData = getDataByBehavior(3)

        val result = useritemClickData.join(useritemBuyData).map(operatorAfterInnerJoin).join(useritemFavData).map(operatorAfterInnerJoin).join(useritemCartData).map(operatorAfterInnerJoin)

        result
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val prefix: String = "hdfs://namenode:9000/givedata/altc/"
        val metadataprefix: String = prefix + "metadata/"

        val offlinetrainfeaturesubdata: String = metadataprefix + "offlinetrainfeaturesubdata"
        val offlinetestfeaturesubdata: String = metadataprefix + "offlinetestfeaturesubdata"
        val onlinetrainfeaturesubdata: String = metadataprefix + "onlinetrainfeaturesubdata"

        val offlineusertrainfeaturesubdata: String = prefix + "subset/offlineusertrainfeaturesubdata"
        val offlineusertestfeaturesubdata: String = prefix + "subset/offlineusertestfeaturesubdata"
        val onlineusertrainfeaturesubdata: String = prefix + "subset/onlineusertrainfeaturesubdata"

        val offlinetrainorigindata = sc.textFile(offlinetrainfeaturesubdata)
        val offlinetestorigindata = sc.textFile(offlinetestfeaturesubdata)
        val onlinetrainorigindata = sc.textFile(onlinetrainfeaturesubdata)

        getUserFeatures(offlinetrainorigindata).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(offlineusertrainfeaturesubdata)
        getUserFeatures(offlinetestorigindata).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(offlineusertestfeaturesubdata)
        getUserFeatures(onlinetrainorigindata).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(onlineusertrainfeaturesubdata)

        /*getUserFeatures(offlinetrainorigindata).join(yaGetUserFeatures(offlinetrainorigindata, "2014-12-17")).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(offlineusertrainfeaturesubdata)
        getUserFeatures(offlinetestorigindata).join(yaGetUserFeatures(offlinetestorigindata, "2014-12-18")).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(offlineusertestfeaturesubdata)
        getUserFeatures(onlinetrainorigindata).join(yaGetUserFeatures(onlinetrainorigindata, "2014-12-19")).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(onlineusertrainfeaturesubdata)*/
    }
}

package org.give.altc.fightfight.features

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.CommonOP._
import org.give.altc.TimeUtils

/**
 * Created by zjh on 15-3-24.
 */
object ItemFeatures {
    def joinWithOriginData(origindata: org.apache.spark.rdd.RDD[String], data: org.apache.spark.rdd.RDD[(String, String)]): org.apache.spark.rdd.RDD[(String, String)] = {
        origindata.map {
            record =>
                val items = record.split(",")
                val itemid = items(1)
                (itemid, record)
        }.leftOuterJoin(data).map(operatorAfterJoinGetSecond(_, "0"))
    }

    def yaGetItemFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        //一个商品 点击总次数 收藏总次数 购物车总次数 购买总次数
        //2841247
        val itemBehaviors = origindata.map {
            record =>
                (record.split(",")(1), record.split(",")(2))
        }.groupByKey.map {
            record =>
                (record._1, record._2.toList)
        }.map {
            record =>
                val visitcount = record._2.count(_.toInt == 1).toFloat
                val favcount = record._2.count(_.toInt == 2).toFloat
                val cartcount = record._2.count(_.toInt == 3).toFloat
                val buycount = record._2.count(_.toInt == 4).toFloat

                (record._1, List(visitcount, favcount, cartcount, buycount).mkString("-"))
        }

        //一个商品 点击总人数 收藏总人数 购物车总人数 购买总人数
        def generateItemUserBehavior(behaviorid: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            origindata.map {
                record =>
                    (record.split(",")(1) + "-" + record.split(",")(0), record.split(",")(2))
            }.filter(_._2.toInt == behaviorid).map {
                record =>
                    val (itemid, userid) = (record._1.split("-")(0), record._1.split("-")(1))
                    (itemid, userid)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.distinct.size.toString)
            }
        }

        val itemvisitusercount = generateItemUserBehavior(1)
        val itemfavusercount = generateItemUserBehavior(2)
        val itemcartusercount = generateItemUserBehavior(3)
        val itembuyusercount = generateItemUserBehavior(4)

        val result = joinWithOriginData(origindata, itemvisitusercount).leftOuterJoin(itemfavusercount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(itemcartusercount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(itembuyusercount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(itemBehaviors)
            .map(operatorAfterJoin(_, "0-0-0-0")).distinct()

        //userbehaviors useritembehaviors两个大小是一样的所以可以join
        //到底这里是要join还是leftouterjoin还需要根据具体数据斟酌一下
        //9992
        result
    }

    def yayaGetItemFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        val buydata = origindata.filter(_.split(",")(2).toInt == 4)

        //用户平均购买次数
        //90749
        val itemUserBuyCountAvg = buydata.map {
            record =>
                (record.split(",")(1) + "-" + record.split(",")(0), 1)
        }.groupByKey.map {
            record =>
                (record._1, record._2.toList.size)
        }.map {
            record =>
                val items = record._1.split("-")
                (items(0), (items(1), record._2))
        }.groupByKey.filter(_._2.toList.size > 0).map {
            record =>
                val usercount = record._2.toList.size
                val totalBuyCount = record._2.map(_._2.toInt).sum
                (record._1, formatter.format(totalBuyCount.toFloat / usercount.toFloat))
        }

        //购买一次的用户 购买两次的用户 购买三次的用户 购买三次以上的用户
        def getBuyItemUserCountWithPredicate(pred: Int => Boolean): org.apache.spark.rdd.RDD[(String, String)] = {
            buydata.map {
                record =>
                    (record.split(",")(1) + "-" + record.split(",")(0), 1)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.size)
            }.filter(record => pred(record._2)).map {
                record =>
                    val items = record._1.split("-")
                    (items(0), items(1))
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.size.toString)
            }
        }

        //购买一次的用户 购买两次的用户 购买三次的用户 购买三次以上的用户
        val buyItemOneTime = getBuyItemUserCountWithPredicate((x: Int) => x == 1)
        val buyItemTwoTime = getBuyItemUserCountWithPredicate((x: Int) => x == 2)
        val buyItemThreeTime = getBuyItemUserCountWithPredicate((x: Int) => x == 3)
        val buyItemMoreThanThreeTime = getBuyItemUserCountWithPredicate((x: Int) => x > 3)

        //平均购买时间间隔
        //8047
        val itemBuyDurationAvg = buydata.map {
            record =>
                val dateItems = record.split(",").last.split(" ")
                val dateHour = dateItems(0) + "-" + dateItems(1)
                (record.split(",")(1), dateHour)
        }.groupByKey.filter(_._2.toList.distinct.size > 1).map {
            record =>
                val buydates = record._2.toList.distinct.sortWith(_ < _)
                val itemid = record._1
                var buyDurations = List[Int]()

                for (i <- 1 to buydates.size - 1) {
                    val earlyBuyDate = buydates(i - 1)
                    val laterBuyDate = buydates(i)
                    buyDurations = buyDurations :+ TimeUtils.calcDateDuration(laterBuyDate, earlyBuyDate)
                }
                (itemid, formatter.format(buyDurations.sum.toFloat / buyDurations.size.toFloat))
        }

        val result = joinWithOriginData(origindata, buyItemOneTime).leftOuterJoin(buyItemTwoTime)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(buyItemThreeTime)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(buyItemMoreThanThreeTime)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(itemUserBuyCountAvg)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(itemBuyDurationAvg)
            .map(operatorAfterJoin(_, "0")).distinct()

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
    def yayayaGetItemFeatures(origindata: org.apache.spark.rdd.RDD[String], targetDate: String): org.apache.spark.rdd.RDD[(String, String)] = {
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
                    (itemid, 1)
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

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))

        val prefix: String = "hdfs://namenode:9000/givedata/altc/"
        val metadataprefix: String = prefix + "metadata/"

        val offlinetrainfeaturesubdata: String = metadataprefix + "offlinetrainfeaturesubdata"
        val offlinetestfeaturesubdata: String = metadataprefix + "offlinetestfeaturesubdata"
        val onlinetrainfeaturesubdata: String = metadataprefix + "onlinetrainfeaturesubdata"

        val offlineitemtrainfeaturesubdata: String = prefix + "subset/offlineitemtrainfeaturesubdata"
        val offlineitemtestfeaturesubdata: String = prefix + "subset/offlineitemtestfeaturesubdata"
        val onlineitemtrainfeaturesubdata: String = prefix + "subset/onlineitemtrainfeaturesubdata"

        val offlinetrainorigindata = sc.textFile(offlinetrainfeaturesubdata)
        val offlinetestorigindata = sc.textFile(offlinetestfeaturesubdata)
        val onlinetrainorigindata = sc.textFile(onlinetrainfeaturesubdata)

        def getFeatureData(origindata: org.apache.spark.rdd.RDD[String], date: String, output: String): Unit = {
            yaGetItemFeatures(origindata).join(yayaGetItemFeatures(origindata)).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(output)
            //yaGetItemFeatures(origindata).join(yayaGetItemFeatures(origindata)).map(operatorAfterInnerJoin).join(yayayaGetItemFeatures(origindata, date)).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(output)
        }

        getFeatureData(offlinetrainorigindata, "2014-12-17", offlineitemtrainfeaturesubdata)
        getFeatureData(offlinetestorigindata, "2014-12-18", offlineitemtestfeaturesubdata)
        getFeatureData(onlinetrainorigindata, "2014-12-19", onlineitemtrainfeaturesubdata)
    }
}
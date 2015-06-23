package org.give.altc.fight

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.CommonOP._
import org.give.altc.{Cota, PathNamespace, TimeUtils}

/**
 * Created by zjh on 15-3-25.
 */
object UserItemFeatures {
    def joinWithOriginData(origindata: org.apache.spark.rdd.RDD[String], data: org.apache.spark.rdd.RDD[(String, String)]): org.apache.spark.rdd.RDD[(String, String)] = {
        origindata.map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                (userid + "-" + itemid, record)
        }.leftOuterJoin(data).map(operatorAfterJoinGetSecond(_, "0"))
    }

    def yaGetUserItemFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        //获取useritem的某一个行为总共的次数
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

        //获取useritem某一个行为的天数
        def getBehaviorDayCount(behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val behaviorData = getSpecificBehaviorData(origindata, behavior)

            behaviorData.map {
                record =>
                    (record.split(",")(0) + "-" + record.split(",")(1), record.split(",").last.split(" ")(0))
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.distinct.size.toString)
            }
        }

        //点击次数 点击天数 购买次数 购买天数
        val userItemTotalClickCount = getBehaviorTotalCount(Cota.CLICK)
        val userItemTotalBuyCount = getBehaviorTotalCount(Cota.BUY)
        val userItemTotalFavCount = getBehaviorTotalCount(Cota.FAV)
        val userItemTotalCartCount = getBehaviorTotalCount(Cota.CART)
        val userItemClickDayCount = getBehaviorDayCount(Cota.CLICK)
        val userItemBuyDayCount = getBehaviorDayCount(Cota.BUY)
        val userItemFavDayCount = getBehaviorDayCount(Cota.FAV)
        val userItemCartDayCount = getBehaviorDayCount(Cota.CART)

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

                val useritem = record._1
                val transform = formatter.format(buycount / clickcount)
                (useritem, transform)
        }

        val result = joinWithOriginData(origindata, userItemTotalClickCount).leftOuterJoin(userItemTotalBuyCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemTotalFavCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemTotalCartCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemClickDayCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemBuyDayCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemFavDayCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemCartDayCount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemTrans)
            .map(operatorAfterJoin(_, "0")).distinct()//.map(e => e._1 + "," + e._2)

        //result.coalesce(1).saveAsTextFile(PathNamespace.prefix + "useritemfeaturepart1test")
        result
    }

    //是否在购物车里：加入购物车之后还没有进行购买为1，若有购买行为则为0
    //是否在收藏夹里：收藏之后还没有进行购买为1，若有购买行为则为0
    //最近一次点击离预测日的时间间隔（如果这次点击之后进行了购买，则为null）。比如我们数据集最后一天是12月18号，用户在12月12号点击并且后面没有购买，则为6；如果用户在12月12号点击，但是在12号也购买了，则为null
    //最后一次点击距离上一次购买的时间。因为有时候用户购买之后还会进行点击，看看自己买的东西的详情，这个点击之后用户不会再购买。为了让机器自动判断是不是购买之后再点击，统计这个时间间隔。比如我在12月12号购买，在13号又点击进行查看，则设为1.如果用户以前没买过，则为null
    def yayaGetUserItemFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        def operatorAfterFullOuterJoin(record: (String, (Option[List[String]], Option[List[String]]))): (String, (List[String], List[String])) = {
            val id = record._1

            def getValueFromOption(data: Option[List[String]]): List[String] = {
                data match {
                    case Some(value) => value
                    case None => List[String]()
                }
            }

            val first = getValueFromOption(record._2._1)
            val second = getValueFromOption(record._2._2)

            (id, (first, second))
        }

        def getUserItemFavOrCartFeature(favOrCartDates: org.apache.spark.rdd.RDD[(String, List[String])], buyDates: org.apache.spark.rdd.RDD[(String, List[String])]): org.apache.spark.rdd.RDD[(String, String)] = {
            val result = {
                favOrCartDates.fullOuterJoin(buyDates)
                    .map(operatorAfterFullOuterJoin)
                    //滤除没有购买日期的记录
                    .filter(!_._2._2.distinct.isEmpty)
            }.map {
                record =>
                    val useritem = record._1
                    val favOrCartDates = record._2._1
                    val buyDates = record._2._2
                    if (favOrCartDates.isEmpty) {
                        //压根就不在购物车或者收藏夹里面
                        (useritem, "0")
                    } else {
                        //查看在放入购物车或者收藏之后是否进行了购买
                        //如果之后进行了购买则为0 如果没有购买行为则为1
                        var isBuyAfterFavOrCart = ""
                        for (i <- 0 to favOrCartDates.size - 1) {
                            val favOrCartDate = favOrCartDates(i)
                            val afterBuyDates = buyDates.filter(_ > favOrCartDate)
                            if (afterBuyDates.isEmpty) {
                                //收藏或者加入购物车之后没有购买行为
                                isBuyAfterFavOrCart = "1"
                            } else {
                                isBuyAfterFavOrCart = "0"
                            }
                        }
                        (useritem, isBuyAfterFavOrCart)
                    }
            }
            result
        }

        val userItemFavWithDate = getSpecificBehaviorUserItemWithDate(origindata, 2, "-")
        val userItemCartWithDate = getSpecificBehaviorUserItemWithDate(origindata, 3, "-")
        val userItemBuyWithDate = getSpecificBehaviorUserItemWithDate(origindata, 4, "-")

        val userItemFavFeature = getUserItemFavOrCartFeature(userItemFavWithDate, userItemBuyWithDate)
        val userItemCartFeature = getUserItemFavOrCartFeature(userItemCartWithDate, userItemBuyWithDate)

        val userItemClickWithDate = getSpecificBehaviorUserItemWithDate(origindata, Cota.CLICK, "-")
        //最近一次点击离预测日的时间间隔(如果这次点击之后进行了购买，则为null)比如我们数据集最后一天是12月18号 用户在12月12号点击并且后面没有购买 则为6 如果用户在12月12号点击 但是在12号也购买了 则为null 预测日也就是数据集中的最后一天也就是12月18号
        //4570440
        val userItemBuyAfterClickFeature = userItemClickWithDate.leftOuterJoin(userItemBuyWithDate).map {
            record =>
                val useritem = record._1
                val clickdates = record._2._1
                val buydates = record._2._2 match {
                    case Some(value) => value
                    case None => List[String]()
                }
                val latestClickDate = clickdates.max
                val buyAfterLatestClickDates = buydates.filter(_ > latestClickDate)

                if (buyAfterLatestClickDates.size > 0) {
                    (useritem, "null")
                } else {
                    val latestClickDateWithOutHour = latestClickDate.split("-").take(3).mkString("-")
                    val duration = TimeUtils.calcDayDuration(Cota.PREDICT_DATE, latestClickDateWithOutHour)
                    (useritem, duration.toString)
                }
        }

        //最后一次点击距离上一次购买的时间 因为有时候用户购买之后还会进行点击 看看自己买的东西的详情 这个点击之后用户不会再购买 为了让机器自动判断是不是购买之后再点击 统计这个时间间隔 比如我在12月12号购买 在13号又点击进行查看 则设为1 如果用户以前没买过 则为null
        //4570440
        val userItemLatestClickBuyDurationFeature = userItemClickWithDate.leftOuterJoin(userItemBuyWithDate).map {
            record =>
                val useritem = record._1
                val clickdates = record._2._1
                val buydates = record._2._2 match {
                    case Some(value) => value
                    case None => List[String]()
                }
                val latestClickDate = clickdates.max
                //找到在最后一次点击之前的购买日期
                val buyBeforeLatestClickDates = buydates.filter(_ < latestClickDate)

                if (buyBeforeLatestClickDates.size == 0) {
                    (useritem, "null")
                } else {
                    //最后一次点击的上一次购买日期
                    val latestBuyDate = buyBeforeLatestClickDates.max.split("-").take(3).mkString("-")
                    val latestClickDateWithOutHour = latestClickDate.split("-").take(3).mkString("-")
                    val duration = TimeUtils.calcDayDuration(latestClickDateWithOutHour, latestBuyDate)
                    (useritem, duration.toString)
                }
        }

        joinWithOriginData(origindata, userItemFavFeature).leftOuterJoin(userItemCartFeature)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(userItemBuyAfterClickFeature)
            .map(operatorAfterJoin(_, "null")).leftOuterJoin(userItemLatestClickBuyDurationFeature)
            .map(operatorAfterJoin(_, "null"))
    }

    def getUserItemFeatures(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
        //获取useritem的某一个行为总共的次数
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

        //获取useritem某一个行为的天数
        def getBehaviorDayCount(behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val behaviorData = getSpecificBehaviorData(origindata, behavior)

            behaviorData.map {
                record =>
                    (record.split(",")(0) + "-" + record.split(",")(1), record.split(",").last.split(" ")(0))
            }.groupByKey.map {
                record =>
                    (record._1, record._2.toList.distinct.size.toString)
            }
        }

        //点击次数 点击天数 购买次数 购买天数
        //4570440
        val userItemTotalClickCount = getBehaviorTotalCount(Cota.CLICK)
        //100337
        val userItemTotalBuyCount = getBehaviorTotalCount(Cota.BUY)
        //4570440
        val userItemClickDayCount = getBehaviorDayCount(Cota.CLICK)
        //100337
        val userItemBuyDayCount = getBehaviorDayCount(Cota.BUY)

        //4570440
        val userItemClickBuyCountFeature = {
            val userItemClick = userItemTotalClickCount.join(userItemClickDayCount).map(operatorAfterInnerJoin)

            val userItemBuy = userItemTotalBuyCount.join(userItemBuyDayCount).map(operatorAfterInnerJoin)

            userItemClick.leftOuterJoin(userItemBuy).map(operatorAfterJoin(_, "0-0"))
        }

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
                //静博士发现的bug 就算是没有购买记录 依然可以得到不为零的转化率
                /*if (buycount == 0) {
                    buycount = 1.0f
                }*/
                if (clickcount == 0) {
                    clickcount = 1.0f
                }
                val useritem = record._1
                val transform = formatter.format(buycount / clickcount)
                (useritem, transform)
        }

        val buydata = getSpecificBehaviorData(origindata, 4)

        //平均购买间隔
        //2587
        val userItemBuyDurationAvg = buydata.map {
            record =>
                val dateItems = record.split(",").last.split(" ")
                val dateHour = dateItems(0) + "-" + dateItems(1)
                (record.split(",")(0) + "-" + record.split(",")(1), dateHour)
        }.groupByKey.filter(_._2.toList.distinct.size > 1).map {
            record =>
                val buydates = record._2.toList.distinct.sortWith(_ < _)
                val useritemid = record._1
                var buyDurations = List[Int]()

                for (i <- 1 to buydates.size - 1) {
                    val earlyBuyDate = buydates(i - 1)
                    val laterBuyDate = buydates(i)
                    buyDurations = buyDurations :+ TimeUtils.calcDateDuration(laterBuyDate, earlyBuyDate)
                }
                (useritemid, formatter.format(buyDurations.sum.toFloat / buyDurations.size.toFloat))
        }

        //212286
        val userItemFavWithDate = getSpecificBehaviorUserItemWithDate(origindata, 2, "-")
        //288212
        val userItemCartWithDate = getSpecificBehaviorUserItemWithDate(origindata, 3, "-")
        //100337
        val userItemBuyWithDate = getSpecificBehaviorUserItemWithDate(origindata, 4, "-")

        def operatorAfterFullOuterJoin(record: (String, (Option[List[String]], Option[List[String]]))): (String, (List[String], List[String])) = {
            val id = record._1

            def getValueFromOption(data: Option[List[String]]): List[String] = {
                data match {
                    case Some(value) => value
                    case None => List[String]()
                }
            }

            val first = getValueFromOption(record._2._1)
            val second = getValueFromOption(record._2._2)

            (id, (first, second))
        }

        //是否在收藏夹里 收藏之后还没有进行购买为1 若有购买行为则为0 压根不在收藏夹里面的也是0
        //是否在购物车里 加入购物车之后还没有进行购买为1 若有购买行为则为0 压根不在购物车里面的也是0
        def getUserItemFavOrCartFeature(favOrCartDates: org.apache.spark.rdd.RDD[(String, List[String])], buyDates: org.apache.spark.rdd.RDD[(String, List[String])]): org.apache.spark.rdd.RDD[(String, String)] = {
            val result = {
                favOrCartDates.fullOuterJoin(buyDates)
                    .map(operatorAfterFullOuterJoin)
                    //滤除没有购买日期的记录
                    .filter(!_._2._2.distinct.isEmpty)
            }.map {
                record =>
                    val useritem = record._1
                    val favOrCartDates = record._2._1
                    val buyDates = record._2._2
                    if (favOrCartDates.isEmpty) {
                        //压根就不在购物车或者收藏夹里面
                        (useritem, "0")
                    } else {
                        //查看在放入购物车或者收藏之后是否进行了购买
                        //如果之后进行了购买则为0 如果没有购买行为则为1
                        var isBuyAfterFavOrCart = ""
                        for (i <- 0 to favOrCartDates.size - 1) {
                            val favOrCartDate = favOrCartDates(i)
                            val afterBuyDates = buyDates.filter(_ > favOrCartDate)
                            if (afterBuyDates.isEmpty) {
                                //收藏或者加入购物车之后没有购买行为
                                isBuyAfterFavOrCart = "1"
                            } else {
                                isBuyAfterFavOrCart = "0"
                            }
                        }
                        (useritem, isBuyAfterFavOrCart)
                    }
            }
            result
        }

        //100337
        val userItemFavFeature = getUserItemFavOrCartFeature(userItemFavWithDate, userItemBuyWithDate)
        //100337
        val userItemCartFeature = getUserItemFavOrCartFeature(userItemCartWithDate, userItemBuyWithDate)

        //4570440
        val userItemClickWithDate = getSpecificBehaviorUserItemWithDate(origindata, Cota.CLICK, "-")

        //最近一次点击离预测日的时间间隔(如果这次点击之后进行了购买，则为null)比如我们数据集最后一天是12月18号 用户在12月12号点击并且后面没有购买 则为6 如果用户在12月12号点击 但是在12号也购买了 则为null 预测日也就是数据集中的最后一天也就是12月18号
        //4570440
        val userItemBuyAfterClickFeature = userItemClickWithDate.leftOuterJoin(userItemBuyWithDate).map {
            record =>
                val useritem = record._1
                val clickdates = record._2._1
                val buydates = record._2._2 match {
                    case Some(value) => value
                    case None => List[String]()
                }
                val latestClickDate = clickdates.max
                val buyAfterLatestClickDates = buydates.filter(_ > latestClickDate)

                if (buyAfterLatestClickDates.size > 0) {
                    (useritem, "null")
                } else {
                    val latestClickDateWithOutHour = latestClickDate.split("-").take(3).mkString("-")
                    val duration = TimeUtils.calcDayDuration(Cota.PREDICT_DATE, latestClickDateWithOutHour)
                    (useritem, duration.toString)
                }
        }

        //最后一次点击距离上一次购买的时间 因为有时候用户购买之后还会进行点击 看看自己买的东西的详情 这个点击之后用户不会再购买 为了让机器自动判断是不是购买之后再点击 统计这个时间间隔 比如我在12月12号购买 在13号又点击进行查看 则设为1 如果用户以前没买过 则为null
        //4570440
        val userItemLatestClickBuyDurationFeature = userItemClickWithDate.leftOuterJoin(userItemBuyWithDate).map {
            record =>
                val useritem = record._1
                val clickdates = record._2._1
                val buydates = record._2._2 match {
                    case Some(value) => value
                    case None => List[String]()
                }
                val latestClickDate = clickdates.max
                //找到在最后一次点击之前的购买日期
                val buyBeforeLatestClickDates = buydates.filter(_ < latestClickDate)

                if (buyBeforeLatestClickDates.size == 0) {
                    (useritem, "null")
                } else {
                    //最后一次点击的上一次购买日期
                    val latestBuyDate = buyBeforeLatestClickDates.max.split("-").take(3).mkString("-")
                    val latestClickDateWithOutHour = latestClickDate.split("-").take(3).mkString("-")
                    val duration = TimeUtils.calcDayDuration(latestClickDateWithOutHour, latestBuyDate)
                    (useritem, duration.toString)
                }
        }

        val userItemFeatures = {
            //4570440
            val userItemCountFeature = userItemClickBuyCountFeature.join(userItemTrans).map(operatorAfterInnerJoin)

            //100337
            val userItemFavCartFeature = userItemFavFeature.join(userItemCartFeature).map(operatorAfterInnerJoin)

            //4570440
            val userItemClickBuyDurationFeature = userItemBuyAfterClickFeature.join(userItemLatestClickBuyDurationFeature).map(operatorAfterInnerJoin)

            userItemCountFeature.join(userItemClickBuyDurationFeature)
                .map(operatorAfterInnerJoin).leftOuterJoin(userItemFavCartFeature)
                .map(operatorAfterJoin(_, "0-0")).leftOuterJoin(userItemBuyDurationAvg)
                .map(operatorAfterJoin(_, "0"))
        }.map {
            record =>
                record._1 + "," + record._2
        }

        userItemFeatures
    }

    /**
     * 计算每一个useriditemidpair
     * 周一购买数
     * 周二购买数
     * 周三购买数
     * 周四购买数
     * 周五购买数
     * 周六购买数
     * 周日购买数
     */
    def getUserItemBuyCountByWeekDay(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        def getBuyCountBySpecificWeekDay(weekday: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            origindata.filter {
                record =>
                    record.split(",")(2).toInt == 4
            }.filter {
                record =>
                    val date = record.split(",").last.split(" ")(0)
                    val realweekday = TimeUtils.getWeekDayFromDate(date)
                    realweekday == weekday
            }.map {
                record =>
                    val items = record.split(",")
                    val (userid, itemid) = (items(0), items(1))
                    (userid + "-" + itemid, 1)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.size.toString)
            }
        }

        val mondaybuycount = getBuyCountBySpecificWeekDay(1)
        val tuesdaybuycount = getBuyCountBySpecificWeekDay(2)
        val wednesdaybuycount = getBuyCountBySpecificWeekDay(3)
        val thursdaybuycount = getBuyCountBySpecificWeekDay(4)
        val fridaybuycount = getBuyCountBySpecificWeekDay(5)
        val saturdaybuycount = getBuyCountBySpecificWeekDay(6)
        val sundaybuycount = getBuyCountBySpecificWeekDay(7)

        val result = joinWithOriginData(origindata, mondaybuycount).leftOuterJoin(tuesdaybuycount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(wednesdaybuycount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(thursdaybuycount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(fridaybuycount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(saturdaybuycount)
            .map(operatorAfterJoin(_, "0")).leftOuterJoin(sundaybuycount)
            .map(operatorAfterJoin(_, "0")).distinct()//.map(e => e._1 + "," + e._2)

        //result.coalesce(1).saveAsTextFile(PathNamespace.prefix + "useritemfeaturepart2test")
        result
    }

    /**
     * 1天前点击数
     * 2天前点击数
     * 3天前点击数
     * 7天前点击数
     * 14天前点击数
     * 21天前点击数
     * 1天前收藏数
     * 2天前收藏数
     * 3天前收藏数
     * 7天前收藏数
     * 14天前收藏数
     * 21天前收藏数
     * 1天前购物车数
     * 2天前购物车数
     * 3天前购物车数
     * 7天前购物车数
     * 14天前购物车数
     * 21天前购物车数
     * 1天前购买数
     * 2天前购买数
     * 3天前购买数
     * 7天前购买数
     * 14天前购买数
     * 21天前购买数
     *
     * @param origindata
     * @param targetDate
     * @return
     */
    def getUserItemBehaviorBeforeTargetDate(origindata: org.apache.spark.rdd.RDD[String], targetDate: String): org.apache.spark.rdd.RDD[(String, String)] = {
        val targetDateNum = targetDate.split("-").mkString("").toInt
        //interval就是多少天以前 behavior是指user item pair发生的行为
        def getDataByIntervalByBehavior(interval: Int, behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val intervaldateNum = targetDateNum - interval
            val intervalData = getFeatureDataByDate(origindata, intervaldateNum, targetDateNum - 1)
            intervalData.filter(_.split(",")(2).toInt == behavior).map {
                record =>
                    val items = record.split(",")
                    val userid = items(0)
                    val itemid = items(1)
                    (userid + "-" + itemid, 1)
            }.groupByKey.map {
                record =>
                    (record._1, record._2.size.toString)
            }
        }

        def getResultByBehavior(behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
            val intervaloneday =  getDataByIntervalByBehavior(1, behavior)
            val intervaltwoday = getDataByIntervalByBehavior(2, behavior)
            val intervalthreeday = getDataByIntervalByBehavior(3, behavior)
            val intervaloneweek = getDataByIntervalByBehavior(7, behavior)
            val intervaltwoweek = getDataByIntervalByBehavior(14, behavior)
            //val intervalthreeweek = getDataByIntervalByBehavior(21, behavior)

            joinWithOriginData(origindata, intervaloneday).leftOuterJoin(intervaltwoday)
                .map(operatorAfterJoin(_, "0")).leftOuterJoin(intervalthreeday)
                .map(operatorAfterJoin(_, "0")).leftOuterJoin(intervaloneweek)
                .map(operatorAfterJoin(_, "0")).leftOuterJoin(intervaltwoweek)
                //.map(operatorAfterJoin(_, "0")).leftOuterJoin(intervalthreeweek)
                .map(operatorAfterJoin(_, "0")).distinct()
        }

        val useritemClickData = getResultByBehavior(Cota.CLICK)
        val useritemBuyData = getResultByBehavior(Cota.BUY)
        val useritemFavData = getResultByBehavior(Cota.FAV)
        val useritemCartData = getResultByBehavior(Cota.CART)

        val result = useritemClickData.join(useritemBuyData)
            .map(operatorAfterInnerJoin).join(useritemFavData)
            .map(operatorAfterInnerJoin).join(useritemCartData)
            .map(operatorAfterInnerJoin)//.map(e => e._1 + "," + e._2)

        //result.coalesce(1).saveAsTextFile(PathNamespace.prefix + "useritemfeaturepart3test")
        result
    }

    /**
     * 在当天点击的所有item中的点击次数排名
     * 在当天点击的所有item中的收藏次数排名
     * 如果行为次数一样那么就是一样的名次不分先后
     * @param origindata
     * @return
     */
    def getUserItemBehaviorCountRank(origindata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
        null
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val offlinetrainorigindata = sc.textFile(PathNamespace.offlinetrainfeaturedata)
        val offlinetestorigindata = sc.textFile(PathNamespace.offlinetestfeaturedata)
        val onlinetrainorigindata = sc.textFile(PathNamespace.onlinetrainfeaturedata)
        /*yaGetUserItemFeatures(offlinetrainorigindata).join(getUserItemBuyCountByWeekDay(offlinetrainorigindata))
            .map(operatorAfterInnerJoin).join(getUserItemBehaviorBeforeTargetDate(offlinetrainorigindata, "2014-12-17"))
            .map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(offlinetrainorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "wj/offlinetrainfeatures")

        yaGetUserItemFeatures(offlinetestorigindata).join(getUserItemBuyCountByWeekDay(offlinetestorigindata))
            .map(operatorAfterInnerJoin).join(getUserItemBehaviorBeforeTargetDate(offlinetestorigindata, "2014-12-18"))
            .map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(offlinetestorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "wj/offlinetestfeatures")

        yaGetUserItemFeatures(onlinetrainorigindata).join(getUserItemBuyCountByWeekDay(onlinetrainorigindata))
            .map(operatorAfterInnerJoin).join(getUserItemBehaviorBeforeTargetDate(onlinetrainorigindata, "2014-12-19"))
            .map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(onlinetrainorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "wj/onlinetrainfeatures")*/

        //yaGetUserItemFeatures(offlinetestorigindata).coalesce(1).saveAsTextFile(PathNamespace.prefix + "debug/offlinetestfeaturespart1")
        //getUserItemBuyCountByWeekDay(offlinetestorigindata).coalesce(1).saveAsTextFile(PathNamespace.prefix + "debug/offlinetestfeaturespart2")
        getUserItemBehaviorBeforeTargetDate(offlinetrainorigindata, "2014-12-17").map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "offlinetrainfeaturespart3")
        getUserItemBehaviorBeforeTargetDate(offlinetestorigindata, "2014-12-18").map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "offlinetestfeaturespart3")
        getUserItemBehaviorBeforeTargetDate(onlinetrainorigindata, "2014-12-19").map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "onlinetestfeaturespart3")
    }
}

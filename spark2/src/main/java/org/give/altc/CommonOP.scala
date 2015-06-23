package org.give.altc

import java.text.DecimalFormat

import org.apache.spark.SparkContext._
import org.give.altc.features.item.ItemFeatures
import org.give.altc.features.user.UserFeatures
import org.give.altc.features.useritem.UserItemFeatures

/**
 * Created by zjh on 15-3-21.
 */
object CommonOP {
    val formatter = new DecimalFormat("#.#########")

    def getYearMonthDay(origin: String): Long = {
        val timeItems = origin.split(",").last.split(" ")(0).split("-")
        val (year, month, day) = (timeItems(0), timeItems(1), timeItems(2))
        (year + month + day).toLong
    }

    def getFeatureDataByDate(data: org.apache.spark.rdd.RDD[String], startdate: Long, enddate: Long): org.apache.spark.rdd.RDD[String] = {
        data.filter {
            record =>
                val date = getYearMonthDay(record)
                date >= startdate && date <= enddate
        }
    }

    /**
     * 生成离线或者线上的特征数据与验证数据或者提交数据
     * 11.18-12.17 predict 12.18 离线测试 需要提取12.18购买的pair对来进行训练和离线测试参数
     * 11.19-12.18 predict 12.19 线上提交 提交预测12.19购买的pair对
     * 所以一共提取三份数据 11.18-12.17的线下训练特征 12.18的线下训练数据 11.19-12.18的线上预测特征
     * @param sc
     * @param input
     * @param offlineFeatureOutput
     * @param onlineFeatureOutput
     * @param offlinetrainOutput
     */
    def generateFeatureAndLabelData(sc: org.apache.spark.SparkContext, input: String, offlineFeatureOutput: String, onlineFeatureOutput: String, offlinetrainOutput: String): Unit = {
        val data = sc.textFile(input)

        val offlineFeatureData = getFeatureDataByDate(data, 20141118, 20141217)
        val onlineFeatureData = getFeatureDataByDate(data, 20141119, 20141218)

        val offlineTrainData = getFeatureDataByDate(data, 20141218, 20141218)

        //11.18-12.17的线下特征数据
        offlineFeatureData.coalesce(1).saveAsTextFile(offlineFeatureOutput)
        //11.19-12.18的线上特征数据
        onlineFeatureData.coalesce(1).saveAsTextFile(onlineFeatureOutput)
        //12.18的线下训练数据
        offlineTrainData.coalesce(1).saveAsTextFile(offlinetrainOutput)
    }

    //离线测试不要再73分了
    //11.18-12.16提取离线训练数据 12.17作为label
    //11.19.12.17提取离线测试数据 12.18作为label
    //12.17的target数据
    //12.18的target数据
    //11.20-12.18作为线上的训练数据
    def generateFeatureAndLabelData(sc: org.apache.spark.SparkContext): Unit ={
        val origindata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_user)

        val offlinetrainfeaturedata = getFeatureDataByDate(origindata, 20141118, 20141216)
        val offlinetestfeaturedata = getFeatureDataByDate(origindata, 20141119, 20141217)
        val offlinetrainlabeldata = getFeatureDataByDate(origindata, 20141217, 20141217)
        val offlinetestlabeldata = getFeatureDataByDate(origindata, 20141218, 20141218)

        val onlinetrainfeaturedata = getFeatureDataByDate(origindata, 20141120, 20141218)

        offlinetrainfeaturedata.coalesce(1).saveAsTextFile(PathNamespace.offlinetrainfeaturedata)
        offlinetestfeaturedata.coalesce(1).saveAsTextFile(PathNamespace.offlinetestfeaturedata)
        offlinetrainlabeldata.coalesce(1).saveAsTextFile(PathNamespace.offlinetrainlabeldata)
        offlinetestlabeldata.coalesce(1).saveAsTextFile(PathNamespace.offlinetestlabeldata)
        onlinetrainfeaturedata.coalesce(1).saveAsTextFile(PathNamespace.onlinetrainfeaturedata)
    }

    def getDataJoinWithItemData(sc: org.apache.spark.SparkContext, inputData: org.apache.spark.rdd.RDD[String], outputpath: String): Unit = {
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val targetresult = inputData.map {
            record =>
                val items = record.split(",")
                val itemid = items(1)
                (itemid, record)
        }

        //473个结果 1218号与item表join以后会购买的pair对为473 这个是离线进行f1计算的target数据集合
        val result = targetresult.join(itemdata).map(_._2._1)

        result.coalesce(1).saveAsTextFile(outputpath)
    }

    //之前训练数据都是从1118-1217或者从1119-1218时间跨度很大 噪音也大 有些较早的数据其实对最后一天的预测影响不大所以可以缩短训练数据的时间范围
    def cutDataWithDates(sc: org.apache.spark.SparkContext, inputpath: String, startdate: Long, enddate: Long): org.apache.spark.rdd.RDD[String] = {
        val data = sc.textFile(inputpath)
        val aftercutData = getFeatureDataByDate(data, startdate, enddate)
        //还需要和itemdata进行join操作
        aftercutData
    }

    def cutDataWithDatesWriteToHDFS(sc: org.apache.spark.SparkContext, inputpath: String, outputpath: String, startdate: Long, enddate: Long): Unit = {
        val data = sc.textFile(inputpath)
        val aftercutData = getFeatureDataByDate(data, startdate, enddate)
        aftercutData.coalesce(1).saveAsTextFile(outputpath)
    }

    def cutDataWithDaysJoinItemData(sc: org.apache.spark.SparkContext, inputpath: String, outputpath: String, startdate: Long, enddate: Long): Unit = {
        getDataJoinWithItemData(sc, cutDataWithDates(sc, inputpath, startdate, enddate), outputpath)
    }

    def getSpecificBehaviorData(origindata: org.apache.spark.rdd.RDD[String], behavior: Int): org.apache.spark.rdd.RDD[String] = {
        origindata.filter(_.split(",")(2).toInt == behavior)
    }

    def getSpecificBehaviorUserItemWithDate(origindata: org.apache.spark.rdd.RDD[String], behavior: Int, seperator: String): org.apache.spark.rdd.RDD[(String, List[String])] = {
        origindata.map {
            record =>
                val dateItems = record.split(",").last.split(" ")
                val dateHour = dateItems(0) + "-" + dateItems(1)
                (record.split(",")(0) + seperator + record.split(",")(1) + "@" + dateHour, record.split(",")(2))
        }.filter(_._2.toInt == behavior).map {
            record =>
                (record._1.split("@")(0), record._1.split("@")(1))
        }.groupByKey.map {
            record =>
                (record._1, record._2.toList.distinct.sortWith(_ < _))
        }
    }

    def incIfZero[T](num: T, inc: T): T = {
        if (num == 0) {
            inc
        } else {
            num
        }
    }

    def operatorAfterJoin(record: (String, (String, Option[String]))): (String, String) = {
        val id = record._1
        val firstbehavior = record._2._1
        val secondbehavior = record._2._2 match {
            case Some(behavior) => behavior
            case None => "0"
        }
        (id, firstbehavior + "-" + secondbehavior)
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

    //统计第一次点击到购买之间的时间表
    def getClickToBuyDuration(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
        val origindata = sc.textFile(input)

        val visitUserItemDates = getSpecificBehaviorUserItemWithDate(origindata, 1, ",")
        val buyUserItemDates = getSpecificBehaviorUserItemWithDate(origindata, 4, ",")

        val userItemBuyVisitDates = visitUserItemDates.join(buyUserItemDates)

        val userItemBuyDuration = userItemBuyVisitDates.map {
            record =>
                val user_item = record._1
                val clickdates = record._2._1
                val buydates = record._2._2

                var buyDurationList = List[String]()

                for (i <- 0 to buydates.size - 1) {
                    var beforeBuyClicks = List[String]()
                    var buydate = ""
                    var clickdate = ""

                    if (i == 0) {
                        buydate = buydates(i)
                        beforeBuyClicks = clickdates.filter(_ < buydate)
                    } else {
                        val lastBuyDate = buydates(i - 1)
                        buydate = buydates(i)
                        beforeBuyClicks = clickdates.filter(c => c < buydate && c > lastBuyDate)
                    }
                    if (!beforeBuyClicks.isEmpty) {
                        clickdate = beforeBuyClicks.min
                        val duration = TimeUtils.calcDateDuration(buydate, clickdate)

                        buyDurationList = buyDurationList :+ (user_item + "," + buydate + "," + duration)
                    }
                }
                buyDurationList
        }.filter(!_.isEmpty).flatMap(t => t)

        userItemBuyDuration.coalesce(1).saveAsTextFile(output)
    }

    //需要把日期解析为星期几，然后统计category在一周的各天内的购买情况
    def getBuyCountEachCategoryWeekDay(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
        sc.textFile(input).filter {
            record =>
                record.split(",")(2).toInt == 4
        }.map {
            record =>
                val date = record.split(",").last.split(" ")(0)
                val weekday = TimeUtils.getWeekDayFromDate(date)
                val categoryid = record.split(",")(4)
                (categoryid + "-" + weekday, 1)
        }.groupByKey.map {
            record =>
                val items = record._1.split("-")
                val (categoryid, weekday) = (items(0), items(1))
                (categoryid, weekday + "-" + record._2.toList.size)
        }.groupByKey.map {
            record =>
                var weekdayBuyCount = List[Int]()
                //处理出来购买数不为0的weekday pair
                val resultMap = record._2.toList.map(record => (record.split("-")(0).toInt, record.split("-")(1).toInt)).toMap

                for (weekday <- List.range(1, 8)) {
                    weekdayBuyCount = weekdayBuyCount :+ resultMap.getOrElse(weekday, 0)
                }
                record._1 + "," + weekdayBuyCount.mkString(",")
        }.coalesce(1).saveAsTextFile(output)
    }

    def generateUserItemFeature(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
        val featuredata = sc.textFile(input)
        UserItemFeatures.getUserItemFeatures(featuredata).coalesce(1).saveAsTextFile(output)
    }

    def generateUserFeature(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
        val featuredata = sc.textFile(input)
        UserFeatures.getUserFeatures(featuredata).coalesce(1).saveAsTextFile(output)
    }

    def generateItemFeature(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
        val featuredata = sc.textFile(input)
        ItemFeatures.getItemFeatures(featuredata).coalesce(1).saveAsTextFile(output)
    }
}
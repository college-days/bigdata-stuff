val ttt = sc.parallelize((1 to 100).toList)
val splits = ttt.randomSplit(Array(0.7, 0.3))
val (first, second) = (splits(0), splits(1))

val sample = ttt.sample(false, 0.3, 11L) //采样还是这个靠谱

def getBuyCountEachCategoryWeekDay(sc: org.apache.spark.SparkContext, input: String, output: String): Unit = {
    sc.textFile(prefix + input).filter {
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
    }.coalesce(1).saveAsTextFile(prefix + output)
}

def getDataByIntervalByBehavior(interval: Int, behavior: Int): org.apache.spark.rdd.RDD[(String, String)] = {
    def getFeatureDataByDate(data: org.apache.spark.rdd.RDD[String], startdate: Long, enddate: Long): org.apache.spark.rdd.RDD[String] = {
        data.filter {
            record =>
                val date = getYearMonthDay(record)
                date >= startdate && date <= enddate
        }
    }
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
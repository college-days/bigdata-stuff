package org.give.newsspark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-5.
 * 提取每个用户的最后一条浏览记录用作label
 */
object GenerateValidateLabel {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Validate Label"))
        val users = sc.textFile("/user/hive/warehouse/users/000000_0")
        val news = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt") //or /user/root/newsdata.txt
        val newsSplits = news.map(line => line.split("\t"))

        //获得最近一次的浏览时间
        def getTargetTimestamp(userid: Int) = {
            val visitTimestamps = newsSplits.filter(line => line(0).toInt == userid.toInt).map(line => line(2).toInt).collect
            scala.util.Sorting.quickSort(visitTimestamps)
            val targetTimestamp = visitTimestamps(visitTimestamps.size - 1)
            println(userid + " " + targetTimestamp)
            Map("userid" -> userid, "timestamp" -> targetTimestamp)
        }

        //根据获取到的浏览时间和用户id来得到用户id对应的测试样本记录
        def getTarget(userid: Int, visitTimestamp: Int) = {
            println(userid + " " + visitTimestamp)
            newsSplits.filter(line => line(0).toInt == userid.toInt && line(2).toInt == visitTimestamp.toInt).map(line => line(0) + "\t" + line(1) + "\t" + line(2) + "\t" + line(3)).collect
        }

        //all in one
        //最终hdfs文件中存储的格式为userid newsid visit_timestamp visit_time
        def generateValidateLabel(users: Array[String], savePath: String) = {
            val targetUserTimestamp = users.map(userid => getTargetTimestamp(userid.toInt))
            val result = targetUserTimestamp.flatMap(usertimepair => getTarget(usertimepair("userid").toInt, usertimepair("timestamp").toInt)) //=> don't forget to use flatMap to merge all the sub Array into one Array
            val finalResult = sc.parallelize(result) //=> don't forget convert Array to a RDD here
            finalResult.cache().coalesce(1).saveAsTextFile(savePath)
        }

        generateValidateLabel(users.collect, "/user/root/validatelabel")
        System.out.println("generate validate label successful")
    }
}

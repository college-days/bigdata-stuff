package org.give.newsspark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-4.
 * 提取每个用户的倒数第二条浏览记录，用作label
 */
object GenerateTrainPositive {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Train Positive"))
        val users = sc.textFile("/user/hive/warehouse/users/000000_0")
        val news = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt") //or /user/root/newsdata.txt
        val newsSplits = news.map(line => line.split("\t"))

        //获得最近第二次的浏览时间
        def getTargetTimestamp(userid:Int) = {
            val visitTimestamps = newsSplits.filter(line => line(0).toInt == userid.toInt).map(line => line(2).toInt).collect
            scala.util.Sorting.quickSort(visitTimestamps)
            val targetTimestamp = visitTimestamps(visitTimestamps.size-2)
            println(userid + " " + targetTimestamp)
            Map("userid" -> userid, "timestamp" -> targetTimestamp)
        }

        //根据获取到的浏览时间和用户id来得到用户id对应的训练样本记录
        def getTarget(userid:Int, visitTimestamp:Int) = {
            println(userid + " " + visitTimestamp)
            newsSplits.filter(line => line(0).toInt == userid.toInt && line(2).toInt == visitTimestamp.toInt).map(line => line(0) + "\t" + line(1) + "\t" + line(2) + "\t" + line(3)).collect
        }

        //all in one
        //最终hdfs文件中存储的格式为userid newsid visit_timestamp visit_time
        def generateTrainPositive(users:Array[String], savePath:String) = {
            val targetUserTimestamp = users.map(userid => getTargetTimestamp(userid.toInt))
            val result = targetUserTimestamp.flatMap(usertimepair => getTarget(usertimepair("userid").toInt, usertimepair("timestamp").toInt)) //=> don't forget to use flatMap to merge all the sub Array into one Array
            val finalResult = sc.parallelize(result) //=> don't forget convert Array to a RDD here
            finalResult.cache().coalesce(1).saveAsTextFile(savePath)
        }

        //just for test
        //generateTrainPositive(users.take(10), "/user/root/target1")

        //start to get train positive data
        generateTrainPositive(users.collect, "/user/root/trainpositive")
        System.out.println("generate train positive successful")

        //validation
        //val trainpositive = sc.textFile("/user/root/trainpositive/part-00000")
        //trainpositive.count
    }
}

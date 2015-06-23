package org.give.newsspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

/**
 * Created by zjh on 14-10-6.
 * 用户特征：
	•	用户总共浏览的新闻数(distinct newsid)
	•	用户总浏览的天数
	•	用户每天浏览的新闻数的均值、中位数、方差
 */
object GenerateUserFeature {
    case class Usernews(userid:Int, newsid:Int, visit_timestamp:Long, visit_time:String, post_timestamp:Long, post_time:String, news_title:String, news_content:String)

    def main(args: Array[String]) {
        //ERROR Client: Required executor memory (8192 MB), is above the max threshold (2148 MB) of this cluster.
        //val sc = new SparkContext(new SparkConf().setAppName("User Feature").set("spark.executor.memory", "8g"))
        val sc = new SparkContext(new SparkConf().setAppName("User Feature"))
        val sqlContext = new SQLContext(sc)
        import sqlContext._

        val users = sc.textFile("/user/hive/warehouse/users/000000_0").cache()
        val usernews = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt").map { record =>
            record.split("\t")
        }.map { record =>
            Usernews(record(0).toInt, record(1).toInt, record(2).toLong, record(3).toString, record(4).toLong, record(5).toString, record(6).toString, record(7).toString)
        }

        usernews.registerAsTable("table_usernews")

        /*
        just for test
        var test = sqlContext.sql("select u.cnt from (select userid, count(*) as cnt from table_usernews group by userid)u where u.userid = 75")
        test = sqlContext.sql("select distinct newsid from table_usernews where userid = 75")
        */

        def square(item:Double):Double = {
            item * item
        }

        def getUserFeatures(userid:Int) = {
            val result = sqlContext.sql("select * from table_usernews where userid = " + userid).cache()
            //用户总共浏览的新闻数(distinct newsid)
            val newsCountRDD = result.map(record => record(1))
            //get dinstinct news count
            val newsCount = newsCountRDD.collect().toSet.size
            //用户总浏览的天数
            //get date
            val dayCountRDD = result.map(record => record(3).toString.split(" ")(0))
            //get dinstinct day count
            val dayCount = dayCountRDD.collect().toSet.size
            //用户每天浏览的新闻数的均值、中位数、方差
            val dayDetailRDD = result.groupBy(_(3).toString.split(" ")(0)).map(record => (record._1, record._2.size))
            val dayCalcRDD = dayDetailRDD.map(record => record._2.toInt).cache()
            //均值
            val visit_avg:Double = dayCalcRDD.reduce(_+_).toDouble / dayCount
            //方差
            val visit_var = dayCalcRDD.map(count => square(count-visit_avg)).reduce(_+_) / dayCount
            //中位数
            val mid_index = dayCount.toInt / 2.toInt
            val visit_mid = dayCalcRDD.collect.sortWith(_ < _)(mid_index)
            println("userid: " + userid)
            println("newscount: " + newsCount)
            println("daycount: " + dayCount)
            println("avg: " + visit_avg)
            println("var: " + visit_var)
            println("mid_index:" + mid_index)
            println("mid: " + visit_mid)
            userid+"\t"+newsCount+"\t"+dayCount+"\t"+visit_avg+"\t"+visit_var+"\t"+visit_mid
        }

        /*
        nested rdd again
        val userFeatures = users.map{ userid =>
            getUserFeatures(userid.toInt)
        }
        */
        val userids = users.collect()
        val userFeatureList = new ListBuffer[String]

        for (i <- 0 to userids.size-1){
            userFeatureList += getUserFeatures(userids(i).toInt)
        }

        val userFeatures = sc.parallelize(userFeatureList)
        userFeatures.cache().coalesce(1).saveAsTextFile("/user/root/userfeature")
        println("generate user feature successful")
    }
}

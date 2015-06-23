package org.give.newsspark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by zjh on 14-10-6.
 * 新闻特征
	•	总点击量
	•	总点击的用户数（distinct userid）
	•	总点击的天数
 */
object GenerateNewsFeature {
    case class Usernews(userid:Int, newsid:Int, visit_timestamp:Long, visit_time:String, post_timestamp:Long, post_time:String, news_title:String, news_content:String)

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("News Feature"))
        val sqlContext = new SQLContext(sc)
        import sqlContext._

        val news = sc.textFile("/user/hive/warehouse/news/000000_0").cache()
        val usernews = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt").map { record =>
            record.split("\t")
        }.map { record =>
            Usernews(record(0).toInt, record(1).toInt, record(2).toLong, record(3).toString, record(4).toLong, record(5).toString, record(6).toString, record(7).toString)
        }

        usernews.registerAsTable("table_usernews")

        /*
        just for test
        var test = sqlContext.sql("select * from table_usernews where newsid = 100650277")
        */

        def getNewsFeatures(newsid:Int) = {
            val result = sqlContext.sql("select * from table_usernews where newsid = " + newsid).cache()
            //总点击量
            val totalClickCount = result.count()
            //总点击的用户数（distinct userid）
            val userCountRDD = result.map(record => record(0))
            val userCount = userCountRDD.collect.toSet.size
            //总点击的天数
            val dayCountRDD = result.map(record => record(3).toString.split(" ")(0))
            val dayCount = dayCountRDD.collect.toSet.size
            println("newsid: " + newsid)
            println("totalclickcount: " + totalClickCount)
            println("usercount: " + userCount)
            println("daycount: " + dayCount)
            newsid+"\t"+totalClickCount+"\t"+userCount+"\t"+dayCount
        }

        val newsids = news.collect()
        val newsFeatureList = new ListBuffer[String]

        for (i <- 0 to newsids.size-1){
            newsFeatureList += getNewsFeatures(newsids(i).toInt)
        }

        val newsFeatures = sc.parallelize(newsFeatureList)
        newsFeatures.cache().coalesce(1).saveAsTextFile("/user/root/newsfeature")
        println("generate news feature successful")
    }
}

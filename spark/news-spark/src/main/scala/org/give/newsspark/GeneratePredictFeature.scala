package org.give.newsspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ListBuffer
/**
 * Created by zjh on 14-10-5.
 * 预测用的特征：用所有数据提取特征。
 * 每一个样本是一个“用户-新闻对”，用户是所有用户，新闻是该用户没有看过的新闻
 * spark-submit --class org.give.newsspark.GeneratePredictFeature --master yarn news-spark-1.0-SNAPSHOT.jar
 */
object GeneratePredictFeature {
    //define a schema to enable use spark-sql
    case class Usernews(userid:Int, newsid:Int, visit_timestamp:Long, visit_time:String, post_timestamp:Long, post_time:String, news_title:String, news_content:String)

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Predict Feature"))
        val sqlContext = new SQLContext(sc)
        import sqlContext._

        val users = sc.textFile("/user/hive/warehouse/users/000000_0").cache()
        val news = sc.textFile("/user/hive/warehouse/news/000000_0").cache()
        val usernews = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt").map { record =>
            record.split("\t")
        }.map { record =>
            Usernews(record(0).toInt, record(1).toInt, record(2).toLong, record(3).toString, record(4).toLong, record(5).toString, record(6).toString, record(7).toString)
        }

        usernews.registerAsTable("table_usernews")

        /*
        //just for test just make sure we can use spark-sql for now
        var test = sqlContext.sql("select distinct userid from table_usernews")
        test = sqlContext.sql("select count(distinct newsid) from table_usernews")
        test = sqlContext.sql("select count(*) from table_usernews where userid = 930 and newsid = 100646087")
        test = sqlContext.sql("select count(*) from table_usernews where userid = 5218791 and newsid = 100648598")
        test = sqlContext.sql("select count(*) from table_usernews where userid = 930 and newsid = 100646153")
        test = sqlContext.sql("select count(*) from table_usernews where userid = 930 and newsid = 123")
        test = sqlContext.sql("select newsid from table_usernews where userid = 930")

        //reference this source code https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/api/java/Row.scala#L53
        test.collect()(0).getLong(0) //=> get the result:Long
        test.collect()(0).getLong(0) == 0
        */

        /*never use nested RDD
        users.map{ user =>
            news.map{ news =>
                (user, news)
            }
        }*/

        /*this is too slow
        val notInteractedUserNewsPair = new ListBuffer[String]
        def isNotAlreadyInteracted(userid:Int, newsid:Int) = {
            val sqlresult = sqlContext.sql("select count(*) from table_usernews where userid = " + userid + " and newsid = " + newsid)
            val result = sqlresult.collect()(0).getLong(0)
            result == 0
        }

        val userids = users.collect
        val newsids = news.collect

        for (i <- 0 to userids.size-1){
            for (j <- 0 to newsids.size-1){
                val userid = userids(i)
                val newsid = newsids(j)
                println(userid + " --- " + newsid)
                val isNotInteracted = isNotAlreadyInteracted(userid.toInt, newsid.toInt)
                if (isNotInteracted){
                    notInteractedUserNewsPair += userid + "\t" + newsid
                }
            }
        }

        val notInteracted = sc.parallelize(notInteractedUserNewsPair)
        notInteracted.cache().coalesce(1).saveAsTextFile("/user/root/predictfeature")
        println("generate predict feature successful")
        */

        //获得某一个用户没有交互过的所有新闻id，以-为分隔符组成字符串返回
        def getNotInteractivedNews(userid: Int): String = {
            val newsList = sqlContext.sql("select newsid from table_usernews where userid = " + userid).map(record => record(0).toString.toInt).collect().toSet
            val notInteractivedNewsList = news.filter(newsid => !newsList.contains(newsid.toInt))
            notInteractivedNewsList.collect.mkString("-")
        }

        /*
        never nested RDD again
        users.map{ userid =>
            println(userid)
            userid + "\t" + getNotInteractivedNews(userid.toInt)
        }
        */
        //如果不分片的话，把所有结果都放在一个ListBuffer中貌似内存就爆掉了，所以才将所有结果分片存储
        val slices = 100
        val userids = users.collect
        val sliceCount = userids.size/slices

        for (k <- 0 to slices-1){
            val notInteractedUserNewsPair = new ListBuffer[String]

            for (i <- k*sliceCount to (k+1)*sliceCount-1) {
                try {
                    val userid = userids(i)
                    println("index: " + i)
                    println("userid: " + userid)
                    println("slice: " + k)
                    notInteractedUserNewsPair += userid + "\t" + getNotInteractivedNews(userid.toInt)
                }catch {
                    case ex:Exception => println("userid: " + userids(i))
                }
            }

            val notInteracted = sc.parallelize(notInteractedUserNewsPair)
            notInteracted.cache().coalesce(1).saveAsTextFile("/user/root/predictfeature/part" + k)
            println("generate predict feature " + k + " successful")
        }

   }
}

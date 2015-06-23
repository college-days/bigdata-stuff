package org.give.playspark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by zjh on 14-10-5.
 */
object SparkSqlHive {
    case class Rating(uid:Int, mid:Int, rating:Float, timestamp:Long)
    case class Movie(mid:Int, title:String, genres:String)

    def main(args: Array[String]){
        val sc = new SparkContext(new SparkConf().setAppName("Sql Hive"))
        val sqlContext = new SQLContext(sc)
        import sqlContext.createSchemaRDD

        val ratings = sc.textFile("hdfs://localhost:9000/ratings.dat").map(_.split("::")).map{ record =>
            Rating(record(0).toInt, record(1).toInt, record(2).toFloat, record(3).toLong)
        }
        val movies = sc.textFile("hdfs://localhost:9000/movies.dat").map(_.split("::")).map{ record =>
            Movie(record(0).toInt, record(1).toString, record(2).toString)
        }
        //println(ratings.count())
        //ratings.first()
        //create a table in memory and the table name is table_ratings
        ratings.registerAsTable("table_ratings")
        movies.registerAsTable("table_movies")

        val ratingsStat = sqlContext.sql("select mid, count(*) as cnt from table_ratings group by mid")
        //ratingsStat.collect
        //ratingsStat.take(10)
        val ratingsStat2 = sqlContext.sql("select mid, count(*) as cnt from table_ratings group by mid order by cnt desc")
        val bestMovies = sqlContext.sql("SELECT mid, AVG(rating) AS score, COUNT(*) as cnt FROM table_ratings GROUP BY mid HAVING cnt > 1000 ORDER BY score DESC")
        //sql having where
        bestMovies.foreach(println)
        bestMovies.collect.foreach(println)
        val movieStat = sqlContext.sql("select genres, count(*) as cnt from table_movies group by genres order by cnt desc")
        movieStat.collect.foreach(println)

        //spark hive is not support in the origin spark if want to use should recompile the spark so just use spark-sql for now
        //val hiveContext = new HiveContext(sc)
    }
}

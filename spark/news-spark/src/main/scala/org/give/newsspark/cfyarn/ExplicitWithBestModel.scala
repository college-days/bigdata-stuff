package org.give.newsspark.cfyarn

import java.lang.Math._
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-13.
 * 最优模型rank=12 numIter = 20现在改成30试试
 */
object ExplicitWithBestModel {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Explicit Feedback"))
        val explicitPositiveRDD = sc.textFile("/user/root/visitcount.txt")
        val explicitNegativeRDD = sc.textFile("/user/root/predictfeature/ultimate/part-00000")

        val positiveRatings = explicitPositiveRDD.map { record =>
            val fields = record.split("\t")
            Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
        }

        val negativeRatings = explicitNegativeRDD.map { record =>
            val fields = record.split("\t")
            Rating(fields(0).toInt, fields(1).toInt, 0.toDouble)
        }

        val random = new Random(0)
        val selectNegativeRatings = negativeRatings.filter(x => random.nextDouble() < 0.0125) //随机采样

        val training = positiveRatings.union(selectNegativeRatings)

        val rank = 12
        val numIter = 30

        val model = ALS.train(training, rank, numIter)

        //为每一个用户推荐的新闻的个数
        val recommendnumForEachUser = 3

        val finalResult = predictForAll(sc, model, recommendnumForEachUser)
        val finalResultRDD = sc.parallelize("userid,newsid" +: finalResult)

        finalResultRDD.cache().coalesce(1).saveAsTextFile("/user/root/explicitfeedbacknum/num" + recommendnumForEachUser)
        println("generate total record: " + finalResult.size)
        println("generate explicit feedback with num " + recommendnumForEachUser + " submit result successful")
    }

    def predictForAll(sc:SparkContext, model:MatrixFactorizationModel, recommendcount:Int) = {
        val sliceList = List.range(0, 100)
        //val sliceList = List.range(0, 3)
        val flattenResult = sliceList.map{ slice =>
            val notInteractedPairRDD = sc.textFile("/user/root/predictfeature/part" + slice).map(record => record.split("\t")).map(record => (record(0), record(1).split("-"))).map{ record =>
                val userid = record._1
                println(userid)
                record._2.map(newsid => (userid.toInt, newsid.toInt))
            }
            notInteractedPairRDD.collect.map { record =>
                val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(recommendcount)
                recommend.map{ record =>
                    record.user + "," + record.product
                }
            }.reduce(_ ++ _)
        }.reduce(_ ++ _)

        flattenResult
    }
}

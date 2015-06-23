package org.give.newsspark.cfyarn

import java.lang.Math._
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-12.
 * implicit只是说用看过作为1没看过作为0，explicit就是使用重复浏览的次数作为分数了
 * 打分用浏览次数来做，而不是0和1，看了2次就是2
 */
object ExplicitFeedBack {
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

        val ranks = List(8, 12)
        val numIters = List(10, 20)

        var bestModel: Option[MatrixFactorizationModel] = None
        var bestValidationRmse = Double.MaxValue
        var bestRank = 0
        var bestNumIter = -1

        for (rank <- ranks; numIter <- numIters) {
            val model = ALS.train(training, rank, numIter)
            val validationRmse = computeRmse(model, training)
            println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", and numIter = " + numIter + ".")
            if (validationRmse < bestValidationRmse) {
                bestModel = Some(model)
                bestValidationRmse = validationRmse
                bestRank = rank
                bestNumIter = numIter
            }
        }

        val finalResult = predictForAll(sc, bestModel.get)
        val finalResultRDD = sc.parallelize("userid,newsid" +: finalResult)

        finalResultRDD.cache().coalesce(1).saveAsTextFile("/user/root/explicitfeedback")
        println("generate explicit feedback submit result successful")
    }

    def predictForAll(sc:SparkContext, model: MatrixFactorizationModel) = {
        val sliceList = List.range(0, 100)
        //val sliceList = List.range(0, 3)
        val flattenResult = sliceList.map{ slice =>
            val notInteractedPairRDD = sc.textFile("/user/root/predictfeature/part" + slice).map(record => record.split("\t")).map(record => (record(0), record(1).split("-"))).map{ record =>
                val userid = record._1
                println(userid)
                record._2.map(newsid => (userid.toInt, newsid.toInt))
            }
            notInteractedPairRDD.collect.map{ record =>
                val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(1)(0)
                //bestModel.get.predict(sc.parallelize(record)).collect.sortBy(-_.rating)
                recommend.user + "," + recommend.product
            }
        }.reduce(_ ++ _)

        flattenResult
    }

    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
        val usersProducts = data.map { case Rating(user, product, rate) =>
            (user, product)
        }
        val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
            ((user, product), rate)
        }
        val ratesAndPreds = data.map { case Rating(user, product, rate) =>
            ((user, product), rate)
        }.join(predictions)
        val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
            val err = (r1 - r2)
            err * err
        }.mean()
        sqrt(MSE)
    }
}

package org.give.newsspark.cfyarn

import java.lang.Math._
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-13.
 * 找到最好的显式反馈的训练模型
 * RMSE (validation) = 0.12016356733989436 for the model trained with rank = 12, and numIter = 20. 找了半天还是这一对基友数值最优 给跪了，而且rmse不高啊...
 * RMSE (validation) = 0.12018939783343864 for the model trained with rank = 12, and numIter = 20.
 * rank = 12 numIter = 20 validationRmse: Double = 0.11762749350444507
 * rank = 12 numIter = 30 validationRmse: Double = 0.11648403003566754
 *
 */
object ExplicitUltimateModel {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Explicit Feedback Best Model"))

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

        //val ranks = List(8, 12)
        //val numIters = List(10, 20)
        val ranks = List.range(8, 13)
        val numIters = List.range(10, 21)

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

        println("RMSE (validation) = " + bestValidationRmse + " for the model trained with rank = "
            + bestRank + ", and numIter = " + bestNumIter + ".")

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

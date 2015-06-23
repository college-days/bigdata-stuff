package org.give.newsspark.cfyarn

import java.lang.Math._
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-13.
 * 找到最好的隐式反馈的模型参数
 * RMSE (validation) = 0.6354172639917445 for the model trained with rank = 12, and numIter = 20.validationRmse: Double = 0.63586072535119
 * rank = 12 numIter = 30 validationRmse: Double = 0.6351767061103223
 * RMSE (validation) = 0.6349322479421079 for the model trained with rank = 12, and numIter = 18.validationRmse: Double = 0.6360763650860715
 * 反正两边都是12,20这对基友参数最优
 */

object ImplicitUltimateModel {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Implicit Feedback Best Model"))

        val implicitPositiveRDD = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt")
        val implicitNegativeRDD = sc.textFile("/user/root/predictfeature/ultimate/part-00000")

        val positiveRatings = implicitPositiveRDD.map { record =>
            //val fields = record.split(" ")
            val fields = record.split("\t")
            //Rating(userid, newsid, implicit rating 0/1)
            Rating(fields(0).toInt, fields(1).toInt, 1.toDouble)
        }
        //=>96263

        val negativeRatings = implicitNegativeRDD.map { record =>
            val fields = record.split("\t")
            //Rating(userid, newsid, implicit rating 0/1)
            Rating(fields(0).toInt, fields(1).toInt, 0.toDouble)
        }//=>61728880 是正例的641倍

        //负例为正例的8倍，所以对原有的负例进行采样，抽取1/80来最终进行训练
        val random = new Random(0)
        val selectNegativeRatings = negativeRatings.filter(x => random.nextDouble() < 0.0125) //随机采样

        //如果不采样就内存溢出了 outofmemory
        //val training = positiveRatings.union(negativeRatings)
        val training = positiveRatings.union(selectNegativeRatings)

        val ranks = List.range(8, 13)
        val numIters = List.range(10, 21)

        var bestModel: Option[MatrixFactorizationModel] = None
        var bestValidationRmse = Double.MaxValue
        var bestRank = 0
        var bestNumIter = -1

        for (rank <- ranks; numIter <- numIters) {
            val model = ALS.trainImplicit(training, rank, numIter)
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

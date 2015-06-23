package org.give.newsspark.cfyarn

import java.lang.Math._
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-9.
 */
object ImplicitFeedBack {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Implicit Feedback"))
        //val implicitPositiveRDD = sc.textFile("/user/root/implicitpositive/part-00000")
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

        val ranks = List(8, 12)
        val numIters = List(10, 20)
        //RMSE (validation) = 0.6353492427645131 for the model trained with rank = 12, and numIter = 20.
        //that will take a lot of time
        //val ranks = List.range(3, 33)
        //val numIters = List.range(3, 33)

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

        val finalResult = predictForAll(sc, bestModel.get)
        val finalResultRDD = sc.parallelize("userid,newsid" +: finalResult)
        //第一次提交啊 天地保佑
        finalResultRDD.cache().coalesce(1).saveAsTextFile("/user/root/implicitfeedback")
        println("generate implicit feedback submit result successful")
        //sc.stop()
    }

    def modelPredict(model: MatrixFactorizationModel, userid: Int, newsid: Int) = {
        model.predict(userid, newsid)
    }

    def predictForAll(sc:SparkContext, model: MatrixFactorizationModel) = {
        //val sliceList = List.range(0, 100)
        val sliceList = List.range(0, 100)
        val flattenResult = sliceList.map{ slice =>
            val notInteractedPairRDD = sc.textFile("/user/root/predictfeature/part" + slice).map(record => record.split("\t")).map(record => (record(0), record(1).split("-"))).map{ record =>
                val userid = record._1
                println(userid)
                //record._2.map(newsid => userid + "\t" + newsid)
                record._2.map(newsid => (userid.toInt, newsid.toInt))
            }
            notInteractedPairRDD.collect.map{ record =>
                val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(1)(0)
                recommend.user + "," + recommend.product
            }
        }.reduce(_ ++ _)

        /*val result = flattenResult.take(3).map{ record =>
            val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(1)(0)
            recommend.user + "," + recommend.product
        }*/

        /*val slices = 20
        for (k <- 0 to slices-1){
            val partition = 500*(k+1)
            val result = flattenResult.take(500*).map{ record =>
                val recommend = bestModel.get.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(1)(0)
                recommend.user + "," + recommend.product
            }
        }*/

        /*val result = flattenResult.map{ record =>
            val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(1)(0)
            recommend.user + "," + recommend.product
        }*/
        //"userid,newsid" +: result
        flattenResult
    }
    //modelPredict(bestModel.get, 52550, 100643946)

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

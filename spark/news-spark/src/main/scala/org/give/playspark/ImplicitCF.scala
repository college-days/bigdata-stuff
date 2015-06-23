package org.give.playspark

import java.lang.Math.sqrt
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-9.
 */
object ImplicitCF {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Collaborate filtering"))
        val movieLensHomeDir = "hdfs://localhost:9000"
        val ratings = sc.textFile(movieLensHomeDir + "/ratings.dat").map { line =>
            val fields = line.split("::")
            val explicitRating = fields(2).toDouble
            var implicitRating = 0
            if (explicitRating >= 4){
                implicitRating = 1
            }
            // format: (timestamp % 10, Rating(userId, movieId, rating)) just checkout the doc or the source code of mllib
            (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, implicitRating.toDouble))
        }

        val movies = sc.textFile(movieLensHomeDir + "/movies.dat").map { line =>
            val fields = line.split("::")
            // format: (movieId, movieName) more specific is the movieTitle
            (fields(0).toInt, fields(1))
        }.collect.toMap

        val numRatings = ratings.count
        val numUsers = ratings.map(_._2.user).distinct.count
        val numMovies = ratings.map(_._2.product).distinct.count
        println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

        //选择出现次数最多的前50个电影的id
        val mostRatedMovieIds = ratings.map(_._2.product) // 获得一个序列，其中的元素是电影id
            .countByValue      // 获得一个键值对，键是电影id，值是某一个电影id在ratings中出现的次数
            .toSeq             // 将Map转换为一个ArrayBuffer，每一个元素是一个tuple，_1为原来的键，_2为原来的值
            .sortBy(- _._2)    // 根据电影出现次数这个值来进行降序排序
            .take(50)          // 选择前50个tuple
            .map(_._1)         // 获得前50个电影id

        val random = new Random(0)

        val selectedMovies = mostRatedMovieIds.filter(x => random.nextDouble() < 0.2) //随机采样出10个电影
            .map(x => (x, movies(x))) //获得电影对应的title
            .toSeq //其实没啥用，map之后已经是一个seq了

        //自己的用户id是0
        val myRatings = selectedMovies.map{ movie =>
            Rating(0, movie._1.toInt, 1)
        }

        val myRatingsRDD = sc.parallelize(myRatings)

        val numPartitions = 20

        //根据时间戳划分训练集，测试集和验证集
        val training = ratings.filter(x => x._1 < 6)
            .values
            .union(myRatingsRDD)
            .repartition(numPartitions)
            .persist

        val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
            .values
            .repartition(numPartitions)
            .persist

        val test = ratings.filter(x => x._1 >= 8).values.persist

        val numTraining = training.count
        val numValidation = validation.count
        val numTest = test.count

        println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

        //checkout this page rank lambda iterator is just the parameter of the als model
        //这里就是rank是8，12 lambda是0.1，10 iterator是10，20，通过对比RMSE来确定最佳的参数
        //http://spark.apache.org/docs/1.0.0/mllib-collaborative-filtering.html
        //上面文档中给的例子里面用到了scala中的case class的模式匹配，再一次感叹ml系的碉堡之处
        val ranks = List(8, 12)
        //val lambdas = List(0.1, 10.0)
        val numIters = List(10, 20)

        //option 防止空值None，用于进行模式匹配，scala身上处处流淌着ml的血液
        var bestModel: Option[MatrixFactorizationModel] = None
        var bestValidationRmse = Double.MaxValue
        var bestRank = 0
        var bestLambda = -1.0
        var bestNumIter = -1

        for (rank <- ranks; numIter <- numIters) {
            //val model = ALS.train(training, rank, numIter, lambda)
            val model = ALS.trainImplicit(training, rank, numIter)
            val validationRmse = computeRmse(model, validation, numValidation)
            println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", and numIter = " + numIter + ".")
            if (validationRmse < bestValidationRmse) {
                bestModel = Some(model)
                bestValidationRmse = validationRmse
                bestRank = rank
                //bestLambda = lambda
                bestNumIter = numIter
            }
        }

        val testRmse = computeRmse(bestModel.get, test, numTest)
        //result is => The best model was trained with rank = 8 and lambda = 10.0, and numIter = 20, and its RMSE on the test set is 0.8814913995480903.
        println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
            + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

        val myRatedMovieIds = myRatings.map(_.product).toSet //获得自己打分的电影
        val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq) //选出我没有打过分的电影id
        val recommendations = bestModel.get
                .predict(candidates.map((0, _))) //自己的用户id是0，上面已经提过了
                .collect //realize from rdd
                .sortBy(-_.rating) //按照评分值降序排序
                .take(50) //取分值最高的前50部电影

        val meanRating = training.union(validation).map(_.rating).mean
        val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating))
            .reduce(_ + _) / numTest)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        //%1只是表示第一个占位符.2f表示保留两位小数的浮点数
        println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

        var i = 1
        println("Movies recommended for you:")
        recommendations.foreach { r =>
            println("%2d".format(i) + ": " + movies(r.product))
            i += 1
        }

        sc.stop();
    }

    /** Compute RMSE (Root Mean Squared Error). rmse is just square root of mse */
    //the rmse implementation is just reference the mse implementation on this example http://spark.apache.org/docs/1.0.0/mllib-collaborative-filtering.html#examples
    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
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

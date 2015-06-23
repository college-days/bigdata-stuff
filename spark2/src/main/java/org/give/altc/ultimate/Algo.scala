package org.give.altc.ultimate

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.give.altc.PathNamespace

/**
 * Created by zjh on 15-4-17.
 */
object Algo {
    def joinPredictResultWithItemData(sc: org.apache.spark.SparkContext, predictResult: org.apache.spark.rdd.RDD[(String, (String, Double))], limit: Int): org.apache.spark.rdd.RDD[String] = {
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val result = predictResult.join(itemdata).map {
            record =>
                val (useritemid, prediction) = record._2._1
                (useritemid, prediction)
        }

        val a = PathNamespace.offlinetestfeaturesubdata
        val predictdata = sc.textFile(a).map {
            record =>
                val items = record.split(",")
                val (userid, itemid, behavior, date) = (items(0), items(1), items(2), items.last.split(" ")(0))
                (date, (userid + "," + itemid, behavior))
        }.filter(_._1 == "2014-12-17").map(_._2).groupByKey.map {
            record =>
                val useritem = record._1
                val behaviorlist = record._2.toList
                (useritem, behaviorlist)
        }.filter(e => e._2.contains("3") && !e._2.contains("4")).map(_._1).distinct.map(e => (e, 1))

        val newresult = result.join(predictdata).map {
            record =>
                val useritemid = record._1
                val prediction = record._2._1
                (prediction, useritemid)
        }.sortByKey(false)


        sc.parallelize(newresult.map(_._2).take(limit))
    }

    def trainLR(sc: org.apache.spark.SparkContext, input: String, output: String*): org.apache.spark.mllib.regression.LinearRegressionModel = {
        // Load and parse the data
        val data = sc.textFile(input)
        val parsedData = data.map { line =>
            val parts = line.split(",")
            LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
        }.cache()

        // Building the model
        val numIterations = 10000
        val model = LinearRegressionWithSGD.train(parsedData, numIterations)

        model
    }

    def offlineLRTest(sc: org.apache.spark.SparkContext, input: String, commitInput: String, limit: Int) = {
        val model = trainLR(sc, input)

        val commitData = sc.textFile(commitInput).map { line =>
            val parts = line.split(',')
            (parts(0), Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        }

        val predictResult = commitData.map {
            commitPoint =>
                //为了与trainpredict中的数据能相交 因为那里useriditemid连接符号是逗号
                val useritemid = commitPoint._1.replace("-", ",")
                val useritemids = commitPoint._1.split("-")
                val (userid, itemid) = (useritemids(0), useritemids(1))
                val prediction = model.predict(commitPoint._2)
                (itemid, (useritemid, prediction))
        }

        joinPredictResultWithItemData(sc, predictResult, limit)
    }

    def trainRandomForest(sc: org.apache.spark.SparkContext, input: String, output: String*): org.apache.spark.mllib.tree.model.RandomForestModel = {
        val data = MLUtils.loadLibSVMFile(sc, input)

        val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 100 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "variance"
        //val maxDepth = 30
        val maxDepth = 10
        val maxBins = 32

        val model = RandomForest.trainRegressor(data, categoricalFeaturesInfo,
            numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        model
    }

    def offlineRFTest(sc: org.apache.spark.SparkContext, input: String, commitInput: String, limit: Int, output: String*): org.apache.spark.rdd.RDD[String] = {
        val model = trainRandomForest(sc, input)

        //用来放入训练好的模型中进行预测的提交特征
        val commitData = sc.textFile(commitInput).map { line =>
            val parts = line.split(',')
            (parts(0), Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        }

        val predictResult = commitData.map {
            commitPoint =>
                //为了与trainpredict中的数据能相交 因为那里useriditemid连接符号是逗号
                val useritemid = commitPoint._1.replace("-", ",")
                val useritemids = commitPoint._1.split("-")
                val (userid, itemid) = (useritemids(0), useritemids(1))
                val prediction = model.predict(commitPoint._2)
                //println("predictresult!!!!! -> " + prediction)
                (itemid, (useritemid, prediction))
        }

        joinPredictResultWithItemData(sc, predictResult, limit)
    }
}

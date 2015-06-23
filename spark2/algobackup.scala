package org.give.altc

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.give.altc.CommonOP._

/**
 * Created by zjh on 15-3-27.
 */
object CommonAlgo {
    def parseLibSVMFileToLabeledData(sc: org.apache.spark.SparkContext, path: String): org.apache.spark.rdd.RDD[(String, org.apache.spark.mllib.linalg.Vector)] = {
        val origindata = sc.textFile(path)

        //特征数量
        //val numFeature = origindata.take(1)(0).split(" ").drop(1).map(_.split(":")(0).toInt).max

        val numFeature = 39

        val parsed = {
            origindata
                .map(_.trim)
                .filter(line => !(line.isEmpty || line.startsWith("#")))
                .map { line =>
                val items = line.split(' ')
                val useritemid = items.head.toString
                val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
                    val indexAndValue = item.split(':')
                    val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
                val value = indexAndValue(1).toDouble
                    (index, value)
                }.unzip
                (useritemid, indices.toArray, values.toArray)
            }
        }

        parsed.map { case (label, indices, values) =>
            (label, Vectors.sparse(numFeature, indices, values))
        }
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

    def trainGBRT(sc: org.apache.spark.SparkContext, input: String, output: String*): org.apache.spark.mllib.tree.model.GradientBoostedTreesModel = {
        // Load and parse the data file.
        val data = MLUtils.loadLibSVMFile(sc, input)

        val boostingStrategy = BoostingStrategy.defaultParams("Regression")

        //竟然特么的能过编译
        boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
        boostingStrategy.treeStrategy.maxDepth = 5
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

        val model = GradientBoostedTrees.train(data, boostingStrategy)

        model
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

        // Evaluate model on training examples and compute training error
        /*val valuesAndPreds = parsedData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
        val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
        println("training Mean Squared Error = " + MSE)*/

        model
    }

    /**
     * 在用rf或者gbrt预测完的结果提交之前需要和itemdata数据表中的itemid进行join
     * 因为最终预测的的useritem对中的item都是itemdata数据表中的itemid的子集
     */
    def joinPredictResultWithItemData(sc: org.apache.spark.SparkContext, predictResult: org.apache.spark.rdd.RDD[(String, (String, Double))], limit: Int): org.apache.spark.rdd.RDD[String] = {
        //itemid需要去重
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        //预测出来的结果也需要与itemdata那张表来进行join 然后再根据预测值进行倒排序取topN
        //如果是倒排序就是false
        /*val result = predictResult.join(itemdata).map {
            record =>
                val (useritemid, prediction) = record._2._1
                (prediction, useritemid)
        }.sortByKey(false)*/

        //result.coalesce(1).saveAsTextFile(PathNamespace.prefix + "predictresultwithprediction")

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
        }.filter(e => e._2.contains("3") && !e._2.contains("4")).map(_._1).distinct.map (e => (e, 1))

        val newresult = result.join(predictdata).map {
            record =>
                val useritemid = record._1
                val prediction = record._2._1
                (prediction, useritemid)
        }.sortByKey(false)


        newresult.coalesce(1).saveAsTextFile(PathNamespace.prefix + "predictjoinwithrule")
        //sc.parallelize(result.map(_._2).take(limit))
        sc.parallelize(newresult.map(_._2).take(limit))
    }

    def offlineGBRTTest(sc: org.apache.spark.SparkContext, input: String, commitInput: String, limit: Int, output: String*): org.apache.spark.rdd.RDD[String] = {
        val model = trainGBRT(sc, input)

        //用来放入训练好的模型中进行预测的提交特征
        val commitData = parseLibSVMFileToLabeledData(sc, commitInput)
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

    def offlineRFTest(sc: org.apache.spark.SparkContext, input: String, commitInput: String, limit: Int, output: String*): org.apache.spark.rdd.RDD[String] = {
        val model = trainRandomForest(sc, input)

        //用来放入训练好的模型中进行预测的提交特征
        val commitData = parseLibSVMFileToLabeledData(sc, commitInput)
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

    def rfClassifierTrain(sc: org.apache.spark.SparkContext, input: String, output: String*): org.apache.spark.mllib.tree.model.RandomForestModel = {
        val data = MLUtils.loadLibSVMFile(sc, input)

        val splits = data.randomSplit(Array(0.7, 0.3))
        val (trainingData, testData) = (splits(0), splits(1))

        /*val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 3 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        //Supported values: "gini" (recommended) or "entropy"
        //基尼系数 熵系数 用来划分决策树的节点
        val impurity = "gini"
        val maxDepth = 4
        val maxBins = 32

        val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
            numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        // Evaluate model on test instances and compute test error
        val labelAndPreds = testData.map { point =>
            val prediction = model.predict(point.features)
            println(prediction)
            (point.label, prediction)
        }
        val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()

        println("Learned classification forest model:\n" + model.toDebugString)
        println("Test Error = " + testErr) //0.021232911012068707*/

        // Train a RandomForest model.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 3 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "variance"
        val maxDepth = 4
        val maxBins = 32

        val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
            numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        // Evaluate model on test instances and compute test error
        val labelsAndPredictions = testData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }

        labelsAndPredictions.take(10).foreach(println)

        model
    }

    def rfClassifierPredict(sc: org.apache.spark.SparkContext, input: String, commitInput: String, limit: Int, output: String*): Unit = {
        val model = trainRandomForest(sc, input)

        //用来放入训练好的模型中进行预测的提交特征
        val commitData = parseLibSVMFileToLabeledData(sc, commitInput)
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

        joinPredictResultWithItemData(sc, predictResult, limit).coalesce(1).saveAsTextFile(output(0))
    }

    def lrClassifierPredict(sc: org.apache.spark.SparkContext, input: String, commitInput: String, limit: Int, joinpath: String, nojoinpath: String) = {
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
                //println("predictresult!!!!! -> " + prediction)
                (itemid, (useritemid, prediction))
        }

        joinPredictResultWithItemData(sc, predictResult, limit).coalesce(1).saveAsTextFile(joinpath)

        val predictResultNoJoinItem = commitData.map {
            commitPoint =>
                //为了与trainpredict中的数据能相交 因为那里useriditemid连接符号是逗号
                val useritemid = commitPoint._1.replace("-", ",")
                val prediction = model.predict(commitPoint._2)
                (prediction, useritemid)
        }.sortByKey(false)

        sc.parallelize(predictResultNoJoinItem.map(_._2).take(limit)).coalesce(1).saveAsTextFile(nojoinpath)
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
                //println("predictresult!!!!! -> " + prediction)
                (itemid, (useritemid, prediction))
        }

        joinPredictResultWithItemData(sc, predictResult, limit)
    }

    def simplePredict(sc: org.apache.spark.SparkContext, input: String, commitInput: String, output: String): Unit = {
        //没有倒排序
        //val model = rfClassifierTrain(sc, input)
        val model = trainRandomForest(sc, input)
        //用来放入训练好的模型中进行预测的提交特征
        val commitData = parseLibSVMFileToLabeledData(sc, commitInput)
        val predictResult = commitData.map {
            commitPoint =>
                val useritemid = commitPoint._1.replace("-", ",")
                useritemid
        }

        sc.parallelize(predictResult.take(461)).coalesce(1).saveAsTextFile(output)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        trainLR(sc, PathNamespace.prefix + "lrdata.data")
    }
}


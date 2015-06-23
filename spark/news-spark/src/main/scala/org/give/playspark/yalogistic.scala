package org.give.playspark

import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}

/**
 * Created by zjh on 14-10-13.
 * reference spark example https://github.com/apache/spark/blob/branch-1.0/examples/src/main/scala/org/apache/spark/examples/mllib/BinaryClassification.scala
 */
object yalogistic {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("yet another logistic regression")
        val sc = new SparkContext(conf)

        Logger.getRootLogger.setLevel(Level.WARN)

        val examples = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/svmdata.txt").cache()
        //val splits = examples.randomSplit(Array(0.8, 0.2))
        val splits = examples.randomSplit(Array(0.6, 0.4), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1).cache()

        val numTraining = training.count()
        val numTest = test.count()
        println(s"Training: $numTraining, test: $numTest.")

        examples.unpersist(blocking = false)

        //l2 regularization
        val updater = new SquaredL2Updater()

        case class Params(input: String = null,
                          numIterations: Int = 100,
                          stepSize: Double = 1.0,
                          regParam: Double = 0.1)

        val params = Params()

        val algorithm = new LogisticRegressionWithSGD()
        algorithm.optimizer
            .setNumIterations(params.numIterations)
            .setStepSize(params.stepSize)
            .setUpdater(updater)
            .setRegParam(params.regParam)


        val model = algorithm.run(training)

        model.clearThreshold()

        val prediction = model.predict(test.map(_.features))
        val predictionAndLabel = prediction.zip(test.map(_.label))

        val metrics = new BinaryClassificationMetrics(predictionAndLabel)

        println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
        println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

        sc.stop()
    }
}

package org.give.playspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by zjh on 14-10-13.
 */
object logistic {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("svm"))
        // Load training data in LIBSVM format.
        val data = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/svmdata.txt")

        // Split data into training (60%) and test (40%).
        val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1)

        // Run training algorithm to build the model
        val numIterations = 100
        val model = LogisticRegressionWithSGD.train(training, numIterations)

        model.clearThreshold()

        //不做clearThreshold操作后的scoreAndLabels
        //Array((1.0,1.0), (1.0,1.0), (0.0,1.0), (0.0,0.0))
        //做了clearThreshold操作后的scoreAndLabels
        //Array((0.7466466573476844,1.0), (0.962399807549331,1.0), (0.3023206648802087,1.0), (2.178482257270694E-6,0.0))
        //可以看到threshold应该是一个逼近0.5的数字，大于0.5是正例，小于0.5是负例
        //在lr中raw output的值越接近于1则越有可能是正例而raw output的数值越接近0，则越有可能是负例

        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()

        println("Area under ROC = " + auROC)
    }
}

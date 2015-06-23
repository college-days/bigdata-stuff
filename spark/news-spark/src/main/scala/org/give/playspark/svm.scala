package org.give.playspark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by zjh on 14-10-13.
 * lib svm format data
 * <label> <index1>:<value1> <index2>:<value2> ...
 * 1 1:0.7 2:1 3:1 represent a record in the data file
 * reference http://stats.stackexchange.com/questions/61328/libsvm-data-format
 */
object svm {
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
        val model = SVMWithSGD.train(training, numIterations)

        // Clear the default threshold.
        //clearThreshold之后其实就是输出的是分类模型原始的输出数值，而不是分类之后的标签值，因为分类模型会根据threshold来判断输出的数值是属于哪一类
        //model.clearThreshold()

        //不做clearThreshold操作后的scoreAndLabels
        //Array((1.0,1.0), (1.0,1.0), (0.0,1.0), (0.0,0.0))
        //做了clearThreshold操作后的scoreAndLabels
        //Array((0.28264656914768027,1.0), (0.8479397074430408,1.0), (-0.04404962562502335,1.0), (-2.492433087712815,0.0))
        //可以看到threshold应该是一个逼近0的数字，大于0是正例，小于0是负例

        // Compute raw scores on the test set.
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

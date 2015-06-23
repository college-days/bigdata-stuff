package org.give.altc.luo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.give.altc.PathNamespace
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-17.
 */
object TrainAndPredictAllFeature {
    val traindatapath = PathNamespace.prefix + "luo/train_featureLabel.featureLabel"
    val testdatapath = PathNamespace.prefix + "luo/test_featureLabel.featureLabel"

    //val lrtraindatapath = PathNamespace.prefix + "luolr/traindata"
    //val lrtraindatapath = PathNamespace.prefix + "cleanthalrfeature"
    val lrtraindatapath = PathNamespace.prefix + "offlinelrfeature"
    //val lrtestdatapath = PathNamespace.prefix + "luolr/testdata"
    //val lrtestdatapath = PathNamespace.prefix + "cleanthalrtest"
    val lrtestdatapath = PathNamespace.prefix + "offlinelrtest"

    val prefix: String = PathNamespace.prefix + "lrluojiayoujiayou3/"
    val yaprefix: String = PathNamespace.prefix + "lrluoyajiayoujiayou3/"

    val targetdataPath = PathNamespace.prefix + "offlinetargetdatajoinitem"

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


        //newresult.coalesce(1).saveAsTextFile(PathNamespace.prefix + "predictjoinwithrule")
        //sc.parallelize(result.map(_._2).take(limit))
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

        // Evaluate model on training examples and compute training error
        /*val valuesAndPreds = parsedData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
        val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
        println("training Mean Squared Error = " + MSE)*/

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
                //println("predictresult!!!!! -> " + prediction)
                (itemid, (useritemid, prediction))
        }

        joinPredictResultWithItemData(sc, predictResult, limit)
    }

    def getData(sc: org.apache.spark.SparkContext): Unit = {
        val luotraindata = sc.textFile(traindatapath)
        val luotestdata = sc.textFile(testdatapath)

        //88个特征
        val lrtraindata = luotraindata.map {
            record =>
                val items = record.split(" ")
                val label = items.last
                val features = items.drop(1).dropRight(1).mkString(" ")
                label + "," + features
        }.coalesce(1).saveAsTextFile(lrtraindatapath)

        val lrtestdata = luotestdata.map {
            record =>
                val items = record.split(" ")
                val useritemid = items(0).replace(",", "-")
                val features = items.drop(1).dropRight(1).mkString(" ")
                useritemid + "," + features
        }.coalesce(1).saveAsTextFile(lrtestdatapath)
    }

    def trainAndPredict(sc: org.apache.spark.SparkContext): Unit = {
        val offlinelabeledtraindata = sc.textFile(lrtraindatapath)

        val targetresult = sc.textFile(targetdataPath)

        //获取正样本数量 后面才能进行正负样本按比例采样
        val allcount = offlinelabeledtraindata.count
        val positivedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 1)
        val negativedata = offlinelabeledtraindata.filter(_.split(",")(0).toInt == 0)
        val positivecount = positivedata.count
        val negativecount = negativedata.count

        var offlinetrainjoinresult = List[String]()
        var offlinetrainnojoinresult = List[String]()
        var offlinetrainresult = List[String]()
        //1:5 - 1:20的正负样本比例
        for (i <- 5 to 20 by 5) {
            val negativesamplecount = positivecount * i
            val sampleproportion = negativesamplecount.toFloat / negativecount.toFloat

            val samplenegativedata = negativedata.sample(false, sampleproportion, 11L)
            val finaltraindata = positivedata.union(samplenegativedata)

            val finaltraindatapath = yaprefix + "offlinefinaltraindatauseritem" + "-" + i
            finaltraindata.coalesce(1).saveAsTextFile(finaltraindatapath)

            //val predictresult = CommonAlgo.offlineRFTest(sc, finaltraindatapath, useritemlabeledtestdataPath, targetresult.count.toInt).distinct
            //val predictresult = CommonAlgo.offlineGBRTTest(sc, finaltraindatapath, offlineprefix + "offlinesubmitdata" + trainweight)

            val predictresult = offlineLRTest(sc, finaltraindatapath, lrtestdatapath, targetresult.count.toInt)

            predictresult.saveAsTextFile(yaprefix + "offlinepredictresult-" + i)

            //val targetresult = getOfflineTrainDataAfterJoinItem(sc)

            val hitcount = targetresult.intersection(predictresult).count.toFloat
            val precision = hitcount / targetresult.count.toFloat
            val recall = hitcount / predictresult.count.toFloat
            val f1 = 2 * precision * recall / (precision + recall)
            offlinetrainresult = offlinetrainresult :+ "@" + i + "-> p: " + precision + " r: " + recall + " f1: " + f1
        }
        sc.parallelize(offlinetrainresult).coalesce(1).saveAsTextFile(yaprefix + "traintestresultuseritem")
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        //getData(sc)
        trainAndPredict(sc)
    }
}

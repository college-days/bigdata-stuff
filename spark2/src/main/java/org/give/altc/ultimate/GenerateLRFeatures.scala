package org.give.altc.ultimate

import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-17.
 */
object GenerateLRFeatures {
    //rawfeaturepath -> PathNamespace.prefix + "subset/offlinealltrainfeatures" / PathNamespace.offlinealltrainfeaturesubdata
    //labeldatapath -> PathNamespace.offlinetestlabelsubdata
    //output -> PathNamespace.prefix + "offlinelrfeature"
    def generateLRTrainFeature(sc: org.apache.spark.SparkContext, rawfeaturepath: String, labeldatapath: String, output: String): Unit = {
        val allfeatures = sc.textFile(rawfeaturepath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                (useritemid, features)
        }

        val traindata = sc.textFile(labeldatapath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = items(2).toInt
                (userid + "-" + itemid, label)
        }.filter(_._2 == 4).distinct

        val b = allfeatures.leftOuterJoin(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2 match {
                    case Some(behavior) => "1"
                    case None => "0"
                }
                (label, features)
        }.map(e => e._1 + "," + e._2)

        b.coalesce(1).saveAsTextFile(output)
    }

    //rawfeaturepath -> PathNamespace.prefix + "subset/offlinealltestfeatures" / PathNamespace.offlinealltestfeaturesubdata
    //output -> PathNamespace.prefix + "offlinelrtest"
    def generateLRTestFeature(sc: org.apache.spark.SparkContext, rawfeaturepath: String, output: String): Unit = {
        val d = sc.textFile(rawfeaturepath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                useritemid + "," + features
        }

        d.coalesce(1).saveAsTextFile(output)
    }
}

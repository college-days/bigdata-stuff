package org.give.altc.features

import org.apache.spark.SparkContext._
import org.give.altc.CommonOP

/**
 * Created by zjh on 15-3-25.
 */

//通过userid itemid将useritem user item三部分的特征join起来作为最终扔进分类器中的特征序列
//其中特征序列还需要处理为libsvm那种形式 index:value
//如果出现null 那么就跳过 这在libsvm格式中是支持的

//useritem 10 features
//user 9 features
//item 14 features
//total 33 features
object MergeFeatures {
    def generateRawFeatures(sc: org.apache.spark.SparkContext, allDataPath: String, useritemFeaturePath: String, userFeaturePath: String, itemFeaturePath: String): Unit = {
        CommonOP.generateUserItemFeature(sc, allDataPath, useritemFeaturePath)
        CommonOP.generateUserFeature(sc, allDataPath, userFeaturePath)
        CommonOP.generateItemFeature(sc, allDataPath, itemFeaturePath)
    }

    def joinUserItemFeatures(sc: org.apache.spark.SparkContext, useritemFeaturePath: String, userFeaturePath: String, itemFeaturePath: String): org.apache.spark.rdd.RDD[(String, String)] = {
        val useritemFeatures = sc.textFile(useritemFeaturePath)
        val userFeatures = sc.textFile(userFeaturePath)
        val itemFeatures = sc.textFile(itemFeaturePath)

        val useritemFeatureByUserid = useritemFeatures.map {
            record =>
                val items = record.split(",")
                val (userid, itemid) = (items(0).split("-")(0), items(0).split("-")(1))
                (userid, itemid + "," + items(1))
        }

        val userFeatureByUserid = userFeatures.map {
            record =>
                (record.split(",")(0), record.split(",")(1))
        }

        val itemFeatureByItemid = itemFeatures.map {
            record =>
                (record.split(",")(0), record.split(",")(1))
        }

        val mergeFeatures = useritemFeatureByUserid.join(userFeatureByUserid).map {
            record =>
                val userid = record._1
                val userItemStuff = record._2._1.split(",")
                val (itemid, userItemFeature) = (userItemStuff(0), userItemStuff(1))
                (itemid, userid + "," + userItemFeature + "-" + record._2._2)
        }.join(itemFeatureByItemid).map {
            record =>
                val itemid = record._1
                val userItemStuff = record._2._1.split(",")
                val (userid, userItemFeature) = (userItemStuff(0), userItemStuff(1))
                val allFeatures = (userItemFeature + "-" + record._2._2).split("-").zipWithIndex.filter(_._1 != "null").map {
                    record =>
                        (record._2.toInt + 1) + ":" + record._1
                }.mkString(" ")

                (userid + "-" + itemid, allFeatures)
        }
        mergeFeatures
    }

    //将useritem user item三类特征join到一起 不以稀疏的形式展示
    def joinUserItemFeaturesNotSparse(sc: org.apache.spark.SparkContext, useritemFeaturePath: String, userFeaturePath: String, itemFeaturePath: String, output: String): Unit = {
        val useritemFeatures = sc.textFile(useritemFeaturePath)
        val userFeatures = sc.textFile(userFeaturePath)
        val itemFeatures = sc.textFile(itemFeaturePath)

        val useritemFeatureByUserid = useritemFeatures.map {
            record =>
                val items = record.split(",")
                val (userid, itemid) = (items(0).split("-")(0), items(0).split("-")(1))
                (userid, itemid + "," + items(1))
        }

        val userFeatureByUserid = userFeatures.map {
            record =>
                (record.split(",")(0), record.split(",")(1))
        }

        val itemFeatureByItemid = itemFeatures.map {
            record =>
                (record.split(",")(0), record.split(",")(1))
        }

        val mergeFeatures = useritemFeatureByUserid.join(userFeatureByUserid).map {
            record =>
                val userid = record._1
                val userItemStuff = record._2._1.split(",")
                val (itemid, userItemFeature) = (userItemStuff(0), userItemStuff(1))
                (itemid, userid + "," + userItemFeature + "-" + record._2._2)
        }.join(itemFeatureByItemid).map {
            record =>
                val itemid = record._1
                val userItemStuff = record._2._1.split(",")
                val (userid, userItemFeature) = (userItemStuff(0), userItemStuff(1))
                /*val allFeatures = (userItemFeature + "-" + record._2._2).split("-").zipWithIndex.filter(_._1 != "null").map {
                    record =>
                        (record._2.toInt + 1) + ":" + record._1
                }.mkString(" ")

                (userid + "-" + itemid, allFeatures)*/
                val allFeatures = userItemFeature + "-" + record._2._2
                (userid + "-" + itemid + "-" + allFeatures).replace("-", ",")
        }
        mergeFeatures.coalesce(1).saveAsTextFile(output)
    }

    //将useriditemid对与特征值序列分开变成libsvmdata的格式
    def generateTestFeatureData(sc: org.apache.spark.SparkContext, useritemFeaturePath: String, userFeaturePath: String, itemFeaturePath: String, outputpath: String): Unit = {
        joinUserItemFeatures(sc, useritemFeaturePath, userFeaturePath, itemFeaturePath).map {
            record =>
                record._1 + " " + record._2
        }.coalesce(1).saveAsTextFile(outputpath)
    }

    def generateTestFeatureDataWithSpecificFeature(sc: org.apache.spark.SparkContext, useritemFeaturePath: String, outputpath: String): Unit = {
        sc.textFile(useritemFeaturePath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).split("-").zipWithIndex.filter(_._1 != "null").map {
                    record =>
                        (record._2.toInt + 1) + ":" + record._1
                }.mkString(" ")

                useritemid + " " + features
        }.coalesce(1).saveAsTextFile(outputpath)
    }

    //购买行为是1 非购买行为是0
    def getLabel(behavior: Int): Int = {
        if (behavior == 4) {
            1
        } else {
            0
        }
    }

    //让合并之后的特征数据和最后一天的数据来join 如果购买了 label就是1 如果没购买label就是0

    def generateLabeledData(sc: org.apache.spark.SparkContext, useritemfeatureinputpath: String, userfeatureinputpath: String, itemfeatureinputpath: String, traindatainputpath: String, output: String): Unit = {
        val allfeatures = joinUserItemFeatures(sc, useritemfeatureinputpath, userfeatureinputpath, itemfeatureinputpath)
        val traindata = sc.textFile(traindatainputpath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = getLabel(items(2).toInt)
                (userid + "-" + itemid, label)
        }

        allfeatures.join(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2
                label + " " + features
        }.coalesce(1).saveAsTextFile(output)
    }

    def generateLabeledDataWithSpecificFeature(sc: org.apache.spark.SparkContext, featureinputpath: String, traindatainputpath: String, output: String): Unit = {
        val allfeatures = sc.textFile(featureinputpath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).split("-").zipWithIndex.filter(_._1 != "null").map {
                    record =>
                        (record._2.toInt + 1) + ":" + record._1
                }.mkString(" ")

                (useritemid, features)
        }

        val traindata = sc.textFile(traindatainputpath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = getLabel(items(2).toInt)
                (userid + "-" + itemid, label)
        }

        allfeatures.join(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2
                label + " " + features
        }.coalesce(1).saveAsTextFile(output)
    }

    def generateLabeledDataNotSparse(sc: org.apache.spark.SparkContext, allfeaturedataPath: String, traindataPath: String, output: String): Unit = {
        val allfeatures = sc.textFile(allfeaturedataPath).map{
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val features = items.drop(2).mkString(",")
                (userid + "," + itemid, features)
        }

        val traindata = sc.textFile(traindataPath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = getLabel(items(2).toInt)
                (userid + "," + itemid, label)
        }

        allfeatures.join(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2
                label + "," + useritem + "," + features
        }.coalesce(1).saveAsTextFile(output)
    }
}

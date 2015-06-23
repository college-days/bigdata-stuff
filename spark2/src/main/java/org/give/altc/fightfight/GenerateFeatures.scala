package org.give.altc.fightfight

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.give.altc.fightfight.features.UserItemFeatures._
import org.give.altc.fightfight.features.UserFeatures._
import org.give.altc.fightfight.features.ItemFeatures._
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-21.
 */
object GenerateFeatures {
    val newprefix: String = "hdfs://namenode:9000/givedata/altc/newdata/"
    val newfeatureprefix: String = newprefix + "features/"

    //useritem的特征数据
    val newofflineuseritemtrainfeaturesubdata: String = newfeatureprefix + "offlineuseritemtrainfeatures"
    val newofflineuseritemtestfeaturesubdata: String = newfeatureprefix + "offlineuseritemtestfeatures"
    val newonlineuseritemtrainfeaturesubdata: String = newfeatureprefix + "onlineuseritemtrainfeatures"

    //user的特征数据
    val newofflineusertrainfeaturesubdata: String = newfeatureprefix + "offlineusertrainfeaturesubdata"
    val newofflineusertestfeaturesubdata: String = newfeatureprefix + "offlineusertestfeaturesubdata"
    val newonlineusertrainfeaturesubdata: String = newfeatureprefix + "onlineusertrainfeaturesubdata"

    //item的特征数据
    val newofflineitemtrainfeaturesubdata: String = newfeatureprefix + "offlineitemtrainfeaturesubdata"
    val newofflineitemtestfeaturesubdata: String = newfeatureprefix + "offlineitemtestfeaturesubdata"
    val newonlineitemtrainfeaturesubdata: String = newfeatureprefix + "onlineitemtrainfeaturesubdata"

    //useritem user item join在一起的所有的特征数据
    val newofflinealltrainfeaturesubdata: String = newfeatureprefix + "offlinealltrainfeatures"
    val newofflinealltestfeaturesubdata: String = newfeatureprefix + "offlinealltestfeatures"
    val newonlinealltrainfeaturesubdata: String = newfeatureprefix + "onlinealltrainfeatures"

    def generateFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        val offlinetrainorigindata = sc.textFile(PathNamespace.offlinetrainfeaturesubdata)
        val offlinetestorigindata = sc.textFile(PathNamespace.offlinetestfeaturesubdata)
        val onlinetrainorigindata = sc.textFile(PathNamespace.onlinetrainfeaturesubdata)

        yaGetUserItemFeatures(offlinetrainorigindata).join(getUserItemBehaviorBeforeTargetDate(offlinetrainorigindata, "2014-12-17")).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.offlineuseritemtrainfeaturesubdata)
        yaGetUserItemFeatures(offlinetestorigindata).join(getUserItemBehaviorBeforeTargetDate(offlinetestorigindata, "2014-12-18")).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.offlineuseritemtestfeaturesubdata)
        yaGetUserItemFeatures(onlinetrainorigindata).join(getUserItemBehaviorBeforeTargetDate(onlinetrainorigindata, "2014-12-19")).map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.onlineuseritemtrainfeaturesubdata)

        getUserFeatures(offlinetrainorigindata).coalesce(1).saveAsTextFile(PathNamespace.offlineusertrainfeaturesubdata)
        getUserFeatures(offlinetestorigindata).coalesce(1).saveAsTextFile(PathNamespace.offlineusertestfeaturesubdata)
        getUserFeatures(onlinetrainorigindata).coalesce(1).saveAsTextFile(PathNamespace.onlineusertrainfeaturesubdata)

        def getItemFeatureData(origindata: org.apache.spark.rdd.RDD[String], output: String): Unit = {
            yaGetItemFeatures(origindata).join(yayaGetItemFeatures(origindata))
                .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(output)
        }

        getItemFeatureData(offlinetrainorigindata, PathNamespace.offlineitemtrainfeaturesubdata)
        getItemFeatureData(offlinetestorigindata, PathNamespace.offlineitemtestfeaturesubdata)
        getItemFeatureData(onlinetrainorigindata, PathNamespace.onlineitemtrainfeaturesubdata)
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateFeatureData(sc)
    }
}

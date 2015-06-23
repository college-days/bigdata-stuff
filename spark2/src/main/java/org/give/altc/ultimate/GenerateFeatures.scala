package org.give.altc.ultimate

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.CommonOP._
import org.give.altc.PathNamespace
import org.give.altc.features.item.ItemFeatures._
import org.give.altc.features.user.UserFeatures._
import org.give.altc.features.useritem.UserItemFeatures._

/**
 * Created by zjh on 15-4-15.
 */
object GenerateFeatures {
    def generateFeatureData(sc: org.apache.spark.SparkContext): Unit = {
        val offlinetrainorigindata = sc.textFile(PathNamespace.offlinetrainfeaturesubdata)
        val offlinetestorigindata = sc.textFile(PathNamespace.offlinetestfeaturesubdata)
        val onlinetrainorigindata = sc.textFile(PathNamespace.onlinetrainfeaturesubdata)

        /*yaGetUserItemFeatures(offlinetrainorigindata) /*.join(getUserItemBuyCountByWeekDay(offlinetrainorigindata))
            .map(operatorAfterInnerJoin)*/ .join(getUserItemBehaviorBeforeTargetDate(offlinetrainorigindata, "2014-12-17"))
            //.map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(offlinetrainorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.offlineuseritemtrainfeaturesubdata)

        yaGetUserItemFeatures(offlinetestorigindata) /*.join(getUserItemBuyCountByWeekDay(offlinetestorigindata))
            .map(operatorAfterInnerJoin)*/ .join(getUserItemBehaviorBeforeTargetDate(offlinetestorigindata, "2014-12-18"))
            //.map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(offlinetestorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.offlineuseritemtestfeaturesubdata)

        yaGetUserItemFeatures(onlinetrainorigindata) /*.join(getUserItemBuyCountByWeekDay(onlinetrainorigindata))
            .map(operatorAfterInnerJoin)*/ .join(getUserItemBehaviorBeforeTargetDate(onlinetrainorigindata, "2014-12-19"))
            //.map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(onlinetrainorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.onlineuseritemtrainfeaturesubdata)

        getUserFeatures(offlinetrainorigindata).coalesce(1).saveAsTextFile(PathNamespace.offlineusertrainfeaturesubdata)
        getUserFeatures(offlinetestorigindata).coalesce(1).saveAsTextFile(PathNamespace.offlineusertestfeaturesubdata)
        getUserFeatures(onlinetrainorigindata).coalesce(1).saveAsTextFile(PathNamespace.onlineusertrainfeaturesubdata)

        def getItemFeatureData(origindata: org.apache.spark.rdd.RDD[String], output: String): Unit = {
            yaGetItemFeatures(origindata).join(yayaGetItemFeatures(origindata))
                .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(output)
        }

        getItemFeatureData(offlinetrainorigindata, PathNamespace.offlineitemtrainfeaturesubdata)
        getItemFeatureData(offlinetestorigindata, PathNamespace.offlineitemtestfeaturesubdata)
        getItemFeatureData(onlinetrainorigindata, PathNamespace.onlineitemtrainfeaturesubdata)*/
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        generateFeatureData(sc)
    }
}

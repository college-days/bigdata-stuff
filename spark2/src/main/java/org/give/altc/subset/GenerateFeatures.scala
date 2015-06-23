package org.give.altc.subset

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.give.altc.features.useritem.UserItemFeatures._
import org.apache.spark.SparkContext._
import org.give.altc.CommonOP._

/**
 * Created by zjh on 15-4-15.
 */
object GenerateFeatures {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        val offlinetrainorigindata = sc.textFile(PathNamespace.offlinetrainfeaturesubdata)
        val offlinetestorigindata = sc.textFile(PathNamespace.offlinetestfeaturesubdata)
        val onlinetrainorigindata = sc.textFile(PathNamespace.onlinetrainfeaturesubdata)
        val part1 = yaGetUserItemFeatures(offlinetrainorigindata)/*.join(getUserItemBuyCountByWeekDay(offlinetrainorigindata))
            .map(operatorAfterInnerJoin)*/.join(getUserItemBehaviorBeforeTargetDate(offlinetrainorigindata, "2014-12-17"))
            //.map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(offlinetrainorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "subset/offlinetrainfeatures")

        yaGetUserItemFeatures(offlinetestorigindata)/*.join(getUserItemBuyCountByWeekDay(offlinetestorigindata))
            .map(operatorAfterInnerJoin)*/.join(getUserItemBehaviorBeforeTargetDate(offlinetestorigindata, "2014-12-18"))
            //.map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(offlinetestorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "subset/offlinetestfeatures")

        yaGetUserItemFeatures(onlinetrainorigindata)/*.join(getUserItemBuyCountByWeekDay(onlinetrainorigindata))
            .map(operatorAfterInnerJoin)*/.join(getUserItemBehaviorBeforeTargetDate(onlinetrainorigindata, "2014-12-19"))
            //.map(operatorAfterInnerJoin).join(yayaGetUserItemFeatures(onlinetrainorigindata))
            .map(operatorAfterInnerJoin).map(e => e._1 + "," + e._2).coalesce(1).saveAsTextFile(PathNamespace.prefix + "subset/onlinetrainfeatures")
    }
}

package org.give.altc.fightfight

import org.give.altc.PathNamespace

/**
 * Created by zjh on 15-4-21.
 */
object SplitData {
    //新的数据集 直接在子集数据上划分数据
    val newprefix: String = "hdfs://namenode:9000/givedata/altc/newdata/"
    val newmetadataprefix: String = newprefix + "metadata/"

    //子集的数据集
    //11.18-12.16提取离线训练数据 12.17offlinetrainfeaturedata作为label
    //useritem的特征数据
    val newofflinetrainfeaturesubdata: String = newmetadataprefix + "offlinetrainfeaturesubdata"
    //11.19.12.17提取离线测试数据 12.18作为label
    val newofflinetestfeaturesubdata: String = newmetadataprefix + "offlinetestfeaturesubdata"
    //12.17的target数据
    val newofflinetrainlabelsubdata: String = newmetadataprefix + "offlinetrainlabelsubdata"
    //12.18的target数据
    val newofflinetestlabelsubdata: String = newmetadataprefix + "offlinetestlabelsubdata"
    //11.20-12.18作为线上的训练数据
    val newonlinetrainfeaturesubdata: String = newmetadataprefix + "onlinetrainfeaturesubdata"

    def getYearMonthDay(origin: String): Long = {
        val timeItems = origin.split(",").last.split(" ")(0).split("-")
        val (year, month, day) = (timeItems(0), timeItems(1), timeItems(2))
        (year + month + day).toLong
    }

    def getFeatureDataByDate(data: org.apache.spark.rdd.RDD[String], startdate: Long, enddate: Long): org.apache.spark.rdd.RDD[String] = {
        data.filter {
            record =>
                val date = getYearMonthDay(record)
                date >= startdate && date <= enddate
        }
    }

    //11.18-12.16提取离线训练数据 12.17作为label
    //11.19.12.17提取离线测试数据 12.18作为label
    //12.17的target数据
    //12.18的target数据
    //11.20-12.18作为线上的训练数据
    def generateFeatureAndLabelData(sc: org.apache.spark.SparkContext): Unit ={
        val joinitemuserdata = newprefix + "joinitemiduserdata"

        val origindata = sc.textFile(joinitemuserdata)

        val offlinetrainfeaturedata = getFeatureDataByDate(origindata, 20141118, 20141216)
        val offlinetestfeaturedata = getFeatureDataByDate(origindata, 20141119, 20141217)
        val offlinetrainlabeldata = getFeatureDataByDate(origindata, 20141217, 20141217)
        val offlinetestlabeldata = getFeatureDataByDate(origindata, 20141218, 20141218)

        val onlinetrainfeaturedata = getFeatureDataByDate(origindata, 20141120, 20141218)

        offlinetrainfeaturedata.coalesce(1).saveAsTextFile(newofflinetrainfeaturesubdata)
        offlinetestfeaturedata.coalesce(1).saveAsTextFile(newofflinetestfeaturesubdata)
        offlinetrainlabeldata.coalesce(1).saveAsTextFile(newofflinetrainlabelsubdata)
        offlinetestlabeldata.coalesce(1).saveAsTextFile(newofflinetestlabelsubdata)
        onlinetrainfeaturedata.coalesce(1).saveAsTextFile(newonlinetrainfeaturesubdata)
    }
}

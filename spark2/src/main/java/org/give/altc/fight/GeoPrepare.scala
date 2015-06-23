package org.give.altc.fight

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 15-4-19.
 */

/**
地理信息预处理
注意（阿里给的那张user表里头的地理位置是用户购买时候的地理位置，不是item的地理位置；item地理位置在item表）
1、item地理位置缺失的：用购买记录中用户地理位置的公共前缀填充，后面用XXX….补全，为了排除个别较远用户购买的情况，可以选用80%（暂定，看数据）以上用户公共前缀。
2、统计用户购买时最常用的地理位置的公共前缀
3、统计用户最常购买的item的公共前缀
以上不是特征，是暂存的信息，可以单独建表存储
 */
object GeoPrepare {
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"
    val metadataprefix: String = prefix + "metadata/"

    val offlinetrainfeaturesubdata: String = metadataprefix + "offlinetrainfeaturesubdata"
    val offlinetestfeaturesubdata: String = metadataprefix + "offlinetestfeaturesubdata"
    val onlinetrainfeaturesubdata: String = metadataprefix + "onlinetrainfeaturesubdata"

    val offlineusertrainfeaturesubdata: String = prefix + "subset/offlineusertrainfeaturesubdata"
    val offlineusertestfeaturesubdata: String = prefix + "subset/offlineusertestfeaturesubdata"
    val onlineusertrainfeaturesubdata: String = prefix + "subset/onlineusertrainfeaturesubdata"

    val itemdatapath: String = prefix + "tianchi_mobile_recommend_train_item.csv"

    val itemdatawithgeoinfopath: String = metadataprefix + "itemdatawithgeoinfo"

    def fillItemGeoInfo(itemdata: org.apache.spark.rdd.RDD[String], userdata: org.apache.spark.rdd.RDD[String]): Unit = {
        itemdata.map {
            record =>
                val itemid = record.split(",")(0)
                itemid
        }.distinct.map(e => (e, Nil))

        /*userdata.map {
            record =>
                val itemid =
        }*/
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))

        val offlinetrainorigindata = sc.textFile(offlinetrainfeaturesubdata)
        val offlinetestorigindata = sc.textFile(offlinetestfeaturesubdata)
        val onlinetrainorigindata = sc.textFile(onlinetrainfeaturesubdata)

    }
}

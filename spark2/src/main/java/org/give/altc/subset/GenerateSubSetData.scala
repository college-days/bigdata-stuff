package org.give.altc.subset

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.PathNamespace
import org.apache.spark.SparkContext._

/**
 * Created by zjh on 15-4-15.
 */

//将所有数据都和自己交一下 然后得到一个子集的训练数据和提交数据
object GenerateSubSetData {
    def joinSubSet(sc: org.apache.spark.SparkContext, inputpath: String, outputpath: String): Unit = {
        val itemdata = sc.textFile(PathNamespace.tianchi_mobile_recommend_train_item).map {
            record =>
                val items = record.split(",")
                val itemid = items(0)
                itemid
        }.distinct().map {
            record =>
                (record, Nil)
        }

        val targetresult = sc.textFile(inputpath).map {
            record =>
                val items = record.split(",")
                val itemid = items(1)
                (itemid, record)
        }

        val result = targetresult.join(itemdata).map (_._2._1)
        result.coalesce(1).saveAsTextFile(outputpath)
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        joinSubSet(sc, PathNamespace.offlinetrainfeaturedata, PathNamespace.offlinetrainfeaturesubdata)
        joinSubSet(sc, PathNamespace.offlinetestfeaturedata, PathNamespace.offlinetestfeaturesubdata)
        joinSubSet(sc, PathNamespace.offlinetrainlabeldata, PathNamespace.offlinetrainlabelsubdata)
        joinSubSet(sc, PathNamespace.offlinetestlabeldata, PathNamespace.offlinetestlabelsubdata)
        joinSubSet(sc, PathNamespace.onlinetrainfeaturedata, PathNamespace.onlinetrainfeaturesubdata)
    }
}

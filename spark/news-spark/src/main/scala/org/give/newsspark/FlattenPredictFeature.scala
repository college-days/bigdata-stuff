package org.give.newsspark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-8.
 * 在GeneratePredictFeature中最终产生的是每行userid newsidlist的形式，但是这样不便于使用sparksql
 * 所以这里将其flatten一下以userid newsid一行的格式来存储所有为交互的userid newsid对
 * 准确的说不应该叫flatten，应该是split才对，anyway，叫了就叫了
 */
object FlattenPredictFeature {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Predict Feature"))

        val sliceList = List.range(0, 100)
        val flattenResult = sliceList.map{ slice =>
            val notInteractedPairRDD = sc.textFile("/user/root/predictfeature/part" + slice).map(record => record.split("\t")).map(record => (record(0), record(1).split("-"))).flatMap{ record =>
                val userid = record._1
                record._2.map(newsid => userid + "\t" + newsid)
            }
            notInteractedPairRDD
        }.reduce(_.union(_))

        //println(flattenResult.count()) => 61728880 is cool

        flattenResult.cache().coalesce(1).saveAsTextFile("/user/root/predictfeature/ultimate")
        println("generate predict feature ultimate successful")
    }
}

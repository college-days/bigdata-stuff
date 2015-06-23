package org.give.newsspark.cfyarn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-8.
 * 最终采用是Collaborative Filtering for Implicit Feedback Datasets这篇文章上面的做法
 * 在评分矩阵中所有为0的点是之前提取出来的所有为交互过的userid newsid 也就是FlattenPredictFeature这个app里面最终得到的
 * 而评分矩阵中所有为1的点是除所有user最近浏览的两次新闻意外的所有userid newsid 也就是之前所有的浏览记录中除去FilterValidateLabel和FilterTrainPositive两个job中得到的userid newsid对
 * 这个job中要得到的就是评分矩阵中要为1的所有userid newsid pair，思路就是从原来所有的交互记录中filter掉FilterValidateLabel和FilterTrainPositive两个job中得到的结果
 */
object GenerateImplicitPositive {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Implicit Positive"))
        val lastPairRDD = sc.textFile("/user/root/finalvalidatelabel/part-00000")
        val lastPairSplits = lastPairRDD.map(line => line.split("\t"))
        val lastPairIdentity = lastPairSplits.map{ record =>
            record(0) + " " + record(1) + " " + record(2)
        }
        val lastPair = lastPairIdentity.collect()

        val lastSecondPairRDD = sc.textFile("/user/root/finaltrainpositive/part-00000")
        val lastSecondSplits = lastSecondPairRDD.map(line => line.split("\t"))
        val lastSecondIdentity = lastSecondSplits.map{ record =>
            record(0) + " " + record(1) + " " + record(2)
        }
        val lastSecondPair = lastSecondIdentity.collect()

        val newsRDD = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt") //or /user/root/newsdata.txt
        val newsSplits = newsRDD.map(line => line.split("\t"))
        val newsIdentity = newsSplits.map{ record =>
            record(0) + " " + record(1) + " " + record(2)
        }
        //val news = newsIdentity.collect()
        //List.concat(lastPair.toList, lastSecondPair.toList).toSet.size
        //=>19962 而不是 20000 说明用户最后两次的浏览记录有重叠的，因为之前也碰到了这个问题就是说用户会在同一时间点点击不同的新闻，是在这里出的问题，导致了说最近两次的浏览记录统计出来出现了少数有重叠的现象

        val implicitPositive = newsIdentity.filter{ record =>
            !lastPair.contains(record)
        }.filter{ record =>
            !lastSecondPair.contains(record)
        }

        //userid newsid visit_timestamp
        implicitPositive.cache().coalesce(1).saveAsTextFile("/user/root/implicitpositive")
        System.out.println("generate implicit positive successful")
    }
}

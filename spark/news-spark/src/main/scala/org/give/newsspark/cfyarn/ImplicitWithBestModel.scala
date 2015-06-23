package org.give.newsspark.cfyarn

import java.lang.Math._
import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-13.
 * 最优模型rank=12 numIter = 20改成30啦
 */
object ImplicitWithBestModel {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Implicit Feedback"))
        //貌似大东西不能cache啊，一cache内存就爆掉了
        //val implicitPositiveRDD = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt").cache()
        //val implicitNegativeRDD = sc.textFile("/user/root/predictfeature/ultimate/part-00000").cache()
        val implicitPositiveRDD = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt")
        val implicitNegativeRDD = sc.textFile("/user/root/predictfeature/ultimate/part-00000")

        val positiveRatings = implicitPositiveRDD.map { record =>
            val fields = record.split("\t")
            //Rating(userid, newsid, implicit rating 0/1)
            Rating(fields(0).toInt, fields(1).toInt, 1.toDouble)
        }//=>96263

        val negativeRatings = implicitNegativeRDD.map { record =>
            val fields = record.split("\t")
            //Rating(userid, newsid, implicit rating 0/1)
            Rating(fields(0).toInt, fields(1).toInt, 0.toDouble)
        }//=>61728880 是正例的641倍

        //val unionRatings = positiveRatings.union(negativeRatings)
        //val distinctUsers = unionRatings.map(_.user).distinct()
        //val distinctKeywords = unionRatings.map(_.product).distinct()

        //笛卡尔积
        //val userKeywords = distinctUsers.cartesian(distinctKeywords)

        val random = new Random(0)
        val selectNegativeRatings = negativeRatings.filter(x => random.nextDouble() < 0.0125) //随机采样

        val training = positiveRatings.union(selectNegativeRatings)

        val rank = 12
        val numIter = 30

        val model = ALS.trainImplicit(training, rank, numIter)
        //为每一个用户推荐的新闻的个数
        val recommendnumForEachUser = 1

        val finalResult = predictForAll(sc, model, recommendnumForEachUser)
        val finalResultRDD = sc.parallelize("userid,newsid" +: finalResult)

        finalResultRDD.cache().coalesce(1).saveAsTextFile("/user/root/implicitfeedbacknum/num" + recommendnumForEachUser)
        println("generate total record: " + finalResult.size)
        println("generate implicit feedback with num " + recommendnumForEachUser + " submit result successful")
    }

    def predictForAll(sc: SparkContext, model: MatrixFactorizationModel, recommendcount:Int) = {
        val sliceList = List.range(0, 100)
        //val sliceList = List.range(0, 3)
        val flattenResult = sliceList.map { slice =>
            val notInteractedPairRDD = sc.textFile("/user/root/predictfeature/part" + slice).map(record => record.split("\t")).map(record => (record(0), record(1).split("-"))).map { record =>
                val userid = record._1
                //record._2.map(newsid => userid + "\t" + newsid)
                //record._2.map(newsid => (userid.toInt, newsid.toInt))
                //record._2.map(newsid => model.predict(userid.toInt, newsid.toInt))
                val usernewsPairs = record._2.map(newsid => (userid.toInt, newsid.toInt))
                //record._2.map(newsid => model.predict(userid.toInt, newsid.toInt))
                usernewsPairs
                //model.predict(sc.parallelize(usernewsPairs))
            }

            notInteractedPairRDD.collect().flatMap { record =>
                val recommend = model.predict(sc.parallelize(record)).collect().sortBy(-_.rating).take(recommendcount)
                recommend.map{ record =>
                    record.user + "," + record.product
                }
            }
        }.reduce(_ ++ _)

        flattenResult
    }

    /*notInteractedPairRDD.collect.map { record =>
                val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(1)(0)
                recommend.user + "," + recommend.product
            }*/
    /*
    notInteractedPairRDD.collect.map { record =>
        model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(3)
    }
    res3: Array[Array[org.apache.spark.mllib.recommendation.Rating]] = Array(Array(Rating(75,100647712,0.5121532123511452), Rating(75,100647745,0.5038389937284881), Rating(75,100634859,0.4876602011356204)), Array(Rating(111,100645359,0.8081932751237731), Rating(111,100645310,0.7454780469648348), Rating(111,100645485,0.6932733570829884)), Array(Rating(195,100644566,0.1590551202444993), Rating(195,100633200,0.15260867187159466), Rating(195,100645485,0.1519540922331155)), Array(Rating(719,100646087,0.40801432041507585), Rating(719,100645338,0.3562337802303325), Rating(719,100645485,0.35522332289737)), Array(Rating(930,100651337,1.608493114312599), Rating(930,100646760,1.4225070743352959), Rating(930,100646778,1.310347238161283)), Array(Rating(985,100634859,0.3527672311207753), Rating(985,10063...
     */

    /*
    notInteractedPairRDD.collect.flatMap { record =>
        model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(3)
    }
    res5: Array[org.apache.spark.mllib.recommendation.Rating] = Array(Rating(75,100647712,0.5121532123511452), Rating(75,100647745,0.5038389937284881), Rating(75,100634859,0.4876602011356204), Rating(111,100645359,0.8081932751237731), Rating(111,100645310,0.7454780469648348), Rating(111,100645485,0.6932733570829884), Rating(195,100644566,0.1590551202444993), Rating(195,100633200,0.15260867187159466), Rating(195,100645485,0.1519540922331155), Rating(719,100646087,0.40801432041507585), Rating(719,100645338,0.3562337802303325), Rating(719,100645485,0.35522332289737), Rating(930,100651337,1.608493114312599), Rating(930,100646760,1.4225070743352959), Rating(930,100646778,1.310347238161283), Rating(985,100634859,0.3527672311207753), Rating(985,100637136,0.2942380805116386), Rating(985,100640893,0...
     */
    /*
    notInteractedPairRDD.collect.flatMap { record =>
        val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(3)
        recommend.map{ record =>
            record.user + "," + record.product
        }
    }
    res6: Array[String] = Array(75,100647712, 75,100647745, 75,100634859, 111,100645359, 111,100645310, 111,100645485, 195,100644566, 195,100633200, 195,100645485, 719,100646087, 719,100645338, 719,100645485, 930,100651337, 930,100646760, 930,100646778, 985,100634859, 985,100637136, 985,100640893, 1137,100646087, 1137,100634859, 1137,100637136, 1357,100648768, 1357,100650702, 1357,100646872, 1393,100637136, 1393,100640893, 1393,100637175, 1626,100640893, 1626,100646153, 1626,100651182, 2043,100647712, 2043,100640893, 2043,100647672, 2606,100649202, 2606,100646087, 2606,100645962, 2640,100646120, 2640,100646087, 2640,100646722, 3137,100645359, 3137,100645485, 3137,100645310, 3727,100646153, 3727,100646080, 3727,100647243, 4034,100653403, 4034,100633200, 4034,100650220, 4101,100634859, 4101,1...
     */

    /*def predictForAll(sc:SparkContext, model:MatrixFactorizationModel, recommendcount:Int) = {
        val sliceList = List.range(0, 100)
        //val sliceList = List.range(0, 3)
        val flattenResult = sliceList.map{ slice =>
            val notInteractedPairRDD = sc.textFile("/user/root/predictfeature/part" + slice).map(record => record.split("\t")).map(record => (record(0), record(1).split("-"))).map{ record =>
                val userid = record._1
                println(userid)
                record._2.map(newsid => (userid.toInt, newsid.toInt))
            }
            notInteractedPairRDD.collect.map { record =>
                val recommend = model.predict(sc.parallelize(record)).collect.sortBy(-_.rating).take(recommendcount)
                recommend.map{ record =>
                    record.user + "," + record.product
                }
            }.reduce(_ ++ _)
        }.reduce(_ ++ _)

        flattenResult
    }*/
}

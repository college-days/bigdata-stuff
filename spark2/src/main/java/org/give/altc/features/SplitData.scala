package org.give.altc.features

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.{PathNamespace, CommonOP}

/**
 * Created by zjh on 15-4-6.
 */

/**
 * 预处理 将数据分为线下特征数据 线上特征数据 线下训练数据
 */
object SplitData {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))

        //CommonOP.generateFeatureAndLabelData(sc, PathNamespace.tianchi_mobile_recommend_train_user, PathNamespace.offlinefeaturedata, PathNamespace.onlinefeaturedata, PathNamespace.offlinetraindata)
        CommonOP.generateFeatureAndLabelData(sc)
    }
}
